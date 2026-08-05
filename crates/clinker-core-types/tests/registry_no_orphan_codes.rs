//! Workspace guard: no diagnostic code literal without a registry row.
//!
//! `clinker_core_types::diagnostic`'s module doc states the contract — every
//! `Diagnostic::error` / `Diagnostic::warning` call site must use a code from
//! `REGISTRY`. Before this test the only thing keeping the list current was an
//! author remembering to edit it, and six live codes had drifted out.
//!
//! This scans the workspace's Rust sources for code literals in the positions
//! that carry a diagnostic code, and fails naming the code and the
//! `file:line` that emits it. It is the half of the contract that covers sites
//! no test executes; the `debug_assert!` in the constructors covers the codes
//! chosen at runtime, which no source scan can resolve.
//!
//! # What this scan cannot see
//!
//! It matches text, not syntax. Parsing Rust properly would mean a parser
//! dependency, so instead the gaps are named here and *counted at run time* —
//! [`every_emitted_diagnostic_code_is_registered`] prints one line per site it
//! recognized as a code-carrying position but could not resolve to a literal.
//! Run with `--nocapture` to see them. They are reported rather than failed
//! because each is a legitimate spelling, not a defect:
//!
//! - **A code chosen at run time.** `Diagnostic::error(code, ..)` where `code`
//!   is a variable, a `match` arm, or a `const`. The constructors'
//!   `debug_assert!` covers these, but only on a path some debug test executes.
//! - **An interpolated `[CODE]` prefix.** `format!("[{code}] ..")` carries its
//!   code the same way a literal `"[E123] .."` does, but the code is not in the
//!   text. Covered only if that code's literal also appears somewhere the scan
//!   does recognize. Every string opening `"[{` is reported, because whether
//!   one is a code prefix or an ordinary `[{}]` list rendering is precisely
//!   what this scan cannot determine.
//! - **A raw-string literal.** `r#"E123"#` in a constructor position is not
//!   matched; the leading `r` is not a quote. (A raw string *message* opening
//!   `[E123]` is matched, since its quote still precedes the bracket.)
//! - **Comments and doc comments are not skipped.** A code-shaped literal
//!   inside one is scanned as though it were code, so a doc example must use a
//!   registered code or it fails the test.
//! - **Only `<workspace>/crates` is walked.** A code emitted from a source
//!   outside that tree is invisible here.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use clinker_core_types::diagnostic::is_registered;

#[test]
fn attempt_diagnostic_codes_are_registered_once_with_stable_meanings() {
    let rows: Vec<_> = clinker_core_types::diagnostic::REGISTRY
        .iter()
        .filter(|entry| matches!(entry.code, "E371" | "E372"))
        .collect();
    assert_eq!(
        rows.len(),
        2,
        "E371 and E372 must each have one registry row"
    );
    assert_eq!(rows[0].code, "E371");
    assert_eq!(
        rows[0].meaning,
        "Unsafe or invalid retained attempt refused"
    );
    assert_eq!(rows[1].code, "E372");
    assert_eq!(
        rows[1].meaning,
        "Attempt cleanup incomplete or budget exhausted"
    );
}

/// A code literal found in source, with where it was found.
struct Site {
    code: String,
    file: PathBuf,
    line: usize,
    shape: Shape,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum Shape {
    /// `Diagnostic::error("E123", ...)` / `Diagnostic::warning("E123", ...)`.
    Constructor,
    /// A `code: "E123"` / `err_code: "E123"` struct-field initializer on a
    /// fault or error type whose code is later handed to `Diagnostic::error`.
    CodeField,
    /// A message that carries its code as a `[E123]` prefix, the convention
    /// plan-time `ConfigError::Validation` and CXL resolver diagnostics use.
    /// These reach the user through the same `clinker explain --code` surface
    /// as a `Diagnostic`, and `split_diagnostic_code` promotes some of them
    /// into real `Diagnostic`s, so they are held to the same rule.
    BracketedPrefix,
}

/// Workspace root, derived from this crate's manifest directory.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("clinker-core-types lives at <workspace>/crates/clinker-core-types")
        .to_path_buf()
}

/// Every `.rs` file under `<workspace>/crates`, recursively.
fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if path.is_dir() {
                // Build outputs hold generated sources that are not part of
                // the contract, and are large enough to matter for runtime.
                if name.starts_with("target") || name == ".git" {
                    continue;
                }
                stack.push(path);
            } else if name.ends_with(".rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// Does `s` look like a diagnostic code — `E123`, `E123a`, `W002`, or
/// `E-SEC-001`?
///
/// Deliberately narrow. A wider rule would sweep unrelated string literals
/// (format tokens, HTTP statuses) into the contract; `RegistryEntry`'s own
/// shape test keeps the registry inside this rule so the narrowness never
/// hides a registered code.
fn looks_like_code(s: &str) -> bool {
    if let Some(rest) = s.strip_prefix("E-SEC-") {
        return !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit());
    }
    let rest = match s.as_bytes().first() {
        Some(b'E') | Some(b'W') => &s[1..],
        _ => return false,
    };
    let digits = rest.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    digits.len() >= 2
        && digits.bytes().all(|b| b.is_ascii_digit())
        && rest.len() - digits.len() <= 1
}

/// Read the string literal that starts at `bytes[at]` (which must be `"`),
/// returning its contents when it is a plain, unescaped literal.
fn literal_at(bytes: &[u8], at: usize) -> Option<&str> {
    if bytes.get(at) != Some(&b'"') {
        return None;
    }
    let mut i = at + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return std::str::from_utf8(&bytes[at + 1..i]).ok(),
            b'\\' | b'\n' => return None,
            _ => i += 1,
        }
    }
    None
}

/// Byte index of the first non-whitespace byte at or after `from`.
fn skip_ws(bytes: &[u8], from: usize) -> usize {
    let mut i = from;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// A diagnostic-carrying position whose code the scan could not read, with why.
struct Unresolved {
    file: PathBuf,
    line: usize,
    reason: &'static str,
}

/// Collect every code-shaped literal sitting in a diagnostic-carrying
/// position in `src`, along with the positions whose code could not be read.
fn sites_in(path: &Path, src: &str) -> (Vec<Site>, Vec<Unresolved>) {
    let bytes = src.as_bytes();
    let mut line_starts = vec![0usize];
    line_starts.extend(src.match_indices('\n').map(|(i, _)| i + 1));
    let line_of = |offset: usize| match line_starts.binary_search(&offset) {
        Ok(i) => i + 1,
        Err(i) => i,
    };

    let mut out = Vec::new();
    let mut unresolved = Vec::new();
    let mut push = |offset: usize, code: &str, shape: Shape| {
        out.push(Site {
            code: code.to_owned(),
            file: path.to_path_buf(),
            line: line_of(offset),
            shape,
        });
    };
    let mut blind = |offset: usize, reason: &'static str| {
        unresolved.push(Unresolved {
            file: path.to_path_buf(),
            line: line_of(offset),
            reason,
        });
    };

    for marker in ["Diagnostic::error(", "Diagnostic::warning("] {
        for (idx, _) in src.match_indices(marker) {
            let after = skip_ws(bytes, idx + marker.len());
            match literal_at(bytes, after) {
                Some(code) if looks_like_code(code) => push(idx, code, Shape::Constructor),
                // A literal that is not code-shaped in the one position that
                // must hold a code. Reported rather than failed: the shape rule
                // is deliberately narrow, so widening it here would be a guess.
                Some(_) => blind(idx, "constructor code literal is not code-shaped"),
                None => blind(idx, "constructor code is not a plain literal"),
            }
        }
    }

    // `format!("[{code}] ..")` carries its code exactly as a literal
    // `"[E123] .."` does, but the code is not in the text to be read. Whether
    // a given one is a code prefix or an ordinary `[{}]` list rendering is
    // exactly what this scan cannot tell, so it says so rather than deciding.
    for (idx, _) in src.match_indices("\"[{") {
        blind(idx, "string opens with an interpolated bracket");
    }

    for marker in ["code:", "err_code:"] {
        for (idx, _) in src.match_indices(marker) {
            // `err_code:` also matches inside `code:`; keep only the longest
            // match by requiring the preceding byte not to be an identifier
            // character.
            if idx > 0 && (bytes[idx - 1].is_ascii_alphanumeric() || bytes[idx - 1] == b'_') {
                continue;
            }
            let after = skip_ws(bytes, idx + marker.len());
            if let Some(code) = literal_at(bytes, after)
                && looks_like_code(code)
            {
                push(idx, code, Shape::CodeField);
            }
        }
    }

    // A string literal opening with `[CODE]`. Matched on the literal's first
    // bytes rather than anywhere inside it, so a message that merely mentions
    // a code -- an assertion, or a `See: clinker explain --code E123` pointer
    // -- is not mistaken for one that carries it.
    for (idx, _) in src.match_indices("\"[") {
        let rest = &src[idx + 2..];
        let Some(close) = rest.find(']') else {
            continue;
        };
        let code = &rest[..close];
        if looks_like_code(code) {
            push(idx, code, Shape::BracketedPrefix);
        }
    }

    (out, unresolved)
}

#[test]
fn every_emitted_diagnostic_code_is_registered() {
    let root = workspace_root();
    let files = rust_sources(&root);
    assert!(
        !files.is_empty(),
        "found no Rust sources under {}/crates — the scan would pass vacuously",
        root.display()
    );

    let mut sites = Vec::new();
    let mut unresolved = Vec::new();
    for path in &files {
        let Ok(src) = std::fs::read_to_string(path) else {
            continue;
        };
        let (found, blind) = sites_in(path, &src);
        sites.extend(found);
        unresolved.extend(blind);
    }

    // Both recognized shapes must still match real code. If a refactor
    // renames the constructors or the fault field, this fails here rather
    // than degrading into a scan that silently checks nothing.
    assert!(
        sites.iter().any(|s| s.shape == Shape::Constructor),
        "scan matched no `Diagnostic::error(\"...\")` call site; the scanner's \
         constructor pattern has gone stale"
    );
    assert!(
        sites.iter().any(|s| s.shape == Shape::CodeField),
        "scan matched no `code: \"...\"` field initializer; the scanner's \
         field pattern has gone stale"
    );
    assert!(
        sites.iter().any(|s| s.shape == Shape::BracketedPrefix),
        "scan matched no `\"[CODE] ...\"` message prefix; the scanner's \
         prefix pattern has gone stale"
    );

    // The coverage the scan does not provide, named rather than left implicit:
    // a reader deciding whether this guard protects a given emission site can
    // see which sites it had to skip. Printed, not failed — every entry is a
    // legitimate spelling, covered instead by the constructors'
    // `debug_assert!` (see this file's module doc).
    println!(
        "registry scan: {} resolved code literal(s) across {} file(s); \
         {} code-carrying position(s) unresolved",
        sites.len(),
        files.len(),
        unresolved.len()
    );
    for u in &unresolved {
        let rel = u.file.strip_prefix(&root).unwrap_or(&u.file);
        println!("  unresolved: {} at {}:{}", u.reason, rel.display(), u.line);
    }

    let mut orphans = String::new();
    for site in &sites {
        if !is_registered(&site.code) {
            let rel = site.file.strip_prefix(&root).unwrap_or(&site.file);
            let _ = writeln!(
                orphans,
                "  {} at {}:{}",
                site.code,
                rel.display(),
                site.line
            );
        }
    }

    assert!(
        orphans.is_empty(),
        "orphan diagnostic code literals — each needs a row in the \
         `diagnostic_registry!` invocation in \
         crates/clinker-core-types/src/diagnostic.rs:\n{orphans}"
    );
}
