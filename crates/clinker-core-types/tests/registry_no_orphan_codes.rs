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

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use clinker_core_types::diagnostic::is_registered;

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

/// Collect every code-shaped literal sitting in a diagnostic-carrying
/// position in `src`.
fn sites_in(path: &Path, src: &str) -> Vec<Site> {
    let bytes = src.as_bytes();
    let mut line_starts = vec![0usize];
    line_starts.extend(src.match_indices('\n').map(|(i, _)| i + 1));
    let line_of = |offset: usize| match line_starts.binary_search(&offset) {
        Ok(i) => i + 1,
        Err(i) => i,
    };

    let mut out = Vec::new();
    let mut push = |offset: usize, code: &str, shape: Shape| {
        out.push(Site {
            code: code.to_owned(),
            file: path.to_path_buf(),
            line: line_of(offset),
            shape,
        });
    };

    for marker in ["Diagnostic::error(", "Diagnostic::warning("] {
        for (idx, _) in src.match_indices(marker) {
            let after = skip_ws(bytes, idx + marker.len());
            if let Some(code) = literal_at(bytes, after)
                && looks_like_code(code)
            {
                push(idx, code, Shape::Constructor);
            }
        }
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

    out
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
    for path in &files {
        let Ok(src) = std::fs::read_to_string(path) else {
            continue;
        };
        sites.extend(sites_in(path, &src));
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
