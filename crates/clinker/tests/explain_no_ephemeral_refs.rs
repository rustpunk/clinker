//! Regression guard: user-facing diagnostic pages and shared test fixtures must
//! not carry internal planning pointers — milestone/phase labels, task ids,
//! locked-decision codes, or resolution/decision-letter codes.
//!
//! The `docs/explain/*.md` pages are embedded into the binary via `include_str!`
//! in `clinker_plan` and printed verbatim by `clinker explain --code <CODE>`, so
//! a stray planning label is shipped straight to a user landing on the page from
//! a CLI error. The `clinker-exec` fixtures are shared test inputs. Both must
//! read as self-contained to an outside reader.

use std::fs;
use std::path::{Path, PathBuf};

/// Workspace root, two levels above this crate's manifest (`crates/clinker`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root is two levels above the clinker crate manifest")
        .to_path_buf()
}

/// Finds `prefix` at a word boundary (not preceded by an ASCII alphanumeric)
/// immediately followed by an ASCII digit — e.g. `LD-16`, `Phase 16`, `D57`.
/// The digit anchor keeps the compiler's real `Phase 1`/`Phase 2` stages and
/// fixed-width sample data like `D000000100` from matching.
fn boundary_prefix_then_digit(line: &str, prefix: &str) -> Option<String> {
    let mut search_from = 0;
    while let Some(rel) = line[search_from..].find(prefix) {
        let start = search_from + rel;
        let preceded_ok = match line[..start].chars().next_back() {
            Some(c) => !c.is_ascii_alphanumeric(),
            None => true,
        };
        if let Some(next) = line[start + prefix.len()..].chars().next()
            && preceded_ok
            && next.is_ascii_digit()
        {
            return Some(format!("{prefix}{next}"));
        }
        search_from = start + prefix.len();
    }
    None
}

/// Matches `RESOLUTION <UPPERCASE>-` decision-letter codes such as
/// `RESOLUTION W-18` or `RESOLUTION B-6`.
fn resolution_code(line: &str) -> Option<String> {
    const KEY: &str = "RESOLUTION ";
    let mut search_from = 0;
    while let Some(rel) = line[search_from..].find(KEY) {
        let start = search_from + rel;
        let mut after = line[start + KEY.len()..].chars();
        if let (Some(letter), Some(dash)) = (after.next(), after.next())
            && letter.is_ascii_uppercase()
            && dash == '-'
        {
            return Some(format!("RESOLUTION {letter}-"));
        }
        search_from = start + KEY.len();
    }
    None
}

/// Returns the first ephemeral internal-reference token found on `line`.
fn ephemeral_token(line: &str) -> Option<String> {
    for literal in ["Task 16", "FORWARD-ONLY", "phase-c"] {
        if line.contains(literal) {
            return Some(literal.to_string());
        }
    }
    for prefix in ["LD-", "Phase 1", "D5"] {
        if let Some(token) = boundary_prefix_then_digit(line, prefix) {
            return Some(token);
        }
    }
    resolution_code(line)
}

/// Collects every file under `dir` (recursively) whose extension is `ext`.
fn collect_files(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            collect_files(&path, ext, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some(ext) {
            out.push(path);
        }
    }
}

/// Scans `files`, returning `<repo-relative path>:<line>: <token>` for every
/// ephemeral reference found.
fn scan(files: &[PathBuf], root: &Path) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files {
        let content =
            fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        for (idx, line) in content.lines().enumerate() {
            if let Some(token) = ephemeral_token(line) {
                let rel = path.strip_prefix(root).unwrap_or(path);
                violations.push(format!("{}:{}: {token}", rel.display(), idx + 1));
            }
        }
    }
    violations
}

#[test]
fn explain_pages_have_no_ephemeral_internal_references() {
    let root = repo_root();
    let explain_dir = root.join("docs/explain");
    let mut files = Vec::new();
    collect_files(&explain_dir, "md", &mut files);
    files.sort();
    assert!(
        !files.is_empty(),
        "no explain pages found under {}",
        explain_dir.display()
    );

    let violations = scan(&files, &root);
    assert!(
        violations.is_empty(),
        "explain pages carry internal planning references (these are shipped \
         verbatim by `clinker explain --code`):\n{}",
        violations.join("\n")
    );

    // Assert the bytes the registry actually serves are clean too, for every
    // page reachable through `clinker explain --code <CODE>`.
    for path in &files {
        let code = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("explain page has a UTF-8 file stem");
        if let Some(shipped) = clinker_plan::plan::explain_provenance::explain_code(code) {
            for (idx, line) in shipped.lines().enumerate() {
                assert!(
                    ephemeral_token(line).is_none(),
                    "explain_code({code}) served an internal reference on line {}: {line}",
                    idx + 1
                );
            }
        }
    }
}

#[test]
fn exec_fixtures_have_no_ephemeral_internal_references() {
    let root = repo_root();
    let fixtures_dir = root.join("crates/clinker-exec/tests/fixtures");
    let mut files = Vec::new();
    collect_files(&fixtures_dir, "yaml", &mut files);
    collect_files(&fixtures_dir, "yml", &mut files);
    files.sort();
    assert!(
        !files.is_empty(),
        "no YAML fixtures found under {}",
        fixtures_dir.display()
    );

    let violations = scan(&files, &root);
    assert!(
        violations.is_empty(),
        "exec test fixtures carry internal planning references:\n{}",
        violations.join("\n")
    );
}
