//! Compile-checks the marked example blocks in `docs/explain/*.md`.
//!
//! `test_explain_docs_all_have_required_sections` guards only that five
//! headings are present in each page; nothing ever compiled the YAML in a
//! page's `## Example` section. So a page could prescribe a configuration that
//! does not parse, does not compile, or fails to fire the diagnostic it
//! documents, and the suite stayed green — the exact class of defect this
//! harness closes.
//!
//! # The marker convention
//!
//! Not every fenced block is compilable: a page's leading "Rejected" block is
//! usually a complete node list, but its follow-up "Corrected" blocks are
//! often deliberate mid-indentation fragments that show only the lines that
//! change. A test cannot simply compile every block. So the author marks the
//! complete ones with an HTML comment on the line directly above the fence
//! (HTML comments render invisibly in mdBook output):
//!
//! ```text
//! <!-- explain-test: expect-code=E349 -->
//! ```yaml
//! # Rejected: ...
//! nodes:
//!   - type: source
//!     ...
//! ```
//! ```
//!
//! Two directives are recognized:
//!
//! - `expect-code=EXXX` — compiling the block must surface the diagnostic
//!   `EXXX`. That code may arrive as a parse-time [`ConfigError::Validation`]
//!   (its message carries the bracketed code, e.g. `[E357]`) or as a
//!   compile-time [`Diagnostic`], so the harness accepts either surface.
//! - `expect-clean` — the block must parse, compile, and produce no diagnostic
//!   at all (no errors and no warnings).
//!
//! Example blocks omit the mandatory `pipeline:` metadata for brevity, so a
//! marked block that has no top-level `pipeline:` key is compiled under a
//! synthetic `pipeline: { name }` header — the single piece of boilerplate
//! every pipeline needs. Nothing else is injected: a marked block must
//! otherwise be a complete, self-contained node list.
//!
//! # What the harness enforces
//!
//! - A marked `expect-code` block whose code never appears fails the test.
//! - A marked `expect-clean` block that emits any diagnostic fails the test.
//! - A marked block whose YAML does not parse fails the test.
//! - A misplaced or malformed `explain-test:` marker fails the test.
//!
//! Unmarked blocks (the fragments) are skipped, and every page that carries no
//! marked block at all is *reported*, not failed — run with `--nocapture` to
//! see the coverage list. Reporting rather than failing is deliberate: new
//! explain pages land continuously, and a page that has not yet been annotated
//! must not break this test the moment it appears.

use std::fs;
use std::path::{Path, PathBuf};

use crate::config::{CompileContext, ConfigError, parse_config};

/// The token that identifies one of our markers inside an HTML comment.
const DIRECTIVE_PREFIX: &str = "explain-test:";

/// What a marked block claims about its own compilation.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Expectation {
    /// Compiling the block must surface this diagnostic code, on either the
    /// parse-time or the compile-time surface.
    Code(String),
    /// The block must parse, compile, and produce no diagnostic at all.
    Clean,
}

/// One marked, compilable example block extracted from a page.
struct MarkedBlock {
    /// Page file name, e.g. `E349.md`.
    page: String,
    /// 1-based line of the marker comment within the page, for error messages.
    marker_line: usize,
    expectation: Expectation,
    /// Raw YAML between the fences (fence lines excluded).
    yaml: String,
}

/// The observed result of compiling one block.
enum Evaluation {
    /// The YAML did not deserialize — a genuinely broken block.
    Unparseable(String),
    /// Parse-time validation rejected the block; the message text is retained
    /// so an `[EXXX]` code embedded in it can be matched.
    ParseRejected(String),
    /// A non-YAML, non-validation parse error (I/O, env-var). Unexpected for an
    /// in-memory block; treated as a failure.
    OtherParseError(String),
    /// Compilation failed; the diagnostic codes it produced.
    CompileErrors(Vec<String>),
    /// Compilation succeeded; any warning diagnostic codes it produced.
    Compiled(Vec<String>),
}

/// Absolute path to `docs/explain/`, resolved from this crate's manifest dir so
/// the harness reads the real files on disk (not the `include_str!` snapshot)
/// and discovers pages dynamically rather than from a hard-coded list.
fn explain_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/explain")
}

/// Parse the directive inside an `explain-test:` HTML comment.
///
/// `Ok(Some(_))` is one of our markers; `Ok(None)` is some other HTML comment
/// we ignore; `Err(_)` is a malformed `explain-test:` marker (a typo we want
/// the test to catch rather than silently skip).
fn parse_marker(line: &str) -> Result<Option<Expectation>, String> {
    let trimmed = line.trim();
    let Some(inner) = trimmed
        .strip_prefix("<!--")
        .and_then(|s| s.strip_suffix("-->"))
    else {
        return Ok(None);
    };
    let inner = inner.trim();
    let Some(directive) = inner.strip_prefix(DIRECTIVE_PREFIX) else {
        return Ok(None);
    };
    let directive = directive.trim();
    if directive == "expect-clean" {
        return Ok(Some(Expectation::Clean));
    }
    if let Some(code) = directive.strip_prefix("expect-code=") {
        let code = code.trim();
        if code.is_empty() {
            return Err(format!(
                "`expect-code=` with no code in marker: {trimmed:?}"
            ));
        }
        return Ok(Some(Expectation::Code(code.to_string())));
    }
    Err(format!(
        "unrecognized `{DIRECTIVE_PREFIX}` directive {directive:?} in marker: {trimmed:?} \
         (expected `expect-clean` or `expect-code=EXXX`)"
    ))
}

/// Whether a line opens or closes a fenced code block.
fn is_fence(line: &str) -> bool {
    line.trim_start().starts_with("```")
}

/// Extract every marked block from one page, plus any marker-placement errors.
fn extract_marked(page: &str, md: &str) -> (Vec<MarkedBlock>, Vec<String>) {
    let mut blocks = Vec::new();
    let mut errors = Vec::new();
    let lines: Vec<&str> = md.lines().collect();
    let mut i = 0;
    let mut in_fence = false;
    while i < lines.len() {
        // Track fenced-block boundaries so a marker-looking line *inside* a
        // code block (e.g. a page that documents the convention) is never
        // mistaken for a real marker.
        if is_fence(lines[i]) {
            in_fence = !in_fence;
            i += 1;
            continue;
        }
        if in_fence {
            i += 1;
            continue;
        }
        let expectation = match parse_marker(lines[i]) {
            Ok(Some(exp)) => exp,
            Ok(None) => {
                i += 1;
                continue;
            }
            Err(msg) => {
                errors.push(format!("{page}:{}: {msg}", i + 1));
                i += 1;
                continue;
            }
        };
        let marker_line = i + 1;
        // The fence must open on the very next line: a marker floating away
        // from its block is an authoring error we surface rather than ignore.
        let fence_idx = i + 1;
        if fence_idx >= lines.len() || !is_fence(lines[fence_idx]) {
            errors.push(format!(
                "{page}:{marker_line}: `{DIRECTIVE_PREFIX}` marker is not directly above a fenced \
                 code block"
            ));
            i += 1;
            continue;
        }
        // Collect the block body up to the closing fence.
        let mut body = Vec::new();
        let mut j = fence_idx + 1;
        let mut closed = false;
        while j < lines.len() {
            if is_fence(lines[j]) {
                closed = true;
                break;
            }
            body.push(lines[j]);
            j += 1;
        }
        if !closed {
            errors.push(format!(
                "{page}:{marker_line}: fenced block opened here is never closed"
            ));
            i = j;
            continue;
        }
        blocks.push(MarkedBlock {
            page: page.to_string(),
            marker_line,
            expectation,
            yaml: body.join("\n"),
        });
        i = j + 1;
    }
    (blocks, errors)
}

/// Whether a file stem names a diagnostic page (`E349`, `W101`, `E150b`,
/// `E15Y`, …): an `E`/`W` followed by a digit. Filters out meta files such as
/// `README.md` while still discovering every code page dynamically.
fn is_code_page(stem: &str) -> bool {
    let mut chars = stem.chars();
    matches!(chars.next(), Some('E' | 'W')) && matches!(chars.next(), Some(c) if c.is_ascii_digit())
}

/// Does the block already declare a top-level `pipeline:` key? A key at column
/// zero starts the line with `pipeline:`; an indented `pipeline:` (e.g. inside
/// a `declares:` block) starts with whitespace and does not match.
fn has_top_level_pipeline(yaml: &str) -> bool {
    yaml.lines().any(|l| l.starts_with("pipeline:"))
}

/// Compile a marked block and report what it produced. Blocks without a
/// `pipeline:` header are wrapped in a minimal one — the only normalization.
fn evaluate(yaml: &str) -> Evaluation {
    let normalized = if has_top_level_pipeline(yaml) {
        yaml.to_string()
    } else {
        format!("pipeline:\n  name: explain_doc_example\n{yaml}")
    };
    let config = match parse_config(&normalized) {
        Ok(config) => config,
        Err(ConfigError::Yaml(e)) => return Evaluation::Unparseable(e.to_string()),
        Err(ConfigError::Validation(msg)) => return Evaluation::ParseRejected(msg),
        Err(other) => return Evaluation::OtherParseError(other.to_string()),
    };
    match config.compile_with_diagnostics(&CompileContext::default()) {
        Ok((_plan, warnings)) => {
            Evaluation::Compiled(warnings.iter().map(|d| d.code.clone()).collect())
        }
        Err(diags) => Evaluation::CompileErrors(diags.iter().map(|d| d.code.clone()).collect()),
    }
}

/// Check one block's observed evaluation against its declared expectation,
/// returning a failure message when they disagree.
fn check(block: &MarkedBlock, eval: &Evaluation) -> Option<String> {
    let where_ = format!("{}:{}", block.page, block.marker_line);
    // A block that claims anything must at least be parseable YAML.
    if let Evaluation::Unparseable(e) = eval {
        return Some(format!("{where_}: marked block did not parse as YAML: {e}"));
    }
    match &block.expectation {
        Expectation::Code(code) => {
            let matched = match eval {
                // Parse-time validation embeds the code in its message text,
                // e.g. `[E357] source '...'`.
                Evaluation::ParseRejected(msg) => msg.contains(code.as_str()),
                Evaluation::CompileErrors(codes) | Evaluation::Compiled(codes) => {
                    codes.iter().any(|c| c == code)
                }
                Evaluation::OtherParseError(_) | Evaluation::Unparseable(_) => false,
            };
            if matched {
                return None;
            }
            let observed = describe(eval);
            Some(format!(
                "{where_}: expected diagnostic {code} but it did not appear ({observed})"
            ))
        }
        Expectation::Clean => match eval {
            Evaluation::Compiled(codes) if codes.is_empty() => None,
            _ => {
                let observed = describe(eval);
                Some(format!(
                    "{where_}: expected a clean compile with no diagnostics, but ({observed})"
                ))
            }
        },
    }
}

/// A short human description of an evaluation for failure messages.
fn describe(eval: &Evaluation) -> String {
    match eval {
        Evaluation::Unparseable(e) => format!("YAML did not parse: {e}"),
        Evaluation::ParseRejected(msg) => format!("parse rejected it: {msg}"),
        Evaluation::OtherParseError(e) => format!("parse errored: {e}"),
        Evaluation::CompileErrors(codes) => {
            if codes.is_empty() {
                "compile failed with no coded diagnostics".to_string()
            } else {
                format!("compile emitted [{}]", codes.join(", "))
            }
        }
        Evaluation::Compiled(codes) => {
            if codes.is_empty() {
                "compiled clean".to_string()
            } else {
                format!("compiled with warnings [{}]", codes.join(", "))
            }
        }
    }
}

#[test]
fn test_explain_doc_marked_examples_compile_as_declared() {
    let dir = explain_dir();
    let mut pages: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .map(|entry| entry.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "md"))
        .filter(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .is_some_and(is_code_page)
        })
        .collect();
    pages.sort();
    assert!(
        !pages.is_empty(),
        "no explain pages found under {}",
        dir.display()
    );

    let mut failures: Vec<String> = Vec::new();
    let mut unmarked: Vec<String> = Vec::new();
    let mut marked_total = 0usize;

    for path in &pages {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("page file name")
            .to_string();
        let md = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        let (blocks, marker_errors) = extract_marked(&name, &md);
        failures.extend(marker_errors);
        if blocks.is_empty() {
            unmarked.push(name.clone());
            continue;
        }
        marked_total += blocks.len();
        for block in &blocks {
            let eval = evaluate(&block.yaml);
            if let Some(failure) = check(block, &eval) {
                failures.push(failure);
            }
        }
    }

    // Criterion 4: report — never fail on — the pages with no marked example,
    // so the unchecked set stays visible as coverage grows page by page.
    println!(
        "explain-doc example coverage: {} of {} pages have >= 1 marked block ({} blocks checked)",
        pages.len() - unmarked.len(),
        pages.len(),
        marked_total,
    );
    if !unmarked.is_empty() {
        println!("  pages with no marked example ({}):", unmarked.len());
        for name in &unmarked {
            println!("    - {name}");
        }
    }

    // Teeth: a broken marker parser or a wholesale marker deletion would leave
    // nothing to check. Fail loudly rather than pass vacuously.
    assert!(
        marked_total > 0,
        "no marked example blocks were found in any explain page — the marker \
         convention or its parser is broken"
    );

    assert!(
        failures.is_empty(),
        "explain-doc example checks failed:\n{}",
        failures.join("\n")
    );
}
