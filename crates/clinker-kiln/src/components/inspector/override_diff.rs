//! Override diff component — unified inline diff with word-level highlighting.
//!
//! Renders base vs channel CXL in a single-column unified diff layout,
//! using left-border colours and inline token highlights to show changes.

use dioxus::prelude::*;

use crate::channel_resolve::{AppliedOverride, OverrideKind, OverrideSource};
use crate::components::yaml_sidebar::tokenizer::{tokenize_cxl, Token, TokenKind};

// ── Diff engine ────────────────────────────────────────────────────────

/// A line in the unified diff output.
enum DiffLine {
    /// Line appears in both base and override, unchanged.
    Context(Vec<Token>),
    /// Line was removed (only in base).
    Removed(Vec<Token>),
    /// Line was added (only in override).
    Added(Vec<Token>),
    /// Line changed: base tokens and override tokens, with per-token change marks.
    Changed {
        base: Vec<(Token, bool)>,     // (token, is_changed)
        override_: Vec<(Token, bool)>, // (token, is_changed)
    },
}

/// Compute a unified diff between two CXL expressions.
fn compute_diff(base: &str, override_: &str) -> Vec<DiffLine> {
    let base_lines: Vec<&str> = base.lines().collect();
    let over_lines: Vec<&str> = override_.lines().collect();

    let lcs = lcs_table(&base_lines, &over_lines);
    let mut result = Vec::new();
    backtrack_diff(&lcs, &base_lines, &over_lines, base_lines.len(), over_lines.len(), &mut result);
    result
}

/// Classic LCS table for line-level diff.
fn lcs_table(a: &[&str], b: &[&str]) -> Vec<Vec<usize>> {
    let m = a.len();
    let n = b.len();
    let mut table = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if a[i - 1] == b[j - 1] {
                table[i][j] = table[i - 1][j - 1] + 1;
            } else {
                table[i][j] = table[i - 1][j].max(table[i][j - 1]);
            }
        }
    }
    table
}

/// Walk the LCS table to produce diff lines.
fn backtrack_diff(
    table: &[Vec<usize>],
    a: &[&str],
    b: &[&str],
    i: usize,
    j: usize,
    out: &mut Vec<DiffLine>,
) {
    if i > 0 && j > 0 && a[i - 1] == b[j - 1] {
        backtrack_diff(table, a, b, i - 1, j - 1, out);
        out.push(DiffLine::Context(tokenize_cxl(a[i - 1])));
    } else if j > 0 && (i == 0 || table[i][j - 1] >= table[i - 1][j]) {
        backtrack_diff(table, a, b, i, j - 1, out);
        out.push(DiffLine::Added(tokenize_cxl(b[j - 1])));
    } else if i > 0 && (j == 0 || table[i][j - 1] < table[i - 1][j]) {
        backtrack_diff(table, a, b, i - 1, j, out);
        // Check if the next line in `b` at this position is a modification
        // (pair a removed line with the following added line as a Changed pair)
        out.push(DiffLine::Removed(tokenize_cxl(a[i - 1])));
    } else {
        // Base case: i == 0 && j == 0
    }
}

/// Post-process the diff to merge adjacent Removed+Added into Changed pairs
/// when the lines are structurally similar (same number of tokens, most match).
fn merge_changed_pairs(lines: Vec<DiffLine>) -> Vec<DiffLine> {
    let mut result = Vec::with_capacity(lines.len());
    let mut iter = lines.into_iter().peekable();

    while let Some(line) = iter.next() {
        match line {
            DiffLine::Removed(ref base_tokens) => {
                if let Some(DiffLine::Added(_)) = iter.peek() {
                    // Peek at the Added line
                    let added = iter.next().unwrap();
                    if let DiffLine::Added(over_tokens) = added {
                        // Try to merge as a Changed pair with word-level diff
                        let (base_marked, over_marked) =
                            word_level_diff(base_tokens, &over_tokens);
                        result.push(DiffLine::Changed {
                            base: base_marked,
                            override_: over_marked,
                        });
                    }
                } else {
                    result.push(line);
                }
            }
            other => result.push(other),
        }
    }

    result
}

/// Compare two token sequences and mark which tokens differ.
fn word_level_diff(
    base: &[Token],
    override_: &[Token],
) -> (Vec<(Token, bool)>, Vec<(Token, bool)>) {
    // Filter out whitespace/indent tokens for comparison, but keep them for rendering
    let base_meaningful: Vec<(usize, &Token)> = base
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t.kind, TokenKind::Indent))
        .collect();
    let over_meaningful: Vec<(usize, &Token)> = override_
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t.kind, TokenKind::Indent))
        .collect();

    // Build change sets by comparing meaningful tokens pairwise
    let mut base_changed = vec![false; base.len()];
    let mut over_changed = vec![false; override_.len()];

    let max_len = base_meaningful.len().max(over_meaningful.len());
    for k in 0..max_len {
        let b = base_meaningful.get(k);
        let o = over_meaningful.get(k);
        match (b, o) {
            (Some(&(bi, bt)), Some(&(oi, ot))) => {
                if bt.text != ot.text {
                    base_changed[bi] = true;
                    over_changed[oi] = true;
                }
            }
            (Some(&(bi, _)), None) => base_changed[bi] = true,
            (None, Some(&(oi, _))) => over_changed[oi] = true,
            (None, None) => {}
        }
    }

    let base_result = base
        .iter()
        .enumerate()
        .map(|(i, t)| (t.clone(), base_changed[i]))
        .collect();
    let over_result = override_
        .iter()
        .enumerate()
        .map(|(i, t)| (t.clone(), over_changed[i]))
        .collect();

    (base_result, over_result)
}

// ── Component ──────────────────────────────────────────────────────────

/// Override diff section displayed in the inspector.
#[component]
pub fn OverrideDiff(applied: AppliedOverride, base_cxl: String, resolved_cxl: String) -> Element {
    let kind_label = match &applied.kind {
        OverrideKind::Modified => "Modified",
        OverrideKind::Added => "Added",
        OverrideKind::Removed => "Removed",
    };

    let source_label = match &applied.source {
        OverrideSource::Channel => "channel override".to_string(),
        OverrideSource::Group(id) => format!("via group {id}"),
    };

    let kind_class = match &applied.kind {
        OverrideKind::Modified => "kiln-override-diff--modified",
        OverrideKind::Added => "kiln-override-diff--added",
        OverrideKind::Removed => "kiln-override-diff--removed",
    };

    let file_name = applied
        .file
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();

    rsx! {
        div { class: "kiln-override-diff {kind_class}",
            // Header
            div { class: "kiln-override-diff__header",
                span { class: "kiln-override-diff__kind", "{kind_label}" }
                span { class: "kiln-override-diff__source", "{source_label}" }
            }

            // Unified diff body
            match applied.kind {
                OverrideKind::Modified => {
                    {
                        let raw_diff = compute_diff(&base_cxl, &resolved_cxl);
                        let diff_lines = merge_changed_pairs(raw_diff);
                        rsx! {
                            div { class: "kiln-override-diff__unified",
                                for (i, dl) in diff_lines.iter().enumerate() {
                                    { render_diff_line(i, dl) }
                                }
                            }
                        }
                    }
                }
                OverrideKind::Added => {
                    rsx! {
                        div { class: "kiln-override-diff__unified",
                            for (i, line) in resolved_cxl.lines().enumerate() {
                                {
                                    let tokens = tokenize_cxl(line);
                                    rsx! {
                                        div {
                                            key: "add-{i}",
                                            class: "kiln-diff-line kiln-diff-line--added",
                                            span { class: "kiln-diff-gutter", "+" }
                                            span { class: "kiln-diff-content",
                                                for (j, token) in tokens.iter().enumerate() {
                                                    span {
                                                        key: "t-{j}",
                                                        "data-token": token.kind.as_data_attr(),
                                                        "{token.text}"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                OverrideKind::Removed => {
                    rsx! {
                        div { class: "kiln-override-diff__unified",
                            for (i, line) in base_cxl.lines().enumerate() {
                                {
                                    let tokens = tokenize_cxl(line);
                                    rsx! {
                                        div {
                                            key: "rem-{i}",
                                            class: "kiln-diff-line kiln-diff-line--removed",
                                            span { class: "kiln-diff-gutter", "\u{2212}" }
                                            span { class: "kiln-diff-content",
                                                for (j, token) in tokens.iter().enumerate() {
                                                    span {
                                                        key: "t-{j}",
                                                        "data-token": token.kind.as_data_attr(),
                                                        "{token.text}"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // File link
            div { class: "kiln-override-diff__file",
                span { class: "kiln-override-diff__file-label", "\u{2192} " }
                span { class: "kiln-override-diff__file-path", "{file_name}" }
            }
        }
    }
}

/// Render a single unified diff line.
fn render_diff_line(i: usize, dl: &DiffLine) -> Element {
    match dl {
        DiffLine::Context(tokens) => rsx! {
            div {
                key: "ctx-{i}",
                class: "kiln-diff-line kiln-diff-line--context",
                span { class: "kiln-diff-gutter", "\u{00A0}" }
                span { class: "kiln-diff-content",
                    for (j, token) in tokens.iter().enumerate() {
                        span {
                            key: "t-{j}",
                            "data-token": token.kind.as_data_attr(),
                            "{token.text}"
                        }
                    }
                }
            }
        },
        DiffLine::Removed(tokens) => rsx! {
            div {
                key: "rem-{i}",
                class: "kiln-diff-line kiln-diff-line--removed",
                span { class: "kiln-diff-gutter", "\u{2212}" }
                span { class: "kiln-diff-content",
                    for (j, token) in tokens.iter().enumerate() {
                        span {
                            key: "t-{j}",
                            "data-token": token.kind.as_data_attr(),
                            "{token.text}"
                        }
                    }
                }
            }
        },
        DiffLine::Added(tokens) => rsx! {
            div {
                key: "add-{i}",
                class: "kiln-diff-line kiln-diff-line--added",
                span { class: "kiln-diff-gutter", "+" }
                span { class: "kiln-diff-content",
                    for (j, token) in tokens.iter().enumerate() {
                        span {
                            key: "t-{j}",
                            "data-token": token.kind.as_data_attr(),
                            "{token.text}"
                        }
                    }
                }
            }
        },
        DiffLine::Changed { base, override_ } => rsx! {
            div {
                key: "chg-base-{i}",
                class: "kiln-diff-line kiln-diff-line--removed",
                span { class: "kiln-diff-gutter", "\u{2212}" }
                span { class: "kiln-diff-content",
                    for (j, (token, changed)) in base.iter().enumerate() {
                        span {
                            key: "t-{j}",
                            "data-token": token.kind.as_data_attr(),
                            class: if *changed { "kiln-diff-highlight--removed" } else { "" },
                            "{token.text}"
                        }
                    }
                }
            }
            div {
                key: "chg-over-{i}",
                class: "kiln-diff-line kiln-diff-line--added",
                span { class: "kiln-diff-gutter", "+" }
                span { class: "kiln-diff-content",
                    for (j, (token, changed)) in override_.iter().enumerate() {
                        span {
                            key: "t-{j}",
                            "data-token": token.kind.as_data_attr(),
                            class: if *changed { "kiln-diff-highlight--added" } else { "" },
                            "{token.text}"
                        }
                    }
                }
            }
        },
    }
}
