//! Search results rendering — file-grouped matches with highlighted text.
//!
//! Spec §S2.3: results grouped by file, line matches with highlighted substring.

use dioxus::prelude::*;

use crate::search::{TextSearchFileResult, TextSearchMatch};

/// Render search results grouped by file.
#[component]
pub fn SearchResults(
    results: Vec<TextSearchFileResult>,
    on_navigate: EventHandler<(String, usize)>,
) -> Element {
    let total_matches: usize = results.iter().map(|r| r.matches.len()).sum();
    let file_count = results.len();
    let summary = format!(
        "{total_matches} {} in {file_count} {}",
        if total_matches == 1 {
            "match"
        } else {
            "matches"
        },
        if file_count == 1 { "file" } else { "files" },
    );

    rsx! {
        div { class: "kiln-search-results",
            div { class: "kiln-search-results__summary",
                "{summary}"
            }

            for file_result in results {
                FileResultGroup {
                    key: "{file_result.path}",
                    result: file_result,
                    on_navigate: on_navigate,
                }
            }
        }
    }
}

/// A group of matches within a single file.
#[component]
fn FileResultGroup(
    result: TextSearchFileResult,
    on_navigate: EventHandler<(String, usize)>,
) -> Element {
    let match_count = result.matches.len();
    let path = result.path.clone();

    rsx! {
        div { class: "kiln-search-file-group",
            div { class: "kiln-search-file-group__header",
                span { class: "kiln-search-file-group__path", "{path}" }
                span { class: "kiln-search-file-group__count", "{match_count}" }
            }

            for m in result.matches {
                MatchRow {
                    key: "{m.line}-{m.match_start}",
                    file_path: path.clone(),
                    m: m,
                    on_navigate: on_navigate,
                }
            }
        }
    }
}

/// A single match line.
#[component]
fn MatchRow(
    file_path: String,
    m: TextSearchMatch,
    on_navigate: EventHandler<(String, usize)>,
) -> Element {
    let pre = m.content[..m.match_start].to_string();
    let matched = m.content[m.match_start..m.match_end].to_string();
    let post = m.content[m.match_end..].to_string();
    let line_num = m.line;
    let path = file_path.clone();

    rsx! {
        div {
            class: "kiln-search-match",
            onclick: move |_| on_navigate.call((path.clone(), line_num)),

            span { class: "kiln-search-match__line", "{line_num}" }
            span { class: "kiln-search-match__content",
                "{pre}"
                span { class: "kiln-search-match__highlight",
                    "{matched}"
                }
                "{post}"
            }
        }
    }
}
