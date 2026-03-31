//! Search panel — left-side slide-in (280px) for workspace-wide search.
//!
//! Two tabs: Text (substring/regex) and Structural (DSL tags).
//! Spec §S2.1–§S2.6.

use dioxus::prelude::*;

use crate::search::{
    self, STRUCTURAL_KEYS, SearchMode, StructuralSearchMatch, StructuralTag, TextSearchFileResult,
    TextSearchOptions,
};
use crate::state::{LeftPanel, TabManagerState};

use super::search_results::SearchResults;

/// Search panel component with text and structural search tabs.
#[component]
pub fn SearchPanel() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let mut mode = use_signal(|| SearchMode::Text);
    let mut query = use_signal(String::new);
    let mut use_regex = use_signal(|| false);
    let mut case_sensitive = use_signal(|| false);
    let mut whole_word = use_signal(|| false);
    let results: Signal<Vec<TextSearchFileResult>> = use_signal(Vec::new);
    let mut show_replace = use_signal(|| false);
    let mut replace_text = use_signal(String::new);

    // Structural search state
    let mut structural_query = use_signal(String::new);
    let structural_results: Signal<Vec<StructuralSearchMatch>> = use_signal(Vec::new);
    let structural_tags: Signal<Vec<StructuralTag>> = use_signal(Vec::new);

    let current_mode = (mode)();
    let current_query = (query)();
    let is_replace_visible = (show_replace)();

    // Execute text search
    let run_search = move |mut results: Signal<Vec<TextSearchFileResult>>| {
        let q = (query)();
        if q.is_empty() {
            results.set(Vec::new());
            return;
        }

        let ws = (tab_mgr.workspace)();
        let Some(ws) = ws else {
            results.set(Vec::new());
            return;
        };

        let opts = TextSearchOptions {
            regex: (use_regex)(),
            case_sensitive: (case_sensitive)(),
            whole_word: (whole_word)(),
        };

        let found = search::text_search(&ws.root, &q, &opts);
        results.set(found);
    };

    // Execute structural search
    let run_structural = move |mut sr: Signal<Vec<StructuralSearchMatch>>,
                               mut st: Signal<Vec<StructuralTag>>| {
        let q = (structural_query)();
        let tags = search::parse_structural_query(&q);
        st.set(tags.clone());

        if tags.is_empty() {
            sr.set(Vec::new());
            return;
        }

        let ws = (tab_mgr.workspace)();
        let Some(ws) = ws else {
            sr.set(Vec::new());
            return;
        };

        let found = search::structural_search(&ws.root, &tags);
        sr.set(found);
    };

    rsx! {
        div {
            class: "kiln-search-panel",

            // ── Header ──────────────────────────────────────────────────
            div { class: "kiln-search-panel__header",
                span { class: "kiln-search-panel__title", "SEARCH" }
                button {
                    class: "kiln-search-panel__close",
                    onclick: move |_| tab_mgr.left_panel.set(LeftPanel::None),
                    "×"
                }
            }

            // ── Mode tabs ───────────────────────────────────────────────
            div { class: "kiln-search-panel__tabs",
                button {
                    class: if current_mode == SearchMode::Text {
                        "kiln-search-mode-tab kiln-search-mode-tab--active"
                    } else {
                        "kiln-search-mode-tab"
                    },
                    onclick: move |_| mode.set(SearchMode::Text),
                    "Text"
                }
                button {
                    class: if current_mode == SearchMode::Structural {
                        "kiln-search-mode-tab kiln-search-mode-tab--active"
                    } else {
                        "kiln-search-mode-tab"
                    },
                    onclick: move |_| mode.set(SearchMode::Structural),
                    "Structural"
                }
            }

            // ── Text search input + toggles ─────────────────────────────
            if current_mode == SearchMode::Text {
                div { class: "kiln-search-panel__input-row",
                    button {
                        class: "kiln-search-toggle kiln-search-toggle--expand",
                        onclick: move |_| show_replace.set(!is_replace_visible),
                        title: "Toggle find and replace",
                        if is_replace_visible { "▾" } else { "▸" }
                    }

                    input {
                        class: "kiln-search-panel__input",
                        r#type: "text",
                        placeholder: "Search across pipelines...",
                        value: "{current_query}",
                        oninput: move |e: FormEvent| {
                            query.set(e.value());
                            run_search(results);
                        },
                    }

                    button {
                        class: if (use_regex)() {
                            "kiln-search-toggle kiln-search-toggle--active"
                        } else {
                            "kiln-search-toggle"
                        },
                        onclick: move |_| {
                            use_regex.set(!(use_regex)());
                            run_search(results);
                        },
                        title: "Use Regular Expression",
                        ".*"
                    }
                    button {
                        class: if (case_sensitive)() {
                            "kiln-search-toggle kiln-search-toggle--active"
                        } else {
                            "kiln-search-toggle"
                        },
                        onclick: move |_| {
                            case_sensitive.set(!(case_sensitive)());
                            run_search(results);
                        },
                        title: "Match Case",
                        "Aa"
                    }
                    button {
                        class: if (whole_word)() {
                            "kiln-search-toggle kiln-search-toggle--active"
                        } else {
                            "kiln-search-toggle"
                        },
                        onclick: move |_| {
                            whole_word.set(!(whole_word)());
                            run_search(results);
                        },
                        title: "Match Whole Word",
                        "\" \""
                    }
                }

                if is_replace_visible {
                    div { class: "kiln-search-panel__replace-row",
                        input {
                            class: "kiln-search-panel__input kiln-search-panel__input--replace",
                            r#type: "text",
                            placeholder: "Replace...",
                            value: "{replace_text}",
                            oninput: move |e: FormEvent| replace_text.set(e.value()),
                        }
                    }
                }
            }

            // ── Structural search input + tag pills ─────────────────────
            if current_mode == SearchMode::Structural {
                div { class: "kiln-search-panel__structural",
                    // Tag pills for active filters
                    {
                        let tags = (structural_tags)();
                        rsx! {
                            if !tags.is_empty() {
                                div { class: "kiln-structural-tags",
                                    for (_i, tag) in tags.iter().enumerate() {
                                        {
                                            let key = tag.key.clone();
                                            let value = tag.value.clone();
                                            rsx! {
                                                span {
                                                    class: "kiln-structural-tag",
                                                    span { class: "kiln-structural-tag__key", "{key}" }
                                                    span { class: "kiln-structural-tag__sep", ":" }
                                                    span { class: "kiln-structural-tag__value", "{value}" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    input {
                        class: "kiln-search-panel__input",
                        r#type: "text",
                        placeholder: "input:name field:email expr:lower(...",
                        value: "{structural_query}",
                        oninput: move |e: FormEvent| {
                            structural_query.set(e.value());
                            run_structural(structural_results, structural_tags);
                        },
                    }

                    div { class: "kiln-structural-hint",
                        "Keys: "
                        for (i, key) in STRUCTURAL_KEYS.iter().enumerate() {
                            span { class: "kiln-structural-hint__key", "{key}" }
                            if i < STRUCTURAL_KEYS.len() - 1 {
                                " "
                            }
                        }
                    }
                }
            }

            // ── Results ─────────────────────────────────────────────────
            div { class: "kiln-search-panel__results",
                if current_mode == SearchMode::Text {
                    {
                        let r = (results)();
                        if !current_query.is_empty() && r.is_empty() {
                            rsx! {
                                div { class: "kiln-search-panel__empty",
                                    "No results found."
                                }
                            }
                        } else if !r.is_empty() {
                            rsx! {
                                SearchResults {
                                    results: r,
                                    on_navigate: move |(_path, _line): (String, usize)| {
                                        // TODO: navigate to file + line
                                    },
                                }
                            }
                        } else {
                            rsx! {}
                        }
                    }
                }

                if current_mode == SearchMode::Structural {
                    {
                        let sr = (structural_results)();
                        let sq = (structural_query)();
                        if !sq.is_empty() && sr.is_empty() {
                            rsx! {
                                div { class: "kiln-search-panel__empty",
                                    "No structural matches found."
                                }
                            }
                        } else if !sr.is_empty() {
                            rsx! {
                                StructuralResults { results: sr }
                            }
                        } else {
                            rsx! {}
                        }
                    }
                }
            }
        }
    }
}

/// Render structural search results grouped by pipeline.
#[component]
fn StructuralResults(results: Vec<StructuralSearchMatch>) -> Element {
    let count = results.len();
    let summary = format!(
        "{count} stage {}",
        if count == 1 { "match" } else { "matches" }
    );

    rsx! {
        div { class: "kiln-structural-results",
            div { class: "kiln-search-results__summary", "{summary}" }

            for m in results {
                {
                    let path = m.pipeline_path.clone();
                    let name = m.stage_name.clone();
                    let stype = m.stage_type.clone();
                    let detail = m.matched_detail.clone();
                    let accent = match stype.as_str() {
                        "input" => "kiln-structural-match--input",
                        "transform" => "kiln-structural-match--transform",
                        "output" => "kiln-structural-match--output",
                        _ => "",
                    };

                    rsx! {
                        div {
                            class: "kiln-structural-match {accent}",
                            div { class: "kiln-structural-match__header",
                                span { class: "kiln-structural-match__led" }
                                span { class: "kiln-structural-match__name", "{name}" }
                                span { class: "kiln-structural-match__type", "{stype}" }
                            }
                            div { class: "kiln-structural-match__path", "{path}" }
                            div { class: "kiln-structural-match__detail", "{detail}" }
                        }
                    }
                }
            }
        }
    }
}
