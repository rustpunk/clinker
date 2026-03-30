//! Search panel — left-side slide-in (280px) for workspace-wide search.
//!
//! Two tabs: Text (substring/regex) and Structural (DSL tags).
//! Spec §S2.1–§S2.6.

use dioxus::prelude::*;

use crate::search::{self, SearchMode, TextSearchFileResult, TextSearchOptions};
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
    let mut results: Signal<Vec<TextSearchFileResult>> = use_signal(Vec::new);
    let mut show_replace = use_signal(|| false);
    let mut replace_text = use_signal(String::new);

    let current_mode = (mode)();
    let current_query = (query)();
    let is_replace_visible = (show_replace)();

    // Execute search reactively when query or options change
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
                    // Replace expand toggle
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

                    // Option toggles
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

                // Replace input row (toggled)
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

            // ── Structural search (placeholder) ─────────────────────────
            if current_mode == SearchMode::Structural {
                div { class: "kiln-search-panel__structural-placeholder",
                    "Structural search — coming in Phase 5"
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
            }
        }
    }
}
