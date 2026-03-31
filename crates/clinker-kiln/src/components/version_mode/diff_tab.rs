//! Diff tab — unified diff viewer with YAML syntax tokens.
//! Spec: clinker-kiln-git-addendum.md §G6.2.

use dioxus::prelude::*;

use clinker_git::GitOps;

use crate::state::TabManagerState;

/// Diff tab component.
#[component]
pub fn DiffTab() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let mut selected_file = use_signal(|| None::<String>);
    let mut diff_content = use_signal(String::new);

    let git = (tab_mgr.git_state)();
    let Some(ref status) = git else {
        return rsx! { div { "No git status available." } };
    };

    let files = &status.files;
    let current_file = (selected_file)();
    let diff_text = (diff_content)();

    rsx! {
        div { class: "kiln-diff-tab",
            // ── File list sidebar ───────────────────────────────────
            div { class: "kiln-diff-sidebar",
                for file in files {
                    {
                        let path_str = file.path.display().to_string();
                        let is_active = current_file.as_deref() == Some(&path_str);
                        let class = if is_active {
                            "kiln-diff-file kiln-diff-file--active"
                        } else {
                            "kiln-diff-file"
                        };
                        let p = path_str.clone();

                        rsx! {
                            div {
                                class: "{class}",
                                onclick: move |_| {
                                    selected_file.set(Some(p.clone()));
                                    // Load diff
                                    let ws = (tab_mgr.workspace)();
                                    if let Some(ws) = ws
                                        && let Ok(ops) = clinker_git::GitCliOps::discover(&ws.root)
                                        && let Ok(diff) = ops.diff_file(std::path::Path::new(&p))
                                    {
                                        diff_content.set(diff);
                                    }
                                },
                                span { class: "kiln-diff-file__status", "{file.status.letter()}" }
                                span { class: "kiln-diff-file__path", "{path_str}" }
                            }
                        }
                    }
                }
            }

            // ── Diff content area ───────────────────────────────────
            div { class: "kiln-diff-content",
                if current_file.is_none() {
                    div { class: "kiln-diff-content__empty",
                        "Select a file to view its diff."
                    }
                } else if diff_text.is_empty() {
                    div { class: "kiln-diff-content__empty",
                        "No changes in this file."
                    }
                } else {
                    div { class: "kiln-diff-content__unified",
                        for (i, line) in diff_text.lines().enumerate() {
                            {
                                let line_str = line.to_string();
                                let class = if line.starts_with('+') && !line.starts_with("+++") {
                                    "kiln-diff-line kiln-diff-line--added"
                                } else if line.starts_with('-') && !line.starts_with("---") {
                                    "kiln-diff-line kiln-diff-line--removed"
                                } else if line.starts_with("@@") {
                                    "kiln-diff-line kiln-diff-line--hunk"
                                } else {
                                    "kiln-diff-line kiln-diff-line--context"
                                };

                                rsx! {
                                    div { class: "{class}",
                                        span { class: "kiln-diff-line__num", "{i + 1}" }
                                        span { class: "kiln-diff-line__text", "{line_str}" }
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
