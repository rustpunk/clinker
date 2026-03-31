//! Log tab — scrollable commit history.
//! Spec: clinker-kiln-git-addendum.md §G6.3.

use dioxus::prelude::*;

use clinker_git::GitOps;

use crate::state::TabManagerState;

/// Log tab component.
#[component]
pub fn LogTab() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let mut commits = use_signal(Vec::new);
    let mut loaded = use_signal(|| false);

    // Load commits on first render
    if !(loaded)() {
        let ws = (tab_mgr.workspace)();
        if let Some(ws) = ws {
            if let Ok(ops) = clinker_git::GitCliOps::discover(&ws.root) {
                if let Ok(log) = ops.log(30) {
                    commits.set(log);
                }
            }
        }
        loaded.set(true);
    }

    let commit_list = (commits)();

    rsx! {
        div { class: "kiln-log-tab",
            if commit_list.is_empty() {
                div { class: "kiln-log-tab__empty",
                    "No commits found."
                }
            }

            for commit in commit_list {
                {
                    let short_hash = if commit.id.len() > 7 {
                        commit.id[..7].to_string()
                    } else {
                        commit.id.clone()
                    };
                    let subject = commit.subject.clone();
                    let author = commit.author.clone();
                    let time = relative_time(commit.timestamp);

                    rsx! {
                        div { class: "kiln-log-entry",
                            div { class: "kiln-log-entry__graph",
                                span { class: "kiln-log-entry__dot", "●" }
                                div { class: "kiln-log-entry__line" }
                            }
                            div { class: "kiln-log-entry__info",
                                div { class: "kiln-log-entry__header",
                                    span { class: "kiln-log-entry__hash", "{short_hash}" }
                                    span { class: "kiln-log-entry__subject", "{subject}" }
                                }
                                div { class: "kiln-log-entry__meta",
                                    span { class: "kiln-log-entry__author", "{author}" }
                                    span { class: "kiln-log-entry__time", " · {time}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Convert unix timestamp to relative time string.
fn relative_time(timestamp: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let diff = now - timestamp;

    if diff < 60 {
        "just now".to_string()
    } else if diff < 3600 {
        let mins = diff / 60;
        format!("{mins}m ago")
    } else if diff < 86400 {
        let hours = diff / 3600;
        format!("{hours}h ago")
    } else if diff < 604800 {
        let days = diff / 86400;
        if days == 1 { "yesterday".to_string() } else { format!("{days} days ago") }
    } else {
        let weeks = diff / 604800;
        format!("{weeks}w ago")
    }
}
