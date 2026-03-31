//! Conflicts tab — structured merge conflict resolution.
//! Spec: clinker-kiln-git-addendum.md §G6.5, §G8.

use dioxus::prelude::*;

use clinker_git::GitOps;

use crate::components::toast::{ToastState, toast_error, toast_success};
use crate::state::TabManagerState;

/// A parsed merge conflict.
#[derive(Clone, Debug, PartialEq)]
struct ConflictEntry {
    /// File path with conflict.
    path: String,
    /// Number of conflict markers found.
    conflict_count: usize,
    /// Whether all conflicts in this file are resolved.
    resolved: bool,
}

/// Conflicts tab component.
#[component]
pub fn ConflictsTab() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let mut conflicts = use_signal(Vec::<ConflictEntry>::new);
    let mut loaded = use_signal(|| false);
    let mut selected_file = use_signal(|| None::<String>);
    let mut file_content = use_signal(String::new);

    // Detect conflicts on first render
    if !(loaded)() {
        let ws = (tab_mgr.workspace)();
        if let Some(ws) = ws
            && let Ok(entries) = detect_conflicts(&ws.root)
        {
            conflicts.set(entries);
        }
        loaded.set(true);
    }

    let conflict_list = (conflicts)();
    let has_conflicts = !conflict_list.is_empty();
    let all_resolved = conflict_list.iter().all(|c| c.resolved);
    let selected = (selected_file)();
    let content = (file_content)();

    rsx! {
        div { class: "kiln-conflicts-tab",
            if !has_conflicts {
                div { class: "kiln-conflicts-tab__empty",
                    span { class: "kiln-conflicts-tab__led kiln-conflicts-tab__led--ok" }
                    "No merge conflicts"
                }
            }

            if has_conflicts {
                // ── Conflict file list ───────────────────────────────
                div { class: "kiln-conflicts-sidebar",
                    div { class: "kiln-conflicts-sidebar__header",
                        "CONFLICTS"
                        span { class: "kiln-conflicts-sidebar__count",
                            "{conflict_list.len()}"
                        }
                    }

                    for entry in &conflict_list {
                        {
                            let path = entry.path.clone();
                            let is_active = selected.as_deref() == Some(&path);
                            let is_resolved = entry.resolved;
                            let class = if is_active {
                                "kiln-conflicts-file kiln-conflicts-file--active"
                            } else {
                                "kiln-conflicts-file"
                            };
                            let p = path.clone();

                            rsx! {
                                div {
                                    class: "{class}",
                                    onclick: move |_| {
                                        selected_file.set(Some(p.clone()));
                                        // Load file content
                                        let ws = (tab_mgr.workspace)();
                                        if let Some(ws) = ws {
                                            let full_path = ws.root.join(&p);
                                            if let Ok(c) = std::fs::read_to_string(&full_path) {
                                                file_content.set(c);
                                            }
                                        }
                                    },

                                    span {
                                        class: if is_resolved {
                                            "kiln-conflicts-file__led kiln-conflicts-file__led--resolved"
                                        } else {
                                            "kiln-conflicts-file__led kiln-conflicts-file__led--unresolved"
                                        },
                                    }
                                    span { class: "kiln-conflicts-file__path", "{path}" }
                                    span { class: "kiln-conflicts-file__count", "{entry.conflict_count}" }
                                }
                            }
                        }
                    }

                    if all_resolved {
                        button {
                            class: "kiln-conflicts-sidebar__merge-btn",
                            onclick: move |_| {
                                complete_merge(&mut tab_mgr, &mut conflicts);
                            },
                            "Complete Merge"
                        }
                    }
                }

                // ── Conflict content area ───────────────────────────
                div { class: "kiln-conflicts-content",
                    if selected.is_none() {
                        div { class: "kiln-conflicts-content__empty",
                            "Select a file to resolve its conflicts."
                        }
                    } else if !content.is_empty() {
                        // Render conflict markers with highlighting
                        div { class: "kiln-conflicts-content__text",
                            for (i, line) in content.lines().enumerate() {
                                {
                                    let line_str = line.to_string();
                                    let class = if line.starts_with("<<<<<<<")
                                        || line.starts_with("=======")
                                        || line.starts_with(">>>>>>>")
                                    {
                                        "kiln-conflict-line kiln-conflict-line--marker"
                                    } else {
                                        "kiln-conflict-line"
                                    };

                                    rsx! {
                                        div { class: "{class}",
                                            span { class: "kiln-conflict-line__num", "{i + 1}" }
                                            span { class: "kiln-conflict-line__text", "{line_str}" }
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

/// Detect files with merge conflicts via `git diff --name-only --diff-filter=U`.
fn detect_conflicts(
    repo_path: &std::path::Path,
) -> Result<Vec<ConflictEntry>, clinker_git::GitError> {
    let output = std::process::Command::new("git")
        .args(["diff", "--name-only", "--diff-filter=U"])
        .current_dir(repo_path)
        .output()
        .map_err(|e| clinker_git::GitError::Cli(e.to_string()))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let entries = stdout
        .lines()
        .filter(|l| !l.is_empty())
        .map(|path| {
            // Count conflict markers in file
            let full_path = repo_path.join(path);
            let count = std::fs::read_to_string(&full_path)
                .map(|c| c.lines().filter(|l| l.starts_with("<<<<<<<")).count())
                .unwrap_or(0);

            ConflictEntry {
                path: path.to_string(),
                conflict_count: count,
                resolved: false,
            }
        })
        .collect();

    Ok(entries)
}

/// Complete the merge — stage resolved files and prepare commit.
fn complete_merge(tab_mgr: &mut TabManagerState, conflicts: &mut Signal<Vec<ConflictEntry>>) {
    let ws = (tab_mgr.workspace)();
    let Some(ws) = ws else { return };

    let Ok(ops) = clinker_git::GitCliOps::discover(&ws.root) else {
        return;
    };
    let mut toast: Signal<Option<ToastState>> = use_context();

    // Stage all resolved conflict files
    if let Err(e) = ops.stage_all() {
        toast_error(&mut toast, format!("staging failed: {e}"));
        return;
    }

    toast_success(
        &mut toast,
        "All conflicts resolved. Use commit to finalize the merge.",
    );
    conflicts.set(Vec::new());

    // Refresh git state
    if let Ok(status) = ops.status() {
        tab_mgr.git_state.set(Some(status));
    }
}
