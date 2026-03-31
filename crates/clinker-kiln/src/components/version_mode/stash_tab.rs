//! Stash tab — saved stash entries with apply/pop/drop actions.
//! Spec: clinker-kiln-git-addendum.md §G6.4.

use dioxus::prelude::*;

use clinker_git::GitOps;

use crate::components::toast::{toast_error, toast_success, ToastState};
use crate::state::TabManagerState;

/// A parsed stash entry.
#[derive(Clone, Debug, PartialEq)]
struct StashEntry {
    index: usize,
    message: String,
}

/// Stash tab component.
#[component]
pub fn StashTab() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let mut stashes = use_signal(Vec::<StashEntry>::new);
    let mut loaded = use_signal(|| false);

    // Load stashes on first render
    if !(loaded)() {
        let ws = (tab_mgr.workspace)();
        if let Some(ws) = ws {
            if let Ok(entries) = load_stashes(&ws.root) {
                stashes.set(entries);
            }
        }
        loaded.set(true);
    }

    let stash_list = (stashes)();

    rsx! {
        div { class: "kiln-stash-tab",
            if stash_list.is_empty() {
                div { class: "kiln-stash-tab__empty",
                    "No stashes saved."
                    br {}
                    "Use Ctrl+Shift+P → git: stash to save working changes."
                }
            }

            for entry in stash_list {
                {
                    let idx = entry.index;
                    let msg = entry.message.clone();

                    rsx! {
                        div { class: "kiln-stash-card",
                            div { class: "kiln-stash-card__header",
                                span { class: "kiln-stash-card__title", "stash@{{{idx}}}" }
                                span { class: "kiln-stash-card__msg", "{msg}" }
                            }

                            div { class: "kiln-stash-card__actions",
                                button {
                                    class: "kiln-stash-card__btn kiln-stash-card__btn--apply",
                                    onclick: move |_| {
                                        run_stash_action(&mut tab_mgr, &mut stashes, idx, "apply");
                                    },
                                    "Apply"
                                }
                                button {
                                    class: "kiln-stash-card__btn kiln-stash-card__btn--pop",
                                    onclick: move |_| {
                                        run_stash_action(&mut tab_mgr, &mut stashes, idx, "pop");
                                    },
                                    "Pop"
                                }
                                button {
                                    class: "kiln-stash-card__btn kiln-stash-card__btn--drop",
                                    onclick: move |_| {
                                        run_stash_action(&mut tab_mgr, &mut stashes, idx, "drop");
                                    },
                                    "Drop"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Load stash entries via git CLI.
fn load_stashes(repo_path: &std::path::Path) -> Result<Vec<StashEntry>, clinker_git::GitError> {
    let output = std::process::Command::new("git")
        .args(["stash", "list"])
        .current_dir(repo_path)
        .output()
        .map_err(|e| clinker_git::GitError::Cli(e.to_string()))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let entries = stdout
        .lines()
        .enumerate()
        .map(|(i, line)| {
            let message = line
                .split(": ")
                .skip(1)
                .collect::<Vec<_>>()
                .join(": ");
            StashEntry {
                index: i,
                message,
            }
        })
        .collect();

    Ok(entries)
}

/// Run a stash action and refresh.
fn run_stash_action(
    tab_mgr: &mut TabManagerState,
    stashes: &mut Signal<Vec<StashEntry>>,
    index: usize,
    action: &str,
) {
    let ws = (tab_mgr.workspace)();
    let Some(ws) = ws else { return };

    let stash_ref = format!("stash@{{{index}}}");
    let result = std::process::Command::new("git")
        .args(["stash", action, &stash_ref])
        .current_dir(&ws.root)
        .output();

    let mut toast: Signal<Option<ToastState>> = use_context();
    match result {
        Ok(output) if output.status.success() => {
            toast_success(&mut toast, &format!("Stash {action} succeeded"));
            // Refresh stash list
            if let Ok(entries) = load_stashes(&ws.root) {
                stashes.set(entries);
            }
            // Refresh git state
            if let Ok(ops) = clinker_git::GitCliOps::discover(&ws.root) {
                if let Ok(status) = ops.status() {
                    tab_mgr.git_state.set(Some(status));
                }
            }
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            toast_error(&mut toast, format!("stash {action} failed: {stderr}"));
        }
        Err(e) => {
            toast_error(&mut toast, format!("stash {action} failed: {e}"));
        }
    }
}
