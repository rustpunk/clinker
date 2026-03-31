//! Branch bar — shows current branch + Push/Pull/Fetch buttons.
//! Spec: clinker-kiln-git-addendum.md §G5.4.

use dioxus::prelude::*;

use clinker_git::GitOps;

use crate::components::toast::{ToastState, toast_error, toast_success};
use crate::state::TabManagerState;

/// Branch bar component for Version Mode.
#[component]
pub fn BranchBar() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let git = (tab_mgr.git_state)();
    let git = git.as_ref();

    let branch = git.map(|g| g.branch.as_str()).unwrap_or("unknown");
    let ahead = git.map(|g| g.ahead).unwrap_or(0);
    let behind = git.map(|g| g.behind).unwrap_or(0);

    rsx! {
        div { class: "kiln-branch-bar",
            // Left: branch info
            div { class: "kiln-branch-bar__info",
                span { class: "kiln-branch-bar__icon", "⑂" }
                span { class: "kiln-branch-bar__name", "{branch}" }
                if ahead > 0 {
                    span { class: "kiln-branch-bar__ahead", "↑{ahead}" }
                }
                if behind > 0 {
                    span { class: "kiln-branch-bar__behind", "↓{behind}" }
                }
            }

            // Right: action buttons
            div { class: "kiln-branch-bar__actions",
                button {
                    class: "kiln-branch-bar__btn",
                    onclick: move |_| {
                        run_git_action(&mut tab_mgr, "push");
                    },
                    if ahead > 0 {
                        "Push ↑{ahead}"
                    } else {
                        "Push"
                    }
                }
                button {
                    class: "kiln-branch-bar__btn",
                    onclick: move |_| {
                        run_git_action(&mut tab_mgr, "pull");
                    },
                    if behind > 0 {
                        "Pull ↓{behind}"
                    } else {
                        "Pull"
                    }
                }
                button {
                    class: "kiln-branch-bar__btn kiln-branch-bar__btn--subtle",
                    onclick: move |_| {
                        run_git_action(&mut tab_mgr, "fetch");
                    },
                    "Fetch"
                }
            }
        }
    }
}

/// Run a git network action and refresh state.
fn run_git_action(tab_mgr: &mut TabManagerState, action: &str) {
    let ws = (tab_mgr.workspace)();
    let Some(ws) = ws else { return };

    let Ok(ops) = clinker_git::GitCliOps::discover(&ws.root) else {
        return;
    };

    let result = match action {
        "push" => ops.push(),
        "pull" => ops.pull(),
        "fetch" => ops.fetch(),
        _ => return,
    };

    // Refresh git state after action
    if let Ok(status) = ops.status() {
        tab_mgr.git_state.set(Some(status));
    }

    // Show toast
    let mut toast: Signal<Option<ToastState>> = use_context();
    match result {
        Ok(_) => toast_success(&mut toast, format!("git {action} completed")),
        Err(e) => toast_error(&mut toast, format!("git {action} failed: {e}")),
    }
}
