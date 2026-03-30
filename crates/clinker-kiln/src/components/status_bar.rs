//! Status bar — 22px persistent bottom bar.
//!
//! Shows: branch + sync state, file change counts, cursor position,
//! encoding, language, git engine. Git segments hidden when no repo.
//! Spec: clinker-kiln-git-addendum.md §G3.

use dioxus::prelude::*;

use crate::state::TabManagerState;

/// Status bar component — anchored to viewport bottom.
#[component]
pub fn StatusBar() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let git = (tab_mgr.git_state)();

    rsx! {
        div {
            class: "kiln-status-bar",

            // ── Git segments (hidden when no repo) ──────────────────────
            if let Some(ref status) = git {
                // Branch segment
                div {
                    class: "kiln-status-segment kiln-status-segment--branch",
                    span { class: "kiln-status__branch-icon", "⑂" }
                    span { class: "kiln-status__branch-name",
                        {
                            if status.branch.len() > 20 {
                                format!("{}…", &status.branch[..19])
                            } else {
                                status.branch.clone()
                            }
                        }
                    }
                    if status.ahead > 0 {
                        span { class: "kiln-status__ahead", "↑{status.ahead}" }
                    }
                    if status.behind > 0 {
                        span { class: "kiln-status__behind", "↓{status.behind}" }
                    }
                }

                div { class: "kiln-status-divider" }

                // Changes segment
                if status.has_changes() {
                    div {
                        class: "kiln-status-segment kiln-status-segment--changes",
                        if status.added > 0 {
                            span { class: "kiln-status__added", "+{status.added}" }
                        }
                        if status.modified > 0 {
                            span { class: "kiln-status__modified", "~{status.modified}" }
                        }
                        if status.deleted > 0 {
                            span { class: "kiln-status__deleted", "−{status.deleted}" }
                        }
                        if status.untracked > 0 {
                            span { class: "kiln-status__untracked", "?{status.untracked}" }
                        }
                    }
                    div { class: "kiln-status-divider" }
                }
            }

            // ── Cursor segment ──────────────────────────────────────────
            div {
                class: "kiln-status-segment kiln-status-segment--cursor",
                "Ln 1, Col 1"  // TODO: wire to YAML editor cursor
            }

            div { class: "kiln-status-divider" }

            // ── Encoding segment ────────────────────────────────────────
            div {
                class: "kiln-status-segment kiln-status-segment--encoding",
                "UTF-8"
            }

            div { class: "kiln-status-divider" }

            // ── Language segment ────────────────────────────────────────
            div {
                class: "kiln-status-segment kiln-status-segment--lang",
                "YAML"
            }

            // ── Spacer ─────────────────────────────────────────────────
            div { class: "kiln-status-spacer" }

            // ── Git engine indicator ────────────────────────────────────
            if git.is_some() {
                div {
                    class: "kiln-status-segment kiln-status-segment--engine",
                    "git"
                }
            }
        }
    }
}
