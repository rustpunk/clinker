//! Version Mode — full-viewport git operations layout.
//!
//! Fifth layout preset: branch bar, tab bar, file list + content area.
//! Spec: clinker-kiln-git-addendum.md §G5.

use dioxus::prelude::*;

use crate::state::TabManagerState;

use super::branch_bar::BranchBar;
use super::changes_tab::ChangesTab;
use super::diff_tab::DiffTab;
use super::log_tab::LogTab;

/// Active sub-tab within Version Mode.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum VersionTab {
    #[default]
    Changes,
    Diff,
    Log,
}

impl VersionTab {
    fn label(self) -> &'static str {
        match self {
            Self::Changes => "Changes",
            Self::Diff => "Diff",
            Self::Log => "Log",
        }
    }
}

const VERSION_TABS: [VersionTab; 3] = [VersionTab::Changes, VersionTab::Diff, VersionTab::Log];

/// Version Mode root component.
#[component]
pub fn VersionMode() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let mut active_tab = use_signal(|| VersionTab::Changes);
    let current_tab = (active_tab)();

    let git = (tab_mgr.git_state)();

    if git.is_none() {
        return rsx! {
            div { class: "kiln-version-mode kiln-version-mode--no-repo",
                div { class: "kiln-version-mode__empty",
                    "No git repository detected."
                    br {}
                    "Open a workspace with a .git directory to use Version Mode."
                }
            }
        };
    }

    rsx! {
        div { class: "kiln-version-mode",
            // ── 2px ok mode indicator ───────────────────────────────
            div { class: "kiln-version-mode__indicator" }

            // ── Branch bar ──────────────────────────────────────────
            BranchBar {}

            // ── Tab bar ─────────────────────────────────────────────
            div { class: "kiln-version-tabs",
                for tab in VERSION_TABS {
                    button {
                        class: if current_tab == tab {
                            "kiln-version-tab kiln-version-tab--active"
                        } else {
                            "kiln-version-tab"
                        },
                        onclick: move |_| active_tab.set(tab),
                        "{tab.label()}"
                    }
                }
            }

            // ── Content area ────────────────────────────────────────
            div { class: "kiln-version-content",
                match current_tab {
                    VersionTab::Changes => rsx! { ChangesTab {} },
                    VersionTab::Diff => rsx! { DiffTab {} },
                    VersionTab::Log => rsx! { LogTab {} },
                }
            }
        }
    }
}
