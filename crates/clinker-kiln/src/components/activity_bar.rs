/// Activity bar — vertical navigation strip on the far left edge.
///
/// Spec N2: 48px fixed-width, icon + label entries, active/inactive/hover states.
/// Each entry maps to a `NavigationContext`. Settings button pinned to bottom.
/// Badge indicators on Git (dirty file count).

use dioxus::prelude::*;

use crate::state::{use_app_state, NavigationContext, TabManagerState};

/// Maximum entries in the navigation history stack.
const NAV_HISTORY_CAP: usize = 50;

/// Activity bar component — always visible (unless focus mode hides it).
#[component]
pub fn ActivityBar() -> Element {
    let state = use_app_state();
    let active = (state.active_context)();
    let tab_mgr = use_context::<TabManagerState>();
    let activity_bar_visible = (tab_mgr.activity_bar_visible)();

    // Git badge: count of dirty files
    let git_badge = (tab_mgr.git_state)().as_ref().map(|gs| {
        gs.files.len()
    }).unwrap_or(0);

    // Channel badge: count of discovered channels
    let channel_badge = (tab_mgr.channel_state)()
        .as_ref()
        .map(|cs| cs.channels.len())
        .unwrap_or(0);

    rsx! {
        nav {
            class: "kiln-activity-bar",
            class: if !activity_bar_visible { "kiln-activity-bar--hidden" },
            role: "navigation",
            "aria-label": "Activity Bar",

            // ── Context entries ──
            for ctx in NavigationContext::ALL {
                ActivityBarEntry {
                    key: "{ctx.as_data_attr()}",
                    context: ctx,
                    is_active: ctx == active,
                    badge: if ctx == NavigationContext::Git && git_badge > 0 {
                        Some(git_badge)
                    } else {
                        None
                    },
                }
            }

            // ── Flex spacer ──
            div { class: "kiln-activity-bar__spacer" }

            // ── Settings (not a context) ──
            SettingsEntry {}
        }
    }
}

/// A single activity bar entry for a navigation context.
#[component]
fn ActivityBarEntry(context: NavigationContext, is_active: bool, badge: Option<usize>) -> Element {
    let state = use_app_state();
    let tab_mgr = use_context::<TabManagerState>();
    let mut active_ctx = state.active_context;
    let mut nav_history = tab_mgr.nav_history;

    rsx! {
        button {
            class: "kiln-activity-bar__entry",
            class: if is_active { "kiln-activity-bar__entry--active" },
            title: "{context.label()} ({context.keyboard_hint()})",
            onclick: move |_| {
                let current = (active_ctx)();
                if current != context {
                    // Push current to history before switching
                    let mut history = nav_history.write();
                    history.push(current);
                    if history.len() > NAV_HISTORY_CAP {
                        history.remove(0);
                    }
                    drop(history);
                    active_ctx.set(context);
                }
            },

            // Icon
            span {
                class: "kiln-activity-bar__icon",
                "{context.icon_char()}"
            }

            // Label
            span {
                class: "kiln-activity-bar__label",
                "{context.short_label()}"
            }

            // Badge (optional)
            if let Some(count) = badge {
                span {
                    class: "kiln-activity-bar__badge",
                    "{count}"
                }
            }
        }
    }
}

/// Settings entry — pinned to bottom of activity bar.
/// Opens settings overlay, not a navigation context.
#[component]
fn SettingsEntry() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let mut show_settings = tab_mgr.show_settings;

    rsx! {
        button {
            class: "kiln-activity-bar__entry kiln-activity-bar__entry--settings",
            title: "Settings (Ctrl+,)",
            onclick: move |_| {
                let current = (show_settings)();
                show_settings.set(!current);
            },

            span {
                class: "kiln-activity-bar__icon",
                "⚙"
            }
            span {
                class: "kiln-activity-bar__label",
                "Set."
            }
        }
    }
}

/// Switch to a new navigation context, pushing the current one onto the history stack.
///
/// Used by the activity bar and keyboard handler. Extracted as a public helper
/// so cross-context navigation actions can reuse it.
pub fn switch_context(state: &crate::state::AppState, tab_mgr: &TabManagerState, target: NavigationContext) {
    let current = (state.active_context)();
    if current != target {
        let mut nav_history = tab_mgr.nav_history;
        let mut history = nav_history.write();
        history.push(current);
        if history.len() > NAV_HISTORY_CAP {
            history.remove(0);
        }
        drop(history);
        let mut active_ctx = state.active_context;
        active_ctx.set(target);
    }
}

/// Navigate back in the context history stack.
///
/// Returns `true` if navigation happened, `false` if history was empty.
pub fn navigate_back(state: &crate::state::AppState, tab_mgr: &TabManagerState) -> bool {
    let mut nav_history = tab_mgr.nav_history;
    let mut history = nav_history.write();
    if let Some(prev) = history.pop() {
        drop(history);
        // Don't push current to history (it's a "back" operation)
        let mut active_ctx = state.active_context;
        active_ctx.set(prev);
        true
    } else {
        false
    }
}
