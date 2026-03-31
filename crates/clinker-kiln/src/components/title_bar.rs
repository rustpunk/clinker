use dioxus::desktop::use_window;
use dioxus::prelude::*;

use crate::components::channel_mode::ChannelSwitcher;
use crate::keyboard;
use crate::state::{NavigationContext, PipelineLayoutMode, TabManagerState, use_app_state};
use crate::tab::TabEntry;

/// Custom frameless title bar with context-aware content.
///
/// Common elements (always visible):
///   [clinker][kiln]  |  workspace-name  |  validation LED
///
/// Pipeline context:
///   [New][Open][Save]  |  filename  |  [Canvas|Hybrid|Editor]
///
/// Other contexts:
///   Context label  |  context-specific actions
///
/// Doc: spec §8, §F5, addendum §N4.
#[component]
pub fn TitleBar() -> Element {
    let window = use_window();
    let state = use_app_state();
    let mut tab_mgr: TabManagerState = use_context();
    let current_ctx = (state.active_context)();

    // Derive filename + dirty state from active tab
    let active_id = (tab_mgr.active_tab_id)();
    let tabs = tab_mgr.tabs.read();
    let active_tab = active_id.and_then(|id| tabs.iter().find(|t| t.id == id));
    let filename = active_tab.map(|t| t.display_name()).unwrap_or_default();
    let is_dirty = active_tab.map(|t| t.is_dirty()).unwrap_or(false);
    let has_active_tab = active_tab.is_some();
    let ws_name = (tab_mgr.workspace)().as_ref().map(|ws| ws.display_name());

    // Validation state (only relevant in Pipeline context)
    let has_errors = !(state.parse_errors)().is_empty();
    let led_class = if has_errors || !has_active_tab {
        "kiln-led-dot kiln-led-dot--err"
    } else {
        "kiln-led-dot kiln-led-dot--ok"
    };
    let led_label = if !has_active_tab {
        ""
    } else if has_errors {
        "ERROR"
    } else {
        "VALID"
    };

    // Mutable signal copy for layout mode switching
    let mut pipeline_layout = state.pipeline_layout;

    // Git state for Git context title bar
    let git_branch = (tab_mgr.git_state)().as_ref().map(|gs| gs.branch.clone());

    // Channel state for badge
    let channel_badge_info = (tab_mgr.channel_state)().as_ref().map(|cs| {
        let label = cs.active_channel.as_deref().unwrap_or("NO CHANNEL");
        let tier = cs.active_summary().and_then(|s| s.tier.clone());
        let has_channel = cs.active_channel.is_some();
        (label.to_uppercase(), tier, has_channel)
    });

    rsx! {
        div {
            class: "kiln-title-bar",
            onmousedown: move |_| { window.drag(); },

            // Brand badge — always visible
            div {
                class: "kiln-brand",
                onmousedown: move |e| e.stop_propagation(),
                span { class: "kiln-brand-label", "clinker" }
                span { class: "kiln-brand-value", "kiln" }
            }

            span { class: "kiln-title-divider" }

            // ── Pipeline context: file actions + filename ────────────────
            if current_ctx == NavigationContext::Pipeline {
                div {
                    class: "kiln-file-actions",
                    onmousedown: move |e| e.stop_propagation(),

                    button {
                        class: "kiln-file-btn",
                        title: "New pipeline (Ctrl+N)",
                        onclick: move |_| {
                            let new_tab = TabEntry::new_untitled(&tab_mgr.tabs.read());
                            let new_id = new_tab.id;
                            tab_mgr.tabs.write().push(new_tab);
                            tab_mgr.active_tab_id.set(Some(new_id));
                        },
                        "New"
                    }
                    button {
                        class: "kiln-file-btn",
                        title: "Open file (Ctrl+O)",
                        onclick: move |_| {
                            keyboard::open_file(&mut tab_mgr);
                        },
                        "Open"
                    }
                    button {
                        class: "kiln-file-btn",
                        title: "Open workspace (Ctrl+Shift+O)",
                        onclick: move |_| {
                            keyboard::open_workspace(&mut tab_mgr);
                        },
                        "Workspace"
                    }
                    if has_active_tab {
                        button {
                            class: "kiln-file-btn",
                            title: "Save (Ctrl+S)",
                            onclick: move |_| {
                                keyboard::save_active_tab(&mut tab_mgr, false);
                            },
                            "Save"
                        }
                    }
                }

                span { class: "kiln-title-divider" }
            }

            // ── Non-Pipeline contexts: context label ────────────────────
            if current_ctx != NavigationContext::Pipeline {
                span {
                    class: "kiln-title-context-label",
                    "{current_ctx.label()}"
                }
                span { class: "kiln-title-divider" }
            }

            // Workspace name (if in workspace mode)
            if let Some(ref name) = ws_name {
                span {
                    class: "kiln-title-workspace",
                    "{name}"
                }
                span { class: "kiln-title-divider" }
            }

            // Channel badge + switcher dropdown (all contexts, when channels configured)
            if let Some((ref label, ref tier, has_channel)) = channel_badge_info {
                {
                    let tier_class = tier
                        .as_deref()
                        .map(|t| format!("kiln-channel-badge--{t}"))
                        .unwrap_or_default();
                    let label = label.clone();
                    rsx! {
                        div {
                            onmousedown: move |e| e.stop_propagation(),
                            ChannelSwitcher {
                                badge_label: label,
                                tier_class,
                                has_channel,
                            }
                        }
                        span { class: "kiln-title-divider" }
                    }
                }
            }

            // Pipeline context: filename with dirty indicator
            if current_ctx == NavigationContext::Pipeline {
                span {
                    class: "kiln-title-filename",
                    if is_dirty { "\u{25CF} " } else { "" }
                    "{filename}"
                }
            }

            // Git context: branch name
            if current_ctx == NavigationContext::Git {
                if let Some(ref branch) = git_branch {
                    span {
                        class: "kiln-title-branch",
                        "⑂ {branch}"
                    }
                }
            }

            // Flex spacer
            span { class: "kiln-title-spacer" }

            // ── Pipeline context: layout mode switcher ───────────────────
            if current_ctx == NavigationContext::Pipeline && has_active_tab {
                div {
                    class: "kiln-layout-switcher",
                    onmousedown: move |e| e.stop_propagation(),

                    for mode in PipelineLayoutMode::ALL {
                        {
                            let is_active = (state.pipeline_layout)() == mode;

                            rsx! {
                                button {
                                    key: "{mode.label()}",
                                    class: "kiln-layout-btn",
                                    "data-active": if is_active { "true" } else { "false" },
                                    onclick: move |_| {
                                        pipeline_layout.set(mode);
                                    },
                                    "{mode.label()}"
                                }
                            }
                        }
                    }
                }
            }

            // Base/Resolved/Diff toggle — visible when channel is active in Pipeline context
            if current_ctx == NavigationContext::Pipeline
                && has_active_tab
                && channel_badge_info.is_some()
                && (tab_mgr.channel_state)()
                    .as_ref()
                    .and_then(|cs| cs.active_channel.as_ref())
                    .is_some()
            {
                {
                    let mut view_mode = state.channel_view_mode;
                    rsx! {
                        div {
                            class: "kiln-channel-view-toggle",
                            onmousedown: move |e| e.stop_propagation(),
                            for mode in crate::state::ChannelViewMode::ALL {
                                {
                                    let is_active = (state.channel_view_mode)() == mode;
                                    rsx! {
                                        button {
                                            key: "{mode.label()}",
                                            class: "kiln-layout-btn",
                                            "data-active": if is_active { "true" } else { "false" },
                                            onclick: move |_| view_mode.set(mode),
                                            "{mode.label()}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Validation LED — always visible when Pipeline has active tab
            if current_ctx == NavigationContext::Pipeline && has_active_tab {
                div {
                    class: "kiln-validation-led",
                    onmousedown: move |e| e.stop_propagation(),
                    span { class: "{led_class}" }
                    span { class: "kiln-led-label", "{led_label}" }
                }
            }
        }

    }
}
