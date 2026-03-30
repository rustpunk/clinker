use dioxus::desktop::use_window;
use dioxus::prelude::*;

use crate::keyboard;
use crate::state::{use_app_state, LayoutPreset, TabManagerState};
use crate::tab::TabEntry;

/// Custom frameless title bar with file action buttons.
///
/// Elements (left -> right):
///   [clinker][kiln]  |  [New][Open][Save]  |  workspace  |  filename
///   ...  layout switcher  VALID
///
/// Doc: spec §8 — Title Bar, §F5 — Title Bar Integration.
#[component]
pub fn TitleBar() -> Element {
    let window = use_window();
    let state = use_app_state();
    let mut tab_mgr: TabManagerState = use_context();

    // Derive filename + dirty state from active tab
    let active_id = (tab_mgr.active_tab_id)();
    let tabs = tab_mgr.tabs.read();
    let active_tab = active_id.and_then(|id| tabs.iter().find(|t| t.id == id));
    let filename = active_tab
        .map(|t| t.display_name())
        .unwrap_or_default();
    let is_dirty = active_tab.map(|t| t.is_dirty()).unwrap_or(false);
    let has_active_tab = active_tab.is_some();
    let ws_name = (tab_mgr.workspace)()
        .as_ref()
        .map(|ws| ws.display_name());

    // Validation state
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

    rsx! {
        div {
            class: "kiln-title-bar",
            onmousedown: move |_| { window.drag(); },

            // Brand badge
            div {
                class: "kiln-brand",
                onmousedown: move |e| e.stop_propagation(),
                span { class: "kiln-brand-label", "clinker" }
                span { class: "kiln-brand-value", "kiln" }
            }

            span { class: "kiln-title-divider" }

            // ─── File action buttons ─────────────────────────────────
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

            // Workspace name (if in workspace mode)
            if let Some(ref name) = ws_name {
                span {
                    class: "kiln-title-workspace",
                    "{name}"
                }
                span { class: "kiln-title-divider" }
            }

            // Filename with dirty indicator
            span {
                class: "kiln-title-filename",
                if is_dirty { "\u{25CF} " } else { "" }
                "{filename}"
            }

            // Flex spacer
            span { class: "kiln-title-spacer" }

            // Layout preset switcher (only show when a tab is active)
            if has_active_tab {
                div {
                    class: "kiln-layout-switcher",
                    onmousedown: move |e| e.stop_propagation(),

                    for preset in [LayoutPreset::CanvasFocus, LayoutPreset::Hybrid, LayoutPreset::EditorFocus, LayoutPreset::Schematics] {
                        button {
                            key: "{preset.label()}",
                            class: "kiln-layout-btn",
                            "data-active": if (state.layout)() == preset { "true" } else { "false" },
                            onclick: move |_| {
                                let mut layout = state.layout;
                                layout.set(preset);
                            },
                            "{preset.label()}"
                        }
                    }
                }
            }

            // Validation LED
            if has_active_tab {
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
