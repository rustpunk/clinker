use dioxus::desktop::use_window;
use dioxus::prelude::*;

use crate::state::{AppState, LayoutPreset, TabManagerState};

/// Custom frameless title bar.
///
/// The outer div carries `-webkit-app-region: drag` so the user can drag the
/// window by clicking anywhere on the bar that isn't an interactive control.
/// Interactive elements (layout switcher buttons) carry `no-drag` to prevent
/// interference.
///
/// Elements (left -> right):
///   [clinker][kiln] brand badge  |  filename  ...  layout switcher  VALID
///
/// Doc: spec §8 — Title Bar, §F5 — Title Bar Integration.
#[component]
pub fn TitleBar() -> Element {
    let window = use_window();
    let state = use_context::<AppState>();
    let tab_mgr: TabManagerState = use_context();

    // Derive filename + dirty state from active tab
    let active_id = (tab_mgr.active_tab_id)();
    let tabs = tab_mgr.tabs.read();
    let active_tab = active_id.and_then(|id| tabs.iter().find(|t| t.id == id));
    let filename = active_tab
        .map(|t| t.display_name())
        .unwrap_or_default();
    let is_dirty = active_tab.map(|t| t.is_dirty()).unwrap_or(false);
    let ws_name = (tab_mgr.workspace)()
        .as_ref()
        .map(|ws| ws.display_name());

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

            // Layout preset switcher
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

            // Validation LED
            div {
                class: "kiln-validation-led",
                onmousedown: move |e| e.stop_propagation(),
                span { class: "kiln-led-dot kiln-led-dot--ok" }
                span { class: "kiln-led-label", "VALID" }
            }
        }
    }
}
