use dioxus::desktop::use_window;
use dioxus::prelude::*;

use crate::state::{AppState, LayoutPreset};

/// Custom frameless title bar.
///
/// The outer div carries `-webkit-app-region: drag` so the user can drag the
/// window by clicking anywhere on the bar that isn't an interactive control.
/// Interactive elements (layout switcher buttons) carry `no-drag` to prevent
/// interference.
///
/// Elements (left → right):
///   [clinker][kiln] brand badge  |  filename  ···  layout switcher  ● VALID
///
/// Doc: spec §8 — Title Bar.
#[component]
pub fn TitleBar() -> Element {
    let window = use_window();
    let state = use_context::<AppState>();

    rsx! {
        div {
            class: "kiln-title-bar",
            // Native window drag — clicking empty bar areas moves the window.
            onmousedown: move |_| { window.drag(); },

            // ─── Brand badge ──────────────────────────────────────────────
            div {
                class: "kiln-brand",
                // Prevent accidental drag-start on the badge itself.
                onmousedown: move |e| e.stop_propagation(),
                span { class: "kiln-brand-label", "clinker" }
                span { class: "kiln-brand-value", "kiln" }
            }

            // Vertical divider
            span { class: "kiln-title-divider" }

            // ─── Filename ─────────────────────────────────────────────────
            span {
                class: "kiln-title-filename",
                "customer_etl.yaml"
            }

            // Flex spacer
            span { class: "kiln-title-spacer" }

            // ─── Layout preset switcher ───────────────────────────────────
            // Three-segment toggle group. Implemented inline to avoid pulling
            // in a dioxus-nox dep for three buttons.
            div {
                class: "kiln-layout-switcher",
                // Prevent window drag from triggering on the switcher area.
                onmousedown: move |e| e.stop_propagation(),

                for preset in [LayoutPreset::CanvasFocus, LayoutPreset::Hybrid, LayoutPreset::EditorFocus, LayoutPreset::Schematics] {
                    button {
                        key: "{preset.label()}",
                        class: "kiln-layout-btn",
                        "data-active": if (state.layout)() == preset { "true" } else { "false" },
                        onclick: move |_| {
                            // Copy the Signal handle into a local mut binding so
                            // that set() can take &mut self (Signal is Copy).
                            let mut layout = state.layout;
                            layout.set(preset);
                        },
                        "{preset.label()}"
                    }
                }
            }

            // ─── Validation LED ───────────────────────────────────────────
            div {
                class: "kiln-validation-led",
                onmousedown: move |e| e.stop_propagation(),
                span { class: "kiln-led-dot kiln-led-dot--ok" }
                span { class: "kiln-led-label", "VALID" }
            }
        }
    }
}
