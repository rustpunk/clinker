use dioxus::prelude::*;

use crate::demo::{DemoStage, NODE_HEIGHT, NODE_WIDTH};
use crate::state::AppState;

/// A single pipeline stage rendered as a rustpunk node card on the canvas.
///
/// Positioned absolutely via inline `left`/`top` style. Click-to-select:
/// clicking the node toggles `AppState.selected_stage`; `onmousedown`
/// stops propagation to prevent the canvas pan from starting.
///
/// Doc: spec §4.2 — Node Rendering, §4.5 — Canvas Interactions.
#[component]
pub fn CanvasNode(stage: DemoStage) -> Element {
    let state = use_context::<AppState>();
    let accent = stage.step_type.accent_color();
    let badge = stage.step_type.badge_label();
    let pass_label = stage.pass.label();
    let stage_id = stage.id;

    // Selection state — read the signal to subscribe.
    let is_selected = (state.selected_stage)() == Some(stage_id);
    let node_class = if is_selected {
        "kiln-node kiln-node--selected"
    } else {
        "kiln-node"
    };

    // Port positioning (see Phase 1 commit for derivation).
    const BORDER_TOP: f32 = 3.0;
    const PORT_HALF: f32 = 4.0;
    let port_y = NODE_HEIGHT / 2.0 - PORT_HALF - BORDER_TOP;

    rsx! {
        div {
            key: "{stage.id}",
            class: "{node_class}",
            style: "left: {stage.canvas_x}px; top: {stage.canvas_y}px; \
                    width: {NODE_WIDTH}px; \
                    border-top-color: {accent};",
            // Prevent mousedown from triggering canvas pan.
            onmousedown: move |e: MouseEvent| e.stop_propagation(),
            // Toggle selection on click.
            onclick: move |_| {
                let mut sel = state.selected_stage;
                let current = *sel.peek();
                if current == Some(stage_id) {
                    sel.set(None);
                } else {
                    sel.set(Some(stage_id));
                }
            },

            // Pass + type badge row
            div {
                class: "kiln-node-badge",
                style: "color: {accent};",
                span { class: "kiln-node-pass-pill", "{pass_label}" }
                span { class: "kiln-node-type-badge", "{badge}" }
            }

            // Primary label
            div { class: "kiln-node-label", "{stage.label}" }

            // Rust-line separator
            hr { class: "kiln-rust-line" }

            // Subtitle
            div { class: "kiln-node-subtitle", "{stage.subtitle}" }

            // Port squares
            div {
                class: "kiln-node-port kiln-node-port--in",
                style: "top: {port_y}px;",
            }
            div {
                class: "kiln-node-port kiln-node-port--out",
                style: "top: {port_y}px;",
            }
        }
    }
}
