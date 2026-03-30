use dioxus::prelude::*;

use crate::pipeline_view::{StageView, NODE_HEIGHT, NODE_WIDTH};
use crate::state::use_app_state;

/// A single pipeline stage rendered as a rustpunk node card on the canvas.
#[component]
pub fn CanvasNode(stage: StageView) -> Element {
    let state = use_app_state();
    let accent = stage.kind.accent_color();
    let badge = stage.kind.badge_label();
    let stage_id = stage.id.clone();

    let is_selected = (state.selected_stage)().as_deref() == Some(stage_id.as_str());
    let node_class = if is_selected {
        "kiln-node kiln-node--selected"
    } else {
        "kiln-node"
    };

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
            onmousedown: move |e: MouseEvent| e.stop_propagation(),
            onclick: {
                let stage_id = stage_id.clone();
                move |e: MouseEvent| {
                    e.stop_propagation();
                    let mut sel = state.selected_stage;
                    let current = sel.peek().clone();
                    if current.as_deref() == Some(stage_id.as_str()) {
                        sel.set(None);
                    } else {
                        sel.set(Some(stage_id.clone()));
                    }
                }
            },

            div {
                class: "kiln-node-badge",
                style: "color: {accent};",
                span { class: "kiln-node-type-badge", "{badge}" }
            }

            div { class: "kiln-node-label", "{stage.label}" }
            hr { class: "kiln-rust-line" }
            div { class: "kiln-node-subtitle", "{stage.subtitle}" }

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
