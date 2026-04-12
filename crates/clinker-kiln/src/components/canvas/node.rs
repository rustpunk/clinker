use dioxus::prelude::*;

use crate::pipeline_view::{NODE_HEIGHT, NODE_WIDTH, StageKind, StageView};
use crate::state::{CompositionDrillFrame, use_app_state};

/// A single pipeline stage rendered as a rustpunk node card on the canvas.
#[component]
pub fn CanvasNode(stage: StageView) -> Element {
    let state = use_app_state();
    let kind_attr = stage.kind.kind_attr();
    let badge = stage.kind.badge_label();
    let stage_id = stage.id.clone();
    let is_composition = matches!(&stage.kind, StageKind::Composition);

    let is_selected = (state.selected_stage)().as_deref() == Some(stage_id.as_str());
    let is_error = matches!(&stage.kind, StageKind::Error);

    let node_class = match (is_selected, is_error, is_composition) {
        (_, _, true) => {
            if is_selected {
                "kiln-node kiln-node--selected kiln-node--composition"
            } else {
                "kiln-node kiln-node--composition"
            }
        }
        (true, true, _) => "kiln-node kiln-node--selected kiln-node--error",
        (false, true, _) => "kiln-node kiln-node--error",
        (true, false, _) => "kiln-node kiln-node--selected",
        (false, false, _) => "kiln-node",
    };

    let border_style = format!(
        "left: {x}px; top: {y}px; width: {w}px; border-top-color: var(--kiln-stage-accent);",
        x = stage.canvas_x,
        y = stage.canvas_y,
        w = NODE_WIDTH
    );

    const BORDER_TOP: f32 = 3.0;
    const PORT_HALF: f32 = 4.0;
    let port_y = NODE_HEIGHT / 2.0 - PORT_HALF - BORDER_TOP;

    rsx! {
        div {
            key: "{stage.id}",
            class: "{node_class}",
            "data-stage-kind": kind_attr,
            style: "{border_style}",
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
                style: "color: var(--kiln-stage-accent);",
                span { class: "kiln-node-type-badge", "{badge}" }
            }

            div { class: "kiln-node-label", "{stage.label}" }
            hr { class: "kiln-rust-line" }
            div { class: "kiln-node-subtitle", "{stage.subtitle}" }

            // Drill-in button for composition nodes
            if is_composition {
                button {
                    class: "kiln-node-drill-btn",
                    title: "Drill into composition",
                    onclick: {
                        let stage_id = stage.id.clone();
                        let subtitle = stage.subtitle.clone();
                        move |e: MouseEvent| {
                            e.stop_propagation();
                            drill_into_composition(&state, &stage_id, &subtitle);
                        }
                    },
                    "▶"
                }
            }

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

/// Push a drill frame for a composition node onto the drill stack.
fn drill_into_composition(state: &crate::state::AppState, node_name: &str, _subtitle: &str) {
    let compiled_guard = state.compiled_plan.read();
    let Some(plan) = compiled_guard.as_ref() else {
        return;
    };

    // Look up the body ID from the compilation artifacts
    let Some(&body_id) = plan.artifacts().composition_body_assignments.get(node_name) else {
        return;
    };

    // Get the use_path from the body
    let use_path = plan
        .body_of(body_id)
        .map(|b| b.signature_path.clone())
        .unwrap_or_default();

    drop(compiled_guard);

    let mut drill = state.composition_drill_stack;
    drill.write().push(CompositionDrillFrame {
        body_id,
        alias: node_name.to_string(),
        use_path,
    });
}
