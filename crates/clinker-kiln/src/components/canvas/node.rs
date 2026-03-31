use dioxus::prelude::*;

use crate::pipeline_view::{StageKind, StageView, NODE_HEIGHT, NODE_WIDTH};
use crate::state::use_app_state;

/// A single pipeline stage rendered as a rustpunk node card on the canvas.
#[component]
pub fn CanvasNode(stage: StageView) -> Element {
    let state = use_app_state();
    let accent = stage.kind.accent_color();
    let badge = stage.kind.badge_label();
    let stage_id = stage.id.clone();

    let is_selected = (state.selected_stage)().as_deref() == Some(stage_id.as_str());
    let is_composition = matches!(&stage.kind, StageKind::Composition(_));
    let is_error = matches!(&stage.kind, StageKind::Error);
    let is_from_composition = stage.from_composition.is_some();

    let node_class = match (is_selected, is_composition, is_error) {
        (true, true, _) => "kiln-node kiln-node--selected kiln-node--composition",
        (false, true, _) => "kiln-node kiln-node--composition",
        (_, _, true) if is_selected => "kiln-node kiln-node--selected kiln-node--error",
        (_, _, true) => "kiln-node kiln-node--error",
        (true, false, false) => "kiln-node kiln-node--selected",
        (false, false, false) => "kiln-node",
    };

    // Composition transforms from an import get a subtle indicator
    let border_style = if is_from_composition {
        format!(
            "left: {x}px; top: {y}px; width: {w}px; border-top-color: {accent}; border-left: 2px dashed {accent};",
            x = stage.canvas_x, y = stage.canvas_y, w = NODE_WIDTH
        )
    } else {
        format!(
            "left: {x}px; top: {y}px; width: {w}px; border-top-color: {accent};",
            x = stage.canvas_x, y = stage.canvas_y, w = NODE_WIDTH
        )
    };

    const BORDER_TOP: f32 = 3.0;
    const PORT_HALF: f32 = 4.0;
    let port_y = NODE_HEIGHT / 2.0 - PORT_HALF - BORDER_TOP;

    // Extra info for composition nodes
    let comp_subtitle = if let StageKind::Composition(ref meta) = stage.kind {
        let version_str = meta
            .version
            .as_deref()
            .map(|v| format!(" v{v}"))
            .unwrap_or_default();
        Some(format!(
            "◈ {}{} · {} transforms",
            meta.name, version_str, meta.transform_count
        ))
    } else {
        None
    };

    rsx! {
        div {
            key: "{stage.id}",
            class: "{node_class}",
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
                style: "color: {accent};",
                span { class: "kiln-node-type-badge", "{badge}" }
            }

            div { class: "kiln-node-label", "{stage.label}" }
            hr { class: "kiln-rust-line" }

            if let Some(ref sub) = comp_subtitle {
                div { class: "kiln-node-subtitle", "{sub}" }
            } else {
                div { class: "kiln-node-subtitle", "{stage.subtitle}" }
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
