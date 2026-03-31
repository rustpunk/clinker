use dioxus::prelude::*;

use crate::channel_resolve::OverrideKind;
use crate::pipeline_view::{INLINE_THRESHOLD, NODE_HEIGHT, NODE_WIDTH, StageKind, StageView};
use crate::state::{DrillInEntry, TabManagerState, use_app_state};

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

    // Channel override indicator
    let _tab_mgr = use_context::<TabManagerState>();
    let channel_resolution = (state.channel_pipeline)();
    let override_info = channel_resolution.as_ref().and_then(|cr| {
        cr.overrides_applied
            .iter()
            .find(|o| o.target_name == stage.id)
    });
    let _has_channel_override = override_info.is_some();
    let is_added = override_info
        .map(|o| o.kind == OverrideKind::Added)
        .unwrap_or(false);
    let is_removed = override_info
        .map(|o| o.kind == OverrideKind::Removed)
        .unwrap_or(false);
    let is_modified = override_info
        .map(|o| o.kind == OverrideKind::Modified)
        .unwrap_or(false);

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
            x = stage.canvas_x,
            y = stage.canvas_y,
            w = NODE_WIDTH
        )
    } else {
        format!(
            "left: {x}px; top: {y}px; width: {w}px; border-top-color: {accent};",
            x = stage.canvas_x,
            y = stage.canvas_y,
            w = NODE_WIDTH
        )
    };

    const BORDER_TOP: f32 = 3.0;
    const PORT_HALF: f32 = 4.0;
    let port_y = NODE_HEIGHT / 2.0 - PORT_HALF - BORDER_TOP;

    // Extra info for composition nodes
    let comp_meta = if let StageKind::Composition(ref meta) = stage.kind {
        Some(meta.clone())
    } else {
        None
    };

    let comp_subtitle = comp_meta.as_ref().map(|meta| {
        let version_str = meta
            .version
            .as_deref()
            .map(|v| format!(" v{v}"))
            .unwrap_or_default();
        format!(
            "\u{25C8} {}{} \u{00b7} {} transforms",
            meta.name, version_str, meta.transform_count
        )
    });

    // Interaction indicator: ▾ for inline expand, → for drill-in
    let expand_indicator = comp_meta.as_ref().map(|meta| {
        if meta.transform_count <= INLINE_THRESHOLD {
            "\u{25BE}" // ▾ inline expand
        } else {
            "\u{2192}" // → drill-in
        }
    });

    rsx! {
        div {
            key: "{stage.id}",
            class: "{node_class}",
            style: "{border_style}",
            onmousedown: move |e: MouseEvent| e.stop_propagation(),
            onclick: {
                let stage_id = stage_id.clone();
                let comp_meta = comp_meta.clone();
                move |e: MouseEvent| {
                    e.stop_propagation();
                    if let Some(ref meta) = comp_meta {
                        // Composition click: inline expand or drill-in
                        if meta.transform_count <= INLINE_THRESHOLD {
                            let mut sig = state.expanded_compositions;
                            let mut expanded = sig.write();
                            if expanded.contains(&meta.path) {
                                expanded.remove(&meta.path);
                            } else {
                                expanded.insert(meta.path.clone());
                            }
                        } else {
                            let mut sig = state.composition_drill_stack;
                            sig.write().push(DrillInEntry {
                                path: meta.path.clone(),
                                name: meta.name.clone(),
                            });
                        }
                    } else {
                        // Regular node: toggle selection
                        let mut sel = state.selected_stage;
                        let current = sel.peek().clone();
                        if current.as_deref() == Some(stage_id.as_str()) {
                            sel.set(None);
                        } else {
                            sel.set(Some(stage_id.clone()));
                        }
                    }
                }
            },

            div {
                class: "kiln-node-badge",
                style: "color: {accent};",
                span { class: "kiln-node-type-badge", "{badge}" }
                // Channel override indicator
                if is_modified {
                    span {
                        class: "kiln-node-override-badge kiln-node-override-badge--modified",
                        title: "Overridden by channel",
                        "\u{27f2}"
                    }
                }
                if is_added {
                    span {
                        class: "kiln-node-override-badge kiln-node-override-badge--added",
                        title: "Added by channel",
                        "+ CHANNEL"
                    }
                }
                if is_removed {
                    span {
                        class: "kiln-node-override-badge kiln-node-override-badge--removed",
                        title: "Removed by channel",
                        "\u{2298} removed"
                    }
                }
            }

            div { class: "kiln-node-label", "{stage.label}" }
            hr { class: "kiln-rust-line" }

            if let Some(ref sub) = comp_subtitle {
                div { class: "kiln-node-subtitle", "{sub}" }
            } else {
                div { class: "kiln-node-subtitle", "{stage.subtitle}" }
            }

            // Expand/drill indicator for composition nodes
            if let Some(indicator) = expand_indicator {
                div { class: "kiln-node-expand-indicator", "{indicator}" }
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
