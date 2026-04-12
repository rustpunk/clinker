//! Provenance panel for the inspector — shows the override chain
//! for a selected field in the pipeline configuration.

use dioxus::prelude::*;

use clinker_core::config::composition::{LayerKind, ProvenanceLayer};

use crate::state::use_app_state;

/// Provenance panel — shows the override chain for a selected config field.
///
/// Reads from `compiled_plan.provenance()` keyed by `(node_name, param_name)`.
/// Won layer shown with [x], shadowed layers shown with [ ].
/// In Raw mode, only CompositionDefault layers are shown.
#[component]
pub fn ProvenancePanel(node_name: String, param_name: String) -> Element {
    let state = use_app_state();
    let view_mode = *state.channel_view_mode.read();

    let compiled_guard = state.compiled_plan.read();

    let resolved = compiled_guard
        .as_ref()
        .and_then(|plan| plan.provenance().get(&node_name, &param_name));

    let Some(resolved) = resolved else {
        return rsx! {};
    };

    // In Raw mode, only show CompositionDefault layers
    let layers: Vec<&ProvenanceLayer> = match view_mode {
        crate::state::ChannelViewMode::Raw => resolved
            .provenance
            .iter()
            .filter(|l| l.kind == LayerKind::CompositionDefault)
            .collect(),
        crate::state::ChannelViewMode::Resolved => resolved.provenance.iter().collect(),
    };

    if layers.is_empty() {
        return rsx! {};
    }

    let value_display = format!("{}", resolved.value);

    rsx! {
        div {
            class: "kiln-provenance-panel",

            div {
                class: "kiln-provenance-header",
                span { class: "kiln-provenance-value", "{value_display}" }
            }

            div {
                class: "kiln-provenance-label",
                "Provenance:"
            }

            div {
                class: "kiln-provenance-layers",
                for (i, layer) in layers.iter().enumerate() {
                    {
                        let kind_label = layer_kind_label(layer.kind);
                        let won_marker = if layer.won { "[x]" } else { "[ ]" };
                        let layer_class = if layer.won {
                            "kiln-provenance-layer kiln-provenance-layer--won"
                        } else {
                            "kiln-provenance-layer kiln-provenance-layer--shadowed"
                        };
                        let span_display = format!(
                            "offset:{}..{}",
                            layer.span.start,
                            layer.span.end()
                        );

                        rsx! {
                            div {
                                key: "prov-{i}",
                                class: "{layer_class}",
                                span { class: "kiln-provenance-marker", "{won_marker}" }
                                span { class: "kiln-provenance-kind", "{kind_label}" }
                                span { class: "kiln-provenance-span", "{span_display}" }
                                if !layer.won {
                                    span { class: "kiln-provenance-shadowed-label", "(shadowed)" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn layer_kind_label(kind: LayerKind) -> &'static str {
    match kind {
        LayerKind::CompositionDefault => "CompositionDefault",
        LayerKind::ChannelDefault => "ChannelDefault",
        LayerKind::ChannelFixed => "ChannelFixed",
        LayerKind::InspectorEdit => "InspectorEdit",
    }
}
