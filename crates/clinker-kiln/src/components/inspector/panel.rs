use dioxus::prelude::*;

use crate::state::AppState;

use super::cxl_input::CxlInput;
use super::scoped_yaml::ScopedYaml;
use super::stage_header::StageHeader;

/// Inspector panel showing the selected stage's configuration.
///
/// Reads from `AppState.pipeline` to find the selected stage by name.
/// Keyed on `stage_id` in the parent so selection changes cause a full
/// remount with fresh signals.
#[component]
pub fn InspectorPanel(stage_id: String) -> Element {
    let state = use_context::<AppState>();

    let pipeline_guard = (state.pipeline).read();
    let Some(config) = pipeline_guard.as_ref() else {
        return rsx! {};
    };

    // Determine stage kind and extract data
    let input = config.inputs.iter().find(|i| i.name == stage_id);
    let transform = config.transformations.iter().find(|t| t.name == stage_id);
    let output = config.outputs.iter().find(|o| o.name == stage_id);

    let (kind_label, accent, subtitle) = if input.is_some() {
        ("SOURCE", "#43B3AE", input.unwrap().path.clone())
    } else if let Some(t) = transform {
        ("TRANSFORM", "#C75B2A", t.description.clone().unwrap_or_default())
    } else if output.is_some() {
        ("OUTPUT", "#B7410E", output.unwrap().path.clone())
    } else {
        return rsx! {};
    };

    let cxl_source = transform.map(|t| t.cxl.clone());

    rsx! {
        div {
            class: "kiln-inspector",
            onmousedown: move |e: MouseEvent| e.stop_propagation(),

            StageHeader {
                stage_id: stage_id.clone(),
                kind_label,
                accent,
                label: stage_id.clone(),
            }

            div {
                class: "kiln-inspector-section",

                div {
                    class: "kiln-section-header",
                    span { class: "kiln-diamond", "\u{25C6}" }
                    span { class: "kiln-section-title", "CONFIGURATION" }
                    span { class: "kiln-section-rule" }
                }

                if !subtitle.is_empty() {
                    div {
                        class: "kiln-cxl-field",
                        label { class: "kiln-cxl-label",
                            if input.is_some() || output.is_some() { "PATH" } else { "DESCRIPTION" }
                        }
                        div {
                            class: "kiln-inspector-value",
                            "{subtitle}"
                        }
                    }
                }

                if let Some(ref cxl) = cxl_source {
                    CxlInput {
                        key: "{stage_id}-cxl",
                        label: "cxl",
                        initial_value: cxl.clone(),
                    }
                }
            }

            // Scoped YAML — shows this stage's YAML block with line numbers
            ScopedYaml {
                stage_id: stage_id.clone(),
                accent,
            }
        }
    }
}
