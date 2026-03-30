use dioxus::prelude::*;

use crate::autodoc::generate_stage_doc;
use crate::notes::parse_notes;
use crate::pipeline_view::derive_pipeline_view;
use crate::state::use_app_state;

use super::flow_bar::FlowBar;
use super::stage_card::StageCard;

/// Schematics layout — full-pipeline documentation view.
///
/// Replaces canvas + inspector + YAML sidebar with a single scrollable
/// document showing the entire pipeline structure in Blueprint sub-aesthetic.
///
/// Spec §A7.2: mode indicator + flow bar + content area.
#[component]
pub fn SchematicsPanel() -> Element {
    let state = use_app_state();

    let pipeline_guard = (state.pipeline).read();
    let Some(config) = pipeline_guard.as_ref() else {
        return rsx! {
            div {
                class: "kiln-schematics",
                div {
                    class: "kiln-schematics-empty",
                    "No pipeline loaded \u{2014} edit the YAML to see schematics"
                }
            }
        };
    };

    let compositions_read = (state.compositions).read();
    let pipeline_view = derive_pipeline_view(config, &compositions_read);
    let stages = pipeline_view.stages;
    let pipeline_name = config.pipeline.name.clone();

    // Pre-compute docs + notes for each stage
    let stage_data: Vec<_> = stages
        .iter()
        .enumerate()
        .map(|(i, stage)| {
            let doc = generate_stage_doc(config, &stage.id)
                .unwrap_or_else(|| crate::autodoc::StageDoc {
                    description: "No documentation available.".to_string(),
                    metadata: vec![],
                    columns_added: vec![],
                });

            let notes_value = config.inputs.iter().find(|inp| inp.name == stage.id).and_then(|inp| inp.notes.as_ref())
                .or_else(|| config.transformations.iter().find(|t| t.name == stage.id).and_then(|t| t.notes.as_ref()))
                .or_else(|| config.outputs.iter().find(|o| o.name == stage.id).and_then(|o| o.notes.as_ref()));
            let notes = parse_notes(notes_value);

            (i, stage.clone(), doc, notes)
        })
        .collect();

    rsx! {
        div {
            class: "kiln-schematics",

            // ── Mode indicator (2px verdigris bar) ────────────────────────
            div { class: "kiln-schematics-indicator" }

            // ── Flow bar (compact horizontal strip) ───────────────────────
            FlowBar { stages: stages.clone() }

            // ── Content area (scrollable, Blueprint gridlines) ────────────
            div {
                class: "kiln-schematics-content",

                // Pipeline summary header
                div {
                    class: "kiln-schematics-summary",
                    div {
                        class: "kiln-schematics-section-header",
                        span { class: "kiln-schematics-diamond", "\u{25C7}" }
                        span { class: "kiln-schematics-section-title", "PIPELINE SUMMARY" }
                        span { class: "kiln-schematics-section-rule" }
                    }
                    div {
                        class: "kiln-schematics-summary-text",
                        "{pipeline_name} \u{2014} {stages.len()} stage(s)"
                    }
                }

                // Stage detail cards with flow arrows
                for (i, stage, doc, notes) in stage_data.into_iter() {
                    // Flow arrow between cards (except before the first)
                    if i > 0 {
                        div {
                            class: "kiln-schematics-arrow",
                            svg {
                                width: "20",
                                height: "24",
                                view_box: "0 0 20 24",
                                line {
                                    x1: "10", y1: "0", x2: "10", y2: "18",
                                    stroke: "var(--kiln-verdigris)",
                                    stroke_width: "1.5",
                                    stroke_dasharray: "4 3",
                                    stroke_opacity: "0.5",
                                }
                                polyline {
                                    points: "5,16 10,22 15,16",
                                    fill: "none",
                                    stroke: "var(--kiln-verdigris)",
                                    stroke_width: "1.5",
                                    stroke_opacity: "0.7",
                                    stroke_linejoin: "round",
                                    stroke_linecap: "round",
                                }
                            }
                        }
                    }

                    StageCard {
                        key: "card-{stage.id}",
                        index: i,
                        stage_id: stage.id.clone(),
                        accent: stage.kind.accent_color(),
                        badge: stage.kind.badge_label(),
                        doc,
                        notes,
                    }
                }

                // Footer
                div {
                    class: "kiln-schematics-footer",
                    span { class: "kiln-schematics-footer-rule" }
                    span { class: "kiln-schematics-footer-label", "CLINKER AUTODOC \u{00B7} BLUEPRINT" }
                    span { class: "kiln-schematics-footer-rule" }
                }
            }
        }
    }
}
