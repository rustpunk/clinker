use dioxus::prelude::*;

use crate::demo::{demo_pipeline, DEMO_YAML};
use crate::state::AppState;

use super::tokenizer::tokenize;

/// Full-height YAML sidebar with line-number gutter and syntax highlighting.
///
/// Selection sync: when a stage is selected (via canvas click or inspector),
/// the corresponding YAML lines receive a tinted background and accent left
/// border. The line range comes from `DemoStage.yaml_line_start/end`.
///
/// Doc: spec §6 — YAML Editor (Sidebar).
#[component]
pub fn YamlSidebar() -> Element {
    let state = use_context::<AppState>();
    let lines = tokenize(DEMO_YAML);

    // Compute the selected line range (1-indexed) from the selected stage.
    let stages = demo_pipeline();
    let selected_range = (state.selected_stage)().and_then(|id| {
        stages
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.yaml_line_start..=s.yaml_line_end)
    });

    rsx! {
        div {
            class: "kiln-yaml-sidebar",

            // Section header
            div {
                class: "kiln-section-header",
                span { class: "kiln-diamond", "\u{25C6}" }
                span { class: "kiln-section-title", "PIPELINE YAML" }
                span { class: "kiln-section-rule" }
                span { class: "kiln-section-filename", "customer_etl.yaml" }
            }

            // Scrollable code area
            div {
                class: "kiln-yaml-code-area",

                // Gutter (line numbers)
                div {
                    class: "kiln-yaml-gutter",
                    for (i, _) in lines.iter().enumerate() {
                        {
                            let line_num = i + 1; // 1-indexed
                            let in_range = selected_range.as_ref()
                                .is_some_and(|r| r.contains(&line_num));
                            rsx! {
                                div {
                                    key: "gutter-{i}",
                                    class: "kiln-yaml-line-num",
                                    "data-selected": if in_range { "true" },
                                    "{line_num}"
                                }
                            }
                        }
                    }
                }

                // Code column
                div {
                    class: "kiln-yaml-code",
                    for (i, line_tokens) in lines.iter().enumerate() {
                        {
                            let line_num = i + 1;
                            let in_range = selected_range.as_ref()
                                .is_some_and(|r| r.contains(&line_num));
                            rsx! {
                                div {
                                    key: "line-{i}",
                                    class: "kiln-yaml-line",
                                    "data-selected": if in_range { "true" },
                                    for (j, token) in line_tokens.iter().enumerate() {
                                        span {
                                            key: "tok-{i}-{j}",
                                            "data-token": token.kind.as_data_attr(),
                                            "{token.text}"
                                        }
                                    }
                                    if line_tokens.iter().all(|t| t.text.is_empty()) {
                                        "\u{00A0}"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
