use dioxus::prelude::*;

use crate::demo::{DemoStage, DEMO_YAML};
use crate::components::yaml_sidebar::tokenizer::tokenize;

/// Bottom section of the inspector showing the selected stage's YAML block.
///
/// Uses the same pure-Rust tokeniser as the full sidebar, but renders only
/// the lines within `stage.yaml_line_start..=stage.yaml_line_end`.
///
/// Doc: spec §5.3 — Scoped YAML Editor.
#[component]
pub fn ScopedYaml(stage: DemoStage) -> Element {
    let all_lines = tokenize(DEMO_YAML);
    let start = stage.yaml_line_start.saturating_sub(1); // 0-indexed
    let end = stage.yaml_line_end.min(all_lines.len());
    let scoped_lines = &all_lines[start..end];

    rsx! {
        div {
            class: "kiln-inspector-yaml",

            // Section header
            div {
                class: "kiln-section-header",
                span { class: "kiln-diamond", "\u{25C6}" }
                span { class: "kiln-section-title", "STAGE YAML" }
                span { class: "kiln-section-rule" }
            }

            // Code lines (no gutter — the inspector has limited width)
            div {
                class: "kiln-inspector-yaml-code",
                for (i, line_tokens) in scoped_lines.iter().enumerate() {
                    div {
                        key: "scoped-{i}",
                        class: "kiln-yaml-line",
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
