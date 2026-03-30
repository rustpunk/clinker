use dioxus::prelude::*;

use crate::components::yaml_sidebar::tokenizer::tokenize;
use crate::demo::{DemoStage, DEMO_YAML};

/// Bottom section of the inspector showing the selected stage's YAML block
/// with line numbers matching the full pipeline YAML and an accent left border.
///
/// Layout mirrors the main YAML sidebar (gutter + code area) but scoped to
/// just this stage's lines. Line numbers are absolute (e.g., lines 14–16 for
/// the filter stage), not relative, so the user can cross-reference with the
/// full sidebar.
///
/// Doc: spec §5.3 — Scoped YAML Editor.
#[component]
pub fn ScopedYaml(stage: DemoStage) -> Element {
    let accent = stage.step_type.accent_color();
    let all_lines = tokenize(DEMO_YAML);
    let start = stage.yaml_line_start.saturating_sub(1); // 0-indexed
    let end = stage.yaml_line_end.min(all_lines.len());
    let scoped_lines = &all_lines[start..end];

    rsx! {
        div {
            class: "kiln-inspector-yaml",
            style: "border-left: 3px solid {accent};",

            // Section header
            div {
                class: "kiln-section-header",
                span { class: "kiln-diamond", "\u{25C6}" }
                span { class: "kiln-section-title", "STAGE YAML" }
                span { class: "kiln-section-rule" }
            }

            // Gutter + code area (same layout as full sidebar)
            div {
                class: "kiln-yaml-code-area kiln-inspector-yaml-area",

                // Gutter — absolute line numbers from the full YAML
                div {
                    class: "kiln-yaml-gutter",
                    for (i, _) in scoped_lines.iter().enumerate() {
                        {
                            let line_num = stage.yaml_line_start + i;
                            rsx! {
                                div {
                                    key: "gutter-{i}",
                                    class: "kiln-yaml-line-num",
                                    "{line_num}"
                                }
                            }
                        }
                    }
                }

                // Code column
                div {
                    class: "kiln-yaml-code",
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
}
