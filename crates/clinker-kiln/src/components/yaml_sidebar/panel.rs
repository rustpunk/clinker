use dioxus::prelude::*;

use crate::demo::DEMO_YAML;

use super::tokenizer::tokenize;

/// Full-height YAML sidebar with line-number gutter and syntax highlighting.
///
/// Phase 1: read-only display of `DEMO_YAML`. Live editing and selection sync
/// are implemented in Phase 2.
///
/// Layout (horizontal):
///   ┌────────────────────────────────────────────┐
///   │ ◆ PIPELINE YAML ─────── customer_etl.yaml  │  ← section header
///   ├──────┬─────────────────────────────────────┤
///   │ line │  key: value                         │  ← code area
///   │  num │                                     │
///   └──────┴─────────────────────────────────────┘
///
/// Doc: spec §6 — YAML Editor (Sidebar).
#[component]
pub fn YamlSidebar() -> Element {
    let lines = tokenize(DEMO_YAML);

    rsx! {
        div {
            class: "kiln-yaml-sidebar",

            // Section header
            div {
                class: "kiln-section-header",
                span { class: "kiln-diamond", "◆" }
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
                        div {
                            key: "gutter-{i}",
                            class: "kiln-yaml-line-num",
                            "{i + 1}"
                        }
                    }
                }

                // Code column
                div {
                    class: "kiln-yaml-code",
                    for (i, line_tokens) in lines.iter().enumerate() {
                        div {
                            key: "line-{i}",
                            class: "kiln-yaml-line",
                            for (j, token) in line_tokens.iter().enumerate() {
                                span {
                                    key: "tok-{i}-{j}",
                                    "data-token": token.kind.as_data_attr(),
                                    "{token.text}"
                                }
                            }
                            // Render a non-breaking space for empty lines so
                            // line-height is preserved.
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
