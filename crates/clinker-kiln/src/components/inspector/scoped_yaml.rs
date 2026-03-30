use dioxus::prelude::*;

use crate::components::yaml_sidebar::tokenizer::tokenize;
use crate::state::use_app_state;
use crate::sync::compute_yaml_ranges;

/// Bottom section of the inspector showing the selected stage's YAML block
/// with absolute line numbers from the full YAML.
#[component]
pub fn ScopedYaml(stage_id: String, accent: &'static str) -> Element {
    let state = use_app_state();
    let text = (state.yaml_text)();
    let pipeline_guard = (state.pipeline).read();

    let Some(config) = pipeline_guard.as_ref() else {
        return rsx! {};
    };

    let ranges = compute_yaml_ranges(&text, config);
    let Some(&(start, end)) = ranges.get(&stage_id) else {
        return rsx! {};
    };

    let all_lines = tokenize(&text);
    let start_idx = start.saturating_sub(1); // 0-indexed
    let end_idx = end.min(all_lines.len());
    let scoped_lines = &all_lines[start_idx..end_idx];

    rsx! {
        div {
            class: "kiln-inspector-yaml",

            div {
                class: "kiln-section-header",
                span { class: "kiln-diamond", "\u{25C6}" }
                span { class: "kiln-section-title", "STAGE YAML" }
                span { class: "kiln-section-rule" }
            }

            div {
                class: "kiln-yaml-code-area kiln-inspector-yaml-area",

                // Gutter — absolute line numbers
                div {
                    class: "kiln-yaml-gutter",
                    for (i, _) in scoped_lines.iter().enumerate() {
                        {
                            let line_num = start + i;
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
