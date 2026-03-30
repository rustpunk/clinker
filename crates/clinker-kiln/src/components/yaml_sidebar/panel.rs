use std::collections::HashMap;

use dioxus::prelude::*;

use crate::pipeline_view::derive_pipeline_view;
use crate::state::AppState;
use crate::sync::EditSource;

use super::tokenizer::tokenize;

/// Full-height YAML sidebar with syntax-highlighted overlay and editable textarea.
///
/// Architecture: a transparent `<textarea>` sits on top of a syntax-coloured
/// `<pre>` that shows the same text with tokenized `<span>` elements. The
/// textarea captures keystrokes; the pre shows the colours. Both scroll
/// together via a shared container.
///
/// Selection sync: when a stage is selected, its YAML lines receive a tinted
/// background and accent left border (via `data-selected` attribute).
///
/// Doc: spec §6 — YAML Editor (Sidebar).
#[component]
pub fn YamlSidebar() -> Element {
    let state = use_context::<AppState>();
    let text = (state.yaml_text)();
    let errors = (state.parse_errors)();
    let lines = tokenize(&text);
    let line_count = lines.len().max(1);

    // Compute selected stage's YAML line range for highlighting.
    let selected_range: Option<(usize, usize)> = {
        let selected = (state.selected_stage)();
        let pipeline_guard = (state.pipeline).read();
        match (selected.as_deref(), pipeline_guard.as_ref()) {
            (Some(stage_id), Some(config)) => {
                let ranges = crate::sync::compute_yaml_ranges(&text, config);
                ranges.get(stage_id).copied()
            }
            _ => None,
        }
    };

    rsx! {
        div {
            class: "kiln-yaml-sidebar",

            // Section header
            div {
                class: "kiln-section-header",
                span { class: "kiln-diamond", "\u{25C6}" }
                span { class: "kiln-section-title", "PIPELINE YAML" }
                span { class: "kiln-section-rule" }
            }

            // Code area: gutter + editor container
            div {
                class: "kiln-yaml-code-area",

                // Line-number gutter
                div {
                    class: "kiln-yaml-gutter",
                    for i in 0..line_count {
                        {
                            let line_num = i + 1;
                            let in_range = selected_range
                                .is_some_and(|(s, e)| line_num >= s && line_num <= e);
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

                // Editor container: syntax overlay + textarea stacked
                div {
                    class: "kiln-yaml-editor",

                    // Syntax-highlighted overlay (read-only visual layer)
                    div {
                        class: "kiln-yaml-highlight",
                        for (i, line_tokens) in lines.iter().enumerate() {
                            {
                                let line_num = i + 1;
                                let in_range = selected_range
                                    .is_some_and(|(s, e)| line_num >= s && line_num <= e);
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

                    // Transparent textarea (captures input)
                    textarea {
                        class: "kiln-yaml-textarea",
                        spellcheck: "false",
                        value: "{text}",
                        oninput: move |e: FormEvent| {
                            let mut src = state.edit_source;
                            src.set(EditSource::Yaml);
                            let mut yaml = state.yaml_text;
                            yaml.set(e.value());
                        },
                    }
                }
            }

            // Parse error bar
            if !errors.is_empty() {
                div {
                    class: "kiln-yaml-errors",
                    for (i, err) in errors.iter().enumerate() {
                        div {
                            key: "err-{i}",
                            class: "kiln-yaml-error",
                            "{err}"
                        }
                    }
                }
            }
        }
    }
}
