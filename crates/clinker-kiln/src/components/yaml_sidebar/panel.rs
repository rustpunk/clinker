use dioxus::prelude::*;

use crate::state::AppState;
use crate::sync::EditSource;

/// Full-height YAML sidebar with editable textarea.
///
/// Phase 2b: replaces the static tokenized display with a live-editable
/// `<textarea>`. On every keystroke, the YAML text signal is updated and
/// the sync engine parses it to PipelineConfig.
///
/// Doc: spec §6 — YAML Editor (Sidebar).
#[component]
pub fn YamlSidebar() -> Element {
    let state = use_context::<AppState>();
    let text = (state.yaml_text)();
    let errors = (state.parse_errors)();
    let line_count = text.lines().count().max(1);

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

            // Editable code area
            div {
                class: "kiln-yaml-code-area",

                // Line-number gutter
                div {
                    class: "kiln-yaml-gutter",
                    for i in 0..line_count {
                        div {
                            key: "gutter-{i}",
                            class: "kiln-yaml-line-num",
                            "{i + 1}"
                        }
                    }
                }

                // Editable textarea
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
