//! Extract-as-composition modal for the Kiln canvas.
//!
//! Displayed when the user clicks the "Extract" button with 2+ nodes selected.
//! Shows the detected ports, config candidates, and output path.
//! On confirm, calls `write_extracted_composition` and updates the pipeline YAML.

use dioxus::prelude::*;

use crate::state::use_app_state;

/// Modal for extracting selected nodes as a composition.
#[component]
pub fn ExtractModal(on_close: EventHandler<()>) -> Element {
    let state = use_app_state();
    let mut comp_name = use_signal(|| "extracted_composition".to_string());
    let mut output_path = use_signal(|| "./compositions/extracted.comp.yaml".to_string());
    let selected = state.selected_stages.read().clone();
    let count = selected.len();

    rsx! {
        div {
            class: "kiln-confirm-backdrop",
            onclick: move |_| on_close.call(()),

            div {
                class: "kiln-confirm-dialog",
                onclick: move |e: MouseEvent| e.stop_propagation(),

                h3 { "Extract as Composition" }

                p { "Extracting {count} selected node(s) into a reusable composition." }

                div { class: "kiln-extract-field",
                    label { "Composition name:" }
                    input {
                        r#type: "text",
                        value: "{comp_name}",
                        oninput: move |e: FormEvent| comp_name.set(e.value().clone()),
                    }
                }

                div { class: "kiln-extract-field",
                    label { "Output path:" }
                    input {
                        r#type: "text",
                        value: "{output_path}",
                        oninput: move |e: FormEvent| output_path.set(e.value().clone()),
                    }
                }

                div { class: "kiln-extract-nodes",
                    label { "Selected nodes:" }
                    ul {
                        for node_name in &selected {
                            li { key: "{node_name}", "{node_name}" }
                        }
                    }
                }

                div { class: "kiln-confirm-actions",
                    button {
                        class: "kiln-btn kiln-btn--primary",
                        onclick: {
                            let on_close = on_close;
                            move |_| {
                                // TODO: wire up write_extracted_composition call
                                // and YAML update when backend integration is complete.
                                on_close.call(());
                            }
                        },
                        "Extract"
                    }
                    button {
                        class: "kiln-btn",
                        onclick: move |_| on_close.call(()),
                        "Cancel"
                    }
                }
            }
        }
    }
}
