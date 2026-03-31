//! Breadcrumb bar + scope indicator for composition drill-in navigation.

use dioxus::prelude::*;

use crate::state::{DrillInEntry, use_app_state};

/// Breadcrumb bar shown above the canvas during drill-in.
///
/// Displays: `pipeline_name › Composition Name`
/// Click a segment to navigate back. Backspace/Esc handled by keyboard.rs.
#[component]
pub fn BreadcrumbBar(drill_stack: Vec<DrillInEntry>) -> Element {
    let state = use_app_state();

    // Pipeline name from the parsed config
    let pipeline_name = (state.pipeline)()
        .as_ref()
        .map(|c| c.pipeline.name.clone())
        .unwrap_or_else(|| "pipeline".to_string());

    rsx! {
        // Breadcrumb bar
        div { class: "kiln-breadcrumb-bar",
            // Root segment — click to exit all drill-ins
            span {
                class: "kiln-breadcrumb__segment kiln-breadcrumb__segment--link",
                onclick: move |_| {
                    let mut sig = state.composition_drill_stack;
                    sig.write().clear();
                },
                "{pipeline_name}"
            }

            // Drill stack segments
            for (i, entry) in drill_stack.iter().enumerate() {
                {
                    let is_last = i == drill_stack.len() - 1;
                    let truncate_to = i + 1;

                    rsx! {
                        span { class: "kiln-breadcrumb__sep", "\u{203A}" }
                        span { class: "kiln-breadcrumb__comp-icon", "\u{25C8}" }
                        if is_last {
                            span {
                                class: "kiln-breadcrumb__segment kiln-breadcrumb__segment--active",
                                "{entry.name}"
                            }
                        } else {
                            span {
                                class: "kiln-breadcrumb__segment kiln-breadcrumb__segment--link",
                                onclick: move |_| {
                                    let mut sig = state.composition_drill_stack;
                                    sig.write().truncate(truncate_to);
                                },
                                "{entry.name}"
                            }
                        }
                    }
                }
            }

            div { class: "kiln-breadcrumb__spacer" }

            span { class: "kiln-breadcrumb__hint",
                "Backspace to go up \u{00b7} Esc to pipeline root"
            }
        }

        // Scope indicator — verdigris gradient bar
        div { class: "kiln-scope-indicator" }
    }
}
