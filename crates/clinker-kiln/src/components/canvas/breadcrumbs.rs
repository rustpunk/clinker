use dioxus::prelude::*;

use crate::state::use_app_state;

/// Breadcrumb navigation bar for composition drill-in.
///
/// Shows `pipeline > alias1 > alias2` from the drill stack.
/// Clicking a segment pops the stack to that level.
/// Clicking "pipeline" returns to the top-level view.
#[component]
pub fn BreadcrumbBar(frames: Vec<String>) -> Element {
    let state = use_app_state();

    rsx! {
        div {
            class: "kiln-breadcrumb-bar",

            // Root breadcrumb — clicking returns to top-level
            span {
                class: "kiln-breadcrumb kiln-breadcrumb--clickable",
                onclick: move |_| {
                    let mut stack = state.composition_drill_stack;
                    stack.write().clear();
                },
                "pipeline"
            }

            for (depth, alias) in frames.iter().enumerate() {
                span { class: "kiln-breadcrumb-sep", " > " }
                {
                    let is_last = depth == frames.len() - 1;
                    let target_depth = depth + 1;
                    rsx! {
                        span {
                            class: if is_last {
                                "kiln-breadcrumb kiln-breadcrumb--current"
                            } else {
                                "kiln-breadcrumb kiln-breadcrumb--clickable"
                            },
                            onclick: move |_| {
                                if !is_last {
                                    let mut stack = state.composition_drill_stack;
                                    stack.write().truncate(target_depth);
                                }
                            },
                            "{alias}"
                        }
                    }
                }
            }
        }
    }
}
