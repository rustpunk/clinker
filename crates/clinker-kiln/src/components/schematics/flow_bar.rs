use dioxus::prelude::*;

use crate::pipeline_view::StageView;

/// Compact horizontal pipeline summary strip.
///
/// Each stage is a small pill showing label + type badge, connected by
/// dashed verdigris SVG connectors. Scrolls horizontally if the pipeline
/// is wider than the viewport.
///
/// Spec §A7.4: Flow Bar.
#[component]
pub fn FlowBar(stages: Vec<StageView>) -> Element {
    rsx! {
        div {
            class: "kiln-flow-bar",

            for (i, stage) in stages.iter().enumerate() {
                // Connector arrow before each stage (except the first)
                if i > 0 {
                    svg {
                        class: "kiln-flow-connector",
                        width: "24",
                        height: "16",
                        view_box: "0 0 24 16",
                        // Dashed line
                        line {
                            x1: "0", y1: "8", x2: "18", y2: "8",
                            stroke: "var(--kiln-verdigris)",
                            stroke_width: "1.5",
                            stroke_dasharray: "4 3",
                            stroke_opacity: "0.5",
                        }
                        // Chevron
                        polyline {
                            points: "16,4 22,8 16,12",
                            fill: "none",
                            stroke: "var(--kiln-verdigris)",
                            stroke_width: "1.5",
                            stroke_opacity: "0.7",
                            stroke_linejoin: "round",
                            stroke_linecap: "round",
                        }
                    }
                }

                // Stage pill
                {
                    let kind_attr = stage.kind.kind_attr();
                    let badge = stage.kind.badge_label();
                    rsx! {
                        div {
                            key: "flow-{stage.id}",
                            class: "kiln-flow-pill",
                            "data-stage-kind": kind_attr,
                            style: "border-top-color: var(--kiln-stage-accent); \
                                    background: color-mix(in srgb, var(--kiln-stage-accent) 8%, var(--kiln-char-surface)); \
                                    border-color: color-mix(in srgb, var(--kiln-stage-accent) 20%, var(--kiln-border-subtle));",

                            span {
                                class: "kiln-flow-pill-label",
                                "{stage.label}"
                            }
                            span {
                                class: "kiln-flow-pill-badge",
                                style: "color: var(--kiln-stage-accent);",
                                "{badge}"
                            }
                        }
                    }
                }
            }
        }
    }
}
