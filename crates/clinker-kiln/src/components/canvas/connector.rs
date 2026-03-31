use dioxus::prelude::*;

use crate::pipeline_view::StageView;

/// Three-layer SVG connector between two adjacent pipeline stages.
///
/// Renders a single `<g>` element containing three `<path>` elements:
/// 1. Glow layer   — wide stroke at 10% opacity for a soft halo effect.
/// 2. Core cable   — dashed stroke at 70% opacity (8px dash, 4px gap).
/// 3. Bright centre — hairline solid stroke at 90% opacity (hot-wire effect).
///
///    Plus an open chevron arrowhead at the target port.
///
/// Doc: spec §4.4 — Connectors.
#[derive(Props, Clone, PartialEq)]
pub struct ConnectorProps {
    pub from: StageView,
    pub to: StageView,
}

#[component]
pub fn Connector(props: ConnectorProps) -> Element {
    let (sx, sy) = props.from.port_out();
    let (tx, ty) = props.to.port_in();

    // Cubic S-curve: control points at 1/3 of horizontal distance from each end.
    let cp_offset = (tx - sx).abs() / 3.0;
    let path = format!(
        "M {sx:.1},{sy:.1} C {:.1},{sy:.1} {:.1},{ty:.1} {tx:.1},{ty:.1}",
        sx + cp_offset,
        tx - cp_offset,
    );

    // Open chevron arrowhead pointing right, positioned at target port.
    let arrow = format!(
        "M {:.1},{:.1} L {tx:.1},{ty:.1} L {:.1},{:.1}",
        tx - 8.0,
        ty - 5.0,
        tx - 8.0,
        ty + 5.0,
    );

    let kind_attr = props.from.kind.kind_attr();

    rsx! {
        g {
            "data-stage-kind": kind_attr,
            // Layer 1 — glow
            path {
                d: "{path}",
                fill: "none",
                stroke_width: "5",
                stroke_opacity: "0.1",
                style: "stroke: var(--kiln-stage-accent);",
            }
            // Layer 2 — dashed core cable
            path {
                d: "{path}",
                fill: "none",
                stroke_width: "2",
                stroke_dasharray: "8 4",
                stroke_opacity: "0.7",
                style: "stroke: var(--kiln-stage-accent);",
            }
            // Layer 3 — bright centre hairline
            path {
                d: "{path}",
                fill: "none",
                stroke_width: "0.75",
                stroke_opacity: "0.9",
                style: "stroke: var(--kiln-stage-accent);",
            }
            // Open chevron arrowhead
            path {
                d: "{arrow}",
                fill: "none",
                stroke_width: "1.5",
                stroke_opacity: "0.8",
                stroke_linejoin: "round",
                stroke_linecap: "round",
                style: "stroke: var(--kiln-stage-accent);",
            }
        }
    }
}
