use dioxus::prelude::*;

use crate::demo::demo_pipeline;

use super::connector::Connector;
use super::node::CanvasNode;

/// The infinite-canvas panel rendering the pipeline node graph.
///
/// Phase 1: static layout only — nodes are positioned at hardcoded world-space
/// coordinates from `demo_pipeline()`. Pan / zoom / drag are added in Phase 3.
///
/// Visual layers (back to front):
///   1. Dot grid background (CSS radial-gradient pattern)
///   2. Noise + scanline overlays (CSS ::before / ::after)
///   3. SVG connector overlay (absolute, inset: 0)
///   4. Node cards (absolute, positioned per DemoStage.canvas_x/y)
///
/// Doc: spec §4 — Canvas System.
#[component]
pub fn CanvasPanel() -> Element {
    let stages = demo_pipeline();

    // Pairs of (source, target) for connector rendering.
    let connections: Vec<_> = stages.windows(2).map(|w| (w[0].clone(), w[1].clone())).collect();

    // SVG overlay bounds — large enough to cover the static demo layout.
    // In Phase 3 this will be driven by the viewport transform.
    let svg_w = 1200.0_f32;
    let svg_h = 400.0_f32;

    rsx! {
        div {
            class: "kiln-canvas-panel",

            // SVG connector overlay — sits behind the node cards via z-index.
            svg {
                class: "kiln-canvas-svg",
                width: "{svg_w}",
                height: "{svg_h}",
                for (from, to) in connections {
                    Connector {
                        key: "{from.id}-{to.id}",
                        from,
                        to,
                    }
                }
            }

            // Node cards
            for stage in stages {
                CanvasNode {
                    key: "{stage.id}",
                    stage,
                }
            }
        }
    }
}
