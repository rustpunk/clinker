use dioxus::prelude::*;

use crate::demo::{DemoStage, NODE_HEIGHT, NODE_WIDTH};

/// A single pipeline stage rendered as a rustpunk node card on the canvas.
///
/// Positioned absolutely via inline `left`/`top` style. No drag/pan/zoom in
/// Phase 1 — positions are taken directly from `DemoStage.canvas_x/y`.
///
/// Doc: spec §4.2 — Node Rendering.
#[component]
pub fn CanvasNode(stage: DemoStage) -> Element {
    let accent = stage.step_type.accent_color();
    let badge = stage.step_type.badge_label();
    let pass_label = stage.pass.label();

    // Port centre must align with the connector endpoint at canvas_y + NODE_HEIGHT/2.
    //
    // `top` on an absolutely-positioned child is measured from the padding box
    // (i.e. INSIDE the border). The node card has `border-top: 3px`, so the
    // padding box starts 3 px below `canvas_y`. To land the port centre at
    // `canvas_y + NODE_HEIGHT/2`, we solve:
    //
    //   canvas_y + 3 (border-top) + port_y + 4 (port_half) = canvas_y + NODE_HEIGHT/2
    //   port_y = NODE_HEIGHT/2 - 4 - 3
    //
    const BORDER_TOP: f32 = 3.0;
    const PORT_HALF: f32 = 4.0; // half of 8px port square
    let port_y = NODE_HEIGHT / 2.0 - PORT_HALF - BORDER_TOP;

    rsx! {
        // Node card — positioned absolutely within the canvas panel.
        div {
            key: "{stage.id}",
            class: "kiln-node",
            style: "left: {stage.canvas_x}px; top: {stage.canvas_y}px; \
                    width: {NODE_WIDTH}px; \
                    border-top-color: {accent};",

            // Pass + type badge row
            div {
                class: "kiln-node-badge",
                style: "color: {accent};",
                // Pass pill
                span {
                    class: "kiln-node-pass-pill",
                    "{pass_label}"
                }
                // Type badge
                span {
                    class: "kiln-node-type-badge",
                    "{badge}"
                }
            }

            // Primary label (Chakra Petch)
            div {
                class: "kiln-node-label",
                "{stage.label}"
            }

            // Rust-line separator
            hr { class: "kiln-rust-line" }

            // Subtitle (JetBrains Mono, iron colour)
            div {
                class: "kiln-node-subtitle",
                "{stage.subtitle}"
            }

            // Left (input) port square
            div {
                class: "kiln-node-port kiln-node-port--in",
                style: "top: {port_y}px;",
            }

            // Right (output) port square
            div {
                class: "kiln-node-port kiln-node-port--out",
                style: "top: {port_y}px;",
            }
        }
    }
}
