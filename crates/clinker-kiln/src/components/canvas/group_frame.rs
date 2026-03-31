//! Composition group frame — dashed verdigris boundary around inline-expanded
//! composition stages on the canvas.

use dioxus::prelude::*;

use crate::pipeline_view::{CompositionGroup, NODE_HEIGHT};
use crate::state::use_app_state;

/// Padding around the group boundary.
const GROUP_PAD_X: f32 = 16.0;
const GROUP_PAD_TOP: f32 = 32.0;
const GROUP_PAD_BOTTOM: f32 = 16.0;

/// Dashed frame rendered behind inline-expanded composition stages.
#[component]
pub fn CompositionGroupFrame(group: CompositionGroup) -> Element {
    let state = use_app_state();
    let path = group.path.clone();

    let version_str = group
        .version
        .as_deref()
        .map(|v| format!(" v{v}"))
        .unwrap_or_default();

    let override_str = if group.override_count > 0 {
        format!(
            " \u{00b7} {} override{}",
            group.override_count,
            if group.override_count > 1 { "s" } else { "" }
        )
    } else {
        String::new()
    };

    let frame_x = group.x - GROUP_PAD_X;
    let frame_y = group.y;
    let frame_w = group.width + GROUP_PAD_X * 2.0;
    let frame_h = NODE_HEIGHT + GROUP_PAD_TOP + GROUP_PAD_BOTTOM;

    rsx! {
        div {
            class: "kiln-composition-group",
            style: "left: {frame_x}px; top: {frame_y}px; width: {frame_w}px; height: {frame_h}px;",
            onmousedown: move |e: MouseEvent| e.stop_propagation(),

            // Header row
            div { class: "kiln-composition-group__header",
                span { class: "kiln-composition-group__icon", "\u{25C8}" }
                span { class: "kiln-composition-group__name", "{group.name}" }
                span { class: "kiln-composition-group__meta", "{version_str}{override_str}" }
                div { class: "kiln-composition-group__spacer" }
                span {
                    class: "kiln-composition-group__collapse",
                    onclick: move |e: MouseEvent| {
                        e.stop_propagation();
                        let mut sig = state.expanded_compositions;
                        sig.write().remove(&path);
                    },
                    "collapse \u{25B4}"
                }
            }
        }
    }
}
