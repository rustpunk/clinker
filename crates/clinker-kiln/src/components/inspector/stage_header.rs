use dioxus::prelude::*;

use crate::demo::DemoStage;
use crate::state::AppState;

/// Inspector header row: accent border-top + type badge + stage label + close button.
///
/// Doc: spec §5.1 — Inspector panel header.
#[component]
pub fn StageHeader(stage: DemoStage) -> Element {
    let state = use_context::<AppState>();
    let accent = stage.step_type.accent_color();
    let badge = stage.step_type.badge_label();

    rsx! {
        div {
            class: "kiln-inspector-header",
            style: "border-top: 3px solid {accent};",

            // Type badge
            span {
                class: "kiln-inspector-badge",
                style: "color: {accent}; border-color: {accent};",
                "{badge}"
            }

            // Stage label
            span {
                class: "kiln-inspector-label",
                "{stage.label}"
            }

            // Flex spacer
            span { style: "flex: 1;" }

            // Close button
            button {
                class: "kiln-inspector-close",
                onclick: move |_| {
                    let mut sel = state.selected_stage;
                    sel.set(None);
                },
                "\u{00D7}" // × character
            }
        }
    }
}
