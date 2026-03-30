use dioxus::prelude::*;

use crate::state::AppState;

/// Inspector header row: accent border-top + type badge + stage label + close button.
#[component]
pub fn StageHeader(
    stage_id: String,
    kind_label: &'static str,
    accent: &'static str,
    label: String,
) -> Element {
    let state = use_context::<AppState>();

    rsx! {
        div {
            class: "kiln-inspector-header",
            style: "border-top: 3px solid {accent};",

            span {
                class: "kiln-inspector-badge",
                style: "color: {accent}; border-color: {accent};",
                "{kind_label}"
            }

            span { class: "kiln-inspector-label", "{label}" }

            span { style: "flex: 1;" }

            button {
                class: "kiln-inspector-close",
                onclick: move |_| {
                    let mut sel = state.selected_stage;
                    sel.set(None);
                },
                "\u{00D7}"
            }
        }
    }
}
