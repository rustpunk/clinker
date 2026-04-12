use dioxus::prelude::*;

use crate::state::use_app_state;

/// Inspector header row: accent border-top + type badge + stage label + close button.
#[component]
pub fn StageHeader(
    stage_id: String,
    kind_label: &'static str,
    kind_attr: &'static str,
    label: String,
) -> Element {
    let state = use_app_state();

    rsx! {
        div {
            class: "kiln-inspector-header",
            "data-stage-kind": kind_attr,
            style: "border-top: 3px solid var(--kiln-stage-accent);",

            span {
                class: "kiln-inspector-badge",
                style: "color: var(--kiln-stage-accent); border-color: var(--kiln-stage-accent);",
                "{kind_label}"
            }

            span { class: "kiln-inspector-label", "{label}" }

            span { style: "flex: 1;" }

            button {
                class: "kiln-inspector-close",
                onclick: move |_| {
                    let mut sel = state.selected_stages;
                    sel.set(std::collections::HashSet::new());
                },
                "\u{00D7}"
            }
        }
    }
}
