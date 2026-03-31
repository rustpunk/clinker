/// Placeholder page for contexts not yet implemented.
///
/// Styled "Coming soon" card in the rustpunk aesthetic.
/// Used for Channels and Runs contexts.
use dioxus::prelude::*;

#[component]
pub fn PlaceholderPage(name: &'static str, description: &'static str) -> Element {
    rsx! {
        div {
            class: "kiln-placeholder-page",

            div {
                class: "kiln-placeholder-card",

                div {
                    class: "kiln-placeholder-icon",
                    "◈"
                }

                h2 {
                    class: "kiln-placeholder-title",
                    "{name}"
                }

                p {
                    class: "kiln-placeholder-desc",
                    "{description}"
                }

                div {
                    class: "kiln-placeholder-status",
                    "Coming soon"
                }
            }
        }
    }
}
