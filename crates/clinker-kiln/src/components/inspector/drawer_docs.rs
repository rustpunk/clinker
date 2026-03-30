use dioxus::prelude::*;

/// Docs drawer stub — Blueprint sub-aesthetic placeholder.
/// Verdigris/bpAccent accent (#43B3AE).
/// Spec §A5: auto-generated documentation with Blueprint gridline overlay.
#[component]
pub fn DrawerDocs() -> Element {
    rsx! {
        div {
            class: "kiln-drawer-content kiln-drawer-content--docs",
            div {
                class: "kiln-drawer-placeholder",
                "Stage documentation will appear here"
            }
        }
    }
}
