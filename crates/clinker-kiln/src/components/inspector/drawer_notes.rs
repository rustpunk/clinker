use dioxus::prelude::*;

/// Notes drawer stub — iron accent placeholder.
/// Spec §A5A: stage-level and field-level annotations.
#[component]
pub fn DrawerNotes() -> Element {
    rsx! {
        div {
            class: "kiln-drawer-content kiln-drawer-content--notes",
            div {
                class: "kiln-drawer-placeholder",
                "Stage notes and annotations will appear here"
            }
        }
    }
}
