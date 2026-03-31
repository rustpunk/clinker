use dioxus::prelude::*;

/// Run drawer stub — shows "no run data" placeholder.
/// Phosphor sub-aesthetic accent (#D4A017).
/// Spec §A3.5: no-run state.
#[component]
pub fn DrawerRun() -> Element {
    rsx! {
        div {
            class: "kiln-drawer-content kiln-drawer-content--run",
            div {
                class: "kiln-drawer-placeholder",
                "No run data \u{2014} execute the pipeline to see results"
            }
        }
    }
}
