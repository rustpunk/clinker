/// Toast notification overlay.
///
/// Spec §F2.2: anchored bottom-right, 16px from edges, auto-dismiss.
/// Verdigris accent for success, oxide-red for errors.

use dioxus::prelude::*;

/// Toast overlay — renders the current toast message if any.
#[component]
pub fn ToastOverlay() -> Element {
    let mut toast: Signal<Option<(String, &'static str)>> = use_context();
    let current = (toast)();

    if let Some((message, accent)) = current {
        rsx! {
            div {
                class: "kiln-toast",
                style: "border-left-color: {accent};",
                onclick: move |_| {
                    toast.set(None);
                },
                "{message}"
            }
        }
    } else {
        rsx! {}
    }
}
