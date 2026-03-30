/// Modal confirmation dialog for unsaved changes.
///
/// Spec §F2.5: centered modal with backdrop blur, hazard accent,
/// Save / Don't Save / Cancel buttons.

use dioxus::prelude::*;

/// What action the user chose in the confirmation dialog.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ConfirmAction {
    Save,
    Discard,
    Cancel,
}

/// Pending confirmation state: which tab is being closed and its filename.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingConfirm {
    pub tab_id: crate::tab::TabId,
    pub filename: String,
}

/// Unsaved-changes confirmation dialog.
///
/// Rendered as a modal overlay. The parent provides a signal with the
/// pending confirm state and handles the result.
#[component]
pub fn ConfirmDialog(
    pending: PendingConfirm,
    on_action: EventHandler<ConfirmAction>,
) -> Element {
    rsx! {
        // Backdrop
        div {
            class: "kiln-confirm-backdrop",
            onclick: move |_| on_action.call(ConfirmAction::Cancel),

            // Dialog (stop propagation so clicking inside doesn't cancel)
            div {
                class: "kiln-confirm-dialog",
                onclick: move |e: MouseEvent| e.stop_propagation(),

                // Title
                div {
                    class: "kiln-confirm-title",
                    span { class: "kiln-confirm-diamond", "\u{25C6}" }
                    " UNSAVED CHANGES"
                }

                // Body
                div {
                    class: "kiln-confirm-body",
                    span { class: "kiln-confirm-filename", "{pending.filename}" }
                    " has unsaved changes."
                }

                // Buttons
                div {
                    class: "kiln-confirm-buttons",

                    button {
                        class: "kiln-confirm-btn kiln-confirm-btn--cancel",
                        onclick: move |_| on_action.call(ConfirmAction::Cancel),
                        "Cancel"
                    }
                    button {
                        class: "kiln-confirm-btn kiln-confirm-btn--discard",
                        onclick: move |_| on_action.call(ConfirmAction::Discard),
                        "Don\u{2019}t Save"
                    }
                    button {
                        class: "kiln-confirm-btn kiln-confirm-btn--save",
                        onclick: move |_| on_action.call(ConfirmAction::Save),
                        "Save"
                    }
                }
            }
        }
    }
}
