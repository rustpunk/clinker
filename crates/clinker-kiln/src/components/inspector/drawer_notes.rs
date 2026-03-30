use dioxus::prelude::*;

use crate::notes::parse_notes;
use crate::state::AppState;

/// Notes drawer — stage-level markdown note + field-level annotations.
///
/// Reads `_notes` from the selected stage's config. Editing is display-only
/// in this initial implementation; a future phase adds inline editing that
/// writes back via the sync engine.
///
/// Spec §A5A.2–A5A.4.
#[component]
pub fn DrawerNotes(stage_id: String) -> Element {
    let state = use_context::<AppState>();

    let pipeline_guard = (state.pipeline).read();
    let Some(config) = pipeline_guard.as_ref() else {
        return rsx! { DrawerNotesEmpty {} };
    };

    // Find the _notes value for this stage
    let notes_value = config
        .inputs
        .iter()
        .find(|i| i.name == stage_id)
        .and_then(|i| i.notes.as_ref())
        .or_else(|| {
            config
                .transformations
                .iter()
                .find(|t| t.name == stage_id)
                .and_then(|t| t.notes.as_ref())
        })
        .or_else(|| {
            config
                .outputs
                .iter()
                .find(|o| o.name == stage_id)
                .and_then(|o| o.notes.as_ref())
        });

    let notes = parse_notes(notes_value);

    if notes.stage_note.is_empty() && notes.field_annotations.is_empty() {
        return rsx! { DrawerNotesEmpty {} };
    }

    let annotation_count = notes.field_annotations.len()
        + if notes.stage_note.is_empty() { 0 } else { 1 };

    rsx! {
        div {
            class: "kiln-drawer-content kiln-drawer-content--notes",

            // Stage note
            if !notes.stage_note.is_empty() {
                div {
                    class: "kiln-notes-section",

                    div {
                        class: "kiln-notes-section-header",
                        span { class: "kiln-notes-section-label", "STAGE NOTE" }
                    }

                    div {
                        class: "kiln-notes-block",
                        "{notes.stage_note}"
                    }
                }
            }

            // Field annotations
            if !notes.field_annotations.is_empty() {
                div {
                    class: "kiln-notes-section",

                    div {
                        class: "kiln-notes-section-header",
                        span { class: "kiln-notes-section-label", "FIELD ANNOTATIONS" }
                        {
                            let count = notes.field_annotations.len();
                            let suffix = if count != 1 { "s" } else { "" };
                            rsx! {
                                span {
                                    class: "kiln-notes-count",
                                    "{count} annotation{suffix}"
                                }
                            }
                        }
                    }

                    for (key, text) in notes.field_annotations.iter() {
                        div {
                            key: "annot-{key}",
                            class: "kiln-notes-annotation",

                            div {
                                class: "kiln-notes-field-key",
                                "\u{270E} {key}"
                            }

                            div {
                                class: "kiln-notes-block",
                                "{text}"
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Empty state when no notes exist.
#[component]
fn DrawerNotesEmpty() -> Element {
    rsx! {
        div {
            class: "kiln-drawer-content kiln-drawer-content--notes",
            div {
                class: "kiln-drawer-placeholder",
                "No notes \u{2014} add _notes to the YAML to annotate this stage"
            }
        }
    }
}
