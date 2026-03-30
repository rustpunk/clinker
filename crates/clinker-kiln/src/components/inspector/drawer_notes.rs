use dioxus::prelude::*;

use crate::notes::{parse_notes, serialize_notes, StageNotes};
use crate::state::AppState;
use crate::sync::EditSource;

/// Notes drawer — editable stage-level note + field-level annotations.
///
/// Edits mutate the PipelineConfig's `_notes` field and trigger the
/// Inspector→YAML sync path (EditSource::Inspector → serialize → YAML).
///
/// Spec §A5A.2–A5A.4.
#[component]
pub fn DrawerNotes(stage_id: String) -> Element {
    let state = use_context::<AppState>();
    let mut editing_stage_note = use_signal(|| false);

    let pipeline_guard = (state.pipeline).read();
    let Some(config) = pipeline_guard.as_ref() else {
        return rsx! { DrawerNotesEmpty {} };
    };

    // Find the _notes value for this stage
    let notes_value = config
        .inputs.iter().find(|i| i.name == stage_id).and_then(|i| i.notes.as_ref())
        .or_else(|| config.transformations.iter().find(|t| t.name == stage_id).and_then(|t| t.notes.as_ref()))
        .or_else(|| config.outputs.iter().find(|o| o.name == stage_id).and_then(|o| o.notes.as_ref()));

    let notes = parse_notes(notes_value);
    let stage_note_text = notes.stage_note.clone();
    let annotations: Vec<(String, String)> = notes.field_annotations.iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Drop the borrow before creating closures that write to pipeline
    drop(pipeline_guard);

    let stage_id_for_save = stage_id.clone();

    // Helper: write updated notes back to the pipeline config
    let save_notes = move |updated: StageNotes| {
        let serialized = serialize_notes(&updated);
        let mut pipeline_sig = state.pipeline;
        let mut edit_src = state.edit_source;

        if let Some(ref mut config) = *pipeline_sig.write() {
            // Find and update the _notes field on the matching stage
            let notes_field = config.inputs.iter_mut()
                .find(|i| i.name == stage_id_for_save)
                .map(|i| &mut i.notes)
                .or_else(|| config.transformations.iter_mut()
                    .find(|t| t.name == stage_id_for_save)
                    .map(|t| &mut t.notes))
                .or_else(|| config.outputs.iter_mut()
                    .find(|o| o.name == stage_id_for_save)
                    .map(|o| &mut o.notes));

            if let Some(field) = notes_field {
                *field = serialized;
            }
        }
        edit_src.set(EditSource::Inspector);
    };

    rsx! {
        div {
            class: "kiln-drawer-content kiln-drawer-content--notes",

            // ── Stage note ────────────────────────────────────────────────
            div {
                class: "kiln-notes-section",

                div {
                    class: "kiln-notes-section-header",
                    span { class: "kiln-notes-section-label", "STAGE NOTE" }
                    button {
                        class: "kiln-notes-edit-btn",
                        onclick: move |_| {
                            let current = *editing_stage_note.peek();
                            editing_stage_note.set(!current);
                        },
                        if (editing_stage_note)() { "\u{270E} done" } else { "\u{270E} edit" }
                    }
                }

                if (editing_stage_note)() {
                    // Edit mode — textarea
                    {
                        let save = save_notes.clone();
                        let current_annotations = annotations.clone();
                        rsx! {
                            textarea {
                                class: "kiln-notes-textarea",
                                value: "{stage_note_text}",
                                oninput: move |e: FormEvent| {
                                    let mut updated = StageNotes {
                                        stage_note: e.value(),
                                        field_annotations: current_annotations.iter()
                                            .cloned().collect(),
                                    };
                                    save(updated);
                                },
                            }
                        }
                    }
                } else {
                    // Display mode
                    if stage_note_text.is_empty() {
                        div {
                            class: "kiln-notes-empty",
                            "No stage note \u{2014} click \u{270E} edit to add context"
                        }
                    } else {
                        div {
                            class: "kiln-notes-block",
                            "{stage_note_text}"
                        }
                    }
                }
            }

            // ── Field annotations ─────────────────────────────────────────
            div {
                class: "kiln-notes-section",

                div {
                    class: "kiln-notes-section-header",
                    span { class: "kiln-notes-section-label", "FIELD ANNOTATIONS" }
                    {
                        let count = annotations.len();
                        let suffix = if count != 1 { "s" } else { "" };
                        rsx! {
                            span {
                                class: "kiln-notes-count",
                                "{count} annotation{suffix}"
                            }
                        }
                    }
                }

                for (key, text) in annotations.iter() {
                    {
                        let save = save_notes.clone();
                        let key = key.clone();
                        let all_annotations = annotations.clone();
                        let current_stage_note = stage_note_text.clone();
                        rsx! {
                            div {
                                key: "annot-{key}",
                                class: "kiln-notes-annotation",

                                div {
                                    class: "kiln-notes-field-key",
                                    "\u{270E} {key}"
                                }

                                // Editable inline input for the annotation text
                                input {
                                    class: "kiln-notes-field-input",
                                    r#type: "text",
                                    value: "{text}",
                                    oninput: {
                                        let key = key.clone();
                                        move |e: FormEvent| {
                                            let mut updated_map: std::collections::HashMap<String, String> =
                                                all_annotations.iter().cloned().collect();
                                            updated_map.insert(key.clone(), e.value());
                                            let updated = StageNotes {
                                                stage_note: current_stage_note.clone(),
                                                field_annotations: updated_map,
                                            };
                                            save(updated);
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Empty state when no notes exist and pipeline isn't loaded.
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
