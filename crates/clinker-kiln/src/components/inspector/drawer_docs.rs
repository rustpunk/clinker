use dioxus::prelude::*;

use crate::autodoc::generate_stage_doc;
use crate::notes::parse_notes;
use crate::state::AppState;

/// Docs drawer — auto-generated stage documentation with Blueprint sub-aesthetic.
///
/// Content (spec §A5.2):
/// 1. Auto-generated description (from templates, §A8.2)
/// 2. User-authored stage note (if present, from _notes)
/// 3. Stage metadata grid (type, pass, expressions, etc.)
/// 4. Column impact (columns added/removed)
/// 5. Footer: "AUTODOC"
#[component]
pub fn DrawerDocs(stage_id: String) -> Element {
    let state = use_context::<AppState>();

    let pipeline_guard = (state.pipeline).read();
    let Some(config) = pipeline_guard.as_ref() else {
        return rsx! {
            div {
                class: "kiln-drawer-content kiln-drawer-content--docs",
                div { class: "kiln-drawer-placeholder", "No pipeline loaded" }
            }
        };
    };

    let Some(doc) = generate_stage_doc(config, &stage_id) else {
        return rsx! {
            div {
                class: "kiln-drawer-content kiln-drawer-content--docs",
                div { class: "kiln-drawer-placeholder", "No documentation for this stage" }
            }
        };
    };

    // Get the stage note from _notes (if any)
    let notes_value = config
        .inputs.iter().find(|i| i.name == stage_id).and_then(|i| i.notes.as_ref())
        .or_else(|| config.transformations.iter().find(|t| t.name == stage_id).and_then(|t| t.notes.as_ref()))
        .or_else(|| config.outputs.iter().find(|o| o.name == stage_id).and_then(|o| o.notes.as_ref()));
    let notes = parse_notes(notes_value);

    rsx! {
        div {
            class: "kiln-drawer-content kiln-drawer-content--docs",

            // ── Auto-generated description ────────────────────────────────
            div {
                class: "kiln-docs-description",
                "{doc.description}"
            }

            // ── User-authored stage note (when present) ───────────────────
            if !notes.stage_note.is_empty() {
                div {
                    class: "kiln-docs-note-section",
                    span { class: "kiln-docs-note-label", "NOTE" }
                    div {
                        class: "kiln-docs-note-block",
                        "{notes.stage_note}"
                    }
                }
            }

            // ── Stage metadata grid ───────────────────────────────────────
            div {
                class: "kiln-docs-metadata",

                for (key, value) in doc.metadata.iter() {
                    div {
                        key: "meta-{key}",
                        class: "kiln-docs-meta-row",
                        span { class: "kiln-docs-meta-key", "{key}" }
                        span { class: "kiln-docs-meta-value", "{value}" }
                    }
                }
            }

            // ── Column impact ─────────────────────────────────────────────
            if !doc.columns_added.is_empty() {
                div {
                    class: "kiln-docs-columns",

                    span { class: "kiln-docs-columns-label", "ADDED" }
                    div {
                        class: "kiln-docs-column-tags",
                        for col in doc.columns_added.iter() {
                            span {
                                key: "col-{col}",
                                class: "kiln-docs-column-tag kiln-docs-column-tag--added",
                                "+{col}"
                            }
                        }
                    }
                }
            }

            // ── Footer ────────────────────────────────────────────────────
            div {
                class: "kiln-docs-footer",
                span { class: "kiln-docs-footer-rule" }
                span { class: "kiln-docs-footer-label", "AUTODOC" }
                span { class: "kiln-docs-footer-rule" }
            }
        }
    }
}
