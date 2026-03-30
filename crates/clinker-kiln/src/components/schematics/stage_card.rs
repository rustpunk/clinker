use dioxus::prelude::*;

use crate::autodoc::StageDoc;
use crate::notes::StageNotes;

/// A single stage detail card in the Schematics content area.
///
/// Spec §A7.8: accent left border, index, label, type badge,
/// auto-description, user note, config metadata, column changes.
#[component]
pub fn StageCard(
    index: usize,
    stage_id: String,
    accent: &'static str,
    badge: &'static str,
    doc: StageDoc,
    notes: StageNotes,
) -> Element {
    let idx = format!("{:02}", index);

    rsx! {
        div {
            class: "kiln-stage-card",
            style: "border-left-color: {accent};",

            // Header: index + label + type badge
            div {
                class: "kiln-stage-card-header",
                span { class: "kiln-stage-card-index", "{idx}" }
                span { class: "kiln-stage-card-label", "{stage_id}" }
                span {
                    class: "kiln-stage-card-badge",
                    style: "color: {accent}; border-color: color-mix(in srgb, {accent} 25%, transparent); \
                            background: color-mix(in srgb, {accent} 12%, transparent);",
                    "{badge}"
                }
            }

            // Verdigris separator
            hr { class: "kiln-stage-card-rule" }

            // Auto-generated description
            div {
                class: "kiln-stage-card-description",
                "{doc.description}"
            }

            // User-authored stage note (if present)
            if !notes.stage_note.is_empty() {
                div {
                    class: "kiln-stage-card-note",
                    span { class: "kiln-stage-card-note-label", "NOTE" }
                    div {
                        class: "kiln-stage-card-note-block",
                        "{notes.stage_note}"
                    }
                }
            }

            // Metadata grid
            if !doc.metadata.is_empty() {
                div {
                    class: "kiln-stage-card-metadata",
                    for (key, value) in doc.metadata.iter() {
                        div {
                            key: "meta-{key}",
                            class: "kiln-stage-card-meta-row",
                            span { class: "kiln-stage-card-meta-key", "{key}" }
                            span { class: "kiln-stage-card-meta-value", "{value}" }
                        }
                    }
                }
            }

            // Column changes
            if !doc.columns_added.is_empty() {
                div {
                    class: "kiln-stage-card-columns",
                    span { class: "kiln-stage-card-columns-heading", "COLUMN CHANGES" }
                    div {
                        class: "kiln-stage-card-column-tags",
                        for col in doc.columns_added.iter() {
                            span {
                                key: "col-{col}",
                                class: "kiln-stage-card-column-tag",
                                "+{col}"
                            }
                        }
                    }
                }
            }
        }
    }
}
