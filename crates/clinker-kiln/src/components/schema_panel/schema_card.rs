//! Individual schema card in the schema panel.
//!
//! Shows: format diamond + schema name + field count, path, format + usage,
//! expandable field list. Click name to open `.schema.yaml` in editor tab.
//!
//! Spec §S3.6: card anatomy.

use dioxus::prelude::*;

use clinker_schema::SourceSchema;

use super::field_list::FieldList;

/// Schema card component — one per discovered schema.
#[component]
pub fn SchemaCard(schema: SourceSchema) -> Element {
    let mut expanded = use_signal(|| false);
    let is_expanded = (expanded)();

    let name = &schema.metadata.name;
    let format_label = schema.metadata.format.label();
    let field_count = schema.total_field_count();
    let path_display = schema.path.display().to_string();
    let pipeline_count = schema.referencing_pipelines.len();
    let accent_class = format!("kiln-schema-card--{format_label}");

    let description = schema
        .metadata
        .description
        .as_deref()
        .unwrap_or("")
        .to_string();

    rsx! {
        div {
            class: "kiln-schema-card {accent_class}",

            // ── Card header (clickable to expand) ───────────────────────
            div {
                class: "kiln-schema-card__header",
                onclick: move |_| expanded.set(!is_expanded),

                span { class: "kiln-schema-card__diamond", "◆" }
                span { class: "kiln-schema-card__name", "{name}" }
                span { class: "kiln-schema-card__count", "{field_count} fields" }
            }

            // ── Card meta ───────────────────────────────────────────────
            div { class: "kiln-schema-card__meta",
                span { class: "kiln-schema-card__path", "{path_display}" }
            }
            div { class: "kiln-schema-card__meta",
                span { class: "kiln-schema-card__format", "{format_label}" }
                span { class: "kiln-schema-card__sep", " · " }
                span { class: "kiln-schema-card__usage",
                    {
                        if pipeline_count == 0 {
                            "unused".to_string()
                        } else if pipeline_count == 1 {
                            "used by 1 pipeline".to_string()
                        } else {
                            format!("used by {pipeline_count} pipelines")
                        }
                    }
                }
            }

            if !description.is_empty() {
                div { class: "kiln-schema-card__desc", "{description}" }
            }

            // ── Field list (expanded) ───────────────────────────────────
            if is_expanded {
                FieldList {
                    fields: schema.fields.clone(),
                    depth: 0,
                }
            }
        }
    }
}
