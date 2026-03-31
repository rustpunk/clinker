//! Field list rendering for schema cards.
//!
//! Renders a list of `FieldDescriptor` entries with type badges, nullable
//! indicators, description snippets, enum counts, and nested indentation.
//! XML attributes show an `attr` tag. Object fields recurse.
//!
//! Spec §S3.6: field list with type badges and indicators.

use dioxus::prelude::*;

use clinker_schema::FieldDescriptor;

/// Render a list of fields, with recursive nesting for object types.
#[component]
pub fn FieldList(fields: Vec<FieldDescriptor>, depth: u32) -> Element {
    let indent = depth * 12;

    rsx! {
        div {
            class: "kiln-field-list",
            style: "padding-left: {indent}px;",

            for field in &fields {
                FieldRow {
                    key: "{field.name}",
                    field: field.clone(),
                    depth,
                }
            }
        }
    }
}

/// A single field row: name, type badge, nullable indicator, extras.
#[component]
fn FieldRow(field: FieldDescriptor, depth: u32) -> Element {
    let type_badge = field.field_type.badge();
    let type_class = format!("kiln-field__type--{type_badge}");
    let nullable_indicator = if field.nullable { "?" } else { "NN" };
    let nullable_class = if field.nullable {
        "kiln-field__nullable--yes"
    } else {
        "kiln-field__nullable--no"
    };

    let is_attr = field.is_xml_attribute();
    let has_enum = field.enum_values.as_ref().is_some_and(|v| !v.is_empty());
    let enum_count = field.enum_values.as_ref().map(|v| v.len()).unwrap_or(0);
    let has_children = field.fields.as_ref().is_some_and(|c| !c.is_empty());
    let child_count = field.fields.as_ref().map(|c| c.len()).unwrap_or(0);

    let desc_snippet = field
        .description
        .as_deref()
        .map(|d| {
            if d.len() > 30 {
                format!("{}…", &d[..27])
            } else {
                d.to_string()
            }
        })
        .unwrap_or_default();

    // Prefix for nested fields
    let prefix = "│ ";

    rsx! {
        div {
            class: "kiln-field-row",

            span { class: "kiln-field__prefix", "{prefix}" }
            span { class: "kiln-field__name",
                if is_attr { "@" } else { "" }
                "{field.name.trim_start_matches('@')}"
            }
            span { class: "kiln-field__type {type_class}", "{type_badge}" }
            span { class: "kiln-field__nullable {nullable_class}", "{nullable_indicator}" }

            if is_attr {
                span { class: "kiln-field__tag kiln-field__tag--attr", "attr" }
            }

            if has_enum {
                span { class: "kiln-field__tag kiln-field__tag--enum", "enum:{enum_count}" }
            }

            if has_children {
                span { class: "kiln-field__tag kiln-field__tag--children", "{child_count} sub" }
            }

            if !desc_snippet.is_empty() {
                span { class: "kiln-field__desc", "\"{desc_snippet}\"" }
            }
        }

        // Recurse into nested fields
        if has_children {
            FieldList {
                fields: field.fields.clone().unwrap_or_default(),
                depth: depth + 1,
            }
        }
    }
}
