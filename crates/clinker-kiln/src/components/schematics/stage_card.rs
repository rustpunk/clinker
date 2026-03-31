use dioxus::prelude::*;

use crate::autodoc::{StageDoc, CxlStatementKind};
use crate::notes::StageNotes;

/// A single stage detail card in the Schematics content area.
///
/// Shows compact highlights: summary, schema badge, contract badge,
/// lineage tags, provenance line. Full detail is in the inspector drawer.
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
    let delay = index as f64 * 0.1;

    // Extract compact highlights
    let schema_badge = doc.schema.as_ref().map(|s| {
        let count = s.fields.len();
        format!("{} field{}", count, if count == 1 { "" } else { "s" })
    });

    let contract_badge = doc.contract.as_ref().map(|c| {
        format!("requires {} / produces {}", c.requires.len(), c.produces.len())
    });

    // CXL analysis: extract emitted fields and statement summary
    let emit_fields: Vec<String> = doc
        .cxl_analysis
        .as_ref()
        .map(|a| a.statements.iter()
            .filter(|s| s.kind == CxlStatementKind::Emit)
            .filter_map(|s| s.output_field.clone())
            .collect())
        .unwrap_or_default();

    let cxl_summary = doc.cxl_analysis.as_ref().map(|a| {
        let emits = a.statements.iter().filter(|s| s.kind == CxlStatementKind::Emit).count();
        let filters = a.statements.iter().filter(|s| s.kind == CxlStatementKind::Filter).count();
        let lets = a.statements.iter().filter(|s| s.kind == CxlStatementKind::Let).count();
        let mut parts = Vec::new();
        if emits > 0 { parts.push(format!("{} emit{}", emits, if emits == 1 { "" } else { "s" })); }
        if filters > 0 { parts.push(format!("{} filter{}", filters, if filters == 1 { "" } else { "s" })); }
        if lets > 0 { parts.push(format!("{} let{}", lets, if lets == 1 { "" } else { "s" })); }
        parts.join(" \u{00B7} ")
    }).filter(|s| !s.is_empty());

    let all_field_refs: Vec<String> = doc
        .cxl_analysis
        .as_ref()
        .map(|a| a.all_field_refs.clone())
        .unwrap_or_default();

    let provenance_label = doc.provenance.as_ref().map(|p| {
        if p.is_overridden {
            format!("from {} (overridden)", p.composition_name)
        } else {
            format!("from {}", p.composition_name)
        }
    });

    let channel_badge = doc.channel_override.as_ref().map(|co| {
        format!("{} via {}", co.override_kind, co.override_source)
    });

    rsx! {
        div {
            class: "kiln-stage-card",
            style: "border-top-color: {accent}; animation: blueprintIn 0.5s ease {delay}s both;",

            // Title block stamp
            span {
                class: "kiln-stage-card-stamp",
                "{badge}"
            }

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

            // Summary
            div {
                class: "kiln-stage-card-description",
                "{doc.summary}"
            }

            // Badges row: schema count + contract + channel override
            if schema_badge.is_some() || contract_badge.is_some() || channel_badge.is_some() {
                div {
                    class: "kiln-stage-card-badges",
                    if let Some(ref sb) = schema_badge {
                        span { class: "kiln-stage-card-chip kiln-stage-card-chip--schema", "{sb}" }
                    }
                    if let Some(ref cb) = contract_badge {
                        span { class: "kiln-stage-card-chip kiln-stage-card-chip--contract", "{cb}" }
                    }
                    if let Some(ref ch) = channel_badge {
                        span { class: "kiln-stage-card-chip kiln-stage-card-chip--channel", "{ch}" }
                    }
                }
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

            // Provenance line
            if let Some(ref prov) = provenance_label {
                div {
                    class: "kiln-stage-card-provenance",
                    "{prov}"
                }
            }

            // CXL statement summary (e.g. "2 emits · 1 filter")
            if let Some(ref summary) = cxl_summary {
                div {
                    class: "kiln-stage-card-cxl-summary",
                    "{summary}"
                }
            }

            // Fields referenced by CXL
            if !all_field_refs.is_empty() {
                div {
                    class: "kiln-stage-card-field-refs",
                    span { class: "kiln-stage-card-columns-heading", "READS" }
                    div {
                        class: "kiln-stage-card-column-tags",
                        for field in all_field_refs.iter() {
                            span {
                                key: "ref-{field}",
                                class: "kiln-stage-card-column-tag",
                                "{field}"
                            }
                        }
                    }
                }
            }

            // Emitted output fields
            if !emit_fields.is_empty() {
                div {
                    class: "kiln-stage-card-columns",
                    span { class: "kiln-stage-card-columns-heading", "EMITS" }
                    div {
                        class: "kiln-stage-card-column-tags",
                        for col in emit_fields.iter() {
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
