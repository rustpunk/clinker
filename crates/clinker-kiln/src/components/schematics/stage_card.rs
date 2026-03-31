use dioxus::prelude::*;

use crate::autodoc::{ConfigCategory, CxlStatementKind, StageDoc};
use crate::notes::StageNotes;

/// A single stage detail card in the Schematics content area.
///
/// Full technical documentation: CXL code, schema tables, field analysis,
/// contracts, config grids, composition sub-cards. The card IS the
/// documentation, not a pointer to it.
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

    // Group config entries by category
    let config_groups = group_config_entries(&doc);

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

            hr { class: "kiln-stage-card-rule" }

            // ── Summary ──────────────────────────────────────────────────
            div {
                class: "kiln-stage-card-description",
                "{doc.summary}"
            }

            // ── User note ────────────────────────────────────────────────
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

            // ── Channel override ─────────────────────────────────────────
            if let Some(ref co) = doc.channel_override {
                div {
                    class: "kiln-card-section",
                    span { class: "kiln-card-section-label", "CHANNEL OVERRIDE" }
                    div { class: "kiln-card-meta",
                        span { class: "kiln-card-meta-key", "{co.override_kind}" }
                        span { class: "kiln-card-meta-value", "via {co.override_source}" }
                    }
                }
            }

            // ── CXL source code ──────────────────────────────────────────
            if let Some(ref cxl) = doc.cxl_source {
                div {
                    class: "kiln-card-section",
                    span { class: "kiln-card-section-label", "CXL" }
                    pre {
                        class: "kiln-card-cxl-block",
                        "{cxl.trim()}"
                    }
                }
            }

            // ── CXL analysis ─────────────────────────────────────────────
            if let Some(ref analysis) = doc.cxl_analysis {
                div {
                    class: "kiln-card-section",
                    span { class: "kiln-card-section-label", "FIELD ANALYSIS" }
                    for (i, stmt) in analysis.statements.iter().enumerate() {
                        div { class: "kiln-card-stmt",
                            key: "stmt-{i}",
                            span {
                                class: "kiln-docs-stmt-badge",
                                "data-kind": stmt.kind.label().to_lowercase(),
                                "{stmt.kind.label()}"
                            }
                            if let Some(ref out) = stmt.output_field {
                                span { class: "kiln-card-emit-field", "+{out}" }
                            }
                            if !stmt.field_refs.is_empty() {
                                span { class: "kiln-card-reads",
                                    "reads "
                                    for (j, r) in stmt.field_refs.iter().enumerate() {
                                        if j > 0 { ", " }
                                        span { class: "kiln-card-field-ref", "{r}" }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // ── Schema table ─────────────────────────────────────────────
            if let Some(ref schema) = doc.schema {
                div {
                    class: "kiln-card-section",
                    span { class: "kiln-card-section-label",
                        match &schema.source {
                            crate::autodoc::SchemaOrigin::File(path) => format!("SCHEMA ({})", path),
                            crate::autodoc::SchemaOrigin::Inline => "SCHEMA".to_string(),
                            crate::autodoc::SchemaOrigin::OverridesOnly => "SCHEMA OVERRIDES".to_string(),
                            crate::autodoc::SchemaOrigin::None => "FIELDS".to_string(),
                        }
                    }
                    if !schema.fields.is_empty() {
                        div { class: "kiln-card-schema",
                            div { class: "kiln-card-schema-header",
                                span { "Name" }
                                span { "Type" }
                                span { "Req" }
                            }
                            for field in schema.fields.iter() {
                                div { class: "kiln-card-schema-row",
                                    key: "sf-{field.name}",
                                    span { class: "kiln-card-schema-name", "{field.name}" }
                                    span { {field.field_type.as_deref().unwrap_or("—")} }
                                    span { if field.required { "yes" } else { "—" } }
                                }
                            }
                        }
                    }
                }
            }

            // ── Contract ─────────────────────────────────────────────────
            if let Some(ref contract) = doc.contract {
                div {
                    class: "kiln-card-section",
                    span { class: "kiln-card-section-label", "CONTRACT" }
                    if !contract.requires.is_empty() {
                        div { class: "kiln-card-contract-group",
                            span { class: "kiln-card-contract-heading", "REQUIRES" }
                            for f in contract.requires.iter() {
                                span { class: "kiln-card-contract-field",
                                    key: "req-{f.name}",
                                    "{f.name}: {f.field_type}"
                                }
                            }
                        }
                    }
                    if !contract.produces.is_empty() {
                        div { class: "kiln-card-contract-group",
                            span { class: "kiln-card-contract-heading", "PRODUCES" }
                            for f in contract.produces.iter() {
                                span { class: "kiln-card-contract-field",
                                    key: "prod-{f.name}",
                                    "{f.name}: {f.field_type}"
                                }
                            }
                        }
                    }
                }
            }

            // ── Composition sub-stages ───────────────────────────────────
            if !doc.sub_stages.is_empty() {
                div {
                    class: "kiln-card-section",
                    span { class: "kiln-card-section-label", "TRANSFORMS" }
                    for (si, sub) in doc.sub_stages.iter().enumerate() {
                        {
                            let sub_name = sub.summary.split(':').next()
                                .or_else(|| sub.summary.split('.').next())
                                .unwrap_or(&sub.summary);
                            // Find the actual stage name from config entries
                            let sub_stage_name = sub.config.entries.iter()
                                .find(|e| e.key == "TYPE")
                                .map(|_| {
                                    // Use provenance or fallback
                                    sub.provenance.as_ref()
                                        .map(|p| p.composition_name.clone())
                                        .unwrap_or_default()
                                })
                                .unwrap_or_default();

                            rsx! {
                                div { class: "kiln-card-substage",
                                    key: "sub-{si}",

                                    // Sub-stage header
                                    div { class: "kiln-card-substage-header",
                                        span { class: "kiln-card-substage-index", "{si:02}" }
                                        span { class: "kiln-card-substage-summary", "{sub.summary}" }
                                    }

                                    // CXL source
                                    if let Some(ref cxl) = sub.cxl_source {
                                        pre {
                                            class: "kiln-card-cxl-block",
                                            "{cxl.trim()}"
                                        }
                                    }

                                    // CXL analysis
                                    if let Some(ref analysis) = sub.cxl_analysis {
                                        div { class: "kiln-card-substage-analysis",
                                            for (i, stmt) in analysis.statements.iter().enumerate() {
                                                div { class: "kiln-card-stmt",
                                                    key: "sub-stmt-{i}",
                                                    span {
                                                        class: "kiln-docs-stmt-badge",
                                                        "data-kind": stmt.kind.label().to_lowercase(),
                                                        "{stmt.kind.label()}"
                                                    }
                                                    if let Some(ref out) = stmt.output_field {
                                                        span { class: "kiln-card-emit-field", "+{out}" }
                                                    }
                                                    if !stmt.field_refs.is_empty() {
                                                        span { class: "kiln-card-reads",
                                                            "reads "
                                                            for (j, r) in stmt.field_refs.iter().enumerate() {
                                                                if j > 0 { ", " }
                                                                span { class: "kiln-card-field-ref", "{r}" }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // Provenance (override info)
                                    if let Some(ref prov) = sub.provenance {
                                        if prov.is_overridden {
                                            div { class: "kiln-card-override-indicator",
                                                "overridden"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // ── Provenance ───────────────────────────────────────────────
            if let Some(ref prov) = doc.provenance {
                div {
                    class: "kiln-card-section",
                    span { class: "kiln-card-section-label", "PROVENANCE" }
                    div { class: "kiln-card-meta",
                        span { class: "kiln-card-meta-key", "from" }
                        span { class: "kiln-card-meta-value", "{prov.composition_name}" }
                    }
                    div { class: "kiln-card-meta",
                        span { class: "kiln-card-meta-key", "path" }
                        span { class: "kiln-card-meta-value kiln-card-meta-path", "{prov.composition_path}" }
                    }
                    if prov.is_overridden {
                        div { class: "kiln-card-meta",
                            span { class: "kiln-card-meta-key", "status" }
                            span { class: "kiln-card-meta-value kiln-card-override-indicator", "overridden" }
                        }
                    }
                }
            }

            // ── Config grid ──────────────────────────────────────────────
            if !config_groups.is_empty() {
                div {
                    class: "kiln-card-section",
                    span { class: "kiln-card-section-label", "CONFIGURATION" }
                    for (label, entries) in config_groups.iter() {
                        div { class: "kiln-card-config-group",
                            if config_groups.len() > 1 {
                                span { class: "kiln-card-config-category", "{label}" }
                            }
                            for entry in entries.iter() {
                                div { class: "kiln-card-meta",
                                    key: "cfg-{entry.key}",
                                    span { class: "kiln-card-meta-key", "{entry.key}" }
                                    span { class: "kiln-card-meta-value", "{entry.value}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Group config entries by category for rendering.
fn group_config_entries(doc: &StageDoc) -> Vec<(String, Vec<&crate::autodoc::ConfigEntry>)> {
    let mut groups: Vec<(ConfigCategory, Vec<&crate::autodoc::ConfigEntry>)> = Vec::new();

    for entry in &doc.config.entries {
        if let Some(group) = groups.iter_mut().find(|(cat, _)| *cat == entry.category) {
            group.1.push(entry);
        } else {
            groups.push((entry.category.clone(), vec![entry]));
        }
    }

    groups
        .into_iter()
        .map(|(cat, entries)| (cat.label().to_string(), entries))
        .collect()
}
