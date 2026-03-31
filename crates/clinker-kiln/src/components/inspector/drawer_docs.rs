use dioxus::prelude::*;

use crate::autodoc::{
    generate_stage_doc, ChannelDocContext, ConfigCategory, StageDoc,
};
use crate::notes::parse_notes;
use crate::state::{use_app_state, ChannelViewMode};

/// Docs drawer — full stage documentation with Blueprint sub-aesthetic.
///
/// Content:
/// 1. Summary + user description
/// 2. Schema table (fields, types, constraints)
/// 3. Lineage table (emit field → input refs)
/// 4. Contract section (requires/produces)
/// 5. Config section (grouped by category)
/// 6. Provenance section (composition origin, override diff)
/// 7. Channel override section
/// 8. Footer: "AUTODOC"
#[component]
pub fn DrawerDocs(stage_id: String) -> Element {
    let state = use_app_state();

    let pipeline_guard = (state.pipeline).read();
    let base_config = match pipeline_guard.as_ref() {
        Some(c) => c,
        None => {
            return rsx! {
                div {
                    class: "kiln-drawer-content kiln-drawer-content--docs",
                    div { class: "kiln-drawer-placeholder", "No pipeline loaded" }
                }
            };
        }
    };

    let compositions_read = (state.compositions).read();

    // Channel-aware pipeline selection
    let channel_view = (state.channel_view_mode)();
    let channel_res = (state.channel_pipeline)();

    let (config, channel_doc_ctx) = match (channel_view, channel_res.as_ref()) {
        (ChannelViewMode::Resolved, Some(cr)) => {
            let tab_mgr = use_context::<crate::state::TabManagerState>();
            let channel_id = (tab_mgr.channel_state)()
                .as_ref()
                .and_then(|cs| cs.active_channel.clone())
                .unwrap_or_else(|| "unknown".to_string());

            let ctx = ChannelDocContext {
                channel_id,
                overrides_applied: cr.overrides_applied.clone(),
            };
            (&cr.resolved_config, Some(ctx))
        }
        _ => (base_config, None),
    };

    let Some(doc) = generate_stage_doc(config, &compositions_read, channel_doc_ctx.as_ref(), &stage_id) else {
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
        .or_else(|| config.transforms().find(|t| t.name == stage_id).and_then(|t| t.notes.as_ref()))
        .or_else(|| config.outputs.iter().find(|o| o.name == stage_id).and_then(|o| o.notes.as_ref()));
    let notes = parse_notes(notes_value);

    // Group config entries by category
    let config_groups = group_config_entries(&doc);

    rsx! {
        div {
            class: "kiln-drawer-content kiln-drawer-content--docs",

            // ── Summary + user description ───────────────────────────────
            div {
                class: "kiln-docs-description",
                style: "position: relative;",
                span {
                    class: "kiln-stage-card-stamp",
                    "autodoc"
                }
                "{doc.summary}"
            }

            if let Some(ref desc) = doc.user_description {
                div {
                    class: "kiln-docs-user-desc",
                    "{desc}"
                }
            }

            // ── User-authored stage note (when present) ──────────────────
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

            // ── Channel override section ─────────────────────────────────
            if let Some(ref co) = doc.channel_override {
                div {
                    class: "kiln-docs-section",
                    span { class: "kiln-docs-section-label", "CHANNEL OVERRIDE" }
                    div {
                        class: "kiln-docs-metadata",
                        div { class: "kiln-docs-meta-row",
                            span { class: "kiln-docs-meta-key", "CHANNEL" }
                            span { class: "kiln-docs-meta-value", "{co.channel_id}" }
                        }
                        div { class: "kiln-docs-meta-row",
                            span { class: "kiln-docs-meta-key", "ACTION" }
                            span { class: "kiln-docs-meta-value kiln-docs-meta-value--badge", "{co.override_kind}" }
                        }
                        div { class: "kiln-docs-meta-row",
                            span { class: "kiln-docs-meta-key", "SOURCE" }
                            span { class: "kiln-docs-meta-value", "{co.override_source}" }
                        }
                        div { class: "kiln-docs-meta-row",
                            span { class: "kiln-docs-meta-key", "FILE" }
                            span { class: "kiln-docs-meta-value kiln-docs-meta-value--path", "{co.override_file}" }
                        }
                    }
                }
            }

            // ── Schema table ─────────────────────────────────────────────
            if let Some(ref schema) = doc.schema {
                div {
                    class: "kiln-docs-section",
                    span { class: "kiln-docs-section-label",
                        match &schema.source {
                            crate::autodoc::SchemaOrigin::File(path) => format!("SCHEMA (from {})", path),
                            crate::autodoc::SchemaOrigin::Inline => "SCHEMA (inline)".to_string(),
                            crate::autodoc::SchemaOrigin::OverridesOnly => "SCHEMA (overrides)".to_string(),
                            crate::autodoc::SchemaOrigin::None => "SCHEMA".to_string(),
                        }
                    }
                    if !schema.fields.is_empty() {
                        div {
                            class: "kiln-docs-schema-table",
                            // Header row
                            div { class: "kiln-docs-schema-row kiln-docs-schema-row--header",
                                span { class: "kiln-docs-schema-cell", "Name" }
                                span { class: "kiln-docs-schema-cell", "Type" }
                                span { class: "kiln-docs-schema-cell", "Required" }
                                span { class: "kiln-docs-schema-cell", "Format" }
                                span { class: "kiln-docs-schema-cell", "Default" }
                            }
                            for field in schema.fields.iter() {
                                div { class: "kiln-docs-schema-row",
                                    key: "schema-{field.name}",
                                    span { class: "kiln-docs-schema-cell kiln-docs-schema-cell--name", "{field.name}" }
                                    span { class: "kiln-docs-schema-cell",
                                        {field.field_type.as_deref().unwrap_or("—")}
                                    }
                                    span { class: "kiln-docs-schema-cell",
                                        if field.required { "yes" } else { "—" }
                                    }
                                    span { class: "kiln-docs-schema-cell",
                                        {field.format.as_deref().unwrap_or("—")}
                                    }
                                    span { class: "kiln-docs-schema-cell",
                                        {field.default_value.as_deref().unwrap_or("—")}
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // ── CXL analysis ─────────────────────────────────────────────
            if let Some(ref analysis) = doc.cxl_analysis {
                div {
                    class: "kiln-docs-section",
                    span { class: "kiln-docs-section-label", "CXL ANALYSIS" }

                    // All fields referenced summary
                    if !analysis.all_field_refs.is_empty() {
                        div { class: "kiln-docs-field-refs-summary",
                            span { class: "kiln-docs-lineage-refs-label", "fields referenced: " }
                            for r in analysis.all_field_refs.iter() {
                                span { class: "kiln-docs-lineage-ref", "{r}" }
                            }
                        }
                    }

                    // Classified statements
                    div {
                        class: "kiln-docs-lineage-table",
                        for (i, stmt) in analysis.statements.iter().enumerate() {
                            div { class: "kiln-docs-lineage-row",
                                key: "stmt-{i}",
                                // Kind badge + output field
                                div { class: "kiln-docs-lineage-field",
                                    span {
                                        class: "kiln-docs-stmt-badge",
                                        "data-kind": stmt.kind.label().to_lowercase(),
                                        "{stmt.kind.label()}"
                                    }
                                    if let Some(ref out) = stmt.output_field {
                                        span { class: "kiln-docs-column-tag kiln-docs-column-tag--added",
                                            "+{out}"
                                        }
                                    }
                                }
                                // Expression
                                div { class: "kiln-docs-lineage-expr",
                                    code { class: "kiln-docs-lineage-code", "{stmt.expression}" }
                                }
                                // Field refs
                                if !stmt.field_refs.is_empty() {
                                    div { class: "kiln-docs-lineage-refs",
                                        span { class: "kiln-docs-lineage-refs-label", "reads: " }
                                        for r in stmt.field_refs.iter() {
                                            span { class: "kiln-docs-lineage-ref", "{r}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // ── Contract section ─────────────────────────────────────────
            if let Some(ref contract) = doc.contract {
                div {
                    class: "kiln-docs-section",
                    span { class: "kiln-docs-section-label",
                        "CONTRACT ({contract.composition_name})"
                    }
                    if !contract.requires.is_empty() {
                        div { class: "kiln-docs-contract-group",
                            span { class: "kiln-docs-contract-heading", "REQUIRES" }
                            for f in contract.requires.iter() {
                                div { class: "kiln-docs-contract-field",
                                    key: "req-{f.name}",
                                    span { class: "kiln-docs-contract-name", "{f.name}" }
                                    span { class: "kiln-docs-contract-type", "{f.field_type}" }
                                }
                            }
                        }
                    }
                    if !contract.produces.is_empty() {
                        div { class: "kiln-docs-contract-group",
                            span { class: "kiln-docs-contract-heading", "PRODUCES" }
                            for f in contract.produces.iter() {
                                div { class: "kiln-docs-contract-field",
                                    key: "prod-{f.name}",
                                    span { class: "kiln-docs-contract-name", "{f.name}" }
                                    span { class: "kiln-docs-contract-type", "{f.field_type}" }
                                }
                            }
                        }
                    }
                }
            }

            // ── Config section (grouped by category) ─────────────────────
            for (label, entries) in config_groups.iter() {
                div {
                    class: "kiln-docs-section",
                    span { class: "kiln-docs-section-label", "{label}" }
                    div {
                        class: "kiln-docs-metadata",
                        for entry in entries.iter() {
                            div {
                                key: "cfg-{entry.key}",
                                class: "kiln-docs-meta-row",
                                span { class: "kiln-docs-meta-key", "{entry.key}" }
                                span { class: "kiln-docs-meta-value", "{entry.value}" }
                            }
                        }
                    }
                }
            }

            // ── Provenance section ───────────────────────────────────────
            if let Some(ref prov) = doc.provenance {
                div {
                    class: "kiln-docs-section",
                    span { class: "kiln-docs-section-label", "PROVENANCE" }
                    div {
                        class: "kiln-docs-metadata",
                        div { class: "kiln-docs-meta-row",
                            span { class: "kiln-docs-meta-key", "COMPOSITION" }
                            span { class: "kiln-docs-meta-value", "{prov.composition_name}" }
                        }
                        div { class: "kiln-docs-meta-row",
                            span { class: "kiln-docs-meta-key", "PATH" }
                            span { class: "kiln-docs-meta-value kiln-docs-meta-value--path", "{prov.composition_path}" }
                        }
                        if let Some(ref ver) = prov.composition_version {
                            div { class: "kiln-docs-meta-row",
                                span { class: "kiln-docs-meta-key", "VERSION" }
                                span { class: "kiln-docs-meta-value", "{ver}" }
                            }
                        }
                        if prov.is_overridden {
                            div { class: "kiln-docs-meta-row",
                                span { class: "kiln-docs-meta-key", "STATUS" }
                                span { class: "kiln-docs-meta-value kiln-docs-meta-value--badge", "OVERRIDDEN" }
                            }
                        }
                    }

                    // Override diff
                    if prov.is_overridden {
                        if let Some(ref original) = prov.original_cxl {
                            div { class: "kiln-docs-diff",
                                span { class: "kiln-docs-diff-label", "ORIGINAL CXL" }
                                pre { class: "kiln-docs-diff-block kiln-docs-diff-block--original", "{original}" }
                            }
                        }
                        if let Some(ref current) = prov.current_cxl {
                            div { class: "kiln-docs-diff",
                                span { class: "kiln-docs-diff-label", "CURRENT CXL" }
                                pre { class: "kiln-docs-diff-block kiln-docs-diff-block--current", "{current}" }
                            }
                        }
                    }
                }
            }

            // ── Footer ───────────────────────────────────────────────────
            div {
                class: "kiln-docs-footer",
                span { class: "kiln-docs-footer-rule" }
                span { class: "kiln-docs-footer-label", "AUTODOC" }
                span { class: "kiln-docs-footer-rule" }
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
