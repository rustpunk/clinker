use std::collections::HashMap;

use clinker_record::{Record, SchemaBuilder, Value};
use indexmap::IndexMap;

use crate::config::{ConfigError, IncludeMetadata, OutputConfig};
use crate::error::PipelineError;

/// Apply schema aliases to emitted fields: rename keys from original to alias names.
///
/// Alias creates an identity boundary (SQL model): CXL uses original field names,
/// but output-facing code (mapping, writers) sees the post-alias names.
pub fn apply_aliases(emitted: &mut IndexMap<String, Value>, aliases: &HashMap<String, String>) {
    if aliases.is_empty() {
        return;
    }
    let entries: Vec<(String, Value)> = emitted.drain(..).collect();
    for (name, value) in entries {
        let output_name = aliases.get(&name).cloned().unwrap_or(name);
        emitted.insert(output_name, value);
    }
}

/// Apply output projection: gather → exclude → mapping.
///
/// 1. **Gather**: Start with CXL-emitted fields. If `include_unmapped`,
///    add all input fields not already emitted.
/// 2. **Exclude**: Remove any field in `exclude` list (by current name).
/// 3. **Mapping**: Rename surviving fields per `mapping` table.
pub fn project_output(
    input_record: &Record,
    emitted: &IndexMap<String, Value>,
    config: &OutputConfig,
) -> Record {
    project_output_with_meta(input_record, emitted, &IndexMap::new(), config)
}

/// Project output directly from a Record (Invariant 3 — no parallel
/// bookkeeping map input).
///
/// Gather order follows `Record::iter_all_fields`: schema columns in
/// declaration order, then overflow entries in insertion (emit) order.
/// Metadata writes land through `Record::set_meta` upstream, so
/// `include_metadata` drives off `record.iter_meta()` here instead of
/// a separately-threaded map.
///
/// Builds the output record in one pass when the config has no
/// exclude / mapping / metadata merge — the hot path avoids an
/// intermediate `IndexMap` entirely. Config-driven rewrites fall into
/// the slow path, which keeps an owned `IndexMap` only for the
/// duration of the call.
pub fn project_output_with_meta(
    input_record: &Record,
    _emitted: &IndexMap<String, Value>,
    _metadata: &IndexMap<String, Value>,
    config: &OutputConfig,
) -> Record {
    project_output_from_record(input_record, config, None)
}

/// Record-driven projection (Invariant 3 implementation).
///
/// `include_unmapped: true` surfaces every column on the record AND any
/// off-schema reader columns captured into `$meta.*` during schema
/// reprojection.
///
/// `include_unmapped: false`: when `cxl_emit_names` is `Some`, the output
/// is restricted to those names — upstream passthroughs the user did
/// NOT explicitly emit are dropped. This matches the documented Output
/// projection semantic. When `cxl_emit_names` is `None` (caller has no
/// upstream PlanNode handle), all upstream columns survive — the
/// permissive fallback used by tests and ad-hoc projections.
pub fn project_output_from_record(
    input_record: &Record,
    config: &OutputConfig,
    cxl_emit_names: Option<&[String]>,
) -> Record {
    let drop_unmapped = !config.include_unmapped && cxl_emit_names.is_some();
    let needs_rewrite = config.exclude.is_some()
        || config.mapping.is_some()
        || !config.include_metadata.is_none()
        || config.include_unmapped
        || drop_unmapped;
    let include_engine_stamped = config.include_correlation_keys;

    if !needs_rewrite {
        // Fast path: no exclude, no mapping, no metadata merge — emit
        // all Record fields in natural iteration order, no
        // intermediate allocation. Engine-stamped columns are dropped
        // unless the Output node opts in.
        let field_count = input_record.total_field_count();
        let mut schema_builder = SchemaBuilder::with_capacity(field_count);
        let mut values: Vec<Value> = Vec::with_capacity(field_count);
        if include_engine_stamped {
            for (name, value) in input_record.iter_all_fields() {
                schema_builder = schema_builder.with_field(name);
                values.push(value.clone());
            }
        } else {
            for (name, value) in input_record.iter_user_fields() {
                schema_builder = schema_builder.with_field(name);
                values.push(value.clone());
            }
        }
        return Record::new(schema_builder.build(), values);
    }

    // Slow path: config requires rewriting field names / dropping
    // fields / merging metadata, which all want the temporary
    // IndexMap's keyed access.
    let mut fields: IndexMap<String, Value> =
        IndexMap::with_capacity(input_record.total_field_count());
    if include_engine_stamped {
        for (name, value) in input_record.iter_all_fields() {
            fields.insert(name.to_string(), value.clone());
        }
    } else {
        for (name, value) in input_record.iter_user_fields() {
            fields.insert(name.to_string(), value.clone());
        }
    }

    // Restrict to user-emitted columns when the caller supplied the
    // upstream node's emit-name list and `include_unmapped: false`.
    // Walk the emit list to preserve declaration order; metadata fields
    // (meta-prefixed below) are added after this restriction so they
    // survive even though they are not in `cxl_emit_names`.
    if drop_unmapped {
        let allowed: std::collections::HashSet<&str> =
            cxl_emit_names.unwrap().iter().map(|s| s.as_str()).collect();
        let kept: IndexMap<String, Value> = cxl_emit_names
            .unwrap()
            .iter()
            .filter_map(|name| fields.get(name.as_str()).map(|v| (name.clone(), v.clone())))
            .collect();
        fields = kept;
        let _ = allowed;
    }

    // Merge metadata into output when include_metadata is set (key is
    // `meta.<name>`) or when include_unmapped is set (key is `<name>`,
    // mirroring the pre-rip overflow-passthrough shape). Both paths
    // share the `iter_meta` iterator; include_unmapped uses the bare
    // key so downstream tools that consumed overflow fields see the
    // same field names they did pre-rip.
    if !config.include_metadata.is_none() {
        let meta_iter: Vec<(String, Value)> = match &config.include_metadata {
            IncludeMetadata::None => vec![],
            IncludeMetadata::All => input_record
                .iter_meta()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            IncludeMetadata::Allowlist(allow) => input_record
                .iter_meta()
                .filter(|(k, _)| allow.iter().any(|a| a.as_str() == *k))
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        };
        for (key, value) in meta_iter {
            let output_name = format!("meta.{key}");
            if !fields.contains_key(&output_name) {
                fields.insert(output_name, value);
            }
        }
    }
    if config.include_unmapped {
        for (key, value) in input_record.iter_meta() {
            if !fields.contains_key(key) {
                fields.insert(key.to_string(), value.clone());
            }
        }
    }

    if let Some(ref exclude_list) = config.exclude {
        for name in exclude_list {
            fields.swap_remove(name.as_str());
        }
    }

    if let Some(ref mapping) = config.mapping {
        let mut renamed = IndexMap::with_capacity(fields.len());
        for (name, value) in fields {
            let output_name = mapping.get(&name).cloned().unwrap_or(name);
            renamed.insert(output_name, value);
        }
        fields = renamed;
    }

    let schema = fields
        .keys()
        .map(|k| Box::<str>::from(k.as_str()))
        .collect::<SchemaBuilder>()
        .build();
    let values: Vec<Value> = fields.into_values().collect();
    Record::new(schema, values)
}

/// Merge per-record metadata into the projected output fields.
///
/// Called after `project_output` when `include_metadata` is set.
/// Metadata fields appear with `meta.` prefix (e.g., `meta.tier`).
/// Returns `Err` if a source field collides with a metadata field.
pub fn merge_metadata_into_output(
    record: &Record,
    fields: &mut IndexMap<String, Value>,
    include: &IncludeMetadata,
) -> Result<(), PipelineError> {
    let meta_keys: Vec<(&str, &Value)> = match include {
        IncludeMetadata::None => return Ok(()),
        IncludeMetadata::All => record.iter_meta().collect(),
        IncludeMetadata::Allowlist(allow) => record
            .iter_meta()
            .filter(|(k, _)| allow.iter().any(|a| a == k))
            .collect(),
    };

    for (key, value) in meta_keys {
        let output_name = format!("meta.{key}");
        if fields.contains_key(&output_name) {
            return Err(PipelineError::Config(ConfigError::Validation(format!(
                "metadata collision: output already contains field '{}' \
                 and metadata key '{}' would produce the same output name",
                output_name, key
            ))));
        }
        fields.insert(output_name, value.clone());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Schema;
    use std::sync::Arc;

    fn make_input() -> Record {
        // Schema is pre-widened to include every field the post-transform
        // Record would carry — `full_name` is declared up front so
        // `Record::set` hits a known slot.
        let schema = Arc::new(Schema::new(vec![
            "first_name".into(),
            "last_name".into(),
            "secret".into(),
            "full_name".into(),
        ]));
        Record::new(
            schema,
            vec![
                Value::String("Alice".into()),
                Value::String("Smith".into()),
                Value::String("password123".into()),
                Value::Null,
            ],
        )
    }

    #[test]
    fn test_gather_emitted_plus_unmapped() {
        // project_output drives off the Record itself, so emitted fields
        // must land on the Record before the projection is invoked. The
        // widened schema guarantees every `Record::set` at emit sites
        // hits a known slot.
        let mut input = make_input();
        input.set("full_name", Value::String("Alice Smith".into()));
        let emitted = IndexMap::new();

        let config = OutputConfig {
            name: "out".into(),
            format: crate::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: true,
            include_header: None,
            mapping: None,
            exclude: None,
            sort_order: None,
            preserve_nulls: None,
            include_metadata: Default::default(),
            include_correlation_keys: false,
            correlation_fanout_policy: None,
            schema: None,
            split: None,
            notes: None,
        };

        let result = project_output(&input, &emitted, &config);
        assert_eq!(
            result.get("full_name"),
            Some(&Value::String("Alice Smith".into()))
        );
        assert_eq!(
            result.get("first_name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            result.get("secret"),
            Some(&Value::String("password123".into()))
        );
    }

    #[test]
    fn test_exclude_removes_fields() {
        let input = make_input();
        let emitted = IndexMap::new();

        let config = OutputConfig {
            name: "out".into(),
            format: crate::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: true,
            include_header: None,
            mapping: None,
            exclude: Some(vec!["secret".into()]),
            sort_order: None,
            preserve_nulls: None,
            include_metadata: Default::default(),
            include_correlation_keys: false,
            correlation_fanout_policy: None,
            schema: None,
            split: None,
            notes: None,
        };

        let result = project_output(&input, &emitted, &config);
        assert!(result.get("secret").is_none());
        assert!(result.get("first_name").is_some());
    }

    #[test]
    fn test_mapping_renames() {
        let input = make_input();
        let emitted = IndexMap::new();

        let mut mapping = IndexMap::new();
        mapping.insert("first_name".to_string(), "given_name".to_string());

        let config = OutputConfig {
            name: "out".into(),
            format: crate::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: true,
            include_header: None,
            mapping: Some(mapping),
            exclude: None,
            sort_order: None,
            preserve_nulls: None,
            include_metadata: Default::default(),
            include_correlation_keys: false,
            correlation_fanout_policy: None,
            schema: None,
            split: None,
            notes: None,
        };

        let result = project_output(&input, &emitted, &config);
        assert!(result.get("first_name").is_none());
        assert_eq!(
            result.get("given_name"),
            Some(&Value::String("Alice".into()))
        );
    }

    fn correlation_key_input() -> Record {
        use clinker_record::FieldMetadata;
        use clinker_record::SchemaBuilder;
        let schema = SchemaBuilder::new()
            .with_field("id")
            .with_field("name")
            .with_field_meta("$ck.id", FieldMetadata::source_correlation("id"))
            .build();
        Record::new(
            schema,
            vec![
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Integer(1),
            ],
        )
    }

    fn fast_path_output_config(include_correlation_keys: bool) -> OutputConfig {
        OutputConfig {
            name: "out".into(),
            format: crate::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: false,
            include_header: None,
            mapping: None,
            exclude: None,
            sort_order: None,
            preserve_nulls: None,
            include_metadata: Default::default(),
            include_correlation_keys,
            correlation_fanout_policy: None,
            schema: None,
            split: None,
            notes: None,
        }
    }

    #[test]
    fn test_projection_fast_path_strips_engine_stamped_by_default() {
        let input = correlation_key_input();
        let config = fast_path_output_config(false);
        let result = project_output_from_record(&input, &config, None);
        let cols: Vec<&str> = result.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["id", "name"]);
        assert!(result.get("$ck.id").is_none());
    }

    #[test]
    fn test_projection_fast_path_keeps_engine_stamped_on_opt_in() {
        let input = correlation_key_input();
        let config = fast_path_output_config(true);
        let result = project_output_from_record(&input, &config, None);
        let cols: Vec<&str> = result.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["id", "name", "$ck.id"]);
        assert_eq!(result.get("$ck.id"), Some(&Value::Integer(1)));
    }
}
