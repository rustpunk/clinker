use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use cxl::typecheck::OutputLayout;
use indexmap::IndexMap;

use crate::config::{ConfigError, IncludeMetadata, OutputConfig};
use crate::error::PipelineError;

/// Apply output projection to a fully-materialized positional record
/// (Option-W): gather → exclude → mapping. No per-record emitted map —
/// the record already carries all output columns in their planner-
/// allocated slots; the producing node's `OutputLayout` identifies
/// which of those columns are CXL-named (emits) vs passthroughs.
///
/// 1. **Gather**: Start with CXL-named columns (positional slots in
///    `layout.emit_slots`). If `include_unmapped`, add all remaining
///    columns of `record.schema()` (passthroughs).
/// 2. **Exclude**: Remove any field in `exclude` list (by current name).
/// 3. **Mapping**: Rename surviving fields per `mapping` table.
pub fn project_output(
    input_record: &Record,
    layout: &OutputLayout,
    config: &OutputConfig,
) -> Record {
    project_output_with_meta(input_record, layout, &IndexMap::new(), config)
}

/// Project output with explicit metadata map (from EvalResult::Emit).
///
/// Metadata is sourced from the `metadata` parameter, not from
/// `input_record.iter_meta()`, because `evaluate_record` works on a
/// clone and the original input record has no metadata.
pub fn project_output_with_meta(
    input_record: &Record,
    layout: &OutputLayout,
    metadata: &IndexMap<String, Value>,
    config: &OutputConfig,
) -> Record {
    // Precompute: which positional slots in `layout.schema` correspond
    // to CXL-named emits vs passthroughs. `layout.emit_slots` is
    // indexed by Statement position; we project it to "is this column
    // slot an emit?" as a fast `Vec<bool>` keyed by column slot.
    let mut is_emit: Vec<bool> = vec![false; layout.schema.column_count()];
    for slot in layout.emit_slots.iter().flatten() {
        is_emit[*slot as usize] = true;
    }

    // Step 1: Gather — walk the record positionally using layout.schema.
    let mut fields: IndexMap<String, Value> = IndexMap::new();
    for (i, col) in layout.schema.columns().iter().enumerate() {
        if is_emit[i] || config.include_unmapped {
            fields.insert(col.as_ref().to_string(), input_record.values()[i].clone());
        }
    }

    // Step 1b: Merge metadata into output when include_metadata is set.
    // Metadata fields are prefixed with `meta.` in the output.
    if !config.include_metadata.is_none() {
        let meta_iter: Vec<(&str, &Value)> = match &config.include_metadata {
            IncludeMetadata::None => vec![],
            IncludeMetadata::All => metadata.iter().map(|(k, v)| (k.as_str(), v)).collect(),
            IncludeMetadata::Allowlist(allow) => metadata
                .iter()
                .filter(|(k, _)| allow.iter().any(|a| a.as_str() == *k))
                .map(|(k, v)| (k.as_str(), v))
                .collect(),
        };
        for (key, value) in meta_iter {
            let output_name = format!("meta.{key}");
            if !fields.contains_key(&output_name) {
                fields.insert(output_name, value.clone());
            }
        }
    }

    // Step 2: Exclude
    if let Some(ref exclude_list) = config.exclude {
        for name in exclude_list {
            fields.swap_remove(name.as_str());
        }
    }

    // Step 3: Mapping (rename)
    if let Some(ref mapping) = config.mapping {
        let mut renamed = IndexMap::with_capacity(fields.len());
        for (name, value) in fields {
            let output_name = mapping.get(&name).cloned().unwrap_or(name);
            renamed.insert(output_name, value);
        }
        fields = renamed;
    }

    // Build output Record
    let column_names: Vec<Box<str>> = fields.keys().map(|k| k.as_str().into()).collect();
    let schema = Arc::new(Schema::new(column_names));
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

    /// Build a test layout where `full_name` is the CXL-emitted column
    /// (slot 3) and `first_name`/`last_name`/`secret` are passthroughs
    /// (slots 0/1/2 from the input).
    fn make_layout_with_full_name_emit() -> OutputLayout {
        // Output schema: input cols (passthroughs) followed by `full_name` emit.
        let schema = Arc::new(Schema::new(vec![
            "first_name".into(),
            "last_name".into(),
            "secret".into(),
            "full_name".into(),
        ]));
        // One statement: Emit full_name. emit_slots[0] = Some(3).
        OutputLayout {
            schema,
            emit_slots: vec![Some(3)],
            passthroughs: vec![(0, 0), (1, 1), (2, 2)],
        }
    }

    fn make_identity_layout(schema: Arc<Schema>) -> OutputLayout {
        // Pure passthrough — no emits, upstream == output.
        let n = schema.column_count() as u32;
        OutputLayout {
            schema,
            emit_slots: vec![],
            passthroughs: (0..n).map(|i| (i, i)).collect(),
        }
    }

    /// Input record aligned to the layout's schema (Option-W: record
    /// carries the producing node's bound output schema). For the
    /// `full_name` emit case this means a 4-column record.
    fn make_record_for_layout(layout: &OutputLayout) -> Record {
        let n = layout.schema.column_count();
        let mut values = Vec::with_capacity(n);
        for col in layout.schema.columns() {
            let v = match col.as_ref() {
                "first_name" => Value::String("Alice".into()),
                "last_name" => Value::String("Smith".into()),
                "secret" => Value::String("password123".into()),
                "full_name" => Value::String("Alice Smith".into()),
                _ => Value::Null,
            };
            values.push(v);
        }
        Record::new(Arc::clone(&layout.schema), values)
    }

    #[test]
    fn test_gather_emitted_plus_unmapped() {
        let layout = make_layout_with_full_name_emit();
        let input = make_record_for_layout(&layout);

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
            schema: None,
            split: None,
            notes: None,
        };

        let result = project_output(&input, &layout, &config);
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
        let schema = Arc::new(Schema::new(vec![
            "first_name".into(),
            "last_name".into(),
            "secret".into(),
        ]));
        let layout = make_identity_layout(Arc::clone(&schema));
        let input = Record::new(
            schema,
            vec![
                Value::String("Alice".into()),
                Value::String("Smith".into()),
                Value::String("password123".into()),
            ],
        );

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
            schema: None,
            split: None,
            notes: None,
        };

        let result = project_output(&input, &layout, &config);
        assert!(result.get("secret").is_none());
        assert!(result.get("first_name").is_some());
    }

    #[test]
    fn test_mapping_renames() {
        let schema = Arc::new(Schema::new(vec![
            "first_name".into(),
            "last_name".into(),
            "secret".into(),
        ]));
        let layout = make_identity_layout(Arc::clone(&schema));
        let input = Record::new(
            schema,
            vec![
                Value::String("Alice".into()),
                Value::String("Smith".into()),
                Value::String("password123".into()),
            ],
        );

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
            schema: None,
            split: None,
            notes: None,
        };

        let result = project_output(&input, &layout, &config);
        assert!(result.get("first_name").is_none());
        assert_eq!(
            result.get("given_name"),
            Some(&Value::String("Alice".into()))
        );
    }
}
