use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use indexmap::IndexMap;

use crate::config::OutputConfig;

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
    // Step 1: Gather
    let mut fields: IndexMap<String, Value> = IndexMap::new();

    // CXL-emitted fields first
    for (name, value) in emitted {
        fields.insert(name.clone(), value.clone());
    }

    // If include_unmapped, add input fields not already present
    if config.include_unmapped {
        for (name, value) in input_record.iter_all_fields() {
            if !fields.contains_key(name) {
                fields.insert(name.to_string(), value.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_input() -> Record {
        let schema = Arc::new(Schema::new(vec![
            "first_name".into(),
            "last_name".into(),
            "secret".into(),
        ]));
        Record::new(
            schema,
            vec![
                Value::String("Alice".into()),
                Value::String("Smith".into()),
                Value::String("password123".into()),
            ],
        )
    }

    #[test]
    fn test_gather_emitted_plus_unmapped() {
        let input = make_input();
        let mut emitted = IndexMap::new();
        emitted.insert("full_name".to_string(), Value::String("Alice Smith".into()));

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
            notes: None,
        };

        let result = project_output(&input, &emitted, &config);
        assert_eq!(result.get("full_name"), Some(&Value::String("Alice Smith".into())));
        assert_eq!(result.get("first_name"), Some(&Value::String("Alice".into())));
        assert_eq!(result.get("secret"), Some(&Value::String("password123".into())));
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
            notes: None,
        };

        let result = project_output(&input, &emitted, &config);
        assert!(result.get("first_name").is_none());
        assert_eq!(result.get("given_name"), Some(&Value::String("Alice".into())));
    }
}
