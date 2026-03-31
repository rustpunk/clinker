pub mod resolve;

use std::path::Path;

use clinker_record::schema_def::{Discriminator, FieldDef, RecordTypeDef, SchemaDefinition};

/// Resolved schema -- inherits merged, validated, ready for pipeline use.
#[derive(Debug, Clone)]
pub enum ResolvedSchema {
    SingleRecord { fields: Vec<FieldDef> },
    MultiRecord {
        discriminator: Discriminator,
        record_types: Vec<RecordTypeDef>,
    },
}

/// Schema subsystem errors.
#[derive(Debug)]
pub enum SchemaError {
    Io(std::io::Error),
    Yaml(serde_saphyr::Error),
    Validation(String),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "schema I/O error: {e}"),
            Self::Yaml(e) => write!(f, "schema YAML parse error: {e}"),
            Self::Validation(msg) => write!(f, "schema validation error: {msg}"),
        }
    }
}

impl std::error::Error for SchemaError {}

impl From<std::io::Error> for SchemaError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_saphyr::Error> for SchemaError {
    fn from(e: serde_saphyr::Error) -> Self {
        Self::Yaml(e)
    }
}

/// Load a schema definition from a YAML file.
pub fn load_schema(path: &Path) -> Result<SchemaDefinition, SchemaError> {
    let yaml = std::fs::read_to_string(path)?;
    let def: SchemaDefinition = serde_saphyr::from_str(&yaml)?;
    Ok(def)
}

/// Resolve a SchemaDefinition: merge inherits, validate constraints,
/// produce a pipeline-ready ResolvedSchema.
pub fn resolve_schema(def: SchemaDefinition) -> Result<ResolvedSchema, SchemaError> {
    // Validate fields/records mutual exclusivity
    if def.fields.is_some() && def.records.is_some() {
        return Err(SchemaError::Validation(
            "schema cannot have both 'fields' and 'records' at the top level".into(),
        ));
    }
    if def.fields.is_none() && def.records.is_none() {
        return Err(SchemaError::Validation(
            "schema must have either 'fields' (single-record) or 'records' (multi-record)".into(),
        ));
    }

    if let Some(records) = def.records {
        // Multi-record path
        let discriminator = def.discriminator.ok_or_else(|| {
            SchemaError::Validation(
                "multi-record schema requires a 'discriminator' section".into(),
            )
        })?;

        // Resolve inherits within each record type's fields
        let defs = def.defs.as_ref();
        let mut resolved_records = Vec::with_capacity(records.len());
        for mut rt in records {
            rt.fields = resolve_inherits(rt.fields, defs)?;
            validate_fields(&rt.fields)?;
            resolved_records.push(rt);
        }

        Ok(ResolvedSchema::MultiRecord {
            discriminator,
            record_types: resolved_records,
        })
    } else {
        // Single-record path
        let fields = def.fields.unwrap();
        let defs = def.defs.as_ref();
        let resolved_fields = resolve_inherits(fields, defs)?;
        validate_fields(&resolved_fields)?;
        Ok(ResolvedSchema::SingleRecord {
            fields: resolved_fields,
        })
    }
}

/// Resolve `inherits:` references in a field list against `defs:` templates.
fn resolve_inherits(
    fields: Vec<FieldDef>,
    defs: Option<&std::collections::HashMap<String, FieldDef>>,
) -> Result<Vec<FieldDef>, SchemaError> {
    let mut resolved = Vec::with_capacity(fields.len());

    for mut field in fields {
        // Validate override-only fields are None in base schema context
        if field.drop == Some(true) {
            return Err(SchemaError::Validation(format!(
                "field '{}': 'drop: true' is only allowed in schema_overrides, not in base schema",
                field.name
            )));
        }
        if field.record.is_some() {
            return Err(SchemaError::Validation(format!(
                "field '{}': 'record' is only allowed in schema_overrides, not in base schema",
                field.name
            )));
        }

        if let Some(template_name) = field.inherits.take() {
            let defs_map = defs.ok_or_else(|| {
                SchemaError::Validation(format!(
                    "field '{}' uses 'inherits: {template_name}' but no 'defs' section exists",
                    field.name
                ))
            })?;

            let template = defs_map.get(&template_name).ok_or_else(|| {
                SchemaError::Validation(format!(
                    "field '{}' inherits from unknown template '{template_name}'",
                    field.name
                ))
            })?;

            // Merge: field-level overrides win (shallow merge)
            field = merge_from_template(field, template);
        }

        resolved.push(field);
    }

    // Validate no inherits chains in defs
    if let Some(defs_map) = defs {
        for (name, def) in defs_map {
            if def.inherits.is_some() {
                return Err(SchemaError::Validation(format!(
                    "defs entry '{name}' cannot use 'inherits' -- inherits chains are not allowed"
                )));
            }
        }
    }

    Ok(resolved)
}

/// Merge a template's properties into a field. Field's own values win.
fn merge_from_template(mut field: FieldDef, template: &FieldDef) -> FieldDef {
    if field.field_type.is_none() {
        field.field_type = template.field_type.clone();
    }
    if field.required.is_none() {
        field.required = template.required;
    }
    if field.format.is_none() {
        field.format = template.format.clone();
    }
    if field.coerce.is_none() {
        field.coerce = template.coerce;
    }
    if field.default.is_none() {
        field.default = template.default.clone();
    }
    if field.allowed_values.is_none() {
        field.allowed_values = template.allowed_values.clone();
    }
    if field.alias.is_none() {
        field.alias = template.alias.clone();
    }
    if field.start.is_none() {
        field.start = template.start;
    }
    if field.width.is_none() {
        field.width = template.width;
    }
    if field.end.is_none() {
        field.end = template.end;
    }
    if field.justify.is_none() {
        field.justify = template.justify.clone();
    }
    if field.pad.is_none() {
        field.pad = template.pad.clone();
    }
    if field.trim.is_none() {
        field.trim = template.trim;
    }
    if field.truncation.is_none() {
        field.truncation = template.truncation.clone();
    }
    if field.precision.is_none() {
        field.precision = template.precision;
    }
    if field.scale.is_none() {
        field.scale = template.scale;
    }
    if field.path.is_none() {
        field.path = template.path.clone();
    }
    field
}

/// Validate field constraints after inherits resolution.
fn validate_fields(fields: &[FieldDef]) -> Result<(), SchemaError> {
    for field in fields {
        // width and end are mutually exclusive
        if field.width.is_some() && field.end.is_some() {
            return Err(SchemaError::Validation(format!(
                "field '{}': 'width' and 'end' are mutually exclusive -- use one or the other",
                field.name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::schema_def::SchemaDefinition;

    #[test]
    fn test_schema_load_basic() {
        let yaml = r#"
fields:
  - name: id
    type: integer
    start: 0
    width: 5
  - name: name
    type: string
    start: 5
    width: 20
  - name: date
    type: date
    start: 25
    width: 8
    format: "%Y%m%d"
"#;
        let def: SchemaDefinition = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(def.fields.as_ref().unwrap().len(), 3);
        assert_eq!(def.fields.as_ref().unwrap()[0].name, "id");
    }

    #[test]
    fn test_schema_defs_template() {
        let yaml = r#"
defs:
  base_string:
    name: _template
    type: string
    width: 20
    trim: true
fields:
  - name: first_name
    inherits: base_string
"#;
        let def: SchemaDefinition = serde_saphyr::from_str(yaml).unwrap();
        let defs = def.defs.as_ref().unwrap();
        assert!(defs.contains_key("base_string"));
        let tmpl = &defs["base_string"];
        assert_eq!(
            tmpl.field_type,
            Some(clinker_record::schema_def::FieldType::String)
        );
        assert_eq!(tmpl.width, Some(20));
    }

    #[test]
    fn test_schema_inherits_resolution() {
        let yaml = r#"
defs:
  base_string:
    name: _template
    type: string
    width: 20
    trim: true
fields:
  - name: city
    inherits: base_string
    start: 0
"#;
        let def: SchemaDefinition = serde_saphyr::from_str(yaml).unwrap();
        let resolved = resolve_schema(def).unwrap();
        match resolved {
            ResolvedSchema::SingleRecord { fields } => {
                assert_eq!(fields[0].name, "city");
                assert_eq!(
                    fields[0].field_type,
                    Some(clinker_record::schema_def::FieldType::String)
                );
                assert_eq!(fields[0].width, Some(20));
                assert_eq!(fields[0].trim, Some(true));
                assert_eq!(fields[0].start, Some(0));
                assert!(fields[0].inherits.is_none());
            }
            _ => panic!("expected SingleRecord"),
        }
    }

    #[test]
    fn test_schema_inherits_override() {
        let yaml = r#"
defs:
  base_field:
    name: _template
    type: string
    width: 10
    trim: true
fields:
  - name: amount
    inherits: base_field
    type: float
    start: 0
"#;
        let def: SchemaDefinition = serde_saphyr::from_str(yaml).unwrap();
        let resolved = resolve_schema(def).unwrap();
        match resolved {
            ResolvedSchema::SingleRecord { fields } => {
                // Field overrides template's type
                assert_eq!(
                    fields[0].field_type,
                    Some(clinker_record::schema_def::FieldType::Float)
                );
                // Template's width preserved
                assert_eq!(fields[0].width, Some(10));
            }
            _ => panic!("expected SingleRecord"),
        }
    }

    #[test]
    fn test_schema_inherits_chain_rejected() {
        let yaml = r#"
defs:
  base:
    name: _base
    type: string
  derived:
    name: _derived
    inherits: base
    width: 10
fields:
  - name: test
    inherits: derived
    start: 0
"#;
        let def: SchemaDefinition = serde_saphyr::from_str(yaml).unwrap();
        let err = resolve_schema(def).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("inherits chains"),
            "error should mention inherits chains: {msg}"
        );
    }

    #[test]
    fn test_schema_inherits_unknown_template() {
        let yaml = r#"
fields:
  - name: test
    inherits: nonexistent
    start: 0
"#;
        let def: SchemaDefinition = serde_saphyr::from_str(yaml).unwrap();
        let err = resolve_schema(def).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent"),
            "error should name the missing template: {msg}"
        );
    }

    #[test]
    fn test_schema_width_end_mutual_exclusion() {
        let yaml = r#"
fields:
  - name: bad_field
    type: string
    start: 0
    width: 10
    end: 10
"#;
        let def: SchemaDefinition = serde_saphyr::from_str(yaml).unwrap();
        let err = resolve_schema(def).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("mutually exclusive"),
            "error should mention mutual exclusion: {msg}"
        );
    }

    #[test]
    fn test_schema_unknown_property_rejected() {
        let yaml = r#"
fields:
  - name: test
    colour: red
"#;
        let err = serde_saphyr::from_str::<SchemaDefinition>(yaml);
        assert!(err.is_err(), "unknown property 'colour' should be rejected");
    }

    #[test]
    fn test_schema_load_yaml_and_yml() {
        let dir = tempfile::tempdir().unwrap();

        // .yaml extension
        let yaml_path = dir.path().join("test.yaml");
        std::fs::write(
            &yaml_path,
            "fields:\n  - name: id\n    type: integer\n",
        )
        .unwrap();
        let def = load_schema(&yaml_path).unwrap();
        assert_eq!(def.fields.as_ref().unwrap().len(), 1);

        // .yml extension
        let yml_path = dir.path().join("test.yml");
        std::fs::write(
            &yml_path,
            "fields:\n  - name: name\n    type: string\n",
        )
        .unwrap();
        let def = load_schema(&yml_path).unwrap();
        assert_eq!(def.fields.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_schema_source_filepath_deser() {
        use crate::config::SchemaSource;
        let yaml = "\"schemas/base.yaml\"";
        let source: SchemaSource = serde_saphyr::from_str(yaml).unwrap();
        match source {
            SchemaSource::FilePath(p) => assert_eq!(p, "schemas/base.yaml"),
            SchemaSource::Inline(_) => panic!("expected FilePath"),
        }
    }

    #[test]
    fn test_schema_source_inline_deser() {
        use crate::config::SchemaSource;
        let yaml = r#"
fields:
  - name: id
    type: integer
"#;
        let source: SchemaSource = serde_saphyr::from_str(yaml).unwrap();
        match source {
            SchemaSource::Inline(def) => {
                assert_eq!(def.fields.as_ref().unwrap().len(), 1);
            }
            SchemaSource::FilePath(_) => panic!("expected Inline"),
        }
    }

    #[test]
    fn test_schema_drop_in_base_rejected() {
        let yaml = r#"
fields:
  - name: to_drop
    type: string
    drop: true
"#;
        let def: SchemaDefinition = serde_saphyr::from_str(yaml).unwrap();
        let err = resolve_schema(def).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("drop"),
            "error should mention drop: {msg}"
        );
    }
}
