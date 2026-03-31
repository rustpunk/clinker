use clinker_record::schema_def::FieldDef;

use super::{ResolvedSchema, SchemaError};

/// Apply override entries onto a resolved schema.
/// Overrides use the same FieldDef struct with drop/record fields.
pub fn resolve_overrides(
    schema: &mut ResolvedSchema,
    overrides: &[FieldDef],
) -> Result<(), SchemaError> {
    for patch in overrides {
        if let Some(true) = patch.drop {
            drop_field(schema, &patch.name, patch.record.as_deref())?;
        } else {
            merge_or_append(schema, patch)?;
        }
    }
    Ok(())
}

/// Find and deep-merge an override into a matching field, or append as new.
fn merge_or_append(schema: &mut ResolvedSchema, patch: &FieldDef) -> Result<(), SchemaError> {
    let fields = get_fields_mut(schema, patch.record.as_deref())?;

    if let Some(base) = fields.iter_mut().find(|f| f.name == patch.name) {
        merge_field(base, patch);
    } else {
        // Append as new field (strip override-only metadata)
        let mut new_field = patch.clone();
        new_field.drop = None;
        new_field.record = None;
        fields.push(new_field);
    }
    Ok(())
}

/// Remove a field by name from the schema. Error if not found.
fn drop_field(
    schema: &mut ResolvedSchema,
    name: &str,
    record_scope: Option<&str>,
) -> Result<(), SchemaError> {
    let fields = get_fields_mut(schema, record_scope)?;

    let pos = fields.iter().position(|f| f.name == name).ok_or_else(|| {
        SchemaError::Validation(format!(
            "schema_overrides: cannot drop field '{name}' — not found in schema"
        ))
    })?;
    fields.remove(pos);
    Ok(())
}

/// Deep-merge: patch's non-None values overwrite base.
fn merge_field(base: &mut FieldDef, patch: &FieldDef) {
    if patch.field_type.is_some() {
        base.field_type = patch.field_type.clone();
    }
    if patch.required.is_some() {
        base.required = patch.required;
    }
    if patch.format.is_some() {
        base.format = patch.format.clone();
    }
    if patch.coerce.is_some() {
        base.coerce = patch.coerce;
    }
    if patch.default.is_some() {
        base.default = patch.default.clone();
    }
    if patch.allowed_values.is_some() {
        base.allowed_values = patch.allowed_values.clone();
    }
    if patch.alias.is_some() {
        base.alias = patch.alias.clone();
    }
    if patch.start.is_some() {
        base.start = patch.start;
    }
    if patch.width.is_some() {
        base.width = patch.width;
    }
    if patch.end.is_some() {
        base.end = patch.end;
    }
    if patch.justify.is_some() {
        base.justify = patch.justify.clone();
    }
    if patch.pad.is_some() {
        base.pad = patch.pad.clone();
    }
    if patch.trim.is_some() {
        base.trim = patch.trim;
    }
    if patch.truncation.is_some() {
        base.truncation = patch.truncation.clone();
    }
    if patch.precision.is_some() {
        base.precision = patch.precision;
    }
    if patch.scale.is_some() {
        base.scale = patch.scale;
    }
    if patch.path.is_some() {
        base.path = patch.path.clone();
    }
}

/// Get mutable reference to the field list, optionally scoped by record type.
fn get_fields_mut<'a>(
    schema: &'a mut ResolvedSchema,
    record_scope: Option<&str>,
) -> Result<&'a mut Vec<FieldDef>, SchemaError> {
    match (schema, record_scope) {
        (ResolvedSchema::SingleRecord { fields }, None) => Ok(fields),
        (ResolvedSchema::SingleRecord { .. }, Some(rec)) => Err(SchemaError::Validation(format!(
            "schema_overrides: record scope '{rec}' used but schema is single-record"
        ))),
        (ResolvedSchema::MultiRecord { record_types, .. }, Some(rec)) => {
            let rt = record_types
                .iter_mut()
                .find(|rt| rt.tag == rec)
                .ok_or_else(|| {
                    SchemaError::Validation(format!(
                        "schema_overrides: record type '{rec}' not found in schema"
                    ))
                })?;
            Ok(&mut rt.fields)
        }
        (ResolvedSchema::MultiRecord { .. }, None) => Err(SchemaError::Validation(
            "schema_overrides: multi-record schema requires 'record' scope on overrides".into(),
        )),
    }
}

/// Build alias map from resolved schema fields: original_name → alias.
pub fn build_alias_map(schema: &ResolvedSchema) -> std::collections::HashMap<String, String> {
    let mut aliases = std::collections::HashMap::new();
    match schema {
        ResolvedSchema::SingleRecord { fields } => {
            for field in fields {
                if let Some(ref alias) = field.alias {
                    aliases.insert(field.name.clone(), alias.clone());
                }
            }
        }
        ResolvedSchema::MultiRecord { record_types, .. } => {
            for rt in record_types {
                for field in &rt.fields {
                    if let Some(ref alias) = field.alias {
                        aliases.insert(field.name.clone(), alias.clone());
                    }
                }
            }
        }
    }
    aliases
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::schema_def::{Discriminator, FieldType, Justify, RecordTypeDef};

    fn field(name: &str) -> FieldDef {
        FieldDef {
            name: name.into(),
            field_type: None,
            required: None,
            format: None,
            coerce: None,
            default: None,
            allowed_values: None,
            alias: None,
            inherits: None,
            start: None,
            width: None,
            end: None,
            justify: None,
            pad: None,
            trim: None,
            truncation: None,
            precision: None,
            scale: None,
            path: None,
            drop: None,
            record: None,
        }
    }

    fn base_schema() -> ResolvedSchema {
        let mut id = field("id");
        id.field_type = Some(FieldType::Integer);
        id.width = Some(5);

        let mut amount = field("amount");
        amount.field_type = Some(FieldType::Integer);
        amount.width = Some(10);
        amount.justify = Some(Justify::Right);
        amount.pad = Some(" ".into());
        amount.required = Some(true);

        let mut name = field("name");
        name.field_type = Some(FieldType::String);
        name.width = Some(20);

        ResolvedSchema::SingleRecord {
            fields: vec![id, amount, name],
        }
    }

    #[test]
    fn test_override_merge_existing_field() {
        let mut schema = base_schema();
        let mut patch = field("amount");
        patch.field_type = Some(FieldType::Float);

        resolve_overrides(&mut schema, &[patch]).unwrap();

        let fields = match &schema {
            ResolvedSchema::SingleRecord { fields } => fields,
            _ => panic!("expected SingleRecord"),
        };
        let amount = fields.iter().find(|f| f.name == "amount").unwrap();
        assert_eq!(amount.field_type, Some(FieldType::Float)); // replaced
        assert_eq!(amount.width, Some(10)); // preserved
    }

    #[test]
    fn test_override_append_new_field() {
        let mut schema = base_schema();
        let mut patch = field("email");
        patch.field_type = Some(FieldType::String);

        resolve_overrides(&mut schema, &[patch]).unwrap();

        let fields = match &schema {
            ResolvedSchema::SingleRecord { fields } => fields,
            _ => panic!("expected SingleRecord"),
        };
        assert_eq!(fields.len(), 4);
        assert_eq!(fields.last().unwrap().name, "email");
        assert_eq!(fields.last().unwrap().field_type, Some(FieldType::String));
    }

    #[test]
    fn test_override_drop_field() {
        let mut schema = base_schema();
        let mut patch = field("amount");
        patch.drop = Some(true);

        resolve_overrides(&mut schema, &[patch]).unwrap();

        let fields = match &schema {
            ResolvedSchema::SingleRecord { fields } => fields,
            _ => panic!("expected SingleRecord"),
        };
        assert_eq!(fields.len(), 2);
        assert!(fields.iter().all(|f| f.name != "amount"));
    }

    #[test]
    fn test_override_drop_nonexistent_field() {
        let mut schema = base_schema();
        let mut patch = field("nonexistent");
        patch.drop = Some(true);

        let err = resolve_overrides(&mut schema, &[patch]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent"),
            "error should name field: {msg}"
        );
        assert!(
            msg.contains("not found"),
            "error should say not found: {msg}"
        );
    }

    #[test]
    fn test_override_alias() {
        let mut schema = base_schema();
        // Add a field to alias
        let fields = match &mut schema {
            ResolvedSchema::SingleRecord { fields } => fields,
            _ => panic!("expected SingleRecord"),
        };
        let mut employee_name = field("employee_name");
        employee_name.field_type = Some(FieldType::String);
        fields.push(employee_name);

        let mut patch = field("employee_name");
        patch.alias = Some("emp_name".into());

        resolve_overrides(&mut schema, &[patch]).unwrap();

        let fields = match &schema {
            ResolvedSchema::SingleRecord { fields } => fields,
            _ => panic!("expected SingleRecord"),
        };
        let f = fields.iter().find(|f| f.name == "employee_name").unwrap();
        assert_eq!(f.alias, Some("emp_name".into()));
        // CXL lookup uses original name — name is unchanged
        assert_eq!(f.name, "employee_name");

        // Build alias map
        let aliases = build_alias_map(&schema);
        assert_eq!(aliases.get("employee_name").unwrap(), "emp_name");
    }

    #[test]
    fn test_override_alias_mapping_interaction() {
        // Alias creates identity boundary: mapping keys use post-alias names
        let mut schema = base_schema();
        let fields = match &mut schema {
            ResolvedSchema::SingleRecord { fields } => fields,
            _ => panic!("expected SingleRecord"),
        };
        let mut employee_id = field("employee_id");
        employee_id.field_type = Some(FieldType::Integer);
        fields.push(employee_id);

        let mut patch = field("employee_id");
        patch.alias = Some("emp_id".into());
        resolve_overrides(&mut schema, &[patch]).unwrap();

        let aliases = build_alias_map(&schema);

        // Simulate alias application to emitted fields
        let mut emitted = indexmap::IndexMap::new();
        emitted.insert(
            "employee_id".to_string(),
            clinker_record::Value::Integer(42),
        );
        crate::projection::apply_aliases(&mut emitted, &aliases);

        // Post-alias name is emp_id
        assert!(
            emitted.contains_key("emp_id"),
            "post-alias name should be emp_id"
        );
        assert!(
            !emitted.contains_key("employee_id"),
            "pre-alias name should be gone"
        );

        // Output mapping uses post-alias name
        let mut mapping = indexmap::IndexMap::new();
        mapping.insert("emp_id".to_string(), "Employee ID".to_string());

        // Pre-alias name should NOT match
        assert!(!mapping.contains_key("employee_id"));
        // Post-alias name should match
        assert!(mapping.contains_key("emp_id"));
    }

    #[test]
    fn test_override_record_scoped() {
        // Multi-record schema: override scoped to DETAIL only
        let mut header_field = field("id");
        header_field.field_type = Some(FieldType::Integer);
        let mut detail_id = field("id");
        detail_id.field_type = Some(FieldType::Integer);
        let mut detail_amount = field("amount");
        detail_amount.field_type = Some(FieldType::Integer);

        let mut schema = ResolvedSchema::MultiRecord {
            discriminator: Discriminator {
                field: Some("rec_type".into()),
                start: None,
                width: None,
            },
            record_types: vec![
                RecordTypeDef {
                    id: "header".into(),
                    tag: "HEADER".into(),
                    description: None,
                    fields: vec![header_field],
                    parent: None,
                    join_key: None,
                },
                RecordTypeDef {
                    id: "detail".into(),
                    tag: "DETAIL".into(),
                    description: None,
                    fields: vec![detail_id, detail_amount],
                    parent: None,
                    join_key: None,
                },
            ],
        };

        // Override amount type in DETAIL only
        let mut patch = field("amount");
        patch.field_type = Some(FieldType::Float);
        patch.record = Some("DETAIL".into());

        resolve_overrides(&mut schema, &[patch]).unwrap();

        match &schema {
            ResolvedSchema::MultiRecord { record_types, .. } => {
                // HEADER should be unchanged (has no amount field)
                let header = record_types.iter().find(|r| r.tag == "HEADER").unwrap();
                assert_eq!(header.fields.len(), 1);

                // DETAIL amount should be Float now
                let detail = record_types.iter().find(|r| r.tag == "DETAIL").unwrap();
                let amount = detail.fields.iter().find(|f| f.name == "amount").unwrap();
                assert_eq!(amount.field_type, Some(FieldType::Float));
            }
            _ => panic!("expected MultiRecord"),
        }
    }

    #[test]
    fn test_override_multiple_fields() {
        let mut schema = base_schema();

        let overrides = vec![
            // 1. Merge: change amount type
            {
                let mut p = field("amount");
                p.field_type = Some(FieldType::Float);
                p
            },
            // 2. Append: add email
            {
                let mut p = field("email");
                p.field_type = Some(FieldType::String);
                p
            },
            // 3. Drop: remove name
            {
                let mut p = field("name");
                p.drop = Some(true);
                p
            },
        ];

        resolve_overrides(&mut schema, &overrides).unwrap();

        let fields = match &schema {
            ResolvedSchema::SingleRecord { fields } => fields,
            _ => panic!("expected SingleRecord"),
        };

        // Started with [id, amount, name], applied merge+append+drop
        assert_eq!(fields.len(), 3); // id, amount(modified), email(new) — name dropped
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[1].name, "amount");
        assert_eq!(fields[1].field_type, Some(FieldType::Float));
        assert_eq!(fields[2].name, "email");
        assert!(fields.iter().all(|f| f.name != "name"));
    }

    #[test]
    fn test_override_deep_merge_preserves_unset() {
        let mut schema = base_schema();

        // Override only sets format — everything else on base should be preserved
        let mut patch = field("amount");
        patch.format = Some("%d".into());

        resolve_overrides(&mut schema, &[patch]).unwrap();

        let fields = match &schema {
            ResolvedSchema::SingleRecord { fields } => fields,
            _ => panic!("expected SingleRecord"),
        };
        let amount = fields.iter().find(|f| f.name == "amount").unwrap();
        assert_eq!(amount.format, Some("%d".into())); // set by override
        assert_eq!(amount.justify, Some(Justify::Right)); // preserved from base
        assert_eq!(amount.pad, Some(" ".into())); // preserved from base
        assert_eq!(amount.required, Some(true)); // preserved from base
        assert_eq!(amount.field_type, Some(FieldType::Integer)); // preserved from base
    }
}
