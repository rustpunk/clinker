//! `.schema.yaml` file parsing.
//!
//! Schema files use a `_schema:` metadata block followed by a `fields:` list.
//! Parsing is format-agnostic — the same parser handles CSV, JSON, and XML
//! schemas. Format-specific semantics (e.g., `@` prefix for XML attributes,
//! nested `fields` for JSON objects) are encoded in the data model, not the
//! parser.

use std::path::Path;

use serde::Deserialize;

use crate::model::{FieldDescriptor, SchemaMetadata, SourceSchema};

/// Intermediate deserialization target matching the `.schema.yaml` file layout.
#[derive(Deserialize)]
struct SchemaFile {
    _schema: SchemaMetadata,
    fields: Vec<FieldDescriptor>,
}

/// Parse a `.schema.yaml` file from a string.
///
/// Returns a `SourceSchema` with `path` set to the provided path. The
/// `referencing_pipelines` field is left empty — it must be populated by
/// the caller (typically during index building).
pub fn parse_schema(yaml: &str, path: &Path) -> Result<SourceSchema, SchemaParseError> {
    let file: SchemaFile =
        serde_saphyr::from_str(yaml).map_err(|e| SchemaParseError::Yaml(e.to_string()))?;

    Ok(SourceSchema {
        metadata: file._schema,
        fields: file.fields,
        path: path.to_path_buf(),
        referencing_pipelines: Vec::new(),
    })
}

/// Parse a `.schema.yaml` file from disk.
pub fn parse_schema_file(path: &Path) -> Result<SourceSchema, SchemaParseError> {
    let content = std::fs::read_to_string(path).map_err(|e| SchemaParseError::Io(e.to_string()))?;
    parse_schema(&content, path)
}

/// Errors during schema parsing.
#[derive(Debug, Clone)]
pub enum SchemaParseError {
    Io(String),
    Yaml(String),
}

impl std::fmt::Display for SchemaParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "schema I/O error: {e}"),
            Self::Yaml(e) => write!(f, "schema YAML parse error: {e}"),
        }
    }
}

impl std::error::Error for SchemaParseError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{FieldType, SourceFormat};
    use std::path::PathBuf;

    #[test]
    fn test_parse_csv_schema() {
        let yaml = r#"
_schema:
  name: customers
  format: csv
  description: "Customer master data from the CRM export"
  version: "1.0"

fields:
  - name: id
    type: int
    nullable: false
    description: "Unique customer identifier"
  - name: email
    type: string
    nullable: false
  - name: status
    type: string
    nullable: false
    enum: [active, inactive, churned]
  - name: amount
    type: float
    nullable: false
  - name: ref_code
    type: string
    nullable: true
    description: "Referral code, null for direct signups"
"#;
        let schema = parse_schema(yaml, Path::new("schemas/customers.schema.yaml")).unwrap();

        assert_eq!(schema.metadata.name, "customers");
        assert_eq!(schema.metadata.format, SourceFormat::Csv);
        assert_eq!(
            schema.metadata.description.as_deref(),
            Some("Customer master data from the CRM export")
        );
        assert_eq!(schema.metadata.version.as_deref(), Some("1.0"));
        assert_eq!(schema.fields.len(), 5);

        // id field
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[0].field_type, FieldType::Int);
        assert!(!schema.fields[0].nullable);

        // status field with enum
        assert_eq!(schema.fields[2].name, "status");
        let enums = schema.fields[2].enum_values.as_ref().unwrap();
        assert_eq!(enums, &["active", "inactive", "churned"]);

        // ref_code nullable
        assert!(schema.fields[4].nullable);

        // Field count
        assert_eq!(schema.total_field_count(), 5);
    }

    #[test]
    fn test_parse_json_nested_schema() {
        let yaml = r#"
_schema:
  name: events
  format: jsonl
  description: "User activity events"

fields:
  - name: event_id
    type: string
    nullable: false
  - name: action
    type: string
    nullable: false
    enum: [click, purchase, view, signup]
  - name: payload
    type: object
    nullable: true
    fields:
      - name: type
        type: string
      - name: amount
        type: float
      - name: metadata
        type: object
"#;
        let schema = parse_schema(yaml, Path::new("schemas/events.schema.yaml")).unwrap();

        assert_eq!(schema.metadata.name, "events");
        assert_eq!(schema.metadata.format, SourceFormat::Jsonl);
        assert_eq!(schema.fields.len(), 3);

        // Nested payload
        let payload = &schema.fields[2];
        assert_eq!(payload.name, "payload");
        assert_eq!(payload.field_type, FieldType::Object);
        let children = payload.fields.as_ref().unwrap();
        assert_eq!(children.len(), 3);
        assert_eq!(children[0].name, "type");
        assert_eq!(children[1].name, "amount");
        assert_eq!(children[2].name, "metadata");

        // Total field count (3 top-level + 3 nested)
        assert_eq!(schema.total_field_count(), 6);

        // Flat field names
        let names = schema.all_field_names();
        assert!(names.contains(&"event_id".to_string()));
        assert!(names.contains(&"payload".to_string()));
        assert!(names.contains(&"payload.type".to_string()));
        assert!(names.contains(&"payload.amount".to_string()));
        assert!(names.contains(&"payload.metadata".to_string()));
    }

    #[test]
    fn test_parse_xml_schema_with_attributes() {
        let yaml = r#"
_schema:
  name: orders
  format: xml
  record_element: order

fields:
  - name: "@id"
    type: int
    nullable: false
    description: "Order ID (XML attribute)"
  - name: "@currency"
    type: string
    nullable: false
    enum: [USD, EUR, GBP]
  - name: customer
    type: string
    nullable: false
  - name: price
    type: float
    nullable: false
"#;
        let schema = parse_schema(yaml, Path::new("schemas/orders.schema.yaml")).unwrap();

        assert_eq!(schema.metadata.name, "orders");
        assert_eq!(schema.metadata.format, SourceFormat::Xml);
        assert_eq!(schema.metadata.record_element.as_deref(), Some("order"));
        assert_eq!(schema.fields.len(), 4);

        // XML attributes
        assert!(schema.fields[0].is_xml_attribute());
        assert_eq!(schema.fields[0].name, "@id");
        assert!(schema.fields[1].is_xml_attribute());
        assert_eq!(schema.fields[1].name, "@currency");

        // Regular elements
        assert!(!schema.fields[2].is_xml_attribute());
    }

    #[test]
    fn test_schema_index_build() {
        let csv_yaml = r#"
_schema:
  name: customers
  format: csv
fields:
  - name: id
    type: int
    nullable: false
  - name: email
    type: string
"#;
        let json_yaml = r#"
_schema:
  name: events
  format: json
fields:
  - name: id
    type: string
    nullable: false
  - name: action
    type: string
"#;
        let s1 = parse_schema(csv_yaml, Path::new("schemas/customers.schema.yaml")).unwrap();
        let s2 = parse_schema(json_yaml, Path::new("schemas/events.schema.yaml")).unwrap();

        let index = crate::model::SchemaIndex::build(vec![s1, s2]);

        assert_eq!(index.len(), 2);
        assert!(!index.is_empty());

        // "id" exists in both schemas
        let id_schemas = index.schemas_with_field("id");
        assert_eq!(id_schemas.len(), 2);

        // "email" only in customers
        let email_schemas = index.schemas_with_field("email");
        assert_eq!(email_schemas.len(), 1);
        assert_eq!(
            email_schemas[0],
            PathBuf::from("schemas/customers.schema.yaml")
        );

        // "action" only in events
        let action_schemas = index.schemas_with_field("action");
        assert_eq!(action_schemas.len(), 1);
    }

    #[test]
    fn test_parse_error_on_invalid_yaml() {
        let yaml = "this is not valid yaml: [";
        let result = parse_schema(yaml, Path::new("bad.schema.yaml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_nullable_defaults_to_true() {
        let yaml = r#"
_schema:
  name: test
  format: csv
fields:
  - name: some_field
    type: string
"#;
        let schema = parse_schema(yaml, Path::new("test.schema.yaml")).unwrap();
        // nullable should default to true when not specified
        assert!(schema.fields[0].nullable);
    }
}
