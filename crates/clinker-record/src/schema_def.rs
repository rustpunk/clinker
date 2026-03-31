use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Field type enum -- parse-time validated, exhaustive match downstream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    String,
    Integer,
    Float,
    Decimal,
    Boolean,
    Date,
    #[serde(rename = "datetime")]
    DateTime,
    Object,
    Array,
}

/// Field justification for fixed-width formatting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Justify {
    Left,
    Right,
}

/// Line separator mode for fixed-width I/O.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineSeparator {
    Lf,
    CrLf,
    None,
}

/// Truncation policy for fixed-width writer.
/// Default resolved by field type: numeric -> Error, string -> Warn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncationPolicy {
    Error,
    Warn,
    Silent,
}

/// A field definition from a schema file, inline schema, or override.
/// All fields except `name` are Optional to support both full definitions
/// and partial overrides (schema_overrides use the same struct).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldDef {
    pub name: String,
    #[serde(default, rename = "type")]
    pub field_type: Option<FieldType>,
    pub required: Option<bool>,
    pub format: Option<String>,
    pub coerce: Option<bool>,
    pub default: Option<serde_json::Value>,
    #[serde(rename = "enum")]
    pub allowed_values: Option<Vec<String>>,
    pub alias: Option<String>,
    pub inherits: Option<String>,
    // Fixed-width positioning
    pub start: Option<usize>,
    pub width: Option<usize>,
    pub end: Option<usize>,
    pub justify: Option<Justify>,
    pub pad: Option<String>,
    pub trim: Option<bool>,
    pub truncation: Option<TruncationPolicy>,
    // Decimal
    pub precision: Option<u8>,
    pub scale: Option<u8>,
    // XML
    pub path: Option<String>,
    // Override-only (validated as None in schema file contexts)
    pub drop: Option<bool>,
    pub record: Option<String>,
}

/// Discriminator for multi-record dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Discriminator {
    pub start: Option<usize>,
    pub width: Option<usize>,
    pub field: Option<String>,
}

/// A record type definition within a multi-record schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordTypeDef {
    pub id: String,
    pub tag: String,
    pub description: Option<String>,
    pub parent: Option<String>,
    pub join_key: Option<String>,
    pub fields: Vec<FieldDef>,
}

/// Structural ordering constraint (parsed, validated in Phase 11).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructureConstraint {
    pub record: String,
    pub count: String,
}

/// Parsed schema -- from file or inline. Single struct, one resolution path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaDefinition {
    pub version: Option<String>,
    pub format: Option<String>,
    pub encoding: Option<String>,
    pub defs: Option<HashMap<String, FieldDef>>,
    pub fields: Option<Vec<FieldDef>>,
    pub records: Option<Vec<RecordTypeDef>>,
    pub discriminator: Option<Discriminator>,
    pub structure: Option<Vec<StructureConstraint>>,
}
