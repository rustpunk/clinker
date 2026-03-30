//! Core schema data model types.
//!
//! `SourceSchema` is the root type representing a `.schema.yaml` file.
//! `FieldDescriptor` is recursive — nested fields for JSON objects and XML
//! elements are represented as children.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// A parsed `.schema.yaml` file describing a data source's structure.
///
/// Schemas are version-controlled YAML files that define field names, types,
/// and constraints. They serve three purposes: autocomplete (field names feed
/// the editor), validation (pipelines referencing missing fields get warnings),
/// and documentation (schemas describe data structure without requiring data).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceSchema {
    /// Metadata from the `_schema` block.
    pub metadata: SchemaMetadata,
    /// Ordered list of top-level fields.
    pub fields: Vec<FieldDescriptor>,
    /// Path to the `.schema.yaml` file on disk.
    #[serde(skip)]
    pub path: PathBuf,
    /// Pipeline files that reference this schema via `schema:`.
    #[serde(skip)]
    pub referencing_pipelines: Vec<PathBuf>,
}

/// Metadata from the `_schema:` block in a schema file.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchemaMetadata {
    /// Human-readable schema name (e.g., "customers").
    pub name: String,
    /// Source format: csv, json, jsonl, xml.
    pub format: SourceFormat,
    /// Optional description of the data source.
    #[serde(default)]
    pub description: Option<String>,
    /// Schema version string (user-managed).
    #[serde(default)]
    pub version: Option<String>,
    /// Schema author.
    #[serde(default)]
    pub author: Option<String>,
    /// XML-only: the element name that wraps each record.
    #[serde(default)]
    pub record_element: Option<String>,
}

/// Supported source formats for schema files.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceFormat {
    Csv,
    Tsv,
    Json,
    Jsonl,
    Xml,
    Parquet,
}

impl SourceFormat {
    /// CSS-friendly label for UI rendering.
    pub fn label(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Json => "json",
            Self::Jsonl => "jsonl",
            Self::Xml => "xml",
            Self::Parquet => "parquet",
        }
    }

    /// Group format into broad categories for panel filtering.
    pub fn category(self) -> FormatCategory {
        match self {
            Self::Csv | Self::Tsv => FormatCategory::Csv,
            Self::Json | Self::Jsonl => FormatCategory::Json,
            Self::Xml => FormatCategory::Xml,
            Self::Parquet => FormatCategory::Parquet,
        }
    }
}

/// Broad format categories for UI filtering tabs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FormatCategory {
    Csv,
    Json,
    Xml,
    Parquet,
}

/// A single field in a schema.
///
/// Fields are recursive: JSON objects and XML elements with child elements
/// have nested `children`. Dot notation paths are derived from the tree
/// structure (e.g., `payload.type` for a nested field).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldDescriptor {
    /// Field name. XML attributes use `@` prefix (e.g., `@id`, `@currency`).
    pub name: String,
    /// Data type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Whether the field can be null/missing.
    #[serde(default = "default_true")]
    pub nullable: bool,
    /// Human-readable description.
    #[serde(default)]
    pub description: Option<String>,
    /// Allowed values (for constrained string fields).
    #[serde(default, rename = "enum")]
    pub enum_values: Option<Vec<String>>,
    /// Nested child fields (for `object` type).
    #[serde(default)]
    pub fields: Option<Vec<FieldDescriptor>>,
}

fn default_true() -> bool {
    true
}

/// Supported field data types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Int,
    Float,
    Bool,
    Date,
    Datetime,
    Object,
    Array,
}

impl FieldType {
    /// Short label for UI badges.
    pub fn badge(self) -> &'static str {
        match self {
            Self::String => "str",
            Self::Int => "int",
            Self::Float => "float",
            Self::Bool => "bool",
            Self::Date => "date",
            Self::Datetime => "dt",
            Self::Object => "obj",
            Self::Array => "arr",
        }
    }
}

impl FieldDescriptor {
    /// Whether this field is an XML attribute (name starts with `@`).
    pub fn is_xml_attribute(&self) -> bool {
        self.name.starts_with('@')
    }

    /// Count total fields including nested children (recursive).
    pub fn field_count(&self) -> usize {
        1 + self
            .fields
            .as_ref()
            .map(|children| children.iter().map(|c| c.field_count()).sum::<usize>())
            .unwrap_or(0)
    }

    /// Collect all field names as dot-notation paths (e.g., `payload.type`).
    pub fn flat_field_names(&self, prefix: &str) -> Vec<String> {
        let full_name = if prefix.is_empty() {
            self.name.clone()
        } else {
            format!("{prefix}.{}", self.name)
        };

        let mut names = vec![full_name.clone()];
        if let Some(ref children) = self.fields {
            for child in children {
                names.extend(child.flat_field_names(&full_name));
            }
        }
        names
    }
}

impl SourceSchema {
    /// Total number of fields (including nested).
    pub fn total_field_count(&self) -> usize {
        self.fields.iter().map(|f| f.field_count()).sum()
    }

    /// All field names as flat dot-notation paths.
    pub fn all_field_names(&self) -> Vec<String> {
        self.fields
            .iter()
            .flat_map(|f| f.flat_field_names(""))
            .collect()
    }
}

/// Index of all schemas in a workspace.
///
/// Provides fast lookup by field name (which schemas contain a given field)
/// and by path (get schema for a `.schema.yaml` file).
#[derive(Clone, Debug, Default)]
pub struct SchemaIndex {
    /// All schemas keyed by their file path.
    pub schemas: HashMap<PathBuf, SourceSchema>,
    /// Field name → list of schema paths containing that field.
    /// Uses dot-notation for nested fields.
    pub field_index: HashMap<String, Vec<PathBuf>>,
}

impl SchemaIndex {
    /// Build an index from a collection of parsed schemas.
    pub fn build(schemas: Vec<SourceSchema>) -> Self {
        let mut field_index: HashMap<String, Vec<PathBuf>> = HashMap::new();
        let mut schema_map = HashMap::new();

        for schema in schemas {
            let path = schema.path.clone();
            for field_name in schema.all_field_names() {
                field_index
                    .entry(field_name)
                    .or_default()
                    .push(path.clone());
            }
            schema_map.insert(path, schema);
        }

        Self {
            schemas: schema_map,
            field_index,
        }
    }

    /// Look up a schema by its file path.
    pub fn get(&self, path: &PathBuf) -> Option<&SourceSchema> {
        self.schemas.get(path)
    }

    /// Find all schemas containing a field name.
    pub fn schemas_with_field(&self, field_name: &str) -> &[PathBuf] {
        self.field_index
            .get(field_name)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Total number of schemas in the index.
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    /// All unique field names across all schemas.
    pub fn all_field_names(&self) -> Vec<&str> {
        self.field_index.keys().map(|s| s.as_str()).collect()
    }
}
