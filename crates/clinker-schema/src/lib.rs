//! Source schema parsing, indexing, and validation for Clinker pipelines.
//!
//! This crate owns `.schema.yaml` file parsing, schema discovery, field
//! indexing, and pipeline validation against linked schemas. It is shared
//! between `clinker-kiln` (IDE) and `clinker` (CLI).

pub mod discovery;
pub mod model;
pub mod parse;
pub mod validate;

pub use discovery::build_workspace_schema_index;
pub use model::*;
pub use parse::{parse_schema, parse_schema_file, SchemaParseError};
pub use validate::{validate_pipeline, SchemaWarning, WarningKind};
