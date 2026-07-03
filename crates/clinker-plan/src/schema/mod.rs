//! External schema-file loading for the unified [`SourceSchema`] model.
//!
//! A source's schema is normally declared inline (see
//! [`clinker_format::SourceSchema`]). The [`SourceSchema::File`] variant instead
//! names a shareable external `.schema.yaml`; [`load_source_schema`] reads and
//! parses that file into an inline [`SourceSchema`]. A schema file may not
//! itself point at another file — one level of indirection only.

use std::path::Path;

use clinker_format::SourceSchema;

/// Schema subsystem errors.
#[derive(Debug)]
pub enum SchemaError {
    Io(std::io::Error),
    Yaml(crate::yaml::YamlError),
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

impl From<crate::yaml::YamlError> for SchemaError {
    fn from(e: crate::yaml::YamlError) -> Self {
        Self::Yaml(e)
    }
}

/// Load an external `.schema.yaml` file and parse it into an inline
/// [`SourceSchema`].
///
/// # Errors
///
/// Returns [`SchemaError::Io`] when the file cannot be read,
/// [`SchemaError::Yaml`] when it does not parse as a [`SourceSchema`], and
/// [`SchemaError::Validation`] when the loaded schema is itself a
/// [`SourceSchema::File`] (a schema file may not point at another file).
pub fn load_source_schema(path: &Path) -> Result<SourceSchema, SchemaError> {
    let yaml = std::fs::read_to_string(path)?;
    let schema: SourceSchema = crate::yaml::from_str(&yaml)?;
    if let SourceSchema::File(inner) = &schema {
        return Err(SchemaError::Validation(format!(
            "schema file '{}' points at another schema file '{inner}'; nested schema files are \
             not allowed — inline the columns",
            path.display()
        )));
    }
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_format::Column;
    use cxl::typecheck::Type;

    #[test]
    fn load_single_record_schema_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orders.schema.yaml");
        std::fs::write(
            &path,
            "- { name: id, type: int }\n- { name: name, type: string }\n",
        )
        .unwrap();
        let schema = load_source_schema(&path).unwrap();
        let cols = schema.as_columns().expect("single-record columns");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], Column::bare("id", Type::Int));
    }

    #[test]
    fn load_multi_record_schema_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mr.schema.yaml");
        std::fs::write(
            &path,
            "discriminator: { start: 0, width: 1 }\n\
             records:\n\
             \x20 - { id: detail, tag: D, columns: [ { name: amount, type: float, start: 1, width: 9 } ] }\n",
        )
        .unwrap();
        let schema = load_source_schema(&path).unwrap();
        assert!(matches!(schema, SourceSchema::MultiRecord { .. }));
    }

    #[test]
    fn nested_schema_file_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested.schema.yaml");
        std::fs::write(&path, "\"other.schema.yaml\"\n").unwrap();
        let err = load_source_schema(&path).unwrap_err();
        assert!(err.to_string().contains("nested schema files"), "{err}");
    }

    #[test]
    fn missing_file_is_io_error() {
        let err =
            load_source_schema(Path::new("/nonexistent/does-not-exist.schema.yaml")).unwrap_err();
        assert!(matches!(err, SchemaError::Io(_)));
    }
}
