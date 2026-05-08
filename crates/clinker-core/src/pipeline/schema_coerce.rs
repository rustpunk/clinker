//! Schema-based type coercion + declared-schema reprojection for source records.
//!
//! Wraps a `FormatReader` and returns records whose `Arc<Schema>` is the
//! source's user-declared schema, with the per-Source `OnUnmapped`
//! policy applied to undeclared input fields:
//!
//! - **`OnUnmapped::Drop`** (matches Snowflake `MATCH_BY_COLUMN_NAME`
//!   "extra columns ignored" and dbt's `on_schema_change=ignore`):
//!   reader columns absent from the declaration drop silently.
//! - **`OnUnmapped::Reject`** (matches dlt's `freeze` mode): any input
//!   record carrying a key not in the declared schema fails the source
//!   with [`FormatError::UndeclaredField`].
//!
//! Declared columns missing from a particular input record materialize
//! as `Value::Null`. Values declared with a concrete type (`Type::Int`,
//! `Type::Float`, `Type::Bool`, etc.) are coerced; `Type::String` /
//! `Type::Any` skip coercion. The `to_*` / `try_*` CXL builtins remain
//! available for derived fields computed during the pipeline.

use std::collections::HashSet;
use std::sync::Arc;

use clinker_format::error::FormatError;
use clinker_format::traits::FormatReader;
use clinker_record::{Record, Schema, SchemaBuilder, Value, coercion};
use cxl::typecheck::Type;

use crate::config::pipeline_node::{ColumnDecl, OnUnmapped};

/// Wraps a `FormatReader` and reprojects every record onto the
/// user-declared `Arc<Schema>`, applying the per-Source `OnUnmapped`
/// policy to undeclared input fields.
pub struct CoercingReader {
    inner: Box<dyn FormatReader>,
    /// Declared column names — lookup set for the `Reject` policy's
    /// "is this key in the declaration?" check.
    declared_names: HashSet<Box<str>>,
    /// Declared schema — the single `Arc<Schema>` every reprojected
    /// record carries.
    declared_schema: Arc<Schema>,
    /// Per-declared-column coercion target (`None` for pass-through).
    /// Indexed by position in the declared schema.
    targets: Vec<Option<Type>>,
    policy: OnUnmapped,
    /// Source identifier for diagnostics.
    source_name: Box<str>,
}

impl CoercingReader {
    /// Build a coercing reader from a format reader, the user-declared
    /// `schema:` block, and the per-Source `on_unmapped` policy.
    pub fn new(
        mut inner: Box<dyn FormatReader>,
        schema_decl: &[ColumnDecl],
        policy: OnUnmapped,
        source_name: &str,
    ) -> Result<Self, FormatError> {
        // Trigger schema discovery on the inner reader so the first
        // record isn't gated behind an on-demand schema call.
        inner.schema()?;

        let declared_schema: Arc<Schema> = schema_decl
            .iter()
            .map(|c| c.name.as_str())
            .collect::<SchemaBuilder>()
            .build();
        let declared_names: HashSet<Box<str>> =
            schema_decl.iter().map(|c| c.name.as_str().into()).collect();
        let targets: Vec<Option<Type>> = schema_decl
            .iter()
            .map(|c| {
                let target = unwrap_nullable(&c.ty);
                match target {
                    Type::String | Type::Any | Type::Null => None,
                    other => Some(other.clone()),
                }
            })
            .collect();

        Ok(CoercingReader {
            inner,
            declared_names,
            declared_schema,
            targets,
            policy,
            source_name: source_name.into(),
        })
    }

    /// Reproject `record` onto the declared schema Arc.
    ///
    /// For each declared column, pull the value from the underlying
    /// record (by name), coerce if the declaration has a target type,
    /// and write at the declared position. When `policy` is
    /// [`OnUnmapped::Reject`], any input key absent from the
    /// declaration fails the source. Otherwise reader columns absent
    /// from the declaration drop silently.
    fn reproject(&self, record: &Record) -> Result<Record, FormatError> {
        if matches!(self.policy, OnUnmapped::Reject) {
            for (k, _) in record.iter_all_fields() {
                if !self.declared_names.contains(k) {
                    return Err(FormatError::UndeclaredField {
                        source: self.source_name.to_string(),
                        field: k.to_string(),
                    });
                }
            }
        }

        let mut values: Vec<Value> = Vec::with_capacity(self.declared_schema.column_count());
        for (i, col) in self.declared_schema.columns().iter().enumerate() {
            let raw = record.get(col).cloned().unwrap_or(Value::Null);
            let coerced = match &self.targets[i] {
                Some(target) => coerce_value(&raw, target).unwrap_or(raw),
                None => raw,
            };
            values.push(coerced);
        }
        Ok(Record::new(Arc::clone(&self.declared_schema), values))
    }
}

impl FormatReader for CoercingReader {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.declared_schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        match self.inner.next_record()? {
            Some(record) => Ok(Some(self.reproject(&record)?)),
            None => Ok(None),
        }
    }

    fn current_source_file(&self) -> Option<&Arc<str>> {
        self.inner.current_source_file()
    }
}

/// Unwrap Nullable to get the inner type for coercion.
fn unwrap_nullable(ty: &Type) -> &Type {
    match ty {
        Type::Nullable(inner) => unwrap_nullable(inner),
        other => other,
    }
}

/// Coerce a value to the target type. Returns None if coercion fails
/// (value passes through unchanged — lenient behavior at read time).
fn coerce_value(value: &Value, target: &Type) -> Option<Value> {
    match target {
        Type::Int => coercion::coerce_to_int_lenient(value),
        Type::Float => coercion::coerce_to_float_lenient(value),
        Type::Bool => coercion::coerce_to_bool_lenient(value),
        Type::Date => coercion::coerce_to_date_lenient(value, &[]),
        Type::DateTime => coercion::coerce_to_datetime_lenient(value, &[]),
        Type::Numeric => {
            // Try int first, then float
            coercion::coerce_to_int_lenient(value)
                .or_else(|| coercion::coerce_to_float_lenient(value))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};

    fn csv_reader(data: &str) -> Box<dyn FormatReader> {
        Box::new(CsvReader::from_reader(
            std::io::Cursor::new(data.as_bytes().to_vec()),
            CsvReaderConfig {
                delimiter: b',',
                quote_char: b'"',
                has_header: true,
            },
        ))
    }

    fn col(name: &str, ty: Type) -> ColumnDecl {
        ColumnDecl {
            name: name.to_string(),
            ty,
        }
    }

    fn drop_policy() -> OnUnmapped {
        OnUnmapped::Drop
    }

    fn reject_policy() -> OnUnmapped {
        OnUnmapped::Reject
    }

    #[test]
    fn test_coerce_int_and_float() {
        let schema = vec![
            col("name", Type::String),
            col("age", Type::Int),
            col("score", Type::Float),
        ];
        let reader = csv_reader("name,age,score\nAlice,30,95.5\nBob,25,88.0\n");
        let mut coercing = CoercingReader::new(reader, &schema, drop_policy(), "src").unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(rec.get("age"), Some(&Value::Integer(30)));
        assert_eq!(rec.get("score"), Some(&Value::Float(95.5)));

        let rec2 = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec2.get("age"), Some(&Value::Integer(25)));
        assert_eq!(rec2.get("score"), Some(&Value::Float(88.0)));
    }

    #[test]
    fn test_coerce_bool() {
        let schema = vec![col("active", Type::Bool)];
        let reader = csv_reader("active\ntrue\nfalse\n");
        let mut coercing = CoercingReader::new(reader, &schema, drop_policy(), "src").unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("active"), Some(&Value::Bool(true)));

        let rec2 = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec2.get("active"), Some(&Value::Bool(false)));
    }

    #[test]
    fn test_coerce_nullable_int() {
        let schema = vec![col("val", Type::nullable(Type::Int))];
        let reader = csv_reader("val\n42\n\n99\n");
        let mut coercing = CoercingReader::new(reader, &schema, drop_policy(), "src").unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("val"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_coerce_failure_passes_through() {
        let schema = vec![col("num", Type::Int)];
        let reader = csv_reader("num\nnot_a_number\n");
        let mut coercing = CoercingReader::new(reader, &schema, drop_policy(), "src").unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        // Coercion fails → value passes through as original string
        assert_eq!(rec.get("num"), Some(&Value::String("not_a_number".into())));
    }

    #[test]
    fn test_string_type_no_coercion() {
        let schema = vec![col("name", Type::String)];
        let reader = csv_reader("name\nAlice\n");
        let mut coercing = CoercingReader::new(reader, &schema, drop_policy(), "src").unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".into())));
    }

    /// `Drop` policy silently strips CSV header columns not in the
    /// declared schema. The output schema equals the declaration.
    #[test]
    fn test_on_unmapped_drop_strips_extras() {
        let schema = vec![col("id", Type::String)];
        let reader = csv_reader("id,extra\n1,foo\n2,bar\n");
        let mut coercing = CoercingReader::new(reader, &schema, drop_policy(), "src").unwrap();

        let schema_arc = coercing.schema().unwrap();
        let cols: Vec<&str> = schema_arc.columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["id"]);
        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("id"), Some(&Value::String("1".into())));
        assert!(rec.get("extra").is_none());
    }

    /// `Reject` policy fails the source on the first record carrying
    /// an undeclared field.
    #[test]
    fn test_on_unmapped_reject_errors_on_extra() {
        let schema = vec![col("id", Type::String)];
        let reader = csv_reader("id,extra\n1,foo\n");
        let mut coercing = CoercingReader::new(reader, &schema, reject_policy(), "src").unwrap();

        let err = coercing.next_record().unwrap_err();
        match err {
            FormatError::UndeclaredField { source, field } => {
                assert_eq!(source, "src");
                assert_eq!(field, "extra");
            }
            other => panic!("expected UndeclaredField, got {other:?}"),
        }
    }
}
