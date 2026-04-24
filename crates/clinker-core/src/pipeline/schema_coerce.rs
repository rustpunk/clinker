//! Schema-based type coercion + declared-schema reprojection for source records.
//!
//! Wraps a `FormatReader` and returns records whose `Arc<Schema>` is
//! the author-declared schema from the YAML `schema:` block. Declared
//! columns missing from the underlying reader surface as `Value::Null`;
//! reader columns missing from the declaration route to `$meta.*`.
//!
//! This single Arc is the plan-time ground truth: downstream operators
//! (bind_schema, Aggregation group_by_indices, `check_input_schema`)
//! all resolve against the same `Arc<Schema>` that actually flows on
//! records.
//!
//! Values declared with a concrete type (`Type::Int`, `Type::Float`,
//! `Type::Bool`, etc.) are coerced; `Type::String` / `Type::Any` skip
//! coercion. The `to_*` / `try_*` CXL builtins remain available for
//! derived fields computed during the pipeline.

use std::sync::Arc;

use clinker_format::error::FormatError;
use clinker_format::traits::FormatReader;
use clinker_record::{Record, Schema, SchemaBuilder, Value, coercion};
use cxl::typecheck::Type;

use crate::config::pipeline_node::ColumnDecl;

/// Wraps a `FormatReader` and reprojects every record onto the
/// author-declared `Arc<Schema>`.
pub struct CoercingReader {
    inner: Box<dyn FormatReader>,
    /// Declared schema — the single `Arc<Schema>` every reprojected
    /// record carries. Shared via `Arc::clone` so downstream
    /// `Arc::ptr_eq` checks hit the fast path.
    declared_schema: Arc<Schema>,
    /// Per-declared-column coercion target (`None` for pass-through).
    /// Indexed by position in the declared schema.
    targets: Vec<Option<Type>>,
}

impl CoercingReader {
    /// Build a coercing reader from a format reader and schema declaration.
    ///
    /// Declared columns missing from the underlying reader materialize
    /// as `Value::Null`; reader columns missing from the declaration
    /// route to `$meta.*`. Fields declared as `type: string` or
    /// `type: any` skip value coercion.
    pub fn new(
        mut inner: Box<dyn FormatReader>,
        schema_decl: &[ColumnDecl],
    ) -> Result<Self, FormatError> {
        // Trigger schema discovery on the inner reader so the first
        // record isn't gated behind an on-demand schema call.
        inner.schema()?;

        let declared_schema: Arc<Schema> = schema_decl
            .iter()
            .map(|c| c.name.as_str())
            .collect::<SchemaBuilder>()
            .build();
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
            declared_schema,
            targets,
        })
    }

    /// Reproject `record` onto the declared schema Arc.
    ///
    /// For each declared column, pull the value from the underlying
    /// record (by name), coerce if the declaration has a target type,
    /// and write at the declared position. Reader columns absent from
    /// the declaration move to `$meta.*` via `Record::set_meta`; the
    /// first 64 such keys are captured, the 65th raises
    /// `FormatError::MetadataCapExceeded` carrying the partial record.
    fn reproject(&self, record: &Record) -> Result<Record, FormatError> {
        let mut values: Vec<Value> = Vec::with_capacity(self.declared_schema.column_count());
        for (i, col) in self.declared_schema.columns().iter().enumerate() {
            let raw = record.get(col).cloned().unwrap_or(Value::Null);
            let coerced = match &self.targets[i] {
                Some(target) => coerce_value(&raw, target).unwrap_or(raw),
                None => raw,
            };
            values.push(coerced);
        }
        let mut out = Record::new(Arc::clone(&self.declared_schema), values);
        // Carry forward any pre-existing metadata the inner reader set
        // (XML/JSON readers route unknown elements here already).
        for (k, v) in record.iter_meta() {
            let _ = out.set_meta(k, v.clone());
        }
        // Route reader columns absent from the declared schema into
        // metadata too, preserving evidence of source drift.
        let mut meta_count: usize = out.iter_meta().count();
        for (name, value) in record.iter_all_fields() {
            if self.declared_schema.index(name).is_some() {
                continue;
            }
            match out.set_meta(name, value.clone()) {
                Ok(()) => meta_count += 1,
                Err(_) => {
                    return Err(FormatError::MetadataCapExceeded {
                        record: out,
                        key: name.to_string(),
                        count: meta_count,
                    });
                }
            }
        }
        Ok(out)
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

    #[test]
    fn test_coerce_int_and_float() {
        let schema = vec![
            col("name", Type::String),
            col("age", Type::Int),
            col("score", Type::Float),
        ];
        let reader = csv_reader("name,age,score\nAlice,30,95.5\nBob,25,88.0\n");
        let mut coercing = CoercingReader::new(reader, &schema).unwrap();

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
        let mut coercing = CoercingReader::new(reader, &schema).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("active"), Some(&Value::Bool(true)));

        let rec2 = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec2.get("active"), Some(&Value::Bool(false)));
    }

    #[test]
    fn test_coerce_nullable_int() {
        let schema = vec![col("val", Type::nullable(Type::Int))];
        let reader = csv_reader("val\n42\n\n99\n");
        let mut coercing = CoercingReader::new(reader, &schema).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("val"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_coerce_failure_passes_through() {
        let schema = vec![col("num", Type::Int)];
        let reader = csv_reader("num\nnot_a_number\n");
        let mut coercing = CoercingReader::new(reader, &schema).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        // Coercion fails → value passes through as original string
        assert_eq!(rec.get("num"), Some(&Value::String("not_a_number".into())));
    }

    #[test]
    fn test_string_type_no_coercion() {
        let schema = vec![col("name", Type::String)];
        let reader = csv_reader("name\nAlice\n");
        let mut coercing = CoercingReader::new(reader, &schema).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".into())));
    }
}
