//! Schema-based type coercion for source records.
//!
//! Wraps a `FormatReader` and coerces each record's field values to
//! the types declared in the source's `schema:` block. This ensures
//! downstream CXL expressions operate on properly typed values
//! (`Value::Integer`, `Value::Float`, etc.) without requiring explicit
//! `.to_int()` / `.to_float()` calls for every field reference.
//!
//! The `to_*` / `try_*` CXL builtins remain available for derived
//! fields computed during the pipeline.

use std::sync::Arc;

use clinker_format::error::FormatError;
use clinker_format::traits::FormatReader;
use clinker_record::{Record, Schema, Value, coercion};
use cxl::typecheck::Type;

use crate::config::pipeline_node::ColumnDecl;

/// A coercion plan for one field: column index + target type.
struct FieldCoercion {
    col_index: usize,
    target: Type,
}

/// Wraps a `FormatReader` and coerces values to declared schema types.
pub struct CoercingReader {
    inner: Box<dyn FormatReader>,
    coercions: Vec<FieldCoercion>,
    schema: Option<Arc<Schema>>,
}

impl CoercingReader {
    /// Build a coercing reader from a format reader and schema declaration.
    ///
    /// Fields not declared in the schema pass through unchanged.
    /// Fields declared as `type: string` or `type: any` skip coercion.
    pub fn new(
        mut inner: Box<dyn FormatReader>,
        schema_decl: &[ColumnDecl],
    ) -> Result<Self, FormatError> {
        let reader_schema = inner.schema()?;
        let mut coercions = Vec::new();

        for decl in schema_decl {
            let target = unwrap_nullable(&decl.ty);
            if matches!(target, Type::String | Type::Any | Type::Null) {
                continue;
            }
            if let Some(idx) = reader_schema.index(&decl.name) {
                coercions.push(FieldCoercion {
                    col_index: idx,
                    target: target.clone(),
                });
            }
        }

        Ok(CoercingReader {
            inner,
            coercions,
            schema: None,
        })
    }

    /// Apply coercions to a record's values in-place.
    fn coerce_record(&self, record: &mut Record) {
        for fc in &self.coercions {
            let field_name = record.schema().columns().get(fc.col_index);
            if let Some(name) = field_name {
                let name = name.clone();
                if let Some(val) = record.get(&name) {
                    let coerced = coerce_value(val, &fc.target);
                    if let Some(new_val) = coerced {
                        record.set(&name, new_val);
                    }
                }
            }
        }
    }
}

impl FormatReader for CoercingReader {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        if let Some(ref s) = self.schema {
            return Ok(Arc::clone(s));
        }
        let s = self.inner.schema()?;
        self.schema = Some(Arc::clone(&s));
        Ok(s)
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        match self.inner.next_record()? {
            Some(mut record) => {
                self.coerce_record(&mut record);
                Ok(Some(record))
            }
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
