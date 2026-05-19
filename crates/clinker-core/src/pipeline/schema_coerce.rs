//! Schema-based type coercion + declared-schema reprojection for source records.
//!
//! Wraps a `FormatReader` and returns records whose `Arc<Schema>` is the
//! source's user-declared schema (extended with the `$widened` engine-
//! stamped sidecar column for `OnUnmapped::AutoWiden`), with the
//! per-Source `OnUnmapped` policy applied to undeclared input fields:
//!
//! - **`OnUnmapped::AutoWiden`** (default): per-record undeclared input
//!   fields land in a `Value::Map` carried by a `$widened` engine-
//!   stamped sidecar column appended to the declared schema. The
//!   typechecker is blind to its contents (CXL has no Map operators
//!   in the user surface); `include_unmapped: true` at an Output node
//!   expands the map back to top-level columns at the sink. Pattern
//!   precedent: Databricks Auto Loader's `_rescued_data` and
//!   ClickHouse's `JSON` column type.
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
use clinker_record::{FieldMetadata, Record, Schema, SchemaBuilder, Value, coercion};
use cxl::typecheck::Type;
use indexmap::IndexMap;

use crate::config::pipeline_node::{ColumnDecl, OnUnmapped, WIDENED_SIDECAR_COLUMN};

/// Wraps a `FormatReader` and reprojects every record onto the
/// user-declared `Arc<Schema>` (plus the `$widened` engine-stamped
/// sidecar slot for `AutoWiden`), applying the per-Source
/// `OnUnmapped` policy to undeclared input fields.
pub struct CoercingReader {
    inner: Box<dyn FormatReader>,
    /// Declared column names — lookup set for the policy's "is this
    /// key in the declaration?" check.
    declared_names: HashSet<Box<str>>,
    /// Output schema — declared columns followed (under `AutoWiden`)
    /// by the `$widened` engine-stamped sidecar column.
    output_schema: Arc<Schema>,
    /// Per-output-column coercion target (`None` for pass-through).
    /// Indexed by position in `output_schema`. The `$widened` sidecar
    /// slot, when present, gets `None` (no coercion — payload is a
    /// `Value::Map`).
    targets: Vec<Option<Type>>,
    /// Position of the `$widened` sidecar column in `output_schema`,
    /// or `None` for `Drop` / `Reject` policies (no sidecar slot).
    widened_idx: Option<usize>,
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

        let declared_names: HashSet<Box<str>> =
            schema_decl.iter().map(|c| c.name.as_str().into()).collect();
        let mut targets: Vec<Option<Type>> = schema_decl
            .iter()
            .map(|c| {
                let target = unwrap_nullable(&c.ty);
                match target {
                    Type::String | Type::Any | Type::Null => None,
                    other => Some(other.clone()),
                }
            })
            .collect();

        let mut builder = SchemaBuilder::new();
        for c in schema_decl {
            builder = builder.with_field(c.name.as_str());
        }
        let widened_idx = if policy.reserves_widened_sidecar() {
            // Append the `$widened` engine-stamped sidecar column. The
            // dispatch canonicalize invariant accepts engine-stamped
            // tail columns; `WidenedSidecar` joins `SourceCorrelation`
            // and `AggregateGroupIndex` in that role.
            let idx = schema_decl.len();
            builder =
                builder.with_field_meta(WIDENED_SIDECAR_COLUMN, FieldMetadata::widened_sidecar());
            targets.push(None);
            Some(idx)
        } else {
            None
        };
        let output_schema: Arc<Schema> = builder.build();

        Ok(CoercingReader {
            inner,
            declared_names,
            output_schema,
            targets,
            widened_idx,
            policy,
            source_name: source_name.into(),
        })
    }

    /// Reproject `record` onto the output schema (declared columns
    /// plus the `$widened` sidecar for `AutoWiden`).
    fn reproject(&self, record: &Record) -> Result<Record, FormatError> {
        // Collect undeclared keys for the policy decision.
        let mut sidecar: Option<IndexMap<Box<str>, Value>> = None;
        for (k, v) in record.iter_all_fields() {
            if !self.declared_names.contains(k) {
                match self.policy {
                    OnUnmapped::Reject => {
                        return Err(FormatError::UndeclaredField {
                            source: self.source_name.to_string(),
                            field: k.to_string(),
                        });
                    }
                    OnUnmapped::Drop => { /* silent strip */ }
                    OnUnmapped::AutoWiden => {
                        sidecar
                            .get_or_insert_with(IndexMap::new)
                            .insert(k.into(), v.clone());
                    }
                }
            }
        }

        let cols = self.output_schema.columns();
        let mut values: Vec<Value> = Vec::with_capacity(cols.len());
        for (i, col) in cols.iter().enumerate() {
            // The widened slot is filled from the sidecar map (if any
            // non-declared keys were observed); otherwise Null.
            if Some(i) == self.widened_idx {
                values.push(match sidecar.take() {
                    Some(map) if !map.is_empty() => Value::Map(Box::new(map)),
                    _ => Value::Null,
                });
                continue;
            }
            let raw = record.get(col).cloned().unwrap_or(Value::Null);
            let coerced = match &self.targets[i] {
                Some(target) => coerce_value(&raw, target).unwrap_or(raw),
                None => raw,
            };
            values.push(coerced);
        }
        Ok(Record::new(Arc::clone(&self.output_schema), values))
    }
}

impl FormatReader for CoercingReader {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.output_schema))
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

    fn auto_widen_policy() -> OnUnmapped {
        OnUnmapped::AutoWiden
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
    /// declared schema. The output schema equals the declaration —
    /// no `$widened` sidecar slot.
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
        assert!(rec.get(WIDENED_SIDECAR_COLUMN).is_none());
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

    /// `AutoWiden` appends `$widened` to the output schema and absorbs
    /// undeclared input fields into a `Value::Map` payload at that slot.
    #[test]
    fn test_on_unmapped_auto_widen_absorbs_into_sidecar() {
        let schema = vec![col("id", Type::String)];
        let reader = csv_reader("id,extra1,extra2\n1,foo,42\n2,bar,99\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, auto_widen_policy(), "src").unwrap();

        let schema_arc = coercing.schema().unwrap();
        let cols: Vec<&str> = schema_arc.columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["id", WIDENED_SIDECAR_COLUMN]);

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("id"), Some(&Value::String("1".into())));
        match rec.get(WIDENED_SIDECAR_COLUMN) {
            Some(Value::Map(m)) => {
                assert_eq!(m.get("extra1"), Some(&Value::String("foo".into())));
                assert_eq!(m.get("extra2"), Some(&Value::String("42".into())));
            }
            other => panic!("expected Map sidecar payload, got {other:?}"),
        }
    }

    /// `AutoWiden` with no extras leaves the `$widened` slot Null —
    /// the column exists on the schema but the payload is absent.
    #[test]
    fn test_on_unmapped_auto_widen_null_when_no_extras() {
        let schema = vec![col("id", Type::String), col("name", Type::String)];
        let reader = csv_reader("id,name\n1,Alice\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, auto_widen_policy(), "src").unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get(WIDENED_SIDECAR_COLUMN), Some(&Value::Null));
    }

    /// Fixed-width sources are structurally incapable of producing
    /// undeclared fields — the schema is positional. A
    /// `CoercingReader` wrapping a fixed-width reader with
    /// `auto_widen` therefore always emits records whose `$widened`
    /// slot is `Value::Null`, regardless of the byte content.
    /// Verified via a synthetic positional reader (a stub that
    /// emits records keyed by the user-declared schema only —
    /// matching the structural shape `FixedWidthReader` produces).
    #[test]
    fn test_auto_widen_inert_for_positional_reader() {
        use clinker_format::traits::FormatReader as FRTrait;
        use clinker_record::Schema as RecordSchema;
        use std::sync::Arc as StdArc;

        struct PositionalReader {
            schema: StdArc<RecordSchema>,
            rows: std::vec::IntoIter<Vec<Value>>,
        }
        impl FRTrait for PositionalReader {
            fn schema(&mut self) -> Result<StdArc<RecordSchema>, FormatError> {
                Ok(StdArc::clone(&self.schema))
            }
            fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
                Ok(self
                    .rows
                    .next()
                    .map(|values| Record::new(StdArc::clone(&self.schema), values)))
            }
        }

        let declared_schema = StdArc::new(RecordSchema::new(vec!["id".into(), "name".into()]));
        let reader = Box::new(PositionalReader {
            schema: StdArc::clone(&declared_schema),
            rows: vec![
                vec![Value::String("1".into()), Value::String("Alice".into())],
                vec![Value::String("2".into()), Value::String("Bob".into())],
            ]
            .into_iter(),
        });
        let decl = vec![col("id", Type::String), col("name", Type::String)];
        let mut coercing =
            CoercingReader::new(reader, &decl, auto_widen_policy(), "fw_src").unwrap();
        for _ in 0..2 {
            let rec = coercing.next_record().unwrap().unwrap();
            assert_eq!(
                rec.get(WIDENED_SIDECAR_COLUMN),
                Some(&Value::Null),
                "auto_widen sidecar must stay Null for positional readers"
            );
        }
    }
}
