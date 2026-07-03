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
//! as `Value::Null`. This is the single coercion pass for the untyped
//! formats (CSV) and the native-typed formats (JSON / XML / REST): values
//! declared with a concrete type (`Type::Int`, `Type::Float`, `Type::Bool`,
//! `Type::Date`, `Type::DateTime`, `Type::Numeric`) are coerced here,
//! honoring each column's `format:` strftime string for `date` / `date_time`;
//! `Type::String` / `Type::Any` skip coercion, and native `Map` / `Array`
//! pass through untouched. The `to_*` / `try_*` CXL builtins remain available
//! for derived fields computed during the pipeline.
//!
//! Positional formats (fixed-width / multi-record) parse their bytes into
//! final typed values in the reader itself (`fixed_width::field::coerce_scalar`,
//! also format-aware), so those readers are wrapped with coercion disabled
//! (`pretyped`) — the value is already typed, and a second parse here would be
//! redundant. Those readers keep every other reprojection service (the
//! `OnUnmapped` policy, the `$widened` sidecar, the `long_unique` storage hint).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clinker_format::error::FormatError;
use clinker_format::traits::FormatReader;
use clinker_record::{FieldMetadata, Record, Schema, SchemaBuilder, Value, coercion};
use cxl::typecheck::Type;
use indexmap::IndexMap;

use clinker_format::Column;
use clinker_plan::config::pipeline_node::{OnUnmapped, WIDENED_SIDECAR_COLUMN};

/// Wraps a `FormatReader` and reprojects every record onto the
/// user-declared `Arc<Schema>` (plus the `$widened` engine-stamped
/// sidecar slot for `AutoWiden`), applying the per-Source
/// `OnUnmapped` policy to undeclared input fields.
pub struct CoercingReader {
    inner: Box<dyn FormatReader>,
    /// Physical input-field names the declaration consumes — lookup set
    /// for the policy's "is this key in the declaration?" check. Uses each
    /// column's PHYSICAL name (`source_name` when aliased, else `name`) so an
    /// aliased source field is recognized as declared rather than widened.
    declared_names: HashSet<Box<str>>,
    /// Physical input-field name to read each declared column FROM, indexed by
    /// position across the declared columns. `None` in the common no-alias case
    /// — the physical name then equals the exposed output-schema column name, so
    /// reproject reads by that name and no per-column Vec is allocated. `Some`
    /// only when at least one column declares a differing `source_name`.
    physical_names: Option<Vec<Box<str>>>,
    /// Exposed-name → physical-name for columns that alias a differently-named
    /// physical field. Empty (unallocated) when no column aliases. Used to
    /// detect an input field whose name clashes with an alias's exposed name,
    /// which would otherwise silently mislocate that field.
    aliased_exposed: HashMap<Box<str>, Box<str>>,
    /// Output schema — declared columns (keyed by exposed name) followed
    /// (under `AutoWiden`) by the `$widened` engine-stamped sidecar column.
    output_schema: Arc<Schema>,
    /// Per-output-column coercion target (`None` for pass-through).
    /// Indexed by position in `output_schema`. The `$widened` sidecar
    /// slot, when present, gets `None` (no coercion — payload is a
    /// `Value::Map`). Every slot is `None` when the inner reader is
    /// `pretyped` (positional formats parse their own final typed values).
    targets: Vec<Option<Type>>,
    /// Per-output-column `format:` strftime string for `date` / `date_time`
    /// coercion, indexed alongside `targets`. `None` uses the default format
    /// chain. The `$widened` sidecar slot is always `None`.
    formats: Vec<Option<String>>,
    /// Per-output-column `long_unique` storage hint, indexed alongside
    /// `targets`. When set for a column, its string values are stored in
    /// the header-free `Box`-backed [`FieldStr`](clinker_record::FieldStr)
    /// arm rather than the default inline-or-`Arc`-shared one. The `$widened`
    /// sidecar slot is always `false` (its payload is a `Value::Map`, never a
    /// top-level string).
    long_unique: Vec<bool>,
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
    ///
    /// `pretyped` is set for positional readers (fixed-width / multi-record)
    /// whose bytes are already parsed into final typed values by the reader
    /// itself; coercion is then disabled (every target `None`) so no value is
    /// parsed twice. It is clear for untyped/native readers (CSV / JSON / XML /
    /// REST), where this is the sole coercion pass and each column's `format:`
    /// is honored for `date` / `date_time`.
    pub fn new(
        mut inner: Box<dyn FormatReader>,
        schema_decl: &[Column],
        policy: OnUnmapped,
        source_name: &str,
        pretyped: bool,
    ) -> Result<Self, FormatError> {
        // Trigger schema discovery on the inner reader so the first
        // record isn't gated behind an on-demand schema call.
        inner.schema()?;

        // A column "aliases" only when its `source_name` names a DIFFERENT
        // physical field; `source_name == name` is a no-op treated as no alias.
        let has_alias = schema_decl
            .iter()
            .any(|c| c.source_name.as_deref().is_some_and(|s| s != c.name));

        // The declared-key set uses each column's PHYSICAL name (the alias when
        // set, else the exposed name) so an aliased source field counts as
        // declared rather than widened.
        let declared_names: HashSet<Box<str>> = schema_decl
            .iter()
            .map(|c| c.source_name.as_deref().unwrap_or(c.name.as_str()).into())
            .collect();

        // Per-column physical name is only materialized when some column
        // aliases; otherwise reproject reads by the exposed output-schema name.
        let physical_names: Option<Vec<Box<str>>> = has_alias.then(|| {
            schema_decl
                .iter()
                .map(|c| c.source_name.as_deref().unwrap_or(c.name.as_str()).into())
                .collect()
        });

        // Exposed-name → physical-name for real aliases, to detect an input
        // field colliding with an alias's exposed name. Empty when no aliases.
        let aliased_exposed: HashMap<Box<str>, Box<str>> = schema_decl
            .iter()
            .filter_map(|c| {
                let physical = c.source_name.as_deref()?;
                (physical != c.name).then(|| (c.name.as_str().into(), physical.into()))
            })
            .collect();
        let mut targets: Vec<Option<Type>> = schema_decl
            .iter()
            .map(|c| {
                // A pretyped (positional) reader already produced the final
                // typed value; a second coercion here would be a redundant
                // re-parse, so leave every target as pass-through.
                if pretyped {
                    return None;
                }
                match unwrap_nullable(&c.ty) {
                    Type::String | Type::Any | Type::Null => None,
                    other => Some(other.clone()),
                }
            })
            .collect();
        let mut formats: Vec<Option<String>> =
            schema_decl.iter().map(|c| c.format.clone()).collect();
        let mut long_unique: Vec<bool> = schema_decl.iter().map(|c| c.is_long_unique()).collect();

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
            formats.push(None);
            long_unique.push(false);
            Some(idx)
        } else {
            None
        };
        let output_schema: Arc<Schema> = builder.build();

        Ok(CoercingReader {
            inner,
            declared_names,
            physical_names,
            aliased_exposed,
            output_schema,
            targets,
            formats,
            long_unique,
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
                // Guard the alias collision before any policy branch: an input
                // field named the same as an alias's exposed name would be
                // silently widened/dropped while the aliased column exposes a
                // different physical field's value under that name.
                if let Some(physical) = self.aliased_exposed.get(k) {
                    return Err(FormatError::AliasNameCollision {
                        source: self.source_name.to_string(),
                        exposed: k.to_string(),
                        physical: physical.to_string(),
                    });
                }
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
        let col_count = cols.len();
        let mut values: Vec<Value> = Vec::with_capacity(col_count);
        for i in 0..col_count {
            // The widened slot is filled from the sidecar map (if any
            // non-declared keys were observed); otherwise Null.
            if Some(i) == self.widened_idx {
                values.push(match sidecar.take() {
                    Some(map) if !map.is_empty() => Value::Map(Box::new(map)),
                    _ => Value::Null,
                });
                continue;
            }
            // Read from the PHYSICAL input field, exposing the value under the
            // declared column at this position. Without aliases the physical
            // name equals the exposed output-schema name, so this reads exactly
            // the same field as before with no per-column Vec.
            let physical: &str = match &self.physical_names {
                Some(names) => names[i].as_ref(),
                None => cols[i].as_ref(),
            };
            let raw = record.get(physical).cloned().unwrap_or(Value::Null);
            let coerced = match &self.targets[i] {
                Some(target) => {
                    coerce_value(&raw, target, self.formats[i].as_deref()).unwrap_or(raw)
                }
                None => raw,
            };
            // Honor the column's `long_unique` storage hint: rebuild a string
            // value in the header-free `Box`-backed arm. Coercion runs first, so
            // a column declared `string` (coercion target `None`) is the usual
            // case; a value that failed numeric coercion and fell back to its
            // string form is also re-homed. Non-string values are untouched.
            let stored = if self.long_unique[i] {
                match coerced {
                    Value::String(s) => Value::string_unique(s.as_str()),
                    other => other,
                }
            } else {
                coerced
            };
            values.push(stored);
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

    fn prepare_document(
        &mut self,
        config: &clinker_format::EnvelopeConfig,
    ) -> Result<indexmap::IndexMap<Box<str>, clinker_record::Value>, clinker_format::FormatError>
    {
        // Envelope sections are extracted from the raw source by the
        // underlying format reader; schema coercion applies to body
        // records only, so forward the pre-scan straight through.
        self.inner.prepare_document(config)
    }

    fn take_envelope_events(&mut self) -> Vec<clinker_format::EnvelopeEvent> {
        // Nested-envelope boundaries are a property of the raw source's
        // structure, untouched by per-record type coercion — forward them
        // verbatim so a multi-level source streamed through coercion keeps
        // its envelope nesting.
        self.inner.take_envelope_events()
    }

    fn advance_to_next_file(&mut self) -> Result<bool, FormatError> {
        // File advancement is the inner multi-file reader's concern; coercion
        // is per-record and stateless across files, so forward verbatim.
        self.inner.advance_to_next_file()
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
///
/// `format` is the column's `format:` strftime string, honored for `date` /
/// `date_time`; `None` falls back to the coercion module's default format
/// chain. Native `Map` / `Array` (and any other type) return `None`, so a
/// JSON/XML composite passes through untouched.
fn coerce_value(value: &Value, target: &Type, format: Option<&str>) -> Option<Value> {
    // `Option<&str>::into_iter` yields the one format when present, else
    // nothing — an empty chain selects the default formats.
    let chain: Vec<&str> = format.into_iter().collect();
    match target {
        Type::Int => coercion::coerce_to_int_lenient(value),
        Type::Float => coercion::coerce_to_float_lenient(value),
        Type::Bool => coercion::coerce_to_bool_lenient(value),
        Type::Date => coercion::coerce_to_date_lenient(value, &chain),
        Type::DateTime => coercion::coerce_to_datetime_lenient(value, &chain),
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
                ..Default::default()
            },
        ))
    }

    fn col(name: &str, ty: Type) -> Column {
        Column::bare(name, ty)
    }

    fn col_unique(name: &str, ty: Type) -> Column {
        Column {
            long_unique: Some(true),
            ..Column::bare(name, ty)
        }
    }

    /// A declared column aliasing a differently-named physical source column.
    fn col_from(name: &str, source_name: &str, ty: Type) -> Column {
        Column {
            source_name: Some(source_name.to_string()),
            ..Column::bare(name, ty)
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
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

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
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("active"), Some(&Value::Bool(true)));

        let rec2 = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec2.get("active"), Some(&Value::Bool(false)));
    }

    #[test]
    fn test_coerce_nullable_int() {
        let schema = vec![col("val", Type::nullable(Type::Int))];
        let reader = csv_reader("val\n42\n\n99\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("val"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_coerce_failure_passes_through() {
        let schema = vec![col("num", Type::Int)];
        let reader = csv_reader("num\nnot_a_number\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        // Coercion fails → value passes through as original string
        assert_eq!(rec.get("num"), Some(&Value::String("not_a_number".into())));
    }

    /// A CSV `date` column honors the column's `format:` strftime string — the
    /// single coercion pass threads it through instead of only trying the
    /// engine default chain.
    #[test]
    fn test_coerce_date_honors_column_format() {
        let schema = vec![Column {
            format: Some("%d/%m/%Y".to_string()),
            ..Column::bare("d", Type::Date)
        }];
        let reader = csv_reader("d\n15/01/2024\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();
        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(
            rec.get("d"),
            Some(&Value::Date(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
            ))
        );
    }

    /// Without the column `format:`, `15/01/2024` matches no default date
    /// format (`%m/%d/%Y` rejects month 15), so lenient coercion leaves it a
    /// String — proving the custom format above is what parsed it.
    #[test]
    fn test_coerce_date_without_format_falls_through() {
        let schema = vec![Column::bare("d", Type::Date)];
        let reader = csv_reader("d\n15/01/2024\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();
        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("d"), Some(&Value::String("15/01/2024".into())));
    }

    /// With `pretyped` set, the second coercion is disabled: a value the reader
    /// emitted passes through untouched even when it would otherwise coerce.
    /// The positional readers rely on this — they already produced final typed
    /// values, so a redundant re-parse is skipped.
    #[test]
    fn test_pretyped_passes_values_through_uncoerced() {
        use clinker_format::traits::FormatReader as FRTrait;
        use clinker_record::Schema as RecordSchema;
        use std::sync::Arc as StdArc;

        struct StubReader {
            schema: StdArc<RecordSchema>,
            rows: std::vec::IntoIter<Vec<Value>>,
        }
        impl FRTrait for StubReader {
            fn schema(&mut self) -> Result<StdArc<RecordSchema>, FormatError> {
                Ok(StdArc::clone(&self.schema))
            }
            fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
                Ok(self
                    .rows
                    .next()
                    .map(|v| Record::new(StdArc::clone(&self.schema), v)))
            }
        }

        let schema_arc = StdArc::new(RecordSchema::new(vec!["n".into()]));
        let reader = Box::new(StubReader {
            schema: StdArc::clone(&schema_arc),
            rows: vec![vec![Value::String("42".into())]].into_iter(),
        });
        let decl = vec![col("n", Type::Int)];
        let mut coercing = CoercingReader::new(reader, &decl, drop_policy(), "p", true).unwrap();
        let rec = coercing.next_record().unwrap().unwrap();
        // pretyped=true disables coercion: the Int column keeps the reader's
        // String value rather than coercing to Integer(42).
        assert_eq!(rec.get("n"), Some(&Value::String("42".into())));
    }

    #[test]
    fn test_string_type_no_coercion() {
        let schema = vec![col("name", Type::String)];
        let reader = csv_reader("name\nAlice\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".into())));
    }

    /// A `source_name` alias reads the physical CSV column and exposes the
    /// value under the declared name — the physical column is recognized as
    /// declared (never widened), and the exposed name carries the real data.
    #[test]
    fn test_source_name_alias_maps_physical_to_exposed() {
        // Physical header is `cust_id`; the declaration exposes it as
        // `customer_id`.
        let schema = vec![
            col("id", Type::String),
            col_from("customer_id", "cust_id", Type::String),
        ];
        let reader = csv_reader("id,cust_id\n1,alice\n2,bob\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, auto_widen_policy(), "src", false).unwrap();

        // Output schema exposes the logical name.
        let schema_arc = coercing.schema().unwrap();
        let cols: Vec<&str> = schema_arc.columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["id", "customer_id", WIDENED_SIDECAR_COLUMN]);

        let rec = coercing.next_record().unwrap().unwrap();
        // The exposed column carries the physical column's value...
        assert_eq!(rec.get("customer_id"), Some(&Value::String("alice".into())));
        // ...the physical name is not a top-level output column...
        assert!(rec.get("cust_id").is_none());
        // ...and it did NOT fall into `$widened` (recognized as declared).
        assert_eq!(rec.get(WIDENED_SIDECAR_COLUMN), Some(&Value::Null));
    }

    /// If an aliased column's exposed name also exists as a real input field,
    /// reading the alias would mislocate that field — the reader fails loudly
    /// instead of silently widening/dropping it.
    #[test]
    fn test_source_name_alias_exposed_name_collision_errors() {
        // Column exposes `customer_id` reading physical `cust_id`, but the CSV
        // ALSO has a real `customer_id` column.
        let schema = vec![col_from("customer_id", "cust_id", Type::String)];
        let reader = csv_reader("cust_id,customer_id\nalice,bob\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, auto_widen_policy(), "src", false).unwrap();

        let err = coercing.next_record().unwrap_err();
        match err {
            FormatError::AliasNameCollision {
                source,
                exposed,
                physical,
            } => {
                assert_eq!(source, "src");
                assert_eq!(exposed, "customer_id");
                assert_eq!(physical, "cust_id");
            }
            other => panic!("expected AliasNameCollision, got {other:?}"),
        }
    }

    /// The collision fires under every policy, not just auto_widen — a `drop`
    /// source would otherwise silently discard the real field.
    #[test]
    fn test_source_name_alias_collision_errors_under_drop() {
        let schema = vec![col_from("customer_id", "cust_id", Type::String)];
        let reader = csv_reader("cust_id,customer_id\nalice,bob\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();
        assert!(matches!(
            coercing.next_record().unwrap_err(),
            FormatError::AliasNameCollision { .. }
        ));
    }

    /// An aliased column is coerced to its declared type just like a
    /// same-named column — the alias only changes which field it reads from.
    #[test]
    fn test_source_name_alias_still_coerces() {
        let schema = vec![col_from("total", "raw_amount", Type::Int)];
        let reader = csv_reader("raw_amount\n100\n250\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("total"), Some(&Value::Integer(100)));
        let rec2 = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec2.get("total"), Some(&Value::Integer(250)));
    }

    /// Backward-compat: a column with `source_name == None` reads the field
    /// whose key equals its `name`, exactly as before the alias field existed.
    #[test]
    fn test_no_alias_reads_by_name_unchanged() {
        let schema = vec![col("id", Type::String), col("name", Type::String)];
        let reader = csv_reader("id,name\n1,Alice\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("id"), Some(&Value::String("1".into())));
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".into())));
    }

    /// `Drop` policy silently strips CSV header columns not in the
    /// declared schema. The output schema equals the declaration —
    /// no `$widened` sidecar slot.
    #[test]
    fn test_on_unmapped_drop_strips_extras() {
        let schema = vec![col("id", Type::String)];
        let reader = csv_reader("id,extra\n1,foo\n2,bar\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

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
        let mut coercing =
            CoercingReader::new(reader, &schema, reject_policy(), "src", false).unwrap();

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
            CoercingReader::new(reader, &schema, auto_widen_policy(), "src", false).unwrap();

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
            CoercingReader::new(reader, &schema, auto_widen_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get(WIDENED_SIDECAR_COLUMN), Some(&Value::Null));
    }

    /// A `long_unique`-flagged column stores its values in the header-free
    /// `Box`-backed arm; an unflagged column keeps the default `Arc`-shared
    /// policy. The two arms are distinguished here through observable clone
    /// semantics rather than an internal arm probe: a unique-arm `FieldStr`
    /// deep-copies its bytes on clone (a fresh allocation, a distinct `str`
    /// pointer), whereas the default `Arc`-shared arm bumps a refcount and the
    /// clone aliases the original allocation (pointer-identical `str`). Both
    /// values exceed the 23-byte inline boundary, so neither lands inline.
    #[test]
    fn test_long_unique_column_lands_in_unique_arm() {
        let schema = vec![
            col_unique("uuid", Type::String),
            col("name_uuid", Type::String),
        ];
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let name = "7c9e6679-7425-40de-944b-e07fc1f90ae7";
        let reader = csv_reader(&format!("uuid,name_uuid\n{uuid},{name}\n"));
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        match rec.get("uuid") {
            Some(Value::String(s)) => {
                assert_eq!(s.as_str(), uuid);
                assert!(s.heap_size() > 0, "a 36-byte UUID is never inline");
                // Unique arm: a clone deep-copies into a fresh allocation, so
                // the clone's backing `str` lives at a different address.
                let cloned = s.clone();
                assert_eq!(cloned.as_str(), s.as_str());
                assert_ne!(
                    cloned.as_str().as_ptr(),
                    s.as_str().as_ptr(),
                    "the flagged column's value must take the deep-copying unique arm"
                );
            }
            other => panic!("expected String, got {other:?}"),
        }
        // The unflagged neighbor keeps the default `Arc`-shared policy: cloning
        // shares the allocation, so the clone's `str` is pointer-identical.
        match rec.get("name_uuid") {
            Some(Value::String(s)) => {
                assert!(s.heap_size() > 0, "a 36-byte UUID is never inline");
                let cloned = s.clone();
                assert_eq!(
                    cloned.as_str().as_ptr(),
                    s.as_str().as_ptr(),
                    "the unflagged column must keep the Arc-shared default arm"
                );
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    /// The default (no flag) leaves every string in the `Arc`-shared default
    /// arm — the pre-existing behavior is byte-for-byte unchanged. Observed
    /// through clone aliasing: a default-arm clone shares the original's
    /// allocation rather than deep-copying as the unique arm would.
    #[test]
    fn test_unflagged_columns_keep_default_arm() {
        let schema = vec![col("uuid", Type::String)];
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let reader = csv_reader(&format!("uuid\n{uuid}\n"));
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        match rec.get("uuid") {
            Some(Value::String(s)) => {
                assert!(s.heap_size() > 0, "a 36-byte UUID is never inline");
                let cloned = s.clone();
                assert_eq!(
                    cloned.as_str().as_ptr(),
                    s.as_str().as_ptr(),
                    "an unflagged column clone must alias the Arc-shared allocation"
                );
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    /// A `long_unique` flag on a non-string column is inert: numeric coercion
    /// runs and the value is not a string, so nothing is re-homed.
    #[test]
    fn test_long_unique_inert_on_numeric_column() {
        let schema = vec![col_unique("n", Type::Int)];
        let reader = csv_reader("n\n42\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(rec.get("n"), Some(&Value::Integer(42)));
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
            CoercingReader::new(reader, &decl, auto_widen_policy(), "fw_src", true).unwrap();
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
