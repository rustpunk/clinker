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
//! Every declared type has an explicit admission policy: coercible scalar
//! types use the canonical coercion helpers, `String` / `Null` / `Array` /
//! `Map` validate their exact native variants, and `Any` explicitly accepts
//! every [`Value`] variant. A `multiple:` column arrives array-valued and its
//! declared type describes each element, so the same scalar admission function
//! is applied one element at a time. The `to_*` / `try_*` CXL builtins remain
//! available for derived fields computed during the pipeline.
//!
//! Positional formats (fixed-width / multi-record) parse their bytes into
//! final typed values in the reader itself (`fixed_width::field::coerce_scalar`,
//! also format-aware), so those readers are wrapped with parsing disabled
//! (`pretyped`) — the value is already typed, and a second parse here would be
//! redundant. The wrapper still verifies the reader's typed proof, including
//! native variants, nullability, decimal constraints, and finite numerics.
//! Those readers keep every other reprojection service (the
//! `OnUnmapped` policy, the `$widened` sidecar, the `long_unique` storage hint).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clinker_format::error::{DeclaredTypeFailure, FormatError};
use clinker_format::traits::FormatReader;
use clinker_record::{FieldMetadata, Record, Schema, SchemaBuilder, Value, coercion};
use cxl::typecheck::Type;
use indexmap::IndexMap;

use clinker_format::{Column, RECORD_TYPE_COLUMN, RecordType};
use clinker_plan::config::pipeline_node::{OnUnmapped, WIDENED_SIDECAR_COLUMN};

/// Exhaustive admission policy for one output-schema slot.
///
/// This replaces the former `Option<Type>` encoding, where `None` meant both
/// intentionally unconstrained (`Any`) and accidentally unchecked (`String`,
/// `Null`, pretyped readers, and the engine-owned widened sidecar).
#[derive(Debug, Clone)]
enum DeclaredTypeTarget {
    /// Parse or convert a decoded scalar through the canonical coercion path.
    Coerce(Type),
    /// Admit only the matching already-typed [`Value`] variant.
    ValidateExact(Type),
    /// Authored `Any`: intentionally admit every supported [`Value`] variant.
    AcceptAny,
    /// A positional reader already parsed the authored type from bytes. Keep
    /// the type at the trust boundary so the emitted native value is still
    /// verified rather than accepted on the strength of an untyped marker.
    ReaderProven { declared: Type },
    /// Engine-owned sidecar populated by reprojection rather than source data.
    EngineManaged,
}

/// One active multi-record column's reader proof. The output row still uses
/// the planner's widened superset schema, but admission must use the local
/// declaration that parsed this record type's physical bytes.
#[derive(Debug, Clone)]
struct ReaderColumnProof {
    target: DeclaredTypeTarget,
    declared_type: Type,
    nullable: bool,
    precision: Option<u8>,
    scale: Option<u8>,
}

/// Record-type id to its authored local column proofs. The multi-record reader
/// stamps that id into the reserved `record_type` slot on every emitted row;
/// retaining this map keeps the identity and typed declaration together until
/// the coercion wrapper verifies the reader's output.
#[derive(Debug, Clone)]
struct MultiRecordProofs {
    by_record_type: HashMap<Box<str>, HashMap<Box<str>, ReaderColumnProof>>,
}

impl MultiRecordProofs {
    fn new(record_types: &[RecordType]) -> Result<Self, FormatError> {
        let mut by_record_type = HashMap::with_capacity(record_types.len());
        for record_type in record_types {
            let mut columns = HashMap::with_capacity(record_type.columns.len());
            for column in &record_type.columns {
                let proof = ReaderColumnProof {
                    target: DeclaredTypeTarget::ReaderProven {
                        declared: column.ty.clone(),
                    },
                    declared_type: column.ty.clone(),
                    nullable: column.ty.is_nullable(),
                    precision: column.precision,
                    scale: column.scale,
                };
                if columns.insert(column.name.clone().into(), proof).is_some() {
                    return Err(FormatError::SchemaInference(format!(
                        "multi-record type '{}' declares column '{}' more than once",
                        record_type.id, column.name
                    )));
                }
            }
            if by_record_type
                .insert(record_type.id.clone().into(), columns)
                .is_some()
            {
                return Err(FormatError::SchemaInference(format!(
                    "multi-record type id '{}' is declared more than once",
                    record_type.id
                )));
            }
        }
        Ok(Self { by_record_type })
    }

    fn active_columns<'a>(
        &'a self,
        record: &Record,
    ) -> Result<&'a HashMap<Box<str>, ReaderColumnProof>, FormatError> {
        let record_type = match record.get(RECORD_TYPE_COLUMN) {
            Some(Value::String(record_type)) => record_type.as_str(),
            Some(value) => {
                return Err(FormatError::SchemaInference(format!(
                    "multi-record reader emitted native {} in reserved '{}' column",
                    native_value_name(value),
                    RECORD_TYPE_COLUMN
                )));
            }
            None => {
                return Err(FormatError::SchemaInference(format!(
                    "multi-record reader omitted reserved '{}' column",
                    RECORD_TYPE_COLUMN
                )));
            }
        };
        self.by_record_type.get(record_type).ok_or_else(|| {
            FormatError::SchemaInference(format!(
                "multi-record reader emitted unknown record type id '{record_type}'"
            ))
        })
    }
}

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
    /// Per-output-column admission target, indexed by position in
    /// `output_schema`. Every slot has one explicit disposition, including
    /// authored `Any`, reader-proven positional values, and the engine-owned
    /// `$widened` sidecar.
    targets: Vec<DeclaredTypeTarget>,
    /// Active-record proof table for a discriminator-driven source. `None` for
    /// every single-schema reader. The row's stamped `record_type` selects one
    /// local table before any value is admitted against the widened superset.
    multi_record_proofs: Option<MultiRecordProofs>,
    /// Full authored type retained for a structured failure diagnostic.
    declared_types: Vec<Type>,
    /// Whether native null is admitted by the authored declaration.
    nullable: Vec<bool>,
    /// Whether authored empty text maps to null. This is narrower than
    /// `nullable`: `nullable(string)`, `nullable(any)`, and `null` keep empty
    /// text distinct from native null.
    empty_is_null: Vec<bool>,
    /// Per-output-column `format:` strftime string for `date` / `date_time`
    /// coercion, indexed alongside `targets`. `None` uses the default format
    /// chain. The `$widened` sidecar slot is always `None`.
    formats: Vec<Option<String>>,
    /// Per-output-column decimal `scale`, indexed alongside `targets`. Used
    /// only by the `Type::Decimal` coercion arm; values that would need
    /// rounding to reach the scale are rejected. `None` for non-decimal
    /// columns and the `$widened` sidecar slot.
    scales: Vec<Option<u8>>,
    /// Per-output-column decimal precision, indexed alongside `scales`.
    precisions: Vec<Option<u8>>,
    /// Per-output-column `long_unique` storage hint, indexed alongside
    /// `targets`. When set for a column, its string values are stored in
    /// the header-free `Box`-backed [`FieldStr`](clinker_record::FieldStr)
    /// arm rather than the default inline-or-`Arc`-shared one. The `$widened`
    /// sidecar slot is always `false` (its payload is a `Value::Map`, never a
    /// top-level string).
    long_unique: Vec<bool>,
    /// Per-output-column `multiple:` declaration, indexed alongside `targets`.
    /// A set column's declared type describes each ELEMENT, so coercion
    /// recurses into the array rather than trying to coerce the array itself.
    /// The `$widened` sidecar slot is always `false`.
    multiple: Vec<bool>,
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
    /// itself; parsing is then disabled while each emitted value is validated
    /// against the retained proof, so no value is parsed twice. It is clear
    /// for untyped/native readers (CSV / JSON / XML /
    /// REST), where this is the sole coercion pass and each column's `format:`
    /// is honored for `date` / `date_time`.
    pub fn new(
        inner: Box<dyn FormatReader>,
        schema_decl: &[Column],
        policy: OnUnmapped,
        source_name: &str,
        pretyped: bool,
    ) -> Result<Self, FormatError> {
        Self::new_inner(inner, schema_decl, policy, source_name, pretyped, None)
    }

    /// Build a coercing reader for a discriminator-driven positional source.
    /// `schema_decl` remains the widened output shape, while `record_types`
    /// retains the local declaration that proves each active row.
    pub(crate) fn new_with_record_types(
        inner: Box<dyn FormatReader>,
        schema_decl: &[Column],
        record_types: &[RecordType],
        policy: OnUnmapped,
        source_name: &str,
    ) -> Result<Self, FormatError> {
        Self::new_inner(
            inner,
            schema_decl,
            policy,
            source_name,
            true,
            Some(MultiRecordProofs::new(record_types)?),
        )
    }

    fn new_inner(
        mut inner: Box<dyn FormatReader>,
        schema_decl: &[Column],
        policy: OnUnmapped,
        source_name: &str,
        pretyped: bool,
        multi_record_proofs: Option<MultiRecordProofs>,
    ) -> Result<Self, FormatError> {
        // Trigger schema discovery on the inner reader so the first
        // record isn't gated behind an on-demand schema call.
        inner.schema()?;

        // Positional readers construct their output schema from the effective
        // declaration and therefore emit the exposed logical names. Decoded
        // map-like readers emit physical source keys and need reprojection.
        let reader_emits_logical_names = pretyped;

        // A column "aliases" only when its `source_name` names a DIFFERENT
        // physical field; `source_name == name` is a no-op treated as no alias.
        // Positional readers already applied that alias while constructing
        // their schema, so only decoded map-like readers need physical lookup.
        let has_alias = !reader_emits_logical_names
            && schema_decl
                .iter()
                .any(|c| c.source_name.as_deref().is_some_and(|s| s != c.name));

        // Match the names the reader actually emits. A positional reader has
        // already relabeled each field to its logical declaration; every other
        // reader still exposes the physical source key at this boundary.
        let declared_names: HashSet<Box<str>> = schema_decl
            .iter()
            .map(|c| {
                if reader_emits_logical_names {
                    c.name.as_str().into()
                } else {
                    c.physical_name().into()
                }
            })
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
        // field colliding with an alias's exposed name. Positional readers emit
        // only the post-alias logical schema, so their logical field is the
        // declaration itself rather than evidence of a collision.
        let aliased_exposed: HashMap<Box<str>, Box<str>> = schema_decl
            .iter()
            .filter_map(|c| {
                if reader_emits_logical_names {
                    return None;
                }
                let physical = c.source_name.as_deref()?;
                (physical != c.name).then(|| (c.name.as_str().into(), physical.into()))
            })
            .collect();
        let mut targets: Vec<DeclaredTypeTarget> = schema_decl
            .iter()
            .map(|column| declared_type_target(&column.ty, pretyped))
            .collect();
        let mut formats: Vec<Option<String>> =
            schema_decl.iter().map(|c| c.format.clone()).collect();
        let mut scales: Vec<Option<u8>> = schema_decl.iter().map(|c| c.scale).collect();
        let mut precisions: Vec<Option<u8>> = schema_decl.iter().map(|c| c.precision).collect();
        let mut declared_types: Vec<Type> = schema_decl.iter().map(|c| c.ty.clone()).collect();
        let mut nullable: Vec<bool> = schema_decl.iter().map(|c| c.ty.is_nullable()).collect();
        let mut empty_is_null: Vec<bool> = schema_decl
            .iter()
            .map(|column| match &column.ty {
                Type::Nullable(inner) => {
                    !matches!(unwrap_nullable(inner), Type::String | Type::Any)
                }
                _ => false,
            })
            .collect();
        let mut long_unique: Vec<bool> = schema_decl.iter().map(|c| c.is_long_unique()).collect();
        let mut multiple: Vec<bool> = schema_decl.iter().map(|c| c.is_multiple()).collect();

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
            targets.push(DeclaredTypeTarget::EngineManaged);
            formats.push(None);
            scales.push(None);
            precisions.push(None);
            declared_types.push(Type::Any);
            nullable.push(true);
            empty_is_null.push(false);
            long_unique.push(false);
            multiple.push(false);
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
            multi_record_proofs,
            declared_types,
            nullable,
            empty_is_null,
            formats,
            scales,
            precisions,
            long_unique,
            multiple,
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
        let active_reader_proofs = match &self.multi_record_proofs {
            Some(proofs) => Some(proofs.active_columns(record)?),
            None => None,
        };
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
            let local_proof = active_reader_proofs.and_then(|proofs| proofs.get(cols[i].as_ref()));
            if active_reader_proofs.is_some()
                && cols[i].as_ref() != RECORD_TYPE_COLUMN
                && local_proof.is_none()
            {
                if matches!(raw, Value::Null) {
                    // This column belongs to a different record type. Sparse
                    // Null is admitted only for that inactive state; an active
                    // column selects a local proof below and must satisfy its
                    // own nullability.
                    values.push(Value::Null);
                    continue;
                }
                let native_type = native_value_name(&raw);
                return Err(FormatError::DeclaredType(Box::new(DeclaredTypeFailure {
                    source: self.source_name.to_string(),
                    field: cols[i].to_string(),
                    column: i + 1,
                    declared_type: "inactive multi-record column".to_string(),
                    original_value: raw,
                    original_record: record.clone(),
                    message: format!(
                        "reader emitted a non-null {} for a column inactive on this record type",
                        native_type
                    ),
                })));
            }
            let (target, format, precision, scale, nullable, empty_is_null, declared_type) =
                match local_proof {
                    Some(proof) => (
                        &proof.target,
                        None,
                        proof.precision,
                        proof.scale,
                        proof.nullable,
                        false,
                        &proof.declared_type,
                    ),
                    None => (
                        &self.targets[i],
                        self.formats[i].as_deref(),
                        self.precisions[i],
                        self.scales[i],
                        self.nullable[i],
                        self.empty_is_null[i],
                        &self.declared_types[i],
                    ),
                };
            let conversion = if self.multiple[i] {
                // A `multiple:` column's declared type describes each ELEMENT,
                // so the same scalar admission rule applies element-wise. A
                // one-off scalar under the same declaration still follows that
                // rule and is normalized to a one-element array.
                match &raw {
                    Value::Array(items) => items
                        .iter()
                        .enumerate()
                        .map(|(index, item)| {
                            validate_declared_value(
                                item,
                                target,
                                format,
                                precision,
                                scale,
                                nullable,
                                empty_is_null,
                            )
                            .map_err(|message| format!("element {}: {message}", index + 1))
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map(Value::Array),
                    // Defensive. E361 owns the invariant that a declared-
                    // multiple column comes from a format whose reader
                    // produces an array, so this arm is unreachable for a
                    // compiled plan. It normalizes rather than panicking —
                    // cheaper, and it keeps no second, drifting copy of the
                    // gate's rule on the record path — but it wraps the
                    // coerced value into a one-element array rather than
                    // passing it through bare, so a gate regression upstream
                    // cannot put a scalar in a slot the planner typed as an
                    // array and have every downstream array expression read it
                    // as the wrong shape.
                    Value::Null => validate_declared_value(
                        &raw,
                        target,
                        format,
                        precision,
                        scale,
                        nullable,
                        empty_is_null,
                    ),
                    scalar => validate_declared_value(
                        scalar,
                        target,
                        format,
                        precision,
                        scale,
                        nullable,
                        empty_is_null,
                    )
                    .map(|value| Value::Array(vec![value])),
                }
            } else {
                validate_declared_value(
                    &raw,
                    target,
                    format,
                    precision,
                    scale,
                    nullable,
                    empty_is_null,
                )
            };
            let coerced = match conversion {
                Ok(value) => value,
                Err(message) => {
                    return Err(FormatError::DeclaredType(Box::new(DeclaredTypeFailure {
                        source: self.source_name.to_string(),
                        field: cols[i].to_string(),
                        column: i + 1,
                        declared_type: declared_type.to_string(),
                        original_value: raw,
                        original_record: record.clone(),
                        message,
                    })));
                }
            };
            // Honor the column's `long_unique` storage hint: rebuild a string
            // value in the header-free `Box`-backed arm. Coercion runs first, so
            // a column declared `string` (exact native validation) is the usual
            // case. Non-string values are untouched.
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

    fn take_source_lifecycle_events(&mut self) -> Vec<clinker_format::SourceLifecycleEvent> {
        self.inner.take_source_lifecycle_events()
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

/// Classify one authored column into an explicit admission target.
fn declared_type_target(ty: &Type, pretyped: bool) -> DeclaredTypeTarget {
    if pretyped {
        return DeclaredTypeTarget::ReaderProven {
            declared: ty.clone(),
        };
    }
    match ty {
        Type::Nullable(inner) => declared_type_target(inner, false),
        Type::String | Type::Null | Type::Array | Type::Map => {
            DeclaredTypeTarget::ValidateExact(ty.clone())
        }
        Type::Any => DeclaredTypeTarget::AcceptAny,
        Type::Bool
        | Type::Int
        | Type::Float
        | Type::Decimal
        | Type::Date
        | Type::DateTime
        | Type::Numeric => DeclaredTypeTarget::Coerce(ty.clone()),
    }
}

/// Admit a decoded scalar according to its explicit declared-type target.
///
/// This function is shared by ordinary scalar columns and every element of a
/// `multiple:` column. It never mutates the original [`Value`], so the caller
/// can retain the complete source value and record when admission fails.
fn validate_declared_value(
    value: &Value,
    target: &DeclaredTypeTarget,
    format: Option<&str>,
    precision: Option<u8>,
    scale: Option<u8>,
    nullable: bool,
    empty_is_null: bool,
) -> Result<Value, String> {
    if empty_is_null && matches!(value, Value::String(text) if text.is_empty()) {
        return Ok(Value::Null);
    }

    if matches!(value, Value::Null) {
        return match target {
            DeclaredTypeTarget::ValidateExact(Type::Null)
            | DeclaredTypeTarget::ReaderProven {
                declared: Type::Null | Type::Any,
            }
            | DeclaredTypeTarget::AcceptAny
            | DeclaredTypeTarget::EngineManaged => Ok(Value::Null),
            _ if nullable => Ok(Value::Null),
            DeclaredTypeTarget::Coerce(declared)
            | DeclaredTypeTarget::ValidateExact(declared)
            | DeclaredTypeTarget::ReaderProven { declared, .. } => {
                Err(format!("null is not a valid {declared}"))
            }
        };
    }

    match target {
        DeclaredTypeTarget::Coerce(declared) => {
            coerce_value(value, declared, format, precision, scale)
        }
        DeclaredTypeTarget::ValidateExact(declared) => {
            if native_value_matches(value, declared) {
                Ok(value.clone())
            } else {
                Err(format!(
                    "native {} does not match declared {declared}",
                    native_value_name(value)
                ))
            }
        }
        DeclaredTypeTarget::ReaderProven { declared, .. } => {
            validate_reader_proof(value, declared, precision, scale)
        }
        DeclaredTypeTarget::AcceptAny | DeclaredTypeTarget::EngineManaged => Ok(value.clone()),
    }
}

/// Validate the value emitted by a positional reader against the facts that
/// reader claims to have established. This is deliberately validation-only:
/// a reader bug cannot be hidden by coercing its output a second time.
fn validate_reader_proof(
    value: &Value,
    declared: &Type,
    precision: Option<u8>,
    scale: Option<u8>,
) -> Result<Value, String> {
    let declared = unwrap_nullable(declared);
    if !native_value_matches(value, declared) {
        return Err(format!(
            "reader emitted native {} for declared {declared}",
            native_value_name(value)
        ));
    }
    match value {
        Value::Float(number) if !number.is_finite() => {
            Err("non-finite float is outside the declared type".into())
        }
        Value::Decimal(decimal) => validate_decimal_constraints(*decimal, precision, scale),
        _ => Ok(value.clone()),
    }
}

fn validate_decimal_constraints(
    decimal: rust_decimal::Decimal,
    precision: Option<u8>,
    scale: Option<u8>,
) -> Result<Value, String> {
    let mut exact = decimal;
    if let Some(scale) = scale {
        let rounded = coercion::round_decimal_to_scale(decimal, Some(scale));
        if rounded != decimal {
            return Err(format!(
                "decimal requires rounding to declared scale {scale}"
            ));
        }
        exact.rescale(u32::from(scale));
    }
    if let Some(max_digits) = precision {
        let digits = exact.mantissa().unsigned_abs().to_string().len();
        if digits > usize::from(max_digits) {
            return Err(format!(
                "decimal uses {digits} digits, exceeding declared precision {max_digits}"
            ));
        }
    }
    Ok(Value::Decimal(exact))
}

fn native_value_matches(value: &Value, declared: &Type) -> bool {
    match declared {
        Type::Null => matches!(value, Value::Null),
        Type::Bool => matches!(value, Value::Bool(_)),
        Type::Int => matches!(value, Value::Integer(_)),
        Type::Float => matches!(value, Value::Float(_)),
        Type::Decimal => matches!(value, Value::Decimal(_)),
        Type::String => matches!(value, Value::String(_)),
        Type::Date => matches!(value, Value::Date(_)),
        Type::DateTime => matches!(value, Value::DateTime(_)),
        Type::Array => matches!(value, Value::Array(_)),
        Type::Map => matches!(value, Value::Map(_)),
        Type::Numeric => matches!(value, Value::Integer(_) | Value::Float(_)),
        Type::Any => true,
        Type::Nullable(inner) => matches!(value, Value::Null) || native_value_matches(value, inner),
    }
}

fn native_value_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Integer(_) => "int",
        Value::Float(_) => "float",
        Value::Decimal(_) => "decimal",
        Value::String(_) => "string",
        Value::Date(_) => "date",
        Value::DateTime(_) => "date_time",
        Value::Array(_) => "array",
        Value::Map(_) => "map",
    }
}

/// Coerce a value transactionally to its declared type.
///
/// `format` is the column's `format:` strftime string, honored for `date` /
/// `date_time`; `None` falls back to the coercion module's default format
/// chain. Failure is returned to the caller with no partially converted row.
fn coerce_value(
    value: &Value,
    target: &Type,
    format: Option<&str>,
    precision: Option<u8>,
    scale: Option<u8>,
) -> Result<Value, String> {
    if matches!(value, Value::String(s) if s.is_empty()) {
        return Err(format!("empty string is not a valid {target}"));
    }
    // `Option<&str>::into_iter` yields the one format when present, else
    // nothing — an empty chain selects the default formats.
    let chain: Vec<&str> = format.into_iter().collect();
    let converted = match target {
        Type::Int => {
            if let Value::Float(number) = value
                && (!number.is_finite()
                    || number.fract() != 0.0
                    || *number < i64::MIN as f64
                    || *number >= -(i64::MIN as f64))
            {
                return Err("conversion to int would overflow or discard a fraction".into());
            }
            coercion::coerce_to_int(value)
        }
        Type::Float => {
            let converted = coercion::coerce_to_float(value).map_err(coercion_failure_reason)?;
            let Value::Float(number) = converted else {
                return Ok(converted);
            };
            if !number.is_finite() {
                return Err("non-finite float is outside the declared type".into());
            }
            if let Value::Integer(integer) = value
                && number as i128 != i128::from(*integer)
            {
                return Err("conversion to float would lose integer precision".into());
            }
            return Ok(Value::Float(number));
        }
        Type::Decimal => {
            let unscaled =
                coercion::coerce_to_decimal(value, None).map_err(coercion_failure_reason)?;
            let Value::Decimal(decimal) = unscaled else {
                return Ok(unscaled);
            };
            return validate_decimal_constraints(decimal, precision, scale);
        }
        Type::Bool => coercion::coerce_to_bool(value),
        Type::Date => coercion::coerce_to_date(value, &chain),
        Type::DateTime => coercion::coerce_to_datetime(value, &chain),
        Type::Numeric => {
            let converted = match value {
                Value::Integer(_) => return Ok(value.clone()),
                Value::Float(number) if number.is_finite() => return Ok(value.clone()),
                Value::Float(_) => {
                    return Err("non-finite float is outside the declared type".into());
                }
                // Textual numeric input prefers an exact integer parse, then a
                // finite float. Native floats never take the integer branch
                // above, so a fractional value cannot be truncated merely
                // because the union also admits integers.
                _ => coercion::coerce_to_int(value)
                    .or_else(|_| coercion::coerce_to_float(value))
                    .map_err(coercion_failure_reason)?,
            };
            if matches!(converted, Value::Float(number) if !number.is_finite()) {
                return Err("non-finite float is outside the declared type".into());
            }
            return Ok(converted);
        }
        _ => return Err(format!("unsupported declared coercion target {target}")),
    };
    converted.map_err(coercion_failure_reason)
}

/// Render the coercion class without echoing the untrusted value embedded in
/// `CoercionError`. The value itself is represented only by the bounded,
/// sanitized diagnostic preview and by the complete DLQ evidence.
fn coercion_failure_reason(error: coercion::CoercionError) -> String {
    match error {
        coercion::CoercionError::TypeMismatch { from, to, .. } => {
            format!("cannot coerce {from} to {to}")
        }
        coercion::CoercionError::ParseFailure { target, .. } => {
            format!("value cannot be parsed as {target}")
        }
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

    /// E361 owns the invariant that a `multiple:` column comes from a format
    /// whose reader produces an array, so the defensive arm is unreachable for
    /// a compiled plan. It still has to leave the slot holding the shape the
    /// planner typed it as: a bare scalar there would let every downstream
    /// array expression read the wrong shape, and a JSON sink would emit a
    /// scalar where the schema promised a list. An absent field stays null,
    /// which is what a `multiple:` column reads as everywhere else.
    #[test]
    fn defensive_scalar_arm_normalizes_a_multi_value_column_to_an_array() {
        let multi = |name: &str| Column {
            multiple: Some(true),
            ..col(name, Type::Int)
        };
        // `absent` is declared but not carried by the input, which is the one
        // route to a null here.
        let schema = vec![
            multi("codes"),
            Column {
                multiple: Some(true),
                ..col("absent", Type::nullable(Type::Int))
            },
        ];
        let reader = csv_reader("codes\n7\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let rec = coercing.next_record().unwrap().unwrap();
        assert_eq!(
            rec.get("codes"),
            Some(&Value::Array(vec![Value::Integer(7)]))
        );
        assert_eq!(rec.get("absent"), Some(&Value::Null));
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
    fn test_coerce_failure_is_declared_type_error() {
        let schema = vec![col("num", Type::Int)];
        let reader = csv_reader("num\nnot_a_number\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();

        let error = coercing.next_record().unwrap_err();
        let FormatError::DeclaredType(failure) = error else {
            panic!("expected declared type failure, got {error:?}");
        };
        assert_eq!(failure.field, "num");
        assert_eq!(failure.original_value, Value::String("not_a_number".into()));
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
    /// format (`%m/%d/%Y` rejects month 15), so strict coercion rejects the
    /// row — proving the custom format above is what admitted it.
    #[test]
    fn test_coerce_date_without_format_is_declared_type_error() {
        let schema = vec![Column::bare("d", Type::Date)];
        let reader = csv_reader("d\n15/01/2024\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "src", false).unwrap();
        let error = coercing.next_record().unwrap_err();
        assert!(matches!(error, FormatError::DeclaredType(_)));
    }

    /// A format-free declared `date_time` accepts the canonical RFC 3339 UTC
    /// spelling used by scenario and operational exports. The source value is
    /// parsed into the declared temporal type rather than retained as raw text.
    #[test]
    fn test_coerce_datetime_accepts_rfc3339_utc_as_typed_value() {
        let schema = vec![Column::bare("opened_at", Type::DateTime)];
        let reader = csv_reader("opened_at\n2026-01-31T08:27:00Z\n");
        let mut coercing =
            CoercingReader::new(reader, &schema, drop_policy(), "tickets", false).unwrap();

        let record = coercing.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("opened_at"),
            Some(&Value::DateTime(
                chrono::NaiveDate::from_ymd_opt(2026, 1, 31)
                    .unwrap()
                    .and_hms_opt(8, 27, 0)
                    .unwrap(),
            )),
        );
    }

    /// A positional reader's proof retains the declared type. Wrong native
    /// variants and nulls in non-nullable columns fail instead of being hidden
    /// behind a marker that merely skipped the second coercion pass.
    #[test]
    fn test_pretyped_proof_rejects_wrong_variant_and_non_nullable_null() {
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
        let decl = vec![col("n", Type::Int)];
        for value in [Value::String("42".into()), Value::Null] {
            let reader = Box::new(StubReader {
                schema: StdArc::clone(&schema_arc),
                rows: vec![vec![value]].into_iter(),
            });
            let mut coercing =
                CoercingReader::new(reader, &decl, drop_policy(), "p", true).unwrap();
            assert!(matches!(
                coercing.next_record(),
                Err(FormatError::DeclaredType(_))
            ));
        }
    }

    #[test]
    fn test_pretyped_proof_enforces_decimal_constraints_and_finite_float() {
        use clinker_format::traits::FormatReader as FRTrait;
        use clinker_record::Schema as RecordSchema;
        use rust_decimal::Decimal;
        use std::sync::Arc as StdArc;

        struct StubReader {
            schema: StdArc<RecordSchema>,
            value: Option<Value>,
        }
        impl FRTrait for StubReader {
            fn schema(&mut self) -> Result<StdArc<RecordSchema>, FormatError> {
                Ok(StdArc::clone(&self.schema))
            }
            fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
                Ok(self
                    .value
                    .take()
                    .map(|value| Record::new(StdArc::clone(&self.schema), vec![value])))
            }
        }

        let schema = StdArc::new(RecordSchema::new(vec!["v".into()]));
        let cases = [
            (
                Value::Decimal(Decimal::new(12_345, 3)),
                Column {
                    precision: Some(8),
                    scale: Some(2),
                    ..col("v", Type::Decimal)
                },
            ),
            (
                Value::Decimal(Decimal::new(12_345, 2)),
                Column {
                    precision: Some(4),
                    scale: Some(2),
                    ..col("v", Type::Decimal)
                },
            ),
            (Value::Float(f64::INFINITY), col("v", Type::Float)),
        ];
        for (value, declaration) in cases {
            let reader = Box::new(StubReader {
                schema: StdArc::clone(&schema),
                value: Some(value),
            });
            let mut coercing =
                CoercingReader::new(reader, &[declaration], drop_policy(), "p", true).unwrap();
            assert!(matches!(
                coercing.next_record(),
                Err(FormatError::DeclaredType(_))
            ));
        }
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
