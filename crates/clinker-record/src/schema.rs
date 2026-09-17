use crate::owned_storage::{
    AllocationResources, AllocationScope, GovernedBox, OwnedKey, OwnedVec, ResourceError,
    ResourceErrorKind, SharedStorage, map_backing_layout, reserve_holder,
};
use ahash::RandomState;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-column annotation distinguishing an engine-stamped column from a
/// user-declared one.
///
/// The annotation is the structural marker writers and the projection
/// fast path consult to decide whether a column is included in default
/// output. Two engine-stamping shapes exist:
///
/// - [`FieldMetadata::SourceCorrelation`] — the source-binding pass
///   widens a Source's schema with a `$ck.<field>` shadow column when
///   that source declares a `correlation_key`. The shadow column is
///   stamped at ingest with the user-declared field's value, then
///   write-protected at the CXL parser (`emit $ck.* = ...` is
///   rejected). Frozen-identity semantics flow through the column for
///   the rest of the DAG so downstream Transforms cannot disturb
///   correlation-group identity.
/// - [`FieldMetadata::AggregateGroupIndex`] — a relaxed aggregate
///   (one whose `group_by` does not cover the upstream correlation
///   key) emits one `$ck.aggregate.<aggregate_name>` column carrying
///   the aggregator's group index. Detect-phase walks this lineage to
///   resolve a downstream failure back to every contributing source
///   row, matching the upstream-failure DLQ-fan-out semantic.
/// - [`FieldMetadata::WidenedSidecar`] — a per-Source absorber column
///   added by `on_unmapped: auto_widen`. Carries a `Value::Map` of
///   undeclared input fields keyed by their original name. The
///   typechecker is blind to its contents (CXL has no Map operators
///   in the user surface); the Sink node opts the contents back
///   into top-level columns via `include_unmapped: true`. Pattern
///   precedent: Databricks Auto Loader's `_rescued_data` and
///   ClickHouse's `JSON` column type.
/// - [`FieldMetadata::SourceFile`] — per-Source lineage column carrying
///   the originating file path Arc per record. Named `$source.file`;
///   stamped at ingest with the path the record was read from
///   (`MultiFileFormatReader::current_source_file()`). Replaces the
///   pre-multi-source `ctx.source_file_arcs` external Vec indexed by
///   row number — required when peer Sources merge downstream and
///   per-source row numbers collide. Filtered out of default Output
///   projection like the other engine-stamped variants; users opt in
///   by projecting `emit source_file = $source.file` explicitly.
/// - [`FieldMetadata::SourceName`] — per-record Source-node identity
///   column. Named `$source.name`; stamped at Source ingest with the
///   originating Source node's name as a shared `Arc<str>`. Survives
///   Merge / Combine so post-fan-in records still resolve to their
///   originating Source by node name (the case schema identity alone
///   cannot answer when peer Sources share a column shape — the
///   silent-corruption topology at the root of #47).
/// - [`FieldMetadata::SourceEventTime`] — per-record event-time stamp,
///   delay-corrected. Named `$source.event_time`; stamped at Source
///   ingest with the i64-nanos value derived from each record's
///   `WatermarkConfig.column`. Read by the time-windowed aggregate
///   operator to assign each record to its window(s) — the unified
///   per-record event-time column across heterogeneous sources whose
///   declared event-time column names may differ.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldMetadata {
    /// Source-CK shadow column. `source_field` is the user-declared
    /// field whose value the engine snapshots into this column at
    /// ingest. The column is named `$ck.<source_field>`.
    SourceCorrelation { source_field: OwnedKey },
    /// Synthetic correlation column emitted by a relaxed aggregate.
    /// `aggregate_name` is the aggregate node's name; the column is
    /// named `$ck.aggregate.<aggregate_name>` and carries the group
    /// index assigned by the aggregator at finalize.
    AggregateGroupIndex { aggregate_name: OwnedKey },
    /// Sidecar absorber column for `on_unmapped: auto_widen`. The
    /// column is named `$widened` and carries a `Value::Map` of every
    /// input-record key that was not in the user-declared schema.
    WidenedSidecar,
    /// Per-record source-file lineage column. Named `$source.file`,
    /// stamped at Source ingest with the originating file path Arc.
    /// Read by `$source.file` / `$source.path` resolution at dispatch
    /// sites; survives merge so post-fan-in records still resolve to
    /// their actual file rather than an external row-keyed lookup.
    SourceFile,
    /// Per-record Source-node identity column. Named `$source.name`,
    /// stamped at Source ingest with the originating Source node's
    /// name as a shared `Arc<str>` (one Arc per Source, cloned per
    /// record). Read by `$source.name` resolution at dispatch sites;
    /// survives merge so post-fan-in records still report origin even
    /// when peer Sources share a column shape.
    SourceName,
    /// Per-record delay-corrected event-time column. Named
    /// `$source.event_time`, value `i64` nanoseconds since the Unix
    /// epoch. Stamped at Source ingest from the source's declared
    /// `WatermarkConfig.column`, with any configured `delay` already
    /// subtracted so the column matches what was folded into
    /// `PerSourceWatermarks`. `Value::Null` when the source declared no
    /// watermark column or the per-record value was Null / unparseable.
    /// Read by the time-windowed aggregate operator
    /// (https://github.com/rustpunk/clinker/issues/61) to assign each
    /// record to its window(s).
    SourceEventTime,
    /// Reshape audit-stamp column (`$meta.synthetic`,
    /// `$meta.synthesized_by`, `$meta.mutated_by`). Engine-written per
    /// Reshape output record so the provenance of a synthesized or
    /// mutated row is queryable downstream while staying out of the
    /// default writer surface — mirroring the `$ck.*` posture.
    ReshapeAudit,
}

impl FieldMetadata {
    fn unaccounted_heap_size(&self, resources: &AllocationResources) -> usize {
        match self {
            Self::SourceCorrelation { source_field: key }
            | Self::AggregateGroupIndex {
                aggregate_name: key,
            } => key.unaccounted_heap_size(resources),
            _ => 0,
        }
    }

    /// Copy textual metadata into independently admitted storage.
    pub fn try_clone_in(&self, scope: &AllocationScope) -> Result<Self, ResourceError> {
        Ok(match self {
            Self::SourceCorrelation { source_field } => Self::SourceCorrelation {
                source_field: OwnedKey::try_new(source_field, scope)?,
            },
            Self::AggregateGroupIndex { aggregate_name } => Self::AggregateGroupIndex {
                aggregate_name: OwnedKey::try_new(aggregate_name, scope)?,
            },
            other => other.clone(),
        })
    }
    fn heap_size(&self, legacy: bool) -> usize {
        match self {
            Self::SourceCorrelation { source_field: key }
            | Self::AggregateGroupIndex {
                aggregate_name: key,
            } => {
                if legacy {
                    key.legacy_heap_size()
                } else {
                    key.heap_size()
                }
            }
            _ => 0,
        }
    }

    /// Marks this column as a source-CK snapshot of `source_field`.
    pub fn source_correlation(source_field: impl Into<Box<str>>) -> Self {
        Self::SourceCorrelation {
            source_field: OwnedKey::from_box(source_field.into()),
        }
    }

    /// Marks this column as the synthetic group-index emitted by a
    /// relaxed aggregate node named `aggregate_name`.
    pub fn aggregate_group_index(aggregate_name: impl Into<Box<str>>) -> Self {
        Self::AggregateGroupIndex {
            aggregate_name: OwnedKey::from_box(aggregate_name.into()),
        }
    }

    /// Marks this column as the per-Source `auto_widen` sidecar
    /// absorber. Always named `$widened`; carries a `Value::Map`
    /// payload.
    pub fn widened_sidecar() -> Self {
        Self::WidenedSidecar
    }

    /// Marks this column as the per-record source-file lineage stamp.
    /// Always named `$source.file`; the value is a `Value::String`
    /// wrapping the originating file path Arc.
    pub fn source_file() -> Self {
        Self::SourceFile
    }

    /// Marks this column as the per-record Source-node identity stamp.
    /// Always named `$source.name`; the value is a `Value::String`
    /// wrapping the originating Source node's name Arc.
    pub fn source_name() -> Self {
        Self::SourceName
    }

    /// Marks this column as the per-record delay-corrected event-time
    /// stamp. Always named `$source.event_time`; the value is a
    /// `Value::Int` carrying i64 nanoseconds since the Unix epoch, or
    /// `Value::Null` when the source has no watermark column or the
    /// per-record value did not parse.
    pub fn source_event_time() -> Self {
        Self::SourceEventTime
    }

    /// Marks this column as a Reshape audit stamp (`$meta.synthetic` /
    /// `$meta.synthesized_by` / `$meta.mutated_by`).
    pub fn reshape_audit() -> Self {
        Self::ReshapeAudit
    }

    /// True for every variant. The presence of [`FieldMetadata`] on a
    /// column is itself the engine-stamp marker; the variant only
    /// distinguishes which engine subsystem stamped it. Centralizes the
    /// writer-strip / projection-skip predicate so future engine-
    /// stamped namespaces extend this enum and surface here without
    /// every consumer having to re-enumerate variants.
    pub fn is_engine_stamped(&self) -> bool {
        true
    }
}

type AdmittedIndex = indexmap::IndexMap<OwnedKey, usize, RandomState>;
#[derive(Debug)]
enum NameIndex {
    Legacy(HashMap<OwnedKey, usize, RandomState>),
    Governed(GovernedBox<AdmittedIndex>),
}
impl NameIndex {
    fn unaccounted_heap_size(&self, resources: &AllocationResources) -> usize {
        match self {
            Self::Legacy(map) => {
                let slots = map.capacity().saturating_mul(2);
                slots * (std::mem::size_of::<(OwnedKey, usize)>() + 1)
                    + usize::from(slots != 0) * 16
                    + map
                        .keys()
                        .map(|key| key.unaccounted_heap_size(resources))
                        .sum::<usize>()
            }
            Self::Governed(map) => {
                map.unaccounted_backing_size(resources)
                    + map
                        .payload()
                        .keys()
                        .map(|key| key.unaccounted_heap_size(resources))
                        .sum::<usize>()
            }
        }
    }

    fn get(&self, name: &str) -> Option<&usize> {
        match self {
            Self::Legacy(map) => map.get(name),
            Self::Governed(map) => map.payload().get(name),
        }
    }
    fn heap_size(&self, legacy: bool) -> usize {
        let key_size = |key: &OwnedKey| {
            if legacy {
                key.legacy_heap_size()
            } else {
                key.heap_size()
            }
        };
        match self {
            Self::Legacy(map) => {
                let slots = map.capacity().saturating_mul(2);
                slots * (std::mem::size_of::<(OwnedKey, usize)>() + 1)
                    + usize::from(slots != 0) * 16
                    + map.keys().map(key_size).sum::<usize>()
            }
            Self::Governed(map) => {
                (if legacy { 0 } else { map.bytes() })
                    + map.payload().keys().map(key_size).sum::<usize>()
            }
        }
    }
}
impl Clone for NameIndex {
    fn clone(&self) -> Self {
        let mut copy = HashMap::with_hasher(RandomState::with_seeds(1, 2, 3, 4));
        match self {
            Self::Legacy(map) => return Self::Legacy(map.clone()),
            Self::Governed(map) => {
                copy.reserve(map.payload().len());
                copy.extend(
                    map.payload()
                        .iter()
                        .map(|(key, index)| (key.clone(), *index)),
                );
            }
        }
        Self::Legacy(copy)
    }
}

/// Fallible schema construction retaining each column, metadata and index owner.
#[derive(Debug)]
pub struct AdmittedSchemaBuilder {
    columns: OwnedVec<OwnedKey>,
    field_metadata: OwnedVec<Option<FieldMetadata>>,
}
impl AdmittedSchemaBuilder {
    pub fn try_with_capacity(
        capacity: usize,
        scope: &AllocationScope,
    ) -> Result<Self, ResourceError> {
        Ok(Self {
            columns: OwnedVec::try_with_capacity(capacity, scope)?,
            field_metadata: OwnedVec::try_with_capacity(capacity, scope)?,
        })
    }
    /// Reserve both vectors before publishing either new positional element.
    pub fn try_push(
        &mut self,
        name: OwnedKey,
        metadata: Option<FieldMetadata>,
        scope: &AllocationScope,
    ) -> Result<(), ResourceError> {
        let needed =
            self.columns.len().checked_add(1).ok_or_else(|| {
                ResourceError::new(ResourceErrorKind::Layout, self.columns.len(), 0)
            })?;
        self.columns.try_reserve(needed, scope)?;
        self.field_metadata.try_reserve(needed, scope)?;
        self.columns.push_reserved(name);
        self.field_metadata.push_reserved(metadata);
        Ok(())
    }
    /// Build an independently admitted name index and shared schema outer owner.
    pub fn finish(self, scope: &AllocationScope) -> Result<SharedStorage<Schema>, ResourceError> {
        let layout = map_backing_layout::<OwnedKey, usize>(self.columns.len())?;
        let lease = reserve_holder::<AdmittedIndex>(scope, layout)?;
        let mut index = AdmittedIndex::with_hasher(RandomState::with_seeds(1, 2, 3, 4));
        if index.try_reserve_exact(self.columns.len()).is_err() {
            drop(index);
            return Err(ResourceError::new(
                ResourceErrorKind::Allocation,
                layout.size(),
                0,
            ));
        }
        for (position, name) in self.columns.iter().enumerate() {
            let key = match OwnedKey::try_new(name, scope) {
                Ok(key) => key,
                Err(error) => {
                    drop(index);
                    return Err(error);
                }
            };
            index.insert(key, position);
        }
        let index = NameIndex::Governed(GovernedBox::try_new(index, lease)?);
        SharedStorage::try_new(
            Schema {
                columns: self.columns,
                field_metadata: self.field_metadata,
                index,
            },
            scope,
        )
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    columns: OwnedVec<OwnedKey>,
    /// Per-column metadata, parallel to `columns` (same length, same
    /// order). `None` for user-declared columns; `Some(...)` for
    /// engine-stamped columns (e.g. `$ck.<field>` snapshot columns).
    field_metadata: OwnedVec<Option<FieldMetadata>>,
    index: NameIndex,
}

impl Schema {
    /// Inner fields outside this ledger; the enclosing shared handle is separate.
    pub fn unaccounted_heap_size(&self, resources: &AllocationResources) -> usize {
        self.columns.unaccounted_backing_size(resources)
            + self.field_metadata.unaccounted_backing_size(resources)
            + self.index.unaccounted_heap_size(resources)
            + self
                .columns
                .iter()
                .map(|key| key.unaccounted_heap_size(resources))
                .sum::<usize>()
            + self
                .field_metadata
                .iter()
                .flatten()
                .map(|metadata| metadata.unaccounted_heap_size(resources))
                .sum::<usize>()
    }

    /// Observed container capacities and owned names for a retained schema.
    /// The caller charges the shared schema once, not once per record or reader.
    /// Hash-table control storage uses a conservative bucket estimate; this is
    /// legacy memory reporting rather than an allocation admission grant.
    pub fn estimated_heap_size(&self) -> usize {
        self.heap_size(false)
    }
    /// Retained legacy backing and independently legacy children only.
    pub fn legacy_estimated_heap_size(&self) -> usize {
        self.heap_size(true)
    }
    fn heap_size(&self, legacy: bool) -> usize {
        let slots = if legacy {
            self.columns.legacy_backing_size() + self.field_metadata.legacy_backing_size()
        } else {
            self.columns.backing_size() + self.field_metadata.backing_size()
        };
        slots
            + self.index.heap_size(legacy)
            + self
                .columns
                .iter()
                .map(|key| {
                    if legacy {
                        key.legacy_heap_size()
                    } else {
                        key.heap_size()
                    }
                })
                .sum::<usize>()
            + self
                .field_metadata
                .iter()
                .flatten()
                .map(|metadata| metadata.heap_size(legacy))
                .sum::<usize>()
    }
    /// Construct a schema from column names alone, with no per-column
    /// metadata. Equivalent to `Schema::with_metadata(columns, vec![None; n])`.
    pub fn new(columns: Vec<OwnedKey>) -> Self {
        let n = columns.len();
        Self::with_metadata(columns, vec![None; n])
    }

    /// Construct a schema from columns plus a parallel metadata vector.
    /// `field_metadata.len()` must equal `columns.len()`.
    pub fn with_metadata(
        columns: Vec<OwnedKey>,
        field_metadata: Vec<Option<FieldMetadata>>,
    ) -> Self {
        debug_assert_eq!(
            columns.len(),
            field_metadata.len(),
            "Schema::with_metadata: columns ({}) vs field_metadata ({}) length mismatch",
            columns.len(),
            field_metadata.len(),
        );
        let hasher = RandomState::with_seeds(1, 2, 3, 4);
        let mut index = HashMap::with_capacity_and_hasher(columns.len(), hasher);
        for (i, name) in columns.iter().enumerate() {
            index.insert(name.clone(), i);
        }
        Self {
            columns: OwnedVec::from_vec(columns),
            field_metadata: OwnedVec::from_vec(field_metadata),
            index: NameIndex::Legacy(index),
        }
    }

    /// All column names in insertion order (determines output field ordering).
    pub fn columns(&self) -> &[OwnedKey] {
        &self.columns
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// O(1) name -> positional index lookup. Returns None if field not in schema.
    pub fn index(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    /// Positional index -> name. Returns None if index out of bounds.
    pub fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| &**s)
    }

    /// Returns true if the schema contains a field with this name.
    pub fn contains(&self, name: &str) -> bool {
        self.index.get(name).is_some()
    }

    /// Per-column metadata at positional index, or `None` when the
    /// column has no engine-stamp annotation or `idx` is out of range.
    pub fn field_metadata(&self, idx: usize) -> Option<&FieldMetadata> {
        self.field_metadata.get(idx).and_then(|m| m.as_ref())
    }

    /// Per-column metadata for the named column. Returns `None` for
    /// unknown names or columns with no engine-stamp annotation.
    pub fn field_metadata_by_name(&self, name: &str) -> Option<&FieldMetadata> {
        self.index(name).and_then(|i| self.field_metadata(i))
    }

    /// Whether column `idx` is engine-stamped rather than user-declared — the
    /// predicate deciding what the default writer surface and the default
    /// projection path emit.
    ///
    /// The by-index form serves callers that must keep the column's schema
    /// position, which the field iterators on [`Record`](crate::Record) do not
    /// hand back — a writer precompiling a plan against `Record::values`, or
    /// any pass building a positional column map.
    pub fn is_engine_stamped(&self, idx: usize) -> bool {
        self.field_metadata(idx)
            .is_some_and(FieldMetadata::is_engine_stamped)
    }
}

/// Fluent builder for `Arc<Schema>` construction.
///
/// Consolidates the many ad-hoc `Arc::new(Schema::new(Vec<OwnedKey>))` call
/// sites across readers, planners, and projection into a single API — chained
/// `.with_field(...)`, `.extend(...)`, or `.collect::<SchemaBuilder>()` all
/// terminate in `.build()`, which is the sole place a fresh `Arc<Schema>` is
/// materialized outside the test suite. Patterned on `arrow_schema::SchemaBuilder`.
#[derive(Debug, Clone, Default)]
pub struct SchemaBuilder {
    columns: Vec<OwnedKey>,
    field_metadata: Vec<Option<FieldMetadata>>,
}

impl SchemaBuilder {
    /// Empty builder; equivalent to `SchemaBuilder::default()`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Preallocate storage for `n` columns so push-style construction
    /// avoids intermediate `Vec` growth.
    pub fn with_capacity(n: usize) -> Self {
        Self {
            columns: Vec::with_capacity(n),
            field_metadata: Vec::with_capacity(n),
        }
    }

    /// Append a single column name with no metadata, and return `self`
    /// for chaining.
    pub fn with_field(mut self, name: impl Into<OwnedKey>) -> Self {
        self.columns.push(name.into());
        self.field_metadata.push(None);
        self
    }

    /// Append a column with attached engine-stamp metadata.
    pub fn with_field_meta(mut self, name: impl Into<OwnedKey>, meta: FieldMetadata) -> Self {
        self.columns.push(name.into());
        self.field_metadata.push(Some(meta));
        self
    }

    /// Append every column yielded by `iter`, converting each item via `Into<OwnedKey>`.
    /// All appended columns receive `None` metadata.
    pub fn extend<I, S>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OwnedKey>,
    {
        for s in iter {
            self.columns.push(s.into());
            self.field_metadata.push(None);
        }
        self
    }

    /// Finalize the builder into an `Arc<Schema>`. Consumes `self`.
    pub fn build(self) -> SharedStorage<Schema> {
        SharedStorage::from_arc(Arc::new(Schema::with_metadata(
            self.columns,
            self.field_metadata,
        )))
    }
}

impl<S: Into<OwnedKey>> FromIterator<S> for SchemaBuilder {
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        let cols: Vec<OwnedKey> = iter.into_iter().map(Into::into).collect();
        let n = cols.len();
        Self {
            columns: cols,
            field_metadata: vec![None; n],
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn retained_schema_estimate_includes_unused_capacity() {
        let mut columns = Vec::with_capacity(64);
        columns.push(OwnedKey::from("field"));
        let schema = super::Schema::new(columns);
        let compact = super::Schema::new(vec![OwnedKey::from("field")]);
        assert_eq!(
            schema.estimated_heap_size() - compact.estimated_heap_size(),
            (schema.columns.capacity() - compact.columns.capacity())
                * std::mem::size_of::<Box<str>>()
        );
    }
    use super::*;

    fn test_schema() -> Schema {
        let cols: Vec<OwnedKey> = vec![
            "id".into(),
            "name".into(),
            "age".into(),
            "email".into(),
            "active".into(),
        ];
        Schema::new(cols)
    }

    #[test]
    fn test_schema_index_lookup() {
        let schema = test_schema();
        assert_eq!(schema.index("id"), Some(0));
        assert_eq!(schema.index("name"), Some(1));
        assert_eq!(schema.index("age"), Some(2));
        assert_eq!(schema.index("email"), Some(3));
        assert_eq!(schema.index("active"), Some(4));
        assert_eq!(schema.column_count(), 5);
        assert_eq!(schema.column_name(0), Some("id"));
        assert_eq!(schema.columns().len(), 5);
    }

    #[test]
    fn test_schema_unknown_field_returns_none() {
        let schema = test_schema();
        assert_eq!(schema.index("nonexistent"), None);
        assert_eq!(schema.column_name(99), None);
    }

    #[test]
    fn test_schema_contains() {
        let schema = test_schema();
        assert!(schema.contains("id"));
        assert!(schema.contains("name"));
        assert!(!schema.contains("nonexistent"));
    }

    #[test]
    fn test_schema_duplicate_column_names() {
        let cols: Vec<OwnedKey> = vec!["a".into(), "b".into(), "a".into()];
        let schema = Schema::new(cols);
        assert_eq!(schema.column_count(), 3);
        assert_eq!(schema.index("a"), Some(2)); // last occurrence wins
    }

    #[test]
    fn test_schema_empty() {
        let schema = Schema::new(vec![]);
        assert_eq!(schema.column_count(), 0);
        assert_eq!(schema.index("anything"), None);
        assert_eq!(schema.column_name(0), None);
        assert!(schema.columns().is_empty());
    }

    #[test]
    fn test_schema_no_metadata_by_default() {
        let schema = test_schema();
        assert!(schema.field_metadata(0).is_none());
        assert!(schema.field_metadata_by_name("id").is_none());
    }

    #[test]
    fn test_schema_with_metadata_attaches_source_correlation() {
        let cols: Vec<OwnedKey> = vec!["employee_id".into(), "$ck.employee_id".into()];
        let meta = vec![None, Some(FieldMetadata::source_correlation("employee_id"))];
        let schema = Schema::with_metadata(cols, meta);
        assert!(schema.field_metadata(0).is_none());
        let stamp = schema.field_metadata(1).expect("metadata attached");
        match stamp {
            FieldMetadata::SourceCorrelation { source_field } => {
                assert_eq!(source_field.as_ref(), "employee_id");
            }
            other => panic!("expected SourceCorrelation, got {other:?}"),
        }
        assert!(stamp.is_engine_stamped());
        match schema.field_metadata_by_name("$ck.employee_id") {
            Some(FieldMetadata::SourceCorrelation { source_field }) => {
                assert_eq!(source_field.as_ref(), "employee_id");
            }
            other => panic!("expected SourceCorrelation, got {other:?}"),
        }
    }

    #[test]
    fn test_schema_with_metadata_attaches_aggregate_group_index() {
        let cols: Vec<OwnedKey> = vec!["region".into(), "$ck.aggregate.dept_totals".into()];
        let meta = vec![
            None,
            Some(FieldMetadata::aggregate_group_index("dept_totals")),
        ];
        let schema = Schema::with_metadata(cols, meta);
        let stamp = schema.field_metadata(1).expect("metadata attached");
        match stamp {
            FieldMetadata::AggregateGroupIndex { aggregate_name } => {
                assert_eq!(aggregate_name.as_ref(), "dept_totals");
            }
            other => panic!("expected AggregateGroupIndex, got {other:?}"),
        }
        assert!(stamp.is_engine_stamped());
    }

    #[test]
    fn test_schema_with_metadata_attaches_source_file() {
        let cols: Vec<OwnedKey> = vec!["id".into(), "$source.file".into()];
        let meta = vec![None, Some(FieldMetadata::source_file())];
        let schema = Schema::with_metadata(cols, meta);
        assert!(schema.field_metadata(0).is_none());
        let stamp = schema.field_metadata(1).expect("metadata attached");
        match stamp {
            FieldMetadata::SourceFile => {}
            other => panic!("expected SourceFile, got {other:?}"),
        }
        assert!(stamp.is_engine_stamped());
        match schema.field_metadata_by_name("$source.file") {
            Some(FieldMetadata::SourceFile) => {}
            other => panic!("expected SourceFile, got {other:?}"),
        }
    }

    #[test]
    fn test_schema_with_metadata_attaches_source_name() {
        let cols: Vec<OwnedKey> = vec!["id".into(), "$source.name".into()];
        let meta = vec![None, Some(FieldMetadata::source_name())];
        let schema = Schema::with_metadata(cols, meta);
        assert!(schema.field_metadata(0).is_none());
        let stamp = schema.field_metadata(1).expect("metadata attached");
        match stamp {
            FieldMetadata::SourceName => {}
            other => panic!("expected SourceName, got {other:?}"),
        }
        assert!(stamp.is_engine_stamped());
        match schema.field_metadata_by_name("$source.name") {
            Some(FieldMetadata::SourceName) => {}
            other => panic!("expected SourceName, got {other:?}"),
        }
    }

    /// Pins the in-memory size of [`FieldMetadata`] so a future variant
    /// addition that bloats the discriminant or payload surfaces here
    /// before it ships through every `Schema` and `Vec<Option<_>>`
    /// allocation in the workspace.
    #[test]
    fn test_field_metadata_size_regression_guard() {
        use std::mem::size_of;
        // Both variants carry one `Box<str>` (16 bytes on 64-bit) plus
        // the discriminant. 24 bytes (16 payload + 8 discriminant with
        // niche packing) is the natural shape; assert <= 24 to allow
        // the niche optimization to keep working without locking us in.
        assert!(
            size_of::<FieldMetadata>() <= 24,
            "FieldMetadata grew beyond 24 bytes ({} B)",
            size_of::<FieldMetadata>(),
        );
    }

    #[test]
    fn test_schema_builder_empty_build() {
        let schema = SchemaBuilder::new().build();
        assert_eq!(schema.column_count(), 0);
    }

    #[test]
    fn test_schema_builder_field_order_preserved() {
        let schema = SchemaBuilder::new().with_field("a").with_field("b").build();
        assert_eq!(&*schema.columns()[0], "a");
        assert_eq!(&*schema.columns()[1], "b");
    }

    #[test]
    fn test_schema_builder_extend_matches_manual() {
        let via_extend = SchemaBuilder::new().extend(["a", "b", "c"]).build();
        let via_chain = SchemaBuilder::new()
            .with_field("a")
            .with_field("b")
            .with_field("c")
            .build();
        assert_eq!(via_extend.columns(), via_chain.columns());
    }

    #[test]
    fn test_schema_builder_from_iterator_collect() {
        let schema = ["a", "b", "c"]
            .into_iter()
            .collect::<SchemaBuilder>()
            .build();
        assert_eq!(schema.column_count(), 3);
        assert_eq!(&*schema.columns()[0], "a");
        assert_eq!(&*schema.columns()[1], "b");
        assert_eq!(&*schema.columns()[2], "c");
    }

    #[test]
    fn test_schema_builder_index_lookup_post_build() {
        let schema = SchemaBuilder::new().with_field("x").with_field("y").build();
        assert_eq!(schema.index("y"), Some(1));
    }

    #[test]
    fn test_schema_builder_duplicate_column_last_wins() {
        let schema = SchemaBuilder::new()
            .with_field("a")
            .with_field("b")
            .with_field("a")
            .build();
        assert_eq!(schema.column_count(), 3);
        assert_eq!(schema.index("a"), Some(2));
    }

    #[test]
    fn test_schema_builder_with_field_meta_attaches_source_correlation() {
        let schema = SchemaBuilder::new()
            .with_field("employee_id")
            .with_field_meta(
                "$ck.employee_id",
                FieldMetadata::source_correlation("employee_id"),
            )
            .build();
        assert_eq!(schema.column_count(), 2);
        assert!(schema.field_metadata(0).is_none());
        match schema.field_metadata(1) {
            Some(FieldMetadata::SourceCorrelation { source_field }) => {
                assert_eq!(source_field.as_ref(), "employee_id");
            }
            other => panic!("expected SourceCorrelation, got {other:?}"),
        }
    }
}
