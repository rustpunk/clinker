//! Compile-time dead-letter layout: which file each dead-lettered row lands
//! in, and the CSV header that file carries.
//!
//! A pipeline's dead-letter output is fixed by its plan, not by the rows that
//! happened to fail. [`DlqLayout`] is derived once from the final
//! [`ExecutionPlanDag`], the bound composition bodies and the
//! `error_handling.dlq` block. Every bucket's header is the fixed
//! `_cxl_dlq_*` engine prelude followed by the stable union of the user
//! columns of every schema a dead-letter site can carry into that bucket, in
//! site order. A row that lacks one of those columns writes an empty cell; a
//! row that carries a user column the layout did not admit is a planner
//! defect the runtime refuses rather than writes.
//!
//! The layout is plan-sized and independent of input. It is tooling and
//! runtime metadata only: it never enters `pipeline_hash` or semantic
//! identity.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use clinker_record::owned_storage::SharedStorage;
use clinker_record::{FieldMetadata, Schema, SchemaBuilder};
use petgraph::Direction;
use petgraph::graph::{DiGraph, NodeIndex};

use super::composition_body::{BoundBody, CompositionBodies};
use super::deferred_region::DeferredRegion;
use super::execution::{ExecutionPlanDag, PlanEdge, PlanNode, PlanSinkPayload};
use crate::config::pipeline_node::{
    SOURCE_EVENT_TIME_COLUMN, SOURCE_FILE_COLUMN, SOURCE_NAME_COLUMN, SOURCE_RAW_RECORD_COLUMN,
};
use crate::config::{DlqConfig, DlqGranularity, ErrorStrategy, OutputFormat};
use clinker_format::OnConflict;

/// Dense identity of one dead-letter bucket inside a [`DlqLayout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DlqBucketId(usize);

impl DlqBucketId {
    /// Position of the bucket in [`DlqLayout::buckets`].
    pub fn index(self) -> usize {
        self.0
    }
}

/// One dead-letter destination file and the header fixed for it at compile
/// time.
#[derive(Debug, Clone)]
pub struct DlqBucket {
    path: PathBuf,
    header: Vec<String>,
    user_columns: Vec<String>,
}

impl DlqBucket {
    /// The authored path that first claimed this bucket: the pipeline-wide
    /// `path` when it names the bucket's file, otherwise the first
    /// `per_source.<name>.path` in source-name order that does.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The complete CSV header, fixed at compile time: the `_cxl_dlq_*`
    /// engine columns, then [`Self::user_columns`]. Every row written to this
    /// bucket has exactly this many cells, whichever sites failed.
    pub fn header(&self) -> &[String] {
        &self.header
    }

    /// The record columns this bucket admits, in header order: the stable
    /// union of every dead-letter site's carried schema that can route here.
    /// Empty under `include_source_row: false`, where no record column is
    /// written.
    pub fn user_columns(&self) -> &[String] {
        &self.user_columns
    }
}

/// Every dead-letter bucket of a compiled pipeline, the rule that routes a
/// row to one, and the header each bucket carries.
///
/// Derived once by the compiler from the final plan; read by the CLI to
/// publish dead-letter files and by `--explain` to show their shape. Holds
/// only plan-sized state.
#[derive(Debug, Clone)]
pub struct DlqLayout {
    buckets: Vec<DlqBucket>,
    bucket_of_source: BTreeMap<String, DlqBucketId>,
    fallback: Option<DlqBucketId>,
    include_reason: bool,
    include_source_row: bool,
}

impl DlqLayout {
    /// Every bucket, in bucket-rule order: the pipeline-wide file first, then
    /// each distinct per-source file in source-name order.
    pub fn buckets(&self) -> &[DlqBucket] {
        &self.buckets
    }

    /// The bucket `id` names. `id` must come from this layout.
    pub fn bucket(&self, id: DlqBucketId) -> &DlqBucket {
        &self.buckets[id.0]
    }

    /// Whether the error category and detail columns are written.
    pub fn include_reason(&self) -> bool {
        self.include_reason
    }

    /// Whether the failing record's columns are written.
    pub fn include_source_row(&self) -> bool {
        self.include_source_row
    }

    /// The bucket a dead-lettered row attributed to `source_name` lands in:
    /// its `per_source.<name>.path` override, else the pipeline-wide file.
    /// `None` means the row has no destination and is counted but not
    /// written.
    pub fn bucket_for_source(&self, source_name: &str) -> Option<DlqBucketId> {
        self.bucket_of_source
            .get(source_name)
            .copied()
            .or(self.fallback)
    }

    /// The `per_source` names whose override routes to bucket `id`, sorted.
    /// Sources that fall through to the pipeline-wide file are not listed.
    pub fn sources_for(&self, id: DlqBucketId) -> impl Iterator<Item = &str> {
        self.bucket_of_source
            .iter()
            .filter(move |(_, bucket)| **bucket == id)
            .map(|(name, _)| name.as_str())
    }

    /// Whether bucket `id` is the pipeline-wide file every source without an
    /// override falls through to.
    pub fn is_fallback(&self, id: DlqBucketId) -> bool {
        self.fallback == Some(id)
    }

    /// Derive the layout from the final plan. `Ok(None)` without a DLQ block.
    ///
    /// Runs after every structural rewrite, body binding and deferred-region
    /// detection, so the schemas it reads are the ones the runtime carries.
    /// The walk visits every scope in plan topological order, a composition
    /// body at its call site's position, and each node whose runtime arm can
    /// dead-letter under the pipeline's strategy adds the schemas of the
    /// records it received.
    ///
    /// **Attribution.** A record reaches the bucket of the Source its
    /// `$source.name` stamp names, so a schema with that stamp is added to
    /// the bucket of every Source whose stamp can ride on it, and a schema
    /// without it to the pipeline-wide file alone. Stamps are traced per input
    /// edge: a Combine's probe row and matched build row are separate entries,
    /// each attributed to its own input's Sources, and downstream of a
    /// Combine a stamp can only be the driver's, because the output row is
    /// built on the driving row. Every other node passes on the stamps of all
    /// its inputs. A composition's output rows are the body's output-port
    /// rows, whose engine columns keep their metadata.
    ///
    /// **Empty global fold.** A global fold (no `group_by`) whose finalize
    /// fails under `continue` with no buffered record dead-letters an empty
    /// row of the Aggregate's own output schema, attributed to the node name,
    /// so that schema is added to the pipeline-wide file. This branch is
    /// structurally reachable: a global fold owes one defaulted row even for
    /// empty input, and the empty-input sentinel drain finalizes it with no
    /// record to attribute. A runtime reproduction is left to the
    /// end-to-end suite.
    ///
    /// Fails, naming the member and column, when a deferred region prunes
    /// commit-pass rows to a buffer column the wide input does not carry: that
    /// is a planner defect, and it would otherwise put a header column no
    /// record can fill.
    pub(crate) fn derive(
        dag: &ExecutionPlanDag,
        bodies: &CompositionBodies,
        dlq: Option<&DlqConfig>,
        strategy: ErrorStrategy,
    ) -> Result<Option<DlqLayout>, String> {
        let Some(dlq) = dlq else {
            return Ok(None);
        };
        let mut layout = bucket_rule(dlq);
        let engine = engine_columns(layout.include_reason);
        let mut union: Vec<ColumnUnion> = (0..layout.buckets.len())
            .map(|_| ColumnUnion::default())
            .collect();
        if layout.include_source_row {
            let facts = PipelineFacts::collect(dag, bodies);
            let mut walker = SiteWalker {
                layout: &layout,
                bodies,
                strategy,
                facts: &facts,
                union: &mut union,
            };
            walker.walk(&Scope::top(dag, bodies))?;
        }
        for (bucket, columns) in layout.buckets.iter_mut().zip(union) {
            bucket.user_columns = columns.names;
            bucket.header = engine.clone();
            bucket.header.extend(bucket.user_columns.iter().cloned());
        }
        Ok(Some(layout))
    }
}

/// Build the buckets and the routing rule, with empty headers.
///
/// Bucket identity is the path's *collision key*, not its raw bytes. On a
/// case-insensitive output filesystem (macOS APFS / Windows NTFS default)
/// `errors.csv` and `Errors.csv` name one physical file; keying buckets on the
/// raw path would open two writers onto it and let the per-source and
/// pipeline-wide records overwrite each other. Two paths are one file through
/// the same [`crate::config::destination_identity`] the E318 config check
/// uses -- case folding conditional on the actual target filesystem, a
/// symlinked parent resolved, a relative and an absolute spelling reconciled
/// -- so case-sensitive Linux still keeps distinct files distinct. The
/// pipeline-wide path is keyed first, then every per-source path in source-name
/// order; the first path to claim a key names the bucket, matching the static
/// check's first-insertion-wins. Identity is resolved once per configured path
/// while the layout is built: it consults the filesystem, so asking again per
/// row would cost a walk per row and let the answer move under a run that is
/// still creating its destination directories.
fn bucket_rule(dlq: &DlqConfig) -> DlqLayout {
    let mut buckets: Vec<DlqBucket> = Vec::new();
    let mut index_of: HashMap<String, DlqBucketId> = HashMap::new();
    let mut claim = |path: &str, buckets: &mut Vec<DlqBucket>| -> DlqBucketId {
        let path = PathBuf::from(path);
        *index_of
            .entry(crate::config::destination_identity(&path))
            .or_insert_with(|| {
                buckets.push(DlqBucket {
                    path,
                    header: Vec::new(),
                    user_columns: Vec::new(),
                });
                DlqBucketId(buckets.len() - 1)
            })
    };
    let fallback = dlq.path.as_deref().map(|p| claim(p, &mut buckets));
    let mut bucket_of_source = BTreeMap::new();
    for (source, per) in &dlq.per_source {
        if let Some(p) = per.path.as_deref() {
            bucket_of_source.insert(source.clone(), claim(p, &mut buckets));
        }
    }
    DlqLayout {
        buckets,
        bucket_of_source,
        fallback,
        include_reason: dlq.include_reason.unwrap_or(true),
        include_source_row: dlq.include_source_row.unwrap_or(true),
    }
}

/// The `_cxl_dlq_*` engine prelude, in the order the runtime writes it.
/// Category and detail appear only when `include_reason`.
fn engine_columns(include_reason: bool) -> Vec<String> {
    let mut header: Vec<String> = [
        "_cxl_dlq_id",
        "_cxl_dlq_timestamp",
        "_cxl_dlq_source_file",
        "_cxl_dlq_source_name",
        "_cxl_dlq_source_row",
        "_cxl_dlq_triggering_field",
        "_cxl_dlq_triggering_value",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    if include_reason {
        header.push("_cxl_dlq_error_category".into());
        header.push("_cxl_dlq_error_detail".into());
    }
    header.push("_cxl_dlq_stage".into());
    header.push("_cxl_dlq_route".into());
    header.push("_cxl_dlq_trigger".into());
    header
}

/// Iterator over schema columns that should appear in DLQ output:
/// user-declared columns, the conditional `_cxl_dlq_source_record` physical-
/// row capture, plus correlation-lattice columns
/// (`$ck.<field>`, `$ck.aggregate.<name>`). The `$widened`
/// `auto_widen` sidecar absorber is filtered out — its
/// `Value::Map` payload has no canonical scalar serialization and
/// would silently JSON-encode into a single CSV cell, hiding
/// routing bugs.
pub fn dlq_user_columns(schema: &Schema) -> impl Iterator<Item = (usize, &str)> {
    schema
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, c)| match schema.field_metadata(i) {
            Some(FieldMetadata::WidenedSidecar)
            | Some(FieldMetadata::SourceFile)
            | Some(FieldMetadata::SourceName)
            | Some(FieldMetadata::SourceEventTime)
            | Some(FieldMetadata::ReshapeAudit) => None,
            Some(FieldMetadata::SourceCorrelation { .. })
            | Some(FieldMetadata::AggregateGroupIndex { .. })
            | None => Some((i, c.as_ref())),
        })
}

/// Build the one source-rejection schema shared by declared-type, reader-
/// classification, and fan-out-ceiling failures. Its fixed shape lets the
/// ordered-source barrier spill a mixed rejection stream without assuming
/// every rejected attempt had a declared record type.
///
/// The runtime builds it from the Source reader's schema at ingest; the
/// compiler builds it from the same reader shape to fix the dead-letter
/// header. One function serves both, so the two cannot disagree.
pub fn source_rejection_schema(reader_schema: &SharedStorage<Schema>) -> SharedStorage<Schema> {
    let mut builder = SchemaBuilder::with_capacity(reader_schema.column_count() + 4);
    for (idx, column) in reader_schema.columns().iter().enumerate() {
        builder = match reader_schema.field_metadata(idx) {
            Some(metadata) => builder.with_field_meta(column.as_ref(), metadata.clone()),
            None => builder.with_field(column.as_ref()),
        };
    }
    builder
        .with_field(SOURCE_RAW_RECORD_COLUMN)
        .with_field_meta(SOURCE_FILE_COLUMN, FieldMetadata::source_file())
        .with_field_meta(SOURCE_NAME_COLUMN, FieldMetadata::source_name())
        .with_field_meta(SOURCE_EVENT_TIME_COLUMN, FieldMetadata::source_event_time())
        .build()
}

/// Per-bucket stable union of user column names, in first-seen order.
#[derive(Default)]
struct ColumnUnion {
    names: Vec<String>,
    seen: HashSet<String>,
}

impl ColumnUnion {
    fn extend(&mut self, schema: &Schema) {
        for (_, name) in dlq_user_columns(schema) {
            if self.seen.insert(name.to_owned()) {
                self.names.push(name.to_owned());
            }
        }
    }
}

/// Pipeline-wide facts a site gate reads, collected across the top-level DAG
/// and every composition body.
struct PipelineFacts {
    /// Any Source declares a `correlation_key:`, so a Sink parks its input in
    /// the correlation buffer and a failed group dead-letters it.
    correlation: bool,
    /// Sources declaring `dlq_granularity: document`, whose documents a Sink
    /// dead-letters whole.
    document_sources: BTreeSet<String>,
}

impl PipelineFacts {
    fn collect(dag: &ExecutionPlanDag, bodies: &CompositionBodies) -> Self {
        let mut facts = PipelineFacts {
            correlation: false,
            document_sources: BTreeSet::new(),
        };
        let graphs = std::iter::once(&dag.graph).chain(bodies.values().map(|b| &b.graph));
        for graph in graphs {
            for node in graph.node_weights() {
                if let PlanNode::Source {
                    name,
                    resolved: Some(payload),
                    ..
                } = node
                {
                    facts.correlation |= payload.body.correlation_key.is_some();
                    if payload.source.dlq_granularity == DlqGranularity::Document {
                        facts.document_sources.insert(name.clone());
                    }
                }
            }
        }
        facts
    }
}

/// One graph scope being walked: the top-level DAG or a composition body.
struct Scope<'a> {
    graph: &'a DiGraph<PlanNode, PlanEdge>,
    topo: &'a [NodeIndex],
    deferred: &'a HashMap<NodeIndex, DeferredRegion>,
    bodies: &'a CompositionBodies,
    /// Schemas arriving at a body-root node from its call site. Empty at the
    /// top level, where every root is a Source.
    root_inputs: Vec<SharedStorage<Schema>>,
    /// Sources whose stamp a record entering the body from its call site can
    /// carry. Empty at the top level.
    root_stamps: BTreeSet<String>,
    /// Every declared Source upstream of the call site. Empty at the top
    /// level.
    root_ancestors: BTreeSet<String>,
}

/// Which upstream edges a Source walk follows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lineage {
    /// Every upstream edge: the Sources a node depends on at all.
    Ancestors,
    /// Only the edges a record's `$source.name` stamp can travel: through a
    /// Combine, the driving (probe) input alone, because a Combine's output
    /// row is built on the driver's row and never on a build row.
    Stamp,
}

impl<'a> Scope<'a> {
    fn top(dag: &'a ExecutionPlanDag, bodies: &'a CompositionBodies) -> Self {
        Scope {
            graph: &dag.graph,
            topo: &dag.topo_order,
            deferred: &dag.deferred_regions,
            bodies,
            root_inputs: Vec::new(),
            root_stamps: BTreeSet::new(),
            root_ancestors: BTreeSet::new(),
        }
    }

    /// The scope of the body a Composition node at `idx` calls, seeded with
    /// what arrives at the call site.
    fn body(&self, idx: NodeIndex, bound: &'a BoundBody) -> Scope<'a> {
        Scope {
            graph: &bound.graph,
            topo: &bound.topo_order,
            deferred: &bound.deferred_regions,
            bodies: self.bodies,
            root_inputs: self.input_schemas(idx),
            root_stamps: self.sources(idx, Lineage::Stamp),
            root_ancestors: self.sources(idx, Lineage::Ancestors),
        }
    }

    /// Upstream nodes of `idx`, deduplicated, in index order.
    fn upstreams(&self, idx: NodeIndex) -> Vec<NodeIndex> {
        let mut ups: Vec<NodeIndex> = self
            .graph
            .neighbors_directed(idx, Direction::Incoming)
            .collect();
        ups.sort_unstable();
        ups.dedup();
        ups
    }

    /// Every schema a record arriving at `idx` can carry: each upstream's
    /// emitted schema, or the call site's inputs for a body-root node.
    fn input_schemas(&self, idx: NodeIndex) -> Vec<SharedStorage<Schema>> {
        self.inputs(idx)
            .into_iter()
            .flat_map(|(schemas, _)| schemas)
            .collect()
    }

    /// The records arriving at `idx`, one entry per input edge: the schemas
    /// that edge can carry and the Sources whose stamp those records can
    /// bear. A body-root node's single entry is its call site's.
    fn inputs(&self, idx: NodeIndex) -> Vec<(Vec<SharedStorage<Schema>>, BTreeSet<String>)> {
        let ups = self.upstreams(idx);
        if ups.is_empty() {
            return vec![(self.root_inputs.clone(), self.root_stamps.clone())];
        }
        ups.into_iter()
            .map(|up| (self.output_schemas(up), self.sources(up, Lineage::Stamp)))
            .collect()
    }

    /// The schemas `idx` emits. Row-preserving nodes (Route, Sort, Sink,
    /// CorrelationCommit) carry none of their own and emit what they receive.
    /// A Composition emits what its body's output-port nodes emit: those rows
    /// keep the body's engine-column metadata, which the call-site node's own
    /// port-row schema does not record.
    fn output_schemas(&self, idx: NodeIndex) -> Vec<SharedStorage<Schema>> {
        if let PlanNode::Composition { body, .. } = &self.graph[idx]
            && let Some(bound) = self.bodies.get(body)
        {
            let inner = self.body(idx, bound);
            return bound
                .output_port_to_node_idx
                .values()
                .flat_map(|&port| inner.output_schemas(port))
                .collect();
        }
        match self.graph[idx].stored_output_schema() {
            Some(schema) => vec![schema.clone()],
            None => self.input_schemas(idx),
        }
    }

    /// Declared Sources that are `idx` itself or upstream of it along
    /// `lineage`, including a body's call-site Sources at a body-root node
    /// and the Sources authored inside a called body.
    fn sources(&self, idx: NodeIndex, lineage: Lineage) -> BTreeSet<String> {
        let mut sources = BTreeSet::new();
        let mut visited = HashSet::new();
        let mut stack = vec![idx];
        while let Some(node) = stack.pop() {
            if !visited.insert(node) {
                continue;
            }
            let mut ups = self.upstreams(node);
            match &self.graph[node] {
                PlanNode::Source { name, .. } => {
                    sources.insert(name.clone());
                }
                PlanNode::Combine {
                    driving_upstream: Some(driver),
                    ..
                } if lineage == Lineage::Stamp && ups.contains(driver) => {
                    ups = vec![*driver];
                }
                PlanNode::Composition { body, .. } => {
                    if let Some(bound) = self.bodies.get(body) {
                        body_source_names(self.bodies, bound, &mut sources);
                    }
                }
                _ => {}
            }
            if ups.is_empty() && !matches!(self.graph[node], PlanNode::Source { .. }) {
                sources.extend(
                    match lineage {
                        Lineage::Stamp => &self.root_stamps,
                        Lineage::Ancestors => &self.root_ancestors,
                    }
                    .iter()
                    .cloned(),
                );
            }
            stack.extend(ups);
        }
        sources
    }

    /// Whether records arriving at `idx` from `up` are commit-pass rows of
    /// `region`, pruned to its buffer columns. The runtime prunes the
    /// producer's emitted rows, and a row-preserving member passes them on
    /// unchanged; any other member builds its output on its own schema.
    fn carries_narrow_rows(&self, region: &DeferredRegion, up: NodeIndex) -> bool {
        if up == region.producer {
            return true;
        }
        region.members.contains(&up)
            && self.graph[up].stored_output_schema().is_none()
            && self
                .upstreams(up)
                .into_iter()
                .any(|before| self.carries_narrow_rows(region, before))
    }
}

/// Every Source authored in `bound` or in a body it calls.
fn body_source_names(bodies: &CompositionBodies, bound: &BoundBody, out: &mut BTreeSet<String>) {
    for node in bound.graph.node_weights() {
        if let PlanNode::Source { name, .. } = node {
            out.insert(name.clone());
        }
    }
    for nested in &bound.nested_body_ids {
        if let Some(inner) = bodies.get(nested) {
            body_source_names(bodies, inner, out);
        }
    }
}

/// Walks every scope in topological order and feeds each dead-letter site's
/// carried schema into the buckets it can reach.
struct SiteWalker<'a> {
    layout: &'a DlqLayout,
    bodies: &'a CompositionBodies,
    strategy: ErrorStrategy,
    facts: &'a PipelineFacts,
    union: &'a mut [ColumnUnion],
}

impl SiteWalker<'_> {
    fn walk(&mut self, scope: &Scope<'_>) -> Result<(), String> {
        let continuing = self.strategy == ErrorStrategy::Continue;
        for &idx in scope.topo {
            // A global fold whose failing window buffered no record
            // dead-letters an empty row of the node's own output schema,
            // attributed to the node name rather than a Source, so it reaches
            // only the pipeline-wide file.
            let mut global_fold_output = None;
            // Whether the node's own arm can dead-letter a record it received.
            // Each gate is the condition the runtime arm checks before it
            // pushes; a node whose arm cannot push contributes nothing, so
            // downstream schemas do not pad every header.
            let input_site = match &scope.graph[idx] {
                PlanNode::Source {
                    name,
                    output_schema,
                    ..
                } => {
                    // Declared-type, structural and fan-out-limit rejections
                    // dead-letter only under `continue`; `fail_fast` aborts.
                    if continuing {
                        let rejection = source_rejection_schema(&reader_shape(output_schema));
                        self.contribute(&rejection, &BTreeSet::from([name.clone()]));
                    }
                    false
                }
                // Evaluation failures dead-letter only under `continue`; the
                // arms return the error under `fail_fast`. A Combine failure
                // dead-letters the probe row and the matched build row as two
                // entries, each carrying its own input's schema and stamp,
                // which the per-input attribution below keeps apart.
                PlanNode::Transform { .. } | PlanNode::Route { .. } | PlanNode::Combine { .. } => {
                    continuing
                }
                // A mutation conflict rolls its group back to the DLQ under
                // every strategy: the arm checks none.
                PlanNode::Reshape { .. } => true,
                PlanNode::Aggregation {
                    config,
                    output_schema,
                    ..
                } => {
                    if continuing && config.group_by.is_empty() {
                        global_fold_output = Some(output_schema);
                    }
                    // Add and finalize failures under `continue`; late
                    // records of a time window under every strategy.
                    continuing || config.time_window.is_some()
                }
                PlanNode::Sink { resolved, .. } => {
                    (self.strategy != ErrorStrategy::FailFast
                        && sink_can_dead_letter_collision(resolved.as_deref()))
                        || self.facts.correlation
                        || scope
                            .sources(idx, Lineage::Ancestors)
                            .iter()
                            .any(|s| self.facts.document_sources.contains(s))
                }
                PlanNode::Composition { body, .. } => {
                    if let Some(bound) = self.bodies.get(body) {
                        self.walk(&scope.body(idx, bound))?;
                    }
                    false
                }
                PlanNode::Merge { .. }
                | PlanNode::Sort { .. }
                | PlanNode::Cull { .. }
                | PlanNode::Envelope { .. }
                | PlanNode::CorrelationCommit { .. } => false,
            };
            if input_site {
                let region = scope
                    .deferred
                    .get(&idx)
                    .filter(|region| region.producer != idx);
                let ups = scope.upstreams(idx);
                for (position, (schemas, sources)) in scope.inputs(idx).into_iter().enumerate() {
                    for schema in &schemas {
                        self.contribute(schema, &sources);
                    }
                    // A deferred-region member re-runs in the commit pass on
                    // rows pruned to the region's buffer columns, which carry
                    // their own narrow schema.
                    if let Some(region) = region
                        && let Some(&up) = ups.get(position)
                        && scope.carries_narrow_rows(region, up)
                    {
                        for schema in &schemas {
                            let narrow = project_to_buffer_schema(
                                schema,
                                &region.buffer_schema,
                                scope.graph[idx].name(),
                            )?;
                            self.contribute(&narrow, &sources);
                        }
                    }
                }
            }
            if let (Some(output), Some(fallback)) = (global_fold_output, self.layout.fallback) {
                self.union[fallback.0].extend(output);
            }
        }
        Ok(())
    }

    /// Feed `schema` into every bucket it can reach. A schema carrying the
    /// per-record Source stamp reaches the bucket of each Source in `sources`;
    /// one without it is attributed to no Source and reaches the
    /// pipeline-wide file only.
    fn contribute(&mut self, schema: &Schema, sources: &BTreeSet<String>) {
        let stamped = (0..schema.column_count())
            .any(|i| matches!(schema.field_metadata(i), Some(FieldMetadata::SourceName)));
        let mut targets: BTreeSet<DlqBucketId> = BTreeSet::new();
        if stamped {
            targets.extend(
                sources
                    .iter()
                    .filter_map(|s| self.layout.bucket_for_source(s)),
            );
        } else {
            targets.extend(self.layout.fallback);
        }
        for bucket in targets {
            self.union[bucket.0].extend(schema);
        }
    }
}

/// The shape a Source's reader produces: its declared columns and the
/// `$widened` sidecar, without the correlation shadows and `$source.*` stamps
/// the bound output schema adds after the reader.
fn reader_shape(output_schema: &Schema) -> SharedStorage<Schema> {
    let mut builder = SchemaBuilder::with_capacity(output_schema.column_count());
    for (idx, column) in output_schema.columns().iter().enumerate() {
        builder = match output_schema.field_metadata(idx) {
            None => builder.with_field(column.as_ref()),
            Some(meta @ FieldMetadata::WidenedSidecar) => {
                builder.with_field_meta(column.as_ref(), meta.clone())
            }
            Some(_) => builder,
        };
    }
    builder.build()
}

/// The narrow schema a commit-pass row carries: the region's buffer columns,
/// each keeping the metadata the wide schema gives it.
///
/// The buffer is a projection of the producer's emitted row, so every buffer
/// column is a column of `wide`. A column `wide` lacks would put a header
/// column no record can fill; it is a planner defect, refused here rather
/// than written.
fn project_to_buffer_schema(
    wide: &Schema,
    buffer_schema: &[String],
    member: &str,
) -> Result<SharedStorage<Schema>, String> {
    let mut builder = SchemaBuilder::with_capacity(buffer_schema.len());
    for column in buffer_schema {
        let Some(index) = wide.index(column) else {
            return Err(format!(
                "deferred-region member {member:?} receives commit-pass rows pruned to \
                 buffer column {column:?}, which its input schema {:?} does not carry",
                wide.columns()
            ));
        };
        builder = match wide.field_metadata(index) {
            Some(meta) => builder.with_field_meta(column.as_str(), meta.clone()),
            None => builder.with_field(column.as_str()),
        };
    }
    Ok(builder.build())
}

/// Whether a Sink's writer can reject a record with a `join_values`
/// `on_conflict: error` collision. Only the CSV writer joins multi-value
/// fields into one delimited cell, and a `multiple:` field without an explicit
/// `join_values` entry takes the CSV default policy, which is `error`.
fn sink_can_dead_letter_collision(payload: Option<&PlanSinkPayload>) -> bool {
    let Some(payload) = payload else {
        return false;
    };
    let sink = &payload.sink;
    if !matches!(sink.format, OutputFormat::Csv(_)) {
        return false;
    }
    let entries = sink.join_values.as_deref().unwrap_or_default();
    sink.declared_multiple.iter().any(|field| {
        match entries.iter().find(|entry| &entry.field == field) {
            Some(entry) => entry.on_conflict == OnConflict::Error,
            None => true,
        }
    })
}
