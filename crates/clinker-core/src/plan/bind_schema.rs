//! Compile-time CXL typecheck + schema propagation (`bind_schema`).
//!
//! Lifted from `config/cxl_compile.rs` in Phase 16c.1.4 (LD-16c-24).
//! Walks the unified `nodes:` DAG in topological order, seeds each
//! source's schema from its author-declared `schema:` block, typechecks
//! every Transform/Aggregate/Route body against the upstream Row, and
//! propagates the output Row downstream. Result: a `CompileArtifacts`
//! map keyed by node name holding one `Arc<TypedProgram>` per CXL-bearing
//! node.
//!
//! For `PipelineNode::Composition` nodes, `bind_composition` recursively
//! binds the composition body at the call-site boundary using
//! row-polymorphic type propagation (LD-16c-21, Leijen 2005).
//!
//! Errors surface as E200 diagnostics (CXL type error), E201
//! diagnostics (missing source schema), and E102–E109 / W101
//! diagnostics for composition binding.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cxl::typecheck::{AggregateMode, OutputLayout, Row, RowTail, Type, TypedProgram};
use indexmap::IndexMap;

use crate::config::compile_context::CompileContext;
use crate::config::composition::{
    CompositionFile, CompositionSignature, CompositionSymbolTable, LayerKind, OutputAlias,
    PortDecl, ProvenanceDb, ResolvedValue,
};
use crate::config::pipeline_node::{PipelineNode, SchemaDecl};
use crate::error::{Diagnostic, LabeledSpan};
use crate::plan::bound_schemas::BoundSchemas;
use crate::plan::composition_body::{BoundBody, CompositionBodyId};
use crate::span::{FileId, Span};
use crate::yaml::Spanned;

/// Maximum composition nesting depth before E107 fires.
const MAX_COMPOSITION_DEPTH: u32 = 50;

/// Compile artifacts produced by `bind_schema` — one entry per node name
/// whose CXL body successfully type-checked, plus per-node row types.
#[derive(Debug, Default, Clone)]
pub struct CompileArtifacts {
    /// TypedPrograms for TOP-LEVEL DAG nodes only (depth == 0).
    ///
    /// Invariant: every entry's `output_layout` has a closed-tail,
    /// upstream-aware layout produced by `build_output_layout` or
    /// `build_aggregate_output_layout`. `Arc<Schema>` identity equal
    /// to `canonical_schemas[name]` and `bound_schemas.schema_of(name)`.
    /// These are the ONLY runtime-canonical layouts.
    pub typed: HashMap<String, Arc<TypedProgram>>,
    /// TypedPrograms for composition BODY nodes (depth > 0).
    ///
    /// Invariant: every entry's `output_layout` is the standalone
    /// layout produced by `type_check_with_mode` (no upstream context;
    /// `passthroughs` empty). Valid for isolated-scope evaluation and
    /// --explain; NOT runtime-canonical. Body upstream rows are
    /// open-tailed (row-polymorphic composition port rows) so
    /// `build_output_layout`'s closed-tail invariant intentionally
    /// never applies here. Split prevents accidental cross-use with
    /// top-level runtime-canonical entries.
    pub composition_body_typed: HashMap<String, Arc<TypedProgram>>,
    /// Per-top-level-node canonical `Arc<OutputLayout>` for the
    /// executor's Output arm predecessor-lookup. Populated for every
    /// top-level DAG node (Source, Transform, Aggregate, Route, Merge).
    /// Composition and Output nodes have NO entry.
    ///
    /// For Transform/Aggregate/Route: `Arc::clone(&typed[name].output_layout)`
    /// — ptr-equal, single-oracle, zero drift. For Source/Merge:
    /// synthesised identity-passthrough layout (`emit_slots: all None`,
    /// `passthroughs: identity`) because Source/Merge columns are
    /// passthroughs by nature — no CXL-emitted columns exist at those
    /// boundaries.
    pub output_layouts: HashMap<String, Arc<OutputLayout>>,
    /// Per-node bound row types (LD-16c-23). Populated during the
    /// `bind_schema` walk; persisted on `CompiledPlan` for downstream
    /// phases to consume.
    pub bound_schemas: BoundSchemas,
    /// Pre-materialized canonical `Arc<Schema>` per node, keyed by name.
    /// Populated inline during `bind_schema_inner` for EVERY top-level
    /// node type (Source/Transform/Aggregate/Route/Merge) so every
    /// node's `OutputLayout.schema` shares `Arc` identity with
    /// `BoundSchemas.schema_of(name)`. One oracle — the executor
    /// `Arc::ptr_eq` drift guardrail holds.
    pub canonical_schemas: HashMap<String, Arc<clinker_record::Schema>>,
    /// All bound composition bodies, keyed by `CompositionBodyId`. The
    /// top-level pipeline is NOT in this map — it lives on
    /// `CompiledPlan.dag()` directly. Only body scopes are here.
    pub composition_bodies: IndexMap<CompositionBodyId, BoundBody>,
    /// Monotonic counter for allocating fresh `CompositionBodyId`s.
    next_body_id: u32,
    /// Mapping from composition node name to its assigned body ID.
    /// Populated by `bind_composition`; consumed by `compile()` Stage 5
    /// to write the body ID back onto the `PipelineNode::Composition`
    /// variant (which was borrowed immutably during bind_schema).
    pub composition_body_assignments: HashMap<String, CompositionBodyId>,
    /// Monotonic counter for allocating fresh `FileId`s for body re-parses.
    next_file_id: u32,
    /// Side-table of provenance-tracked config values (D-H.5 / LD-16c-11).
    /// Populated by `bind_composition` for each composition node's config params.
    pub provenance: ProvenanceDb,
}

impl CompileArtifacts {
    /// Allocate a fresh, unique `CompositionBodyId`.
    pub fn fresh_body_id(&mut self) -> CompositionBodyId {
        let id = CompositionBodyId(self.next_body_id);
        self.next_body_id += 1;
        id
    }

    /// Insert a bound body. Panics if the id is already present.
    pub fn insert_body(&mut self, id: CompositionBodyId, body: BoundBody) {
        let prev = self.composition_bodies.insert(id, body);
        assert!(
            prev.is_none(),
            "CompositionBodyId({}) already present in composition_bodies",
            id.0
        );
    }

    /// Look up a bound body by id.
    pub fn body_of(&self, id: CompositionBodyId) -> Option<&BoundBody> {
        self.composition_bodies.get(&id)
    }

    /// Allocate a fresh `FileId` for body re-parses.
    fn fresh_file_id(&mut self) -> FileId {
        self.next_file_id += 1;
        FileId::new(NonZeroU32::new(self.next_file_id).expect("file_id counter overflow at 0"))
    }
}

/// Per-recursion-level context for `bind_schema` + `bind_composition`.
pub(crate) struct BindContext<'a> {
    pub ctx: &'a CompileContext,
    pub symbol_table: &'a CompositionSymbolTable,
    /// Stack of use-paths for cycle detection. Each entry is the
    /// workspace-relative path of a composition currently being bound
    /// in an ancestor frame. Push before entering a body; pop after.
    pub use_path_stack: Vec<PathBuf>,
    /// Current recursion depth. Incremented at each `bind_composition` call.
    pub depth: u32,
    /// Node names visible in the enclosing scope. Body nodes that
    /// reference any of these names trigger E108 (IsolatedFromAbove).
    pub enclosing_scope_names: HashSet<String>,
    /// Workspace-relative directory of the file whose nodes are being
    /// bound. Used to resolve relative `use:` paths. For the top-level
    /// pipeline this is typically `"pipelines/"` or similar; for nested
    /// compositions it's the directory of the `.comp.yaml` file.
    pub origin_dir: PathBuf,
}

// ─── Public entry point ─────────────────────────────────────────────

/// Top-level entry point for compile-time CXL typechecking + schema
/// propagation. Called from `PipelineConfig::compile()`.
///
/// `pipeline_dir` is the workspace-relative directory of the pipeline
/// file being compiled. Used to resolve relative `use:` paths on
/// `PipelineNode::Composition` nodes. Pass `""` if unknown.
pub fn bind_schema(
    nodes: &[Spanned<PipelineNode>],
    diags: &mut Vec<Diagnostic>,
    ctx: &CompileContext,
    symbol_table: &CompositionSymbolTable,
    pipeline_dir: &Path,
) -> CompileArtifacts {
    let mut artifacts = CompileArtifacts::default();
    let mut schema_by_name: HashMap<String, Row> = HashMap::new();
    let mut bind_ctx = BindContext {
        ctx,
        symbol_table,
        use_path_stack: Vec::new(),
        depth: 0,
        enclosing_scope_names: HashSet::new(),
        origin_dir: pipeline_dir.to_path_buf(),
    };
    bind_schema_inner(
        nodes,
        diags,
        &mut bind_ctx,
        &mut artifacts,
        &mut schema_by_name,
    );

    // Persist all bound rows into BoundSchemas (LD-16c-23). When the
    // node has a pre-materialized canonical `Arc<Schema>` from
    // layout construction (CXL-bearing Transform/Aggregate/Route), reuse
    // that exact Arc so `BoundSchemas::schema_of(name)` ptr-equals the
    // node's `OutputLayout::schema`. One oracle. For Source/Merge/Output
    // nodes that have no TypedProgram, we fall through to `set_output`
    // which materializes a fresh Arc.
    for (node_name, row) in schema_by_name {
        if let Some(canonical) = artifacts.canonical_schemas.remove(&node_name) {
            artifacts
                .bound_schemas
                .set_output_with_schema(node_name, row, canonical);
        } else {
            artifacts.bound_schemas.set_output(node_name, row);
        }
    }

    artifacts
}

// ─── OutputLayout construction (Option-W codegen) ──────────────────────

/// Materialize an `Arc<Schema>` from a bound output `Row`'s declared
/// columns. The resulting Arc is the canonical schema for records
/// produced by the node; it is shared with the node's
/// [`cxl::typecheck::OutputLayout::schema`] and (at top-level flush) with
/// [`BoundSchemas::schema_of`].
fn materialize_schema(row: &Row) -> Arc<clinker_record::Schema> {
    let cols: Vec<Box<str>> = row
        .declared
        .keys()
        .map(|k| Box::<str>::from(k.as_str()))
        .collect();
    Arc::new(clinker_record::Schema::new(cols))
}

/// Build the Option-W positional layout for a Transform/Route node:
///
/// - `emit_slots[i]` = position of the i-th statement's name in `out` if it
///   is a non-meta Emit, else `None`.
/// - `passthroughs` = `(upstream_slot, out_slot)` copies for every upstream
///   column NOT shadowed by a non-meta emit (emit-name shadowing matches
///   `propagate_row`'s IndexMap::insert semantics).
///
/// Invariant: `out.tail == RowTail::Closed`. Open-tailed rows are a
/// composition-scope concern and never reach the layout constructor for
/// the top-level DAG.
fn build_output_layout(
    upstream: &Row,
    out: &Row,
    typed: &TypedProgram,
    schema: Arc<clinker_record::Schema>,
) -> OutputLayout {
    // Amendment A1 (restored): closed-row outputs are the only
    // architecturally-valid inputs to this layout constructor.
    // Open-tailed rows are a composition-body concern and are routed
    // via `CompileArtifacts.composition_body_typed` without layout
    // replacement — they never reach this constructor.
    debug_assert!(
        matches!(out.tail, RowTail::Closed),
        "build_output_layout requires a closed-tail output row; \
         open-tailed rows arise only in composition bodies and must \
         route to composition_body_typed without layout replacement"
    );
    debug_assert_eq!(
        out.declared.len(),
        schema.column_count(),
        "out.declared.len() != schema.column_count()"
    );

    // Out-column name → slot index (positional in schema).
    let out_index: HashMap<&str, u32> = out
        .declared
        .keys()
        .enumerate()
        .map(|(i, k)| (k.as_str(), i as u32))
        .collect();

    // Collect non-meta emit names for passthrough shadow detection.
    let mut emit_name_set: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for stmt in &typed.program.statements {
        if let cxl::ast::Statement::Emit {
            name,
            is_meta: false,
            ..
        } = stmt
        {
            emit_name_set.insert(name.as_ref());
        }
    }

    // emit_slots: per-statement positional slot, or None.
    let mut emit_slots: Vec<Option<u32>> = Vec::with_capacity(typed.program.statements.len());
    for stmt in &typed.program.statements {
        match stmt {
            cxl::ast::Statement::Emit {
                name,
                is_meta: false,
                ..
            } => {
                let slot = out_index
                    .get(name.as_ref())
                    .copied()
                    .expect("emit name must be present in out row");
                emit_slots.push(Some(slot));
            }
            _ => emit_slots.push(None),
        }
    }

    // passthroughs: upstream_slot → out_slot for every upstream column not
    // shadowed by an emit with the same name.
    let mut passthroughs: Vec<(u32, u32)> = Vec::new();
    for (upstream_slot, upstream_name) in upstream.declared.keys().enumerate() {
        if emit_name_set.contains(upstream_name.as_str()) {
            continue;
        }
        if let Some(&out_slot) = out_index.get(upstream_name.as_str()) {
            passthroughs.push((upstream_slot as u32, out_slot));
        }
    }

    OutputLayout {
        schema,
        emit_slots,
        passthroughs,
    }
}

/// Build the Option-W layout for an Aggregate node. The aggregate's
/// output row is `group_by ++ emits` (fresh — no passthroughs from the
/// upstream record; aggregate finalize constructs values from
/// accumulator state). `emit_slots` still maps non-meta Emit statements
/// to their positional slot in the output schema, used by any
/// post-finalize residual evaluation path.
fn build_aggregate_output_layout(
    out: &Row,
    typed: &TypedProgram,
    schema: Arc<clinker_record::Schema>,
) -> OutputLayout {
    debug_assert!(matches!(out.tail, RowTail::Closed));
    debug_assert_eq!(out.declared.len(), schema.column_count());

    let out_index: HashMap<&str, u32> = out
        .declared
        .keys()
        .enumerate()
        .map(|(i, k)| (k.as_str(), i as u32))
        .collect();

    let mut emit_slots: Vec<Option<u32>> = Vec::with_capacity(typed.program.statements.len());
    for stmt in &typed.program.statements {
        match stmt {
            cxl::ast::Statement::Emit {
                name,
                is_meta: false,
                ..
            } => {
                let slot = out_index
                    .get(name.as_ref())
                    .copied()
                    .expect("aggregate emit name must be present in out row");
                emit_slots.push(Some(slot));
            }
            _ => emit_slots.push(None),
        }
    }

    OutputLayout {
        schema,
        emit_slots,
        passthroughs: Vec::new(),
    }
}

/// Synthesise an identity-passthrough `OutputLayout` for Source and
/// Merge nodes. Source/Merge columns are all passthroughs by nature
/// — no CXL `emit` statement produces them — so `emit_slots` is all
/// `None` and `passthroughs` is the identity mapping `(i, i)` for
/// every column of the node's bound output schema.
///
/// The inserted layout's `schema` field shares `Arc` identity with
/// `artifacts.canonical_schemas[name]` (and therefore with
/// `bound_schemas.schema_of(name)` at finalisation). This upholds the
/// Option W ptr-eq drift guardrail uniformly across all top-level node
/// types.
fn insert_identity_layout(
    artifacts: &mut CompileArtifacts,
    name: &str,
    schema: &Arc<clinker_record::Schema>,
) {
    let n = schema.column_count();
    let layout = OutputLayout {
        schema: Arc::clone(schema),
        emit_slots: vec![None; n],
        passthroughs: (0..n as u32).map(|i| (i, i)).collect(),
    };
    artifacts
        .output_layouts
        .insert(name.to_string(), Arc::new(layout));
}

// ─── Internal recursive bind_schema ─────────────────────────────────

/// Internal recursive bind_schema. Walks nodes in topological order,
/// seeds source schemas, typechecks CXL-bearing nodes, and recurses
/// into composition bodies via `bind_composition`.
fn bind_schema_inner(
    nodes: &[Spanned<PipelineNode>],
    diags: &mut Vec<Diagnostic>,
    bind_ctx: &mut BindContext<'_>,
    artifacts: &mut CompileArtifacts,
    schema_by_name: &mut HashMap<String, Row>,
) {
    let span_for = |spanned: &Spanned<PipelineNode>| -> Span {
        let line = spanned.referenced.line() as u32;
        if line > 0 {
            Span::line_only(line)
        } else {
            Span::SYNTHETIC
        }
    };

    for spanned in nodes {
        let node = &spanned.value;
        let name = node.name().to_string();
        let span = span_for(spanned);

        match node {
            PipelineNode::Source { config, .. } => {
                let schema_decl: &SchemaDecl = &config.schema;
                let columns = columns_from_decl(schema_decl);
                let cxl_span = cxl::lexer::Span::new(span.start as usize, span.start as usize);
                let row = Row::closed(columns, cxl_span);
                if bind_ctx.depth == 0 {
                    // Option W: populate canonical_schemas and
                    // output_layouts for every top-level node so the
                    // executor's uniform Arc<OutputLayout> lookup works
                    // across Source/Transform/Aggregate/Route/Merge.
                    // Sources' columns are all passthroughs (no CXL
                    // emits), so the synthesised layout has
                    // emit_slots: all None, passthroughs: identity.
                    let out_schema = materialize_schema(&row);
                    artifacts
                        .canonical_schemas
                        .insert(name.clone(), Arc::clone(&out_schema));
                    insert_identity_layout(artifacts, &name, &out_schema);
                }
                schema_by_name.insert(name, row);
            }
            PipelineNode::Transform { header, config } => {
                // E108: check for enclosing-scope reference BEFORE upstream lookup.
                if let Some(target) = upstream_target_name(&header.input)
                    && !schema_by_name.contains_key(target)
                    && bind_ctx.enclosing_scope_names.contains(target)
                {
                    diags.push(Diagnostic::error(
                        "E108",
                        format!(
                            "node {name:?} references {target:?} from the enclosing scope; \
                             composition bodies are isolated from the parent pipeline"
                        ),
                        LabeledSpan::primary(span, String::new()),
                    ));
                    continue;
                }
                let upstream = match upstream_schema(&header.input, schema_by_name) {
                    Some(s) => s.clone(),
                    None => continue,
                };
                match typecheck_cxl(
                    &name,
                    &config.cxl.source,
                    &upstream,
                    AggregateMode::Row,
                    span,
                ) {
                    Ok(typed) => {
                        let out = propagate_row(&upstream, &typed);
                        if bind_ctx.depth == 0 {
                            // Top-level: build upstream-aware layout
                            // (closed-tail invariant structurally
                            // guaranteed); insert into runtime-canonical
                            // `typed` + populate output_layouts.
                            let out_schema = materialize_schema(&out);
                            let layout = build_output_layout(
                                &upstream,
                                &out,
                                &typed,
                                Arc::clone(&out_schema),
                            );
                            let typed_with_layout = typed.with_output_layout(Arc::new(layout));
                            artifacts.canonical_schemas.insert(name.clone(), out_schema);
                            artifacts
                                .output_layouts
                                .insert(name.clone(), Arc::clone(&typed_with_layout.output_layout));
                            schema_by_name.insert(name.clone(), out);
                            artifacts.typed.insert(name, Arc::new(typed_with_layout));
                        } else {
                            // Composition body: upstream row is
                            // open-tailed (row-polymorphic port row).
                            // Keep the standalone layout built by
                            // `type_check_with_mode`; insert into
                            // composition_body_typed. `build_output_layout`
                            // is NOT called — its closed-tail invariant
                            // would fire on open upstream.
                            schema_by_name.insert(name.clone(), out);
                            artifacts
                                .composition_body_typed
                                .insert(name, Arc::new(typed));
                        }
                    }
                    Err(d) => diags.push(d),
                }
            }
            PipelineNode::Aggregate { header, config } => {
                let upstream = match upstream_schema(&header.input, schema_by_name) {
                    Some(s) => s.clone(),
                    None => continue,
                };
                // Validate group_by fields exist in the upstream schema.
                let mut missing = Vec::new();
                for gb in &config.group_by {
                    if !upstream.declared.contains_key(gb) {
                        missing.push(gb.clone());
                    }
                }
                if !missing.is_empty() {
                    diags.push(
                        Diagnostic::error(
                            "E200",
                            format!(
                                "aggregate {name:?}: group_by field(s) {missing:?} \
                                 not present in upstream schema"
                            ),
                            LabeledSpan::primary(span, String::new()),
                        )
                        .with_help(
                            "declare the column in the source's `schema:` block \
                             or emit it from an upstream transform",
                        ),
                    );
                    continue;
                }
                let agg_mode = AggregateMode::GroupBy {
                    group_by_fields: config.group_by.iter().cloned().collect(),
                };
                match typecheck_cxl(&name, &config.cxl.source, &upstream, agg_mode, span) {
                    Ok(typed) => {
                        let out = propagate_aggregate(&config.group_by, &upstream, &typed);
                        if bind_ctx.depth == 0 {
                            // Top-level: build aggregate output layout
                            // (closed-tail invariant structurally
                            // guaranteed); insert into `typed` +
                            // populate output_layouts.
                            let out_schema = materialize_schema(&out);
                            let layout = build_aggregate_output_layout(
                                &out,
                                &typed,
                                Arc::clone(&out_schema),
                            );
                            let typed_with_layout = typed.with_output_layout(Arc::new(layout));
                            artifacts.canonical_schemas.insert(name.clone(), out_schema);
                            artifacts
                                .output_layouts
                                .insert(name.clone(), Arc::clone(&typed_with_layout.output_layout));
                            schema_by_name.insert(name.clone(), out);
                            artifacts.typed.insert(name, Arc::new(typed_with_layout));
                        } else {
                            // Composition body aggregate: keep
                            // standalone layout from type_check; insert
                            // into composition_body_typed. (Note:
                            // body Aggregates are W100-deferred at
                            // lowering today — pre-existing latent
                            // issue, out of scope for this fix.)
                            schema_by_name.insert(name.clone(), out);
                            artifacts
                                .composition_body_typed
                                .insert(name, Arc::new(typed));
                        }
                    }
                    Err(d) => diags.push(d),
                }
            }
            PipelineNode::Route { header, config: _ } => {
                if let Some(upstream) = upstream_schema(&header.input, schema_by_name) {
                    let cloned = upstream.clone();
                    if let Ok(empty) = typecheck_cxl(&name, "", &cloned, AggregateMode::Row, span) {
                        if bind_ctx.depth == 0 {
                            // Top-level: Route is transparent — it
                            // dispatches to branches without changing
                            // record shape. We maintain TWO layouts with
                            // distinct purposes:
                            //
                            // 1. `artifacts.typed[route].output_layout`:
                            //    RUNTIME EVALUATOR layout. Must match the
                            //    upstream schema column-wise so the
                            //    ProgramEvaluator produces correctly-sized
                            //    output records (Route's CXL is empty —
                            //    no emits, identity passthroughs copy all
                            //    upstream columns through).
                            //
                            // 2. `artifacts.output_layouts[route]`:
                            //    PROJECTION layout for the Output arm.
                            //    Inherits the upstream's layout (emit
                            //    semantics preserved through transparent
                            //    Route). Using an identity layout here
                            //    would drop upstream's CXL-named emit
                            //    slots and break `include_unmapped: false`
                            //    semantics.
                            //
                            // Both purposes are architecturally distinct
                            // (evaluator record construction vs
                            // projection filtering); having them diverge
                            // at Route/Merge boundaries is not a
                            // single-oracle violation.
                            let out_schema = materialize_schema(&cloned);
                            // (1) Evaluator layout = identity passthrough.
                            let eval_layout = build_output_layout(
                                &cloned,
                                &cloned,
                                &empty,
                                Arc::clone(&out_schema),
                            );
                            let typed_with_layout = empty.with_output_layout(Arc::new(eval_layout));
                            // (2) Projection layout = upstream's layout.
                            let upstream_name = input_target(&header.input);
                            let projection_layout = artifacts
                                .output_layouts
                                .get(upstream_name)
                                .cloned()
                                .unwrap_or_else(|| {
                                    // Upstream has no emits (e.g., Source);
                                    // fall back to evaluator layout (they
                                    // coincide in the emit-free case).
                                    Arc::clone(&typed_with_layout.output_layout)
                                });
                            artifacts.canonical_schemas.insert(name.clone(), out_schema);
                            artifacts
                                .output_layouts
                                .insert(name.clone(), projection_layout);
                            artifacts
                                .typed
                                .insert(name.clone(), Arc::new(typed_with_layout));
                        } else {
                            // Composition body route: keep standalone
                            // layout; insert into composition_body_typed.
                            artifacts
                                .composition_body_typed
                                .insert(name.clone(), Arc::new(empty));
                        }
                    }
                    schema_by_name.insert(name, cloned);
                }
            }
            PipelineNode::Merge { header, .. } => {
                // Option-W invariant: under positional records, every
                // predecessor of a Merge must share the same bound output
                // schema. If branches emitted divergent columns, records
                // would arrive with incompatible Arc<Schema>s and
                // positional access would be wrong. Enforce row equality
                // on the declared key set; type-level differences are
                // tolerated (typecheck upstream has already unified).
                let expected_keys: Option<Vec<&String>> = header
                    .inputs
                    .first()
                    .and_then(|first| schema_by_name.get(input_target(first)))
                    .map(|row| row.declared.keys().collect());
                let mut divergent = false;
                if let Some(ref expected) = expected_keys {
                    for inp in header.inputs.iter().skip(1) {
                        if let Some(row) = schema_by_name.get(input_target(inp)) {
                            let keys: Vec<&String> = row.declared.keys().collect();
                            if keys != *expected {
                                divergent = true;
                                break;
                            }
                        }
                    }
                }
                if divergent {
                    diags.push(
                        Diagnostic::error(
                            "E202",
                            format!(
                                "merge {name:?}: predecessor output rows differ — \
                                 Option-W positional records require all inputs to a \
                                 Merge node to share identical declared column sets in \
                                 the same order"
                            ),
                            LabeledSpan::primary(span, String::new()),
                        )
                        .with_help(
                            "ensure each upstream transform emits the same columns in \
                             the same order, or insert a reshaping transform before the merge",
                        ),
                    );
                    continue;
                }
                if let Some(first) = header.inputs.first()
                    && let Some(upstream) = schema_by_name.get(input_target(first))
                {
                    let cloned = upstream.clone();
                    if bind_ctx.depth == 0 {
                        // Option W: Merge is a TRANSPARENT passthrough
                        // (concatenates predecessors without changing
                        // shape; Amendment A5 enforces predecessors share
                        // declared row). Its runtime output layout
                        // inherits from ANY input's layout — they are
                        // equivalent. Using an identity-passthrough layout
                        // here would drop upstream emit information.
                        let first_input_name = input_target(first);
                        let upstream_layout =
                            artifacts.output_layouts.get(first_input_name).cloned();
                        let out_schema = materialize_schema(&cloned);
                        artifacts
                            .canonical_schemas
                            .insert(name.clone(), Arc::clone(&out_schema));
                        if let Some(upstream_layout) = upstream_layout {
                            artifacts
                                .output_layouts
                                .insert(name.clone(), upstream_layout);
                        } else {
                            // All Merge inputs are Source-like (no
                            // emits): synthesise identity layout.
                            insert_identity_layout(artifacts, &name, &out_schema);
                        }
                    }
                    schema_by_name.insert(name, cloned);
                }
            }
            PipelineNode::Output { header, .. } => {
                if let Some(upstream) = upstream_schema(&header.input, schema_by_name) {
                    schema_by_name.insert(name, upstream.clone());
                }
            }
            PipelineNode::Composition {
                r#use,
                inputs,
                outputs,
                config,
                resources,
                ..
            } => {
                bind_composition(
                    &name,
                    r#use,
                    inputs,
                    outputs,
                    config,
                    resources,
                    span,
                    diags,
                    bind_ctx,
                    artifacts,
                    schema_by_name,
                );
            }
        }
    }
}

// ─── Composition binding ────────────────────────────────────────────

/// Bind a composition body at its call-site boundary.
///
/// This is the `PipelineNode::Composition` arm of `bind_schema_inner`.
/// It recursively binds the body as a sub-problem within a nested scope
/// per LD-16c-21 (preserve-and-recurse, not flatten-and-splice).
#[allow(clippy::too_many_arguments)]
fn bind_composition(
    node_name: &str,
    use_path: &Path,
    call_inputs: &IndexMap<String, String>,
    call_outputs: &IndexMap<String, String>,
    call_config: &IndexMap<String, serde_json::Value>,
    call_resources: &IndexMap<String, serde_json::Value>,
    span: Span,
    diags: &mut Vec<Diagnostic>,
    bind_ctx: &mut BindContext<'_>,
    artifacts: &mut CompileArtifacts,
    parent_schema_by_name: &mut HashMap<String, Row>,
) {
    // 1. Resolve use_path to workspace-relative key and look up in symbol_table.
    let resolved_path = resolve_use_path(
        use_path,
        &bind_ctx.origin_dir,
        bind_ctx.ctx.workspace_root(),
        bind_ctx.symbol_table,
    );
    let signature = match bind_ctx.symbol_table.get(&resolved_path) {
        Some(sig) => sig,
        None => {
            diags.push(Diagnostic::error(
                "E103",
                format!(
                    "composition node {node_name:?}: `use: {}` does not match any \
                     .comp.yaml in the workspace",
                    use_path.display()
                ),
                LabeledSpan::primary(span, String::new()),
            ));
            return;
        }
    };

    // 2. Cycle detection: check use_path_stack and depth guard.
    if bind_ctx.use_path_stack.contains(&resolved_path) {
        let cycle: Vec<String> = bind_ctx
            .use_path_stack
            .iter()
            .map(|p| p.display().to_string())
            .chain(std::iter::once(resolved_path.display().to_string()))
            .collect();
        diags.push(Diagnostic::error(
            "E107",
            format!("cycle in composition `use:` graph: {}", cycle.join(" -> ")),
            LabeledSpan::primary(span, String::new()),
        ));
        return;
    }
    if bind_ctx.depth > MAX_COMPOSITION_DEPTH {
        diags.push(Diagnostic::error(
            "E107",
            format!(
                "composition nesting too deep (>{MAX_COMPOSITION_DEPTH}) \
                 at node {node_name:?}"
            ),
            LabeledSpan::primary(span, String::new()),
        ));
        return;
    }

    // 3. Validate call-site inputs against signature.
    let mut has_errors = false;
    if !validate_inputs(call_inputs, signature, node_name, span, diags) {
        has_errors = true;
    }

    // 4. Validate call-site config against signature.
    if !validate_config(call_config, signature, node_name, span, diags) {
        has_errors = true;
    }

    // 5. Validate resources (stub for 16c.2).
    if !validate_resources(call_resources, signature, node_name, span, diags) {
        has_errors = true;
    }

    if has_errors {
        return;
    }

    // 5b. Populate provenance entries for each resolved config param.
    populate_config_provenance(
        call_config,
        signature,
        node_name,
        span,
        &mut artifacts.provenance,
    );

    // 6. Build input port rows: validate upstream satisfies port schema,
    //    build Row::open(declared, fresh_tail) for each port.
    let input_port_rows = match build_input_port_rows(
        call_inputs,
        signature,
        parent_schema_by_name,
        &mut artifacts.bound_schemas,
        node_name,
        span,
        diags,
    ) {
        Some(rows) => rows,
        None => return, // E102 errors already pushed
    };

    // 7. Re-read body from disk.
    let body_file =
        match read_and_parse_body(&signature.source_path, artifacts, node_name, span, diags) {
            Some(f) => f,
            None => return,
        };

    // 8. Enter nested scope.
    let body_id = artifacts.fresh_body_id();

    // Save parent state and set up nested scope.
    let saved_enclosing = std::mem::take(&mut bind_ctx.enclosing_scope_names);
    let saved_origin_dir = std::mem::replace(
        &mut bind_ctx.origin_dir,
        // Set origin_dir to the composition file's directory (workspace-relative)
        // so nested `use:` paths resolve correctly.
        resolved_path
            .parent()
            .unwrap_or(Path::new(""))
            .to_path_buf(),
    );
    // The new enclosing_scope is the parent's node names (for E108).
    bind_ctx.enclosing_scope_names = parent_schema_by_name.keys().cloned().collect();
    // Also include the enclosing scope's names so nested compositions
    // can detect references to any ancestor scope.
    bind_ctx
        .enclosing_scope_names
        .extend(saved_enclosing.iter().cloned());

    bind_ctx.use_path_stack.push(resolved_path.clone());
    bind_ctx.depth += 1;

    // Seed body_schema_by_name with input port rows.
    let mut body_schema_by_name: HashMap<String, Row> = HashMap::new();
    for (port_name, row) in &input_port_rows {
        body_schema_by_name.insert(port_name.clone(), row.clone());
    }

    // Recursive bind_schema on body nodes.
    bind_schema_inner(
        &body_file.nodes,
        diags,
        bind_ctx,
        artifacts,
        &mut body_schema_by_name,
    );

    // Restore parent state.
    bind_ctx.depth -= 1;
    bind_ctx.use_path_stack.pop();
    bind_ctx.enclosing_scope_names = saved_enclosing;
    bind_ctx.origin_dir = saved_origin_dir;

    // 9. Compute output rows from signature.outputs → body schemas.
    let output_port_rows =
        compute_output_rows(&signature.outputs, &body_schema_by_name, call_outputs);

    // 10. W101 check: body-declared columns shadowing input port pass-throughs.
    check_w101_shadows(
        &input_port_rows,
        &output_port_rows,
        &body_schema_by_name,
        signature,
        node_name,
        span,
        diags,
    );

    // 11. Collect nested body ids from artifacts (bodies added during recursive call).
    let nested_body_ids: Vec<CompositionBodyId> = artifacts
        .composition_body_assignments
        .values()
        .copied()
        .filter(|id| {
            // Only include bodies that were added after our body_id was allocated.
            id.0 > body_id.0
        })
        .collect();

    // 12. Persist body BoundSchemas from body_schema_by_name.
    let mut body_bound_schemas = BoundSchemas::default();
    for (node_name, row) in &body_schema_by_name {
        body_bound_schemas.set_output(node_name.clone(), row.clone());
    }

    // 13. Lower body nodes to PlanNodes via the shared lowering function.
    let body_plan_nodes: Vec<crate::plan::execution::PlanNode> = body_file
        .nodes
        .iter()
        .filter_map(|spanned| {
            let saphyr_line = spanned.referenced.line();
            let body_span = if saphyr_line > 0 {
                crate::span::Span::line_only(saphyr_line as u32)
            } else {
                crate::span::Span::SYNTHETIC
            };
            let n = &spanned.value;
            let n_name = n.name().to_string();
            crate::config::lower_node_to_plan_node(
                n,
                &n_name,
                body_span,
                artifacts,
                crate::config::LowerScope::Body,
                diags,
            )
        })
        .collect();

    // 14. Build and insert BoundBody.
    let bound_body = BoundBody {
        signature_path: resolved_path,
        nodes: body_plan_nodes,
        bound_schemas: body_bound_schemas,
        output_port_rows: output_port_rows.clone(),
        input_port_rows,
        nested_body_ids,
    };
    artifacts.insert_body(body_id, bound_body);
    artifacts
        .composition_body_assignments
        .insert(node_name.to_string(), body_id);

    // 15. Write composition's output row to parent scope.
    // Use the first output port's row as the composition node's output.
    if let Some((_, row)) = output_port_rows.into_iter().next() {
        parent_schema_by_name.insert(node_name.to_string(), row);
    }
}

// ─── Validation helpers ─────────────────────────────────────────────

/// Validate call-site `inputs:` against the signature's declared input ports.
/// Returns `true` if validation passes (no errors emitted).
fn validate_inputs(
    call_site: &IndexMap<String, String>,
    signature: &CompositionSignature,
    node_name: &str,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) -> bool {
    let mut ok = true;

    // Check for missing required ports.
    for (port_name, port_decl) in &signature.inputs {
        if port_decl.required && !call_site.contains_key(port_name) {
            diags.push(Diagnostic::error(
                "E104",
                format!(
                    "composition node {node_name:?}: missing required input port {port_name:?}"
                ),
                LabeledSpan::primary(span, String::new()),
            ));
            ok = false;
        }
    }

    // Check for extra keys not in signature.
    for key in call_site.keys() {
        if !signature.inputs.contains_key(key) {
            diags.push(Diagnostic::error(
                "E103",
                format!(
                    "composition node {node_name:?}: input port {key:?} is not declared \
                     in the composition signature"
                ),
                LabeledSpan::primary(span, String::new()),
            ));
            ok = false;
        }
    }

    ok
}

/// Validate call-site `config:` against the signature's config_schema.
/// Returns `true` if validation passes.
fn validate_config(
    call_site: &IndexMap<String, serde_json::Value>,
    signature: &CompositionSignature,
    node_name: &str,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) -> bool {
    let mut ok = true;

    // Check for missing required config params (no default).
    for (param_name, param_decl) in &signature.config_schema {
        if param_decl.required
            && param_decl.default.is_none()
            && !call_site.contains_key(param_name)
        {
            diags.push(Diagnostic::error(
                "E104",
                format!(
                    "composition node {node_name:?}: missing required config param {param_name:?}"
                ),
                LabeledSpan::primary(span, String::new()),
            ));
            ok = false;
        }
    }

    // Check for extra keys not in config_schema.
    for key in call_site.keys() {
        if !signature.config_schema.contains_key(key) {
            diags.push(Diagnostic::error(
                "E103",
                format!(
                    "composition node {node_name:?}: config param {key:?} is not declared \
                     in the composition signature"
                ),
                LabeledSpan::primary(span, String::new()),
            ));
            ok = false;
        }
    }

    ok
}

/// Validate call-site `resources:` — stub for 16c.2. Real validation
/// lands in 16c.3 when the Resource enum is fully defined.
fn validate_resources(
    _call_site: &IndexMap<String, serde_json::Value>,
    _signature: &CompositionSignature,
    _node_name: &str,
    _span: Span,
    _diags: &mut Vec<Diagnostic>,
) -> bool {
    true
}

/// Populate [`ProvenanceDb`] entries for each resolved config param.
///
/// For each param in the signature's `config_schema`, the resolved value is:
/// - The call-site override if present, or
/// - The signature's default if present.
///
/// All values at this stage are tagged as [`LayerKind::CompositionDefault`]
/// since channel-level resolution does not exist until 16c.4.
fn populate_config_provenance(
    call_site: &IndexMap<String, serde_json::Value>,
    signature: &CompositionSignature,
    node_name: &str,
    span: Span,
    provenance: &mut ProvenanceDb,
) {
    for (param_name, param_decl) in &signature.config_schema {
        let resolved_value = if let Some(call_value) = call_site.get(param_name) {
            // Call-site override: use the call-site span (the composition node's span).
            call_value.clone()
        } else if let Some(default_value) = &param_decl.default {
            // Signature default: use the param declaration's span.
            default_value.clone()
        } else {
            // No value available (required param without default — should
            // have been caught by validate_config, but guard defensively).
            continue;
        };

        // Use param_decl.span for defaults, node span for call-site overrides.
        let value_span = if call_site.contains_key(param_name) {
            span
        } else {
            param_decl.span
        };

        provenance.insert(
            node_name.to_owned(),
            param_name.clone(),
            ResolvedValue::new(resolved_value, LayerKind::CompositionDefault, value_span),
        );
    }
}

// ─── Input port row building ────────────────────────────────────────

/// Build input port rows for a composition call site.
///
/// For each declared input port, validates that the upstream row satisfies
/// the declared minimum schema, then builds `Row::open(declared, fresh_tail)`.
/// Returns `None` if any E102 errors were emitted.
fn build_input_port_rows(
    call_inputs: &IndexMap<String, String>,
    signature: &CompositionSignature,
    parent_schemas: &HashMap<String, Row>,
    bound_schemas: &mut BoundSchemas,
    node_name: &str,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) -> Option<IndexMap<String, Row>> {
    let mut rows = IndexMap::new();
    let mut has_errors = false;

    for (port_name, port_decl) in &signature.inputs {
        // Look up the upstream node binding for this port.
        let upstream_name = match call_inputs.get(port_name) {
            Some(name) => name,
            None => {
                if !port_decl.required {
                    // Optional port not bound — skip.
                    continue;
                }
                // Required port missing — already diagnosed by validate_inputs.
                continue;
            }
        };

        // Look up upstream row.
        let upstream_row = match parent_schemas.get(upstream_name.as_str()) {
            Some(row) => row,
            None => {
                diags.push(Diagnostic::error(
                    "E102",
                    format!(
                        "composition node {node_name:?}: input port {port_name:?} \
                         binds to {upstream_name:?} which has no bound schema"
                    ),
                    LabeledSpan::primary(span, String::new()),
                ));
                has_errors = true;
                continue;
            }
        };

        // Validate declared minimum columns against upstream.
        let declared_columns = port_columns(port_decl);
        for (col_name, col_type) in &declared_columns {
            match upstream_row.declared.get(col_name) {
                None => {
                    // Column might be available via pass-through (Open tail).
                    if matches!(upstream_row.tail, RowTail::Closed) {
                        diags.push(Diagnostic::error(
                            "E102",
                            format!(
                                "composition node {node_name:?}: input port {port_name:?} \
                                 requires column {col_name:?} but upstream {upstream_name:?} \
                                 does not provide it"
                            ),
                            LabeledSpan::primary(span, String::new()),
                        ));
                        has_errors = true;
                    }
                    // Open upstream: column may pass through — allow it.
                }
                Some(upstream_type) => {
                    // Type compatibility check: Any matches anything.
                    if *upstream_type != Type::Any
                        && *col_type != Type::Any
                        && upstream_type != col_type
                    {
                        diags.push(Diagnostic::error(
                            "E102",
                            format!(
                                "composition node {node_name:?}: input port {port_name:?} \
                                 declares column {col_name:?} as {col_type:?} but upstream \
                                 {upstream_name:?} provides {upstream_type:?}"
                            ),
                            LabeledSpan::primary(span, String::new()),
                        ));
                        has_errors = true;
                    }
                }
            }
        }

        // Build Row::open(declared, fresh_tail) for the port.
        let tail_var = bound_schemas.fresh_tail();
        let cxl_span = cxl::lexer::Span::new(span.start as usize, span.start as usize);
        let row = Row {
            declared: declared_columns,
            declared_span: cxl_span,
            tail: RowTail::Open(tail_var),
        };
        rows.insert(port_name.clone(), row);
    }

    if has_errors { None } else { Some(rows) }
}

/// Extract declared columns from a `PortDecl` as an `IndexMap<String, Type>`.
fn port_columns(decl: &PortDecl) -> IndexMap<String, Type> {
    match &decl.schema {
        Some(schema) => schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.ty.clone()))
            .collect(),
        None => IndexMap::new(),
    }
}

// ─── Body re-parse ──────────────────────────────────────────────────

/// Re-read and parse a composition body from disk.
fn read_and_parse_body(
    source_path: &Path,
    artifacts: &mut CompileArtifacts,
    node_name: &str,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) -> Option<CompositionFile> {
    let yaml = match std::fs::read_to_string(source_path) {
        Ok(s) => s,
        Err(e) => {
            diags.push(Diagnostic::error(
                "E103",
                format!(
                    "composition node {node_name:?}: failed to read body from {}: {e}",
                    source_path.display()
                ),
                LabeledSpan::primary(span, String::new()),
            ));
            return None;
        }
    };
    let file_id = artifacts.fresh_file_id();
    match CompositionFile::parse(&yaml, file_id, source_path.to_path_buf()) {
        Ok(file) => Some(file),
        Err(e) => {
            diags.push(Diagnostic::error(
                "E101",
                format!(
                    "composition node {node_name:?}: body parse failed for {}: {e}",
                    source_path.display()
                ),
                LabeledSpan::primary(span, String::new()),
            ));
            None
        }
    }
}

// ─── Output row computation ─────────────────────────────────────────

/// Compute output port rows from signature outputs by resolving
/// internal node references against the body's schema map.
fn compute_output_rows(
    signature_outputs: &IndexMap<String, OutputAlias>,
    body_schemas: &HashMap<String, Row>,
    _call_outputs: &IndexMap<String, String>,
) -> IndexMap<String, Row> {
    let mut rows = IndexMap::new();
    for (port_name, alias) in signature_outputs {
        // Parse "node_name.channel" or just "node_name".
        let internal_ref = &alias.internal_ref.value;
        let node_name = internal_ref.split('.').next().unwrap_or(internal_ref);
        if let Some(row) = body_schemas.get(node_name) {
            rows.insert(port_name.clone(), row.clone());
        }
    }
    rows
}

// ─── W101 shadow detection ──────────────────────────────────────────

/// Check for W101: body-declared columns that shadow input port
/// pass-through columns at the output boundary.
fn check_w101_shadows(
    input_port_rows: &IndexMap<String, Row>,
    output_port_rows: &IndexMap<String, Row>,
    _body_schemas: &HashMap<String, Row>,
    _signature: &CompositionSignature,
    node_name: &str,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) {
    // Collect all columns declared in input ports (the minimum-required set).
    // Columns NOT in this set but flowing through via Open tail are pass-throughs.
    let mut input_declared: HashSet<String> = HashSet::new();
    for row in input_port_rows.values() {
        for col_name in row.declared.keys() {
            input_declared.insert(col_name.clone());
        }
    }

    // For each output row, check if any body-added column has the same name
    // as a column that would pass through from the input tail.
    for (port_name, output_row) in output_port_rows {
        for col_name in output_row.declared.keys() {
            // If this column is NOT in the input declared set but COULD
            // pass through via an Open tail, and the body also declares it,
            // that's a shadow. However, in practice the shadow case is:
            // the body declares a column with the same name as a pass-through.
            // A pass-through column is one that exists in the upstream row
            // but is NOT in the port's declared minimum.
            if !input_declared.contains(col_name) {
                // This column was added by the body (not declared in input ports).
                // Check if any input port has an Open tail — if so, this column
                // name could shadow a pass-through from the upstream.
                let has_open_input = input_port_rows
                    .values()
                    .any(|r| matches!(r.tail, RowTail::Open(_)));
                if has_open_input {
                    diags.push(Diagnostic::warning(
                        "W101",
                        format!(
                            "composition node {node_name:?}, output port {port_name:?}: \
                             body-declared column {col_name:?} may shadow a pass-through \
                             column from the input port"
                        ),
                        LabeledSpan::primary(span, "body creates this column".to_string()),
                    ));
                }
            }
        }
    }
}

// ─── Use-path resolution ────────────────────────────────────────────

/// Resolve a `use:` path to a symbol table key.
///
/// Tries two strategies:
/// 1. Resolve relative to `origin_dir` (workspace-relative directory
///    of the file containing the `use:` reference).
/// 2. Try the normalized path directly against the symbol table.
///
/// Falls back to the normalized path for the E103 diagnostic.
fn resolve_use_path(
    use_path: &Path,
    origin_dir: &Path,
    workspace_root: &Path,
    symbol_table: &CompositionSymbolTable,
) -> PathBuf {
    if use_path.is_absolute() {
        let rel = use_path
            .strip_prefix(workspace_root)
            .unwrap_or(use_path)
            .to_path_buf();
        if symbol_table.contains_key(&rel) {
            return rel;
        }
        return rel;
    }

    // Strategy 1: resolve relative to origin_dir.
    let joined = origin_dir.join(use_path);
    let normalized = normalize_path(&joined);
    if symbol_table.contains_key(&normalized) {
        return normalized;
    }

    // Strategy 2: try just normalizing the use_path itself.
    let direct = normalize_path(use_path);
    if symbol_table.contains_key(&direct) {
        return direct;
    }

    // Strategy 3: try matching by filename across all symbol table keys.
    if let Some(filename) = use_path.file_name() {
        for key in symbol_table.keys() {
            if key.file_name() == Some(filename) {
                return key.clone();
            }
        }
    }

    // Fallback: return the normalized resolved path for error messages.
    normalized
}

/// Normalize a path by resolving `.` and `..` without filesystem access.
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                components.pop();
            }
            c => components.push(c),
        }
    }
    components.iter().collect()
}

// ─── Upstream schema helpers ────────────────────────────────────────

fn columns_from_decl(decl: &SchemaDecl) -> IndexMap<String, Type> {
    decl.columns
        .iter()
        .map(|c| (c.name.clone(), c.ty.clone()))
        .collect()
}

fn upstream_schema<'a>(
    input: &crate::config::node_header::NodeInput,
    schemas: &'a HashMap<String, Row>,
) -> Option<&'a Row> {
    schemas.get(input_target(input))
}

fn input_target(input: &crate::config::node_header::NodeInput) -> &str {
    match input {
        crate::config::node_header::NodeInput::Single(s) => s.as_str(),
        crate::config::node_header::NodeInput::Port { node, .. } => node.as_str(),
    }
}

/// Extract the target node name from a `NodeInput` (for E108 checks).
fn upstream_target_name(input: &crate::config::node_header::NodeInput) -> Option<&str> {
    Some(input_target(input))
}

#[allow(clippy::result_large_err)]
fn typecheck_cxl(
    node_name: &str,
    source: &str,
    schema: &Row,
    mode: AggregateMode,
    span: Span,
) -> Result<TypedProgram, Diagnostic> {
    let parse_result = cxl::parser::Parser::parse(source);
    if !parse_result.errors.is_empty() {
        let messages: Vec<String> = parse_result
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect();
        return Err(Diagnostic::error(
            "E200",
            format!("CXL parse error in {node_name:?}: {}", messages.join("; ")),
            LabeledSpan::primary(span, String::new()),
        ));
    }
    let field_refs: Vec<&str> = schema.declared.keys().map(|s| s.as_str()).collect();
    let resolved =
        cxl::resolve::resolve_program(parse_result.ast, &field_refs, parse_result.node_count)
            .map_err(|diags| {
                Diagnostic::error(
                    "E200",
                    format!(
                        "CXL name resolution failed in {node_name:?}: {}",
                        diags
                            .into_iter()
                            .map(|d| d.message)
                            .collect::<Vec<_>>()
                            .join("; ")
                    ),
                    LabeledSpan::primary(span, String::new()),
                )
            })?;
    cxl::typecheck::type_check_with_mode(resolved, schema, mode).map_err(|diags| {
        let errors: Vec<String> = diags
            .iter()
            .filter(|d| !d.is_warning)
            .map(|d| d.message.clone())
            .collect();
        let joined = if errors.is_empty() {
            diags
                .into_iter()
                .map(|d| d.message)
                .collect::<Vec<_>>()
                .join("; ")
        } else {
            errors.join("; ")
        };
        Diagnostic::error(
            "E200",
            format!("CXL type error in {node_name:?}: {joined}"),
            LabeledSpan::primary(span, String::new()),
        )
    })
}

fn propagate_row(upstream: &Row, typed: &TypedProgram) -> Row {
    let mut out = upstream.declared.clone();
    for stmt in &typed.program.statements {
        if let cxl::ast::Statement::Emit { name, expr, .. } = stmt {
            let emit_type = typed
                .types
                .get(expr.node_id().0 as usize)
                .and_then(|t| t.clone())
                .unwrap_or(Type::Any);
            out.insert(name.to_string(), emit_type);
        }
    }
    Row {
        declared: out,
        declared_span: upstream.declared_span,
        tail: upstream.tail.clone(),
    }
}

fn propagate_aggregate(group_by: &[String], upstream: &Row, typed: &TypedProgram) -> Row {
    let mut out: IndexMap<String, Type> = IndexMap::new();
    for gb in group_by {
        let t = upstream.declared.get(gb).cloned().unwrap_or(Type::Any);
        out.insert(gb.clone(), t);
    }
    for stmt in &typed.program.statements {
        if let cxl::ast::Statement::Emit { name, expr, .. } = stmt {
            let emit_type = typed
                .types
                .get(expr.node_id().0 as usize)
                .and_then(|t| t.clone())
                .unwrap_or(Type::Any);
            out.insert(name.to_string(), emit_type);
        }
    }
    Row {
        declared: out,
        declared_span: upstream.declared_span,
        tail: upstream.tail.clone(),
    }
}

/// E201 diagnostic for a Source with no `schema:` field.
#[allow(dead_code)]
pub fn e201_missing_schema(source_name: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E201",
        format!("source {source_name:?} is missing required `schema:` field"),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "declare the source columns inline:\n  schema:\n    - { name: col1, type: string }\n    - { name: col2, type: int }",
    )
}
