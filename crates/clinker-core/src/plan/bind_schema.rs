//! Compile-time CXL typecheck + schema propagation (`bind_schema`).
//!
//! Walks the unified `nodes:` DAG in topological order, seeds each
//! source's schema from its author-declared `schema:` block, typechecks
//! every Transform/Aggregate/Route body against the upstream Row, and
//! propagates the output Row downstream. Result: a `CompileArtifacts`
//! map keyed by node name holding one `Arc<TypedProgram>` per CXL-bearing
//! node.
//!
//! For `PipelineNode::Composition` nodes, `bind_composition` recursively
//! binds the composition body at the call-site boundary using
//! row-polymorphic type propagation (Leijen 2005).
//!
//! Errors surface as E200 diagnostics (CXL type error), E201
//! diagnostics (missing source schema), and E102–E109 / W101
//! diagnostics for composition binding.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cxl::ast::{Expr, Statement};
use cxl::typecheck::{
    AggregateMode, ColumnLookup, QualifiedField, Row, RowTail, Type, TypedProgram,
};
use indexmap::IndexMap;

use crate::config::compile_context::CompileContext;
use crate::config::composition::{
    CompositionFile, CompositionSignature, CompositionSymbolTable, LayerKind, OutputAlias,
    PortDecl, ProvenanceDb, ResolvedValue,
};
use crate::config::node_header::CombineHeader;
use crate::config::pipeline_node::{
    CombineBody, MatchMode, PipelineNode, PropagateCkSpec, SchemaDecl,
};
use crate::error::{Diagnostic, LabeledSpan};
use crate::plan::combine::{
    CombineInput, DecomposedPredicate, decompose_predicate, select_driving_input,
};
use crate::plan::composition_body::{BoundBody, CompositionBodyId};
use crate::span::{FileId, Span};
use crate::yaml::Spanned;

/// Maximum composition nesting depth.
///
/// Compile-time depth violations emit E107 from `bind_composition`.
/// The runtime composition-body executor reuses this same constant as
/// its recursion-depth guard and emits a distinct E112 on overflow,
/// so log-grep on either code finds exactly one emission site.
pub(crate) const MAX_COMPOSITION_DEPTH: u32 = 50;

/// Compile artifacts produced by `bind_schema` — one entry per node name
/// whose CXL body successfully type-checked, plus per-node row types.
#[derive(Debug, Default, Clone)]
pub struct CompileArtifacts {
    pub typed: HashMap<String, Arc<TypedProgram>>,
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
    /// Side-table of provenance-tracked config values. Populated by
    /// `bind_composition` for each composition node's config params.
    pub provenance: ProvenanceDb,
    /// Decomposed `where:` predicates per combine node, keyed by combine
    /// node name. Populated by the predicate-decomposition pass; consumed
    /// by combine strategy selection.
    pub combine_predicates: HashMap<String, DecomposedPredicate>,
    /// Per-input metadata per combine node, keyed by combine node name.
    /// The inner `IndexMap` preserves declaration order of the inputs
    /// (matches `CombineHeader.input` iteration order). Populated during
    /// schema propagation.
    pub combine_inputs: HashMap<String, IndexMap<String, CombineInput>>,
    /// Driving-input qualifier per combine node, chosen at `bind_combine`
    /// time by [`select_driving_input`] (explicit `drive:` → cardinality
    /// → first-in-IndexMap default). The `select_combine_strategies`
    /// post-pass reads this to stamp the runtime
    /// `PlanNode::Combine.driving_input` field. Combines that fail driver
    /// selection (E306) are absent from this map and their post-pass
    /// entry is skipped.
    pub combine_driving: HashMap<String, String>,
    /// User-supplied [`CombineStrategyHint`] per combine node, keyed by
    /// node name. Populated from `CombineBody.strategy` during
    /// `bind_combine`. The `select_combine_strategies` post-pass reads
    /// this to override the planner's predicate-shape default — today
    /// only `GraceHash` on pure-equi predicates produces a non-default
    /// outcome. Combines absent from this map default to
    /// [`CombineStrategyHint::Auto`].
    pub combine_strategy_hints: HashMap<String, crate::config::CombineStrategyHint>,
    /// Per-combine pre-resolved column map produced by the CXL
    /// typechecker's combine-body walk. Key is the combine node's
    /// name; value is the `ResolvedColumnMap` shape expected by
    /// `CombineResolverMapping::from_pre_resolved`. Combines that fail
    /// body typecheck (or the collect-mode path, which has no body)
    /// surface an empty map here.
    pub combine_resolved_columns: HashMap<String, crate::plan::execution::ResolvedColumnMap>,
    /// Monotonic counter for fresh tail-variable IDs allocated during
    /// row-polymorphic propagation (composition input-port binding).
    /// Threaded as `&mut u32` into `build_input_port_rows`; IDs are
    /// only comparable within a single `compile()` run.
    pub next_tail_var: u32,
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
///
/// Each `Source` node's declared schema is widened with one
/// `$ck.<field>` shadow column per field listed on that source's own
/// `correlation_key:`, tail-appended in declaration order. The shadow
/// columns preserve correlation-group identity through downstream
/// Transforms that may rewrite the user-declared field. Composition
/// body Sources inherit the parent caller's widening transparently —
/// the body's port-synthetic Source carries every column flowing in,
/// including any `$ck.*` shadows.
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

    artifacts
}

/// Build a placeholder `TypedProgram` carrying only `output_row`, for
/// node variants without a CXL body (Source/Merge/Output/Composition).
fn synthetic_typed_program(output_row: Row) -> TypedProgram {
    TypedProgram {
        program: cxl::ast::Program {
            statements: Vec::new(),
            span: cxl::lexer::Span::default(),
        },
        bindings: Vec::new(),
        types: Vec::new(),
        field_types: IndexMap::new(),
        regexes: Vec::new(),
        node_count: 0,
        output_row,
    }
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
                let (columns, missing) =
                    columns_from_decl(schema_decl, config.correlation_key.as_ref());
                for missing_field in &missing {
                    diags.push(Diagnostic::error(
                        "E153",
                        format!(
                            "source {name:?} declares correlation_key field {missing_field:?} \
                             but the field is not present in this source's `schema:` block. \
                             Add the column to the source's schema, or remove the field from \
                             `correlation_key`."
                        ),
                        LabeledSpan::primary(span, String::new()),
                    ));
                }
                let cxl_span = cxl::lexer::Span::new(span.start as usize, span.start as usize);
                let row = Row::closed(columns, cxl_span);
                schema_by_name.insert(name.clone(), row.clone());
                artifacts
                    .typed
                    .insert(name, Arc::new(synthetic_typed_program(row)));
            }
            PipelineNode::Transform { header, config } => {
                // E108: check for enclosing-scope reference BEFORE upstream lookup.
                if let Some(target) = upstream_target_name(&header.input.value)
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
                let upstream = match upstream_schema(&header.input.value, schema_by_name) {
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
                    Ok(mut typed) => {
                        let out = propagate_row(&upstream, &typed);
                        schema_by_name.insert(name.clone(), out.clone());
                        typed.output_row = out;
                        artifacts.typed.insert(name, Arc::new(typed));
                    }
                    Err(d) => diags.push(d),
                }
            }
            PipelineNode::Aggregate { header, config } => {
                let upstream = match upstream_schema(&header.input.value, schema_by_name) {
                    Some(s) => s.clone(),
                    None => continue,
                };
                // Validate group_by fields exist in the upstream schema.
                let mut missing = Vec::new();
                for gb in &config.group_by {
                    if !upstream.has_field(gb) {
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
                    Ok(mut typed) => {
                        let out = propagate_aggregate(&name, &config.group_by, &upstream, &typed);
                        schema_by_name.insert(name.clone(), out.clone());
                        typed.output_row = out;
                        artifacts.typed.insert(name, Arc::new(typed));
                    }
                    Err(d) => diags.push(d),
                }
            }
            PipelineNode::Route { header, config: _ } => {
                if let Some(upstream) = upstream_schema(&header.input.value, schema_by_name) {
                    let cloned = upstream.clone();
                    if let Ok(mut empty) =
                        typecheck_cxl(&name, "", &cloned, AggregateMode::Row, span)
                    {
                        empty.output_row = cloned.clone();
                        artifacts.typed.insert(name.clone(), Arc::new(empty));
                    }
                    schema_by_name.insert(name, cloned);
                }
            }
            PipelineNode::Merge { header, .. } => {
                if let Some(first) = header.inputs.first()
                    && let Some(upstream) = schema_by_name.get(input_target(&first.value))
                {
                    let row = upstream.clone();
                    schema_by_name.insert(name.clone(), row.clone());
                    artifacts
                        .typed
                        .insert(name, Arc::new(synthetic_typed_program(row)));
                }
            }
            PipelineNode::Output { header, .. } => {
                if let Some(upstream) = upstream_schema(&header.input.value, schema_by_name) {
                    let row = upstream.clone();
                    schema_by_name.insert(name.clone(), row.clone());
                    artifacts
                        .typed
                        .insert(name, Arc::new(synthetic_typed_program(row)));
                }
            }
            // Phase Combine C.1.1 + C.1.2 + C.1.3 (single-pass arm).
            //
            // Combine compile runs as a single pass in `bind_combine`.
            // Every mature engine does merged-schema build, predicate
            // typecheck, body typecheck, and output-row publication as
            // a single bottom-up traversal. Splitting them into three
            // side-table handoffs is the pattern Spark SPIP-49834 is
            // actively migrating away from.
            PipelineNode::Combine { header, config } => {
                bind_combine(
                    &name,
                    header,
                    config,
                    span,
                    diags,
                    artifacts,
                    schema_by_name,
                );
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
/// (preserve-and-recurse, not flatten-and-splice).
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

    // 5. Validate resources (stub pending full Resource enum).
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
        &mut artifacts.next_tail_var,
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

    // 12. Persist body per-node rows.
    let body_rows: HashMap<String, Row> = body_schema_by_name.clone();

    // 13. Empty-body rejection (E111). A composition with zero body
    // nodes has nothing to execute and would silently no-op at runtime,
    // which is the strict rejection pattern enforced by
    // StreamSets / Camel / Logstash. Reject at bind time so authors
    // see the bug at compile rather than as missing output rows.
    if body_file.nodes.is_empty() {
        diags.push(Diagnostic::error(
            "E111",
            format!(
                "composition node {node_name:?}: body file {} has zero nodes",
                resolved_path.display()
            ),
            LabeledSpan::primary(span, String::new()),
        ));
        return;
    }

    // 14. Lower body nodes to PlanNodes via the shared lowering
    // function and build a mini-DAG keyed by NodeIndex. The body
    // executor walks this graph topologically using the same
    // dispatcher entry as the top-level walker — edges come from
    // each body node's declared `input:` field, mirroring the
    // edge-wiring loop in top-level `PipelineConfig::compile()`.
    use crate::plan::execution::{DependencyType, PlanEdge};
    use petgraph::graph::DiGraph;

    let mut body_graph: DiGraph<crate::plan::execution::PlanNode, PlanEdge> = DiGraph::new();
    let mut body_name_to_idx: std::collections::HashMap<String, petgraph::graph::NodeIndex> =
        std::collections::HashMap::new();

    // 14a. Synthesize a `PlanNode::Source` per declared input port. The
    // body's mini-DAG sees ports as first-class root nodes — body
    // consumers reference them by name through the same edge-wiring
    // pass that handles body-internal references, and the dispatcher
    // walks them via the standard Source arm with pre-seeded records
    // from the parent scope. Without these synthetics, multi-port
    // consumers (notably combine, whose `input:` map keys ARE port
    // names) would have no incoming edges in the body DAG and fail
    // their predecessor-by-name lookup at dispatch time.
    //
    // The synthetic node's `output_schema` adopts the parent source's
    // declared columns (the schema records actually carry at runtime),
    // not the port's declared columns — port rows declare a subset
    // and rely on Open-tail pass-through, but records flowing in
    // already carry every parent column.
    for (port_name, _port_row) in &input_port_rows {
        let parent_node_name = match call_inputs.get(port_name) {
            Some(n) => n.as_str(),
            None => continue, // optional port not bound — skip
        };
        let parent_row = match parent_schema_by_name.get(parent_node_name) {
            Some(r) => r,
            None => continue, // upstream missing — already diagnosed
        };
        let port_schema =
            schema_from_field_names(parent_row.field_names().map(|qf| qf.name.as_ref()));
        let port_source = crate::plan::execution::PlanNode::Source {
            name: port_name.clone(),
            span,
            resolved: None,
            output_schema: port_schema,
        };
        let idx = body_graph.add_node(port_source);
        body_name_to_idx.insert(port_name.clone(), idx);
    }

    for spanned in &body_file.nodes {
        let saphyr_line = spanned.referenced.line();
        let body_span = if saphyr_line > 0 {
            crate::span::Span::line_only(saphyr_line as u32)
        } else {
            crate::span::Span::SYNTHETIC
        };
        let n = &spanned.value;
        let n_name = n.name().to_string();
        // Body nodes are lowered without the top-level
        // parallelism/index enrichment — those live on the
        // top-level mini-DAG. Body aggregate retraction-mode flags are
        // re-derived after the body's mini-DAG is built, alongside the
        // top-level pass that walks the lattice. Lowering only stamps
        // the strict default; `apply_retraction_flags` rewrites it.
        let lower_ctx = crate::config::LoweringCtx::default();
        if let Some(plan_node) = crate::config::lower_node_to_plan_node(
            n, &n_name, body_span, artifacts, &lower_ctx, diags,
        ) {
            let idx = body_graph.add_node(plan_node);
            body_name_to_idx.insert(n_name, idx);
        }
    }

    // Wire edges by walking each body node's `input:` reference(s).
    // `input_full_reference` strips ports lazily; the producer key is
    // the bare node name (the part before `.`).
    fn body_strip_port(r: &str) -> &str {
        r.split('.').next().unwrap_or(r)
    }
    for spanned in &body_file.nodes {
        let n = &spanned.value;
        let consumer_name = n.name();
        let Some(&consumer_idx) = body_name_to_idx.get(consumer_name) else {
            continue;
        };
        let mut wire = |producer_full: &str, port: Option<String>| {
            let producer_key = body_strip_port(producer_full);
            // Body-internal references resolve through name_to_idx;
            // references to a signature input port don't produce a
            // graph edge — port seeding is handled at composition
            // entry by the executor through the live edge graph.
            if let Some(&producer_idx) = body_name_to_idx.get(producer_key) {
                body_graph.add_edge(
                    producer_idx,
                    consumer_idx,
                    PlanEdge {
                        dependency_type: DependencyType::Data,
                        port,
                    },
                );
            }
        };
        match n {
            // Sources are roots and Compositions inside the body are
            // call-sites whose own input(s) come from upstream nodes
            // in the body — wire those, the same way top-level
            // compile() does.
            PipelineNode::Source { .. } => {}
            PipelineNode::Transform { header, .. }
            | PipelineNode::Aggregate { header, .. }
            | PipelineNode::Route { header, .. }
            | PipelineNode::Output { header, .. } => {
                let r = match &header.input.value {
                    crate::config::node_header::NodeInput::Single(s) => s.clone(),
                    crate::config::node_header::NodeInput::Port { node, port } => {
                        format!("{node}.{port}")
                    }
                };
                wire(&r, None);
            }
            PipelineNode::Composition {
                inputs: nested_inputs,
                ..
            } => {
                // Composition's named-port `inputs:` map is the
                // authoritative call-site binding. Each entry produces
                // one port-tagged edge — the dispatcher walks
                // incoming edges and reads the tag to harvest records
                // per port. `header.input:` is YAML-shape obligation
                // for the shared `NodeHeader` struct but is redundant
                // for a Composition's wiring (every required port is
                // already covered by `inputs:` per E104), so it does
                // not produce its own edge.
                for (port_name, upstream) in nested_inputs {
                    wire(upstream, Some(port_name.clone()));
                }
            }
            PipelineNode::Merge { header, .. } => {
                for inp in &header.inputs {
                    let r = match &inp.value {
                        crate::config::node_header::NodeInput::Single(s) => s.clone(),
                        crate::config::node_header::NodeInput::Port { node, port } => {
                            format!("{node}.{port}")
                        }
                    };
                    wire(&r, None);
                }
            }
            PipelineNode::Combine { header, .. } => {
                for ni in header.input.values() {
                    let r = match &ni.value {
                        crate::config::node_header::NodeInput::Single(s) => s.clone(),
                        crate::config::node_header::NodeInput::Port { node, port } => {
                            format!("{node}.{port}")
                        }
                    };
                    wire(&r, None);
                }
            }
        }
    }

    // Topo sort. Cycles inside a body are surfaced as bind-time
    // errors with the composition node's call-site span — distinct
    // from E108 (enclosing-scope reference) which fires earlier on
    // body-internal CXL evaluation.
    let body_topo = match petgraph::algo::toposort(&body_graph, None) {
        Ok(order) => order,
        Err(_) => {
            diags.push(Diagnostic::error(
                "E107",
                format!("cycle detected inside composition body for node {node_name:?}"),
                LabeledSpan::primary(span, String::new()),
            ));
            return;
        }
    };

    // Resolve each input port name to its synthetic-source NodeIndex.
    // The synthetic port-source was added to `body_name_to_idx` in
    // step 14a; the body executor seeds parent-scope records into
    // that index's buffer at composition entry, and the dispatcher's
    // standard Source arm reads them through. Each port owns its own
    // NodeIndex — multi-port consumers (notably combine) read each
    // port's records from a distinct buffer slot, with no collision.
    let mut port_name_to_node_idx: std::collections::HashMap<String, petgraph::graph::NodeIndex> =
        std::collections::HashMap::new();
    for port_name in signature.inputs.keys() {
        if let Some(&idx) = body_name_to_idx.get(port_name.as_str()) {
            port_name_to_node_idx.insert(port_name.clone(), idx);
        }
    }

    // `output_port_to_node_idx` resolves each declared output port
    // alias to the body NodeIndex that produces it. Aliases of the
    // form `node_name` or `node_name.channel` strip to the bare
    // node name; the executor harvests records from this index at
    // end of the body walk.
    let mut output_port_to_node_idx: IndexMap<String, petgraph::graph::NodeIndex> = IndexMap::new();
    for (port_name, alias) in &signature.outputs {
        let internal_ref = &alias.internal_ref.value;
        let bare = internal_ref.split('.').next().unwrap_or(internal_ref);
        if let Some(&idx) = body_name_to_idx.get(bare) {
            output_port_to_node_idx.insert(port_name.clone(), idx);
        }
    }

    // Populate `node_input_refs` and `route_bodies` from the body
    // file so the body executor can resolve `<route>.<branch>`
    // references and compile per-route conditions without having to
    // re-parse the .comp.yaml at runtime.
    let mut node_input_refs: HashMap<String, Vec<String>> = HashMap::new();
    let mut route_bodies: HashMap<String, crate::config::pipeline_node::RouteBody> = HashMap::new();
    for spanned in &body_file.nodes {
        let n = &spanned.value;
        let n_name = n.name().to_string();
        let inputs: Vec<String> = match n {
            PipelineNode::Source { .. } => Vec::new(),
            PipelineNode::Composition { header, .. }
            | PipelineNode::Transform { header, .. }
            | PipelineNode::Aggregate { header, .. }
            | PipelineNode::Route { header, .. }
            | PipelineNode::Output { header, .. } => {
                vec![match &header.input.value {
                    crate::config::node_header::NodeInput::Single(s) => s.clone(),
                    crate::config::node_header::NodeInput::Port { node, port } => {
                        format!("{node}.{port}")
                    }
                }]
            }
            PipelineNode::Merge { header, .. } => header
                .inputs
                .iter()
                .map(|inp| match &inp.value {
                    crate::config::node_header::NodeInput::Single(s) => s.clone(),
                    crate::config::node_header::NodeInput::Port { node, port } => {
                        format!("{node}.{port}")
                    }
                })
                .collect(),
            PipelineNode::Combine { header, .. } => header
                .input
                .values()
                .map(|ni| match &ni.value {
                    crate::config::node_header::NodeInput::Single(s) => s.clone(),
                    crate::config::node_header::NodeInput::Port { node, port } => {
                        format!("{node}.{port}")
                    }
                })
                .collect(),
        };
        node_input_refs.insert(n_name.clone(), inputs);

        if let PipelineNode::Route { config, .. } = n {
            route_bodies.insert(n_name, config.clone());
        }
    }

    // Capture analytic-window configs from body Transform nodes so
    // the post-parent-DAG-build pass can build `body_indices_to_build`
    // without re-reading the body file. Body lowering does not stamp
    // `window_index` on the PlanNode::Transform — that backfill is
    // owned by the body-window pass once parent NodeIndex space is
    // allocated.
    let mut body_window_configs: HashMap<String, crate::plan::index::LocalWindowConfig> =
        HashMap::new();
    for spanned in &body_file.nodes {
        if let PipelineNode::Transform { header, config } = &spanned.value
            && let Some(raw) = config.analytic_window.as_ref()
        {
            match crate::plan::index::parse_analytic_window_value(&Some(raw.clone()), &header.name)
            {
                Ok(Some(wc)) => {
                    body_window_configs.insert(header.name.clone(), wc);
                }
                Ok(None) => {}
                Err(e) => {
                    diags.push(Diagnostic::error(
                        "E003",
                        format!(
                            "composition body for {node_name:?} carries an \
                             invalid analytic_window on transform {:?}: {e}",
                            header.name
                        ),
                        LabeledSpan::primary(span, String::new()),
                    ));
                    return;
                }
            }
        }
    }

    // 15. Build and insert BoundBody. `body_indices_to_build` is
    // empty here — the post-parent-DAG-build pass populates it once
    // the parent's NodeIndex space exists, threading the parent's
    // `name_to_idx` so body-internal port-input references can resolve
    // to a parent-DAG `NodeIndex` and emit
    // `PlanIndexRoot::ParentNode { upstream, .. }`.
    let bound_body = BoundBody {
        signature_path: resolved_path,
        graph: body_graph,
        topo_order: body_topo,
        name_to_idx: body_name_to_idx,
        port_name_to_node_idx,
        body_rows,
        node_input_refs,
        route_bodies,
        output_port_rows: output_port_rows.clone(),
        output_port_to_node_idx,
        input_port_rows,
        nested_body_ids,
        body_indices_to_build: Vec::new(),
        body_window_configs,
    };
    artifacts.insert_body(body_id, bound_body);
    artifacts
        .composition_body_assignments
        .insert(node_name.to_string(), body_id);

    // 16. Write composition's output row to parent scope.
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

/// Validate call-site `resources:` — stub pending full Resource enum. Real validation
/// lands when the Resource enum is fully defined.
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
/// since channel-level resolution is not yet wired.
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
    next_tail_var: &mut u32,
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
            let col_str = col_name.name.as_ref();
            match upstream_row.lookup(col_str) {
                cxl::typecheck::ColumnLookup::Unknown => {
                    // Closed upstream, no match — required column missing.
                    diags.push(Diagnostic::error(
                        "E102",
                        format!(
                            "composition node {node_name:?}: input port {port_name:?} \
                             requires column {col_str:?} but upstream {upstream_name:?} \
                             does not provide it"
                        ),
                        LabeledSpan::primary(span, String::new()),
                    ));
                    has_errors = true;
                }
                cxl::typecheck::ColumnLookup::PassThrough(_) => {
                    // Open upstream: column may pass through — allow it.
                }
                cxl::typecheck::ColumnLookup::Declared(upstream_type) => {
                    // Type compatibility check: Any matches anything.
                    if *upstream_type != Type::Any
                        && *col_type != Type::Any
                        && upstream_type != col_type
                    {
                        diags.push(Diagnostic::error(
                            "E102",
                            format!(
                                "composition node {node_name:?}: input port {port_name:?} \
                                 declares column {col_str:?} as {col_type:?} but upstream \
                                 {upstream_name:?} provides {upstream_type:?}"
                            ),
                            LabeledSpan::primary(span, String::new()),
                        ));
                        has_errors = true;
                    }
                }
                cxl::typecheck::ColumnLookup::Ambiguous(_) => {
                    // Composition ports don't traffic in qualified fields —
                    // an ambiguous match can only arise from a misconstructed
                    // upstream row. Report as E102.
                    diags.push(Diagnostic::error(
                        "E102",
                        format!(
                            "composition node {node_name:?}: input port {port_name:?} \
                             column {col_str:?} is ambiguous in upstream {upstream_name:?}"
                        ),
                        LabeledSpan::primary(span, String::new()),
                    ));
                    has_errors = true;
                }
            }
        }

        // Append the parent's engine-stamped tail columns ($ck.<field>
        // shadow columns from each parent source's `correlation_key:`
        // widening) to the body's port declared set. The runtime port-
        // synthetic Source built at body entry adopts every parent
        // column, so the body sees these at runtime; declaring them
        // here keeps the compile-time Row aligned with that runtime
        // shape — without this step the open-tail mechanism would
        // silently drop engine-stamped columns from the body's
        // compile-time view and surface as a schema mismatch when
        // records flow back to the parent (the composition's
        // `output_schema` is derived from the body's terminal Row).
        let mut declared_columns = declared_columns;
        for (qf, ty) in upstream_row.fields() {
            if qf.name.starts_with("$ck.") && !declared_columns.contains_key(qf) {
                declared_columns.insert(qf.clone(), ty.clone());
            }
        }
        // Build Row::open(declared, fresh_tail) for the port.
        let tail_var = cxl::typecheck::row::TailVarId(*next_tail_var);
        *next_tail_var += 1;
        let cxl_span = cxl::lexer::Span::new(span.start as usize, span.start as usize);
        let row = Row::open(declared_columns, cxl_span, tail_var);
        rows.insert(port_name.clone(), row);
    }

    if has_errors { None } else { Some(rows) }
}

/// Extract declared columns from a `PortDecl` as bare
/// `IndexMap<QualifiedField, Type>`. Composition ports never carry
/// qualifiers — qualification is a Combine-only concept.
fn port_columns(decl: &PortDecl) -> IndexMap<QualifiedField, Type> {
    match &decl.schema {
        Some(schema) => schema
            .columns
            .iter()
            .map(|c| (QualifiedField::bare(c.name.as_str()), c.ty.clone()))
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
    // Composition ports are never qualified, so `.name.as_ref()` is the
    // full identifier.
    let mut input_declared: HashSet<String> = HashSet::new();
    for row in input_port_rows.values() {
        for qf in row.field_names() {
            input_declared.insert(qf.name.to_string());
        }
    }

    // For each output row, check if any body-added column has the same name
    // as a column that would pass through from the input tail.
    for (port_name, output_row) in output_port_rows {
        for col_qf in output_row.field_names() {
            let col_name = col_qf.name.as_ref();
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

/// Build a runtime `Arc<Schema>` from an iterator of column names,
/// stamping engine-stamp metadata on every column whose name begins
/// with the `$ck.` prefix. Two prefix shapes are recognized:
///
/// - `$ck.aggregate.<aggregate_name>` — synthetic group-index column
///   emitted by a relaxed aggregate. Stamped
///   [`FieldMetadata::AggregateGroupIndex`].
/// - `$ck.<source_field>` — source-CK shadow column. Stamped
///   [`FieldMetadata::SourceCorrelation`].
///
/// The aggregate prefix is checked first because `$ck.aggregate.x`
/// also matches the generic `$ck.` prefix; misordering would mis-
/// classify aggregate columns as source-CK shadows.
///
/// Mirrors the deduction in `lower_node_to_plan_node`'s
/// `schema_from_bound` closure so a composition body's port-synthetic
/// Source carries the same marker.
pub(crate) fn schema_from_field_names<'a, I>(names: I) -> Arc<clinker_record::Schema>
where
    I: IntoIterator<Item = &'a str>,
{
    use clinker_record::{FieldMetadata, SchemaBuilder};
    let mut builder = SchemaBuilder::new();
    for name in names {
        builder = if let Some(aggregate_name) = name.strip_prefix("$ck.aggregate.") {
            builder.with_field_meta(name, FieldMetadata::aggregate_group_index(aggregate_name))
        } else if let Some(field) = name.strip_prefix("$ck.") {
            builder.with_field_meta(name, FieldMetadata::source_correlation(field))
        } else {
            builder.with_field(name)
        };
    }
    builder.build()
}

/// Build the `Row.declared` map for a Source from its author-declared
/// `schema:` block, tail-appending one `$ck.<field>` shadow column per
/// field listed in the source's own `correlation_key:`.
///
/// Each shadow column is typed identically to the user-declared field
/// of the same name. Tail-append preserves user-declared positional
/// indices, mirroring Spark `_metadata`.
///
/// Returns the column map paired with a list of `correlation_key`
/// field names that did NOT appear in `decl.columns`. The caller emits
/// E153 for each such name — every CK field a source declares MUST be
/// in that source's own schema.
fn columns_from_decl(
    decl: &SchemaDecl,
    correlation_key: Option<&crate::config::CorrelationKey>,
) -> (IndexMap<QualifiedField, Type>, Vec<String>) {
    let mut cols: IndexMap<QualifiedField, Type> = decl
        .columns
        .iter()
        .map(|c| (QualifiedField::bare(c.name.as_str()), c.ty.clone()))
        .collect();
    let mut missing: Vec<String> = Vec::new();
    if let Some(ck) = correlation_key {
        for field in ck.fields() {
            match decl
                .columns
                .iter()
                .find(|c| c.name.as_str() == field)
                .map(|c| c.ty.clone())
            {
                Some(ty) => {
                    let shadow_name = format!("$ck.{field}");
                    cols.insert(QualifiedField::bare(shadow_name), ty);
                }
                None => missing.push(field.to_string()),
            }
        }
    }
    (cols, missing)
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
    // Collect unqualified field names for CXL resolve. In the non-combine
    // case all fields are bare; in the combine case (C.1.1+) qualified
    // fields collapse to their `.name` — the resolver only needs to know
    // which identifiers are "field-like," and combine bodies address
    // qualified fields via `Expr::QualifiedFieldRef` (handled in
    // typecheck/pass.rs, not resolve).
    let field_refs: Vec<&str> = schema.field_names().map(|qf| qf.name.as_ref()).collect();
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

/// Propagate an upstream row through a Transform node: start with all
/// upstream fields, then append (or overwrite) one entry per `emit`
/// statement. Output field names come from the `emit name = expr` LHS
/// and are always bare (`QualifiedField::bare`). Preserves the upstream
/// tail (Closed / Open) so pass-through semantics propagate across
/// Transform boundaries.
fn propagate_row(upstream: &Row, typed: &TypedProgram) -> Row {
    let mut out = upstream.declared_map().clone();
    for stmt in &typed.program.statements {
        if let cxl::ast::Statement::Emit {
            name,
            expr,
            is_meta,
            ..
        } = stmt
        {
            // Meta emits write to per-record metadata (`$meta.*`), not
            // to the output row — skip them so the row/schema view
            // downstream operators see only reflects user-visible data
            // fields.
            if *is_meta {
                continue;
            }
            let emit_type = typed
                .types
                .get(expr.node_id().0 as usize)
                .and_then(|t| t.clone())
                .unwrap_or(Type::Any);
            out.insert(QualifiedField::bare(name.as_ref()), emit_type);
        }
    }
    Row::from_parts(out, upstream.declared_span, upstream.tail.clone())
}

/// Propagate an upstream row through an Aggregate node: start with the
/// `group_by` fields (typed against upstream), then append one entry
/// per `emit` statement. All output fields are bare.
///
/// When the aggregate's `group_by` omits any source-CK field visible
/// upstream (relaxed mode), the row gains one synthetic
/// `$ck.aggregate.<aggregate_name>` column carrying the aggregator's
/// in-memory group index. The schema-widening pass at the planner
/// stage stamps the same column on the lowered `output_schema`; this
/// addition keeps bind_schema's typed output row in sync so
/// downstream Transform / Combine bind passes see the synthetic
/// column and propagate it forward.
fn propagate_aggregate(
    aggregate_name: &str,
    group_by: &[String],
    upstream: &Row,
    typed: &TypedProgram,
) -> Row {
    let mut out: IndexMap<QualifiedField, Type> = IndexMap::new();
    for gb in group_by {
        let t = match upstream.lookup(gb) {
            cxl::typecheck::ColumnLookup::Declared(ty) => ty.clone(),
            _ => Type::Any,
        };
        out.insert(QualifiedField::bare(gb.as_str()), t);
    }
    for stmt in &typed.program.statements {
        if let cxl::ast::Statement::Emit {
            name,
            expr,
            is_meta,
            ..
        } = stmt
        {
            if *is_meta {
                continue;
            }
            let emit_type = typed
                .types
                .get(expr.node_id().0 as usize)
                .and_then(|t| t.clone())
                .unwrap_or(Type::Any);
            out.insert(QualifiedField::bare(name.as_ref()), emit_type);
        }
    }
    // Detect relaxed mode by walking the upstream row for `$ck.<field>`
    // shadow columns whose source field is missing from `group_by`.
    // Mirror the lattice rule the planner applies post-bind in
    // `compute_one`'s Aggregate arm — both must agree on which
    // aggregates are relaxed so the typed output row, the lowered
    // `output_schema`, and the runtime stamp stay coherent.
    let mut omits_any_source_ck = false;
    for (qf, _) in upstream.fields() {
        let col = qf.name.as_ref();
        if let Some(field) = col.strip_prefix("$ck.")
            && !field.starts_with("aggregate.")
            && !group_by.iter().any(|gb| gb == field)
        {
            omits_any_source_ck = true;
            break;
        }
    }
    if omits_any_source_ck {
        out.insert(
            QualifiedField::bare(format!("$ck.aggregate.{aggregate_name}").as_str()),
            Type::Int,
        );
    }
    Row::from_parts(out, upstream.declared_span, upstream.tail.clone())
}

// ─── Combine arm (Phase Combine C.1.1 + C.1.2 + C.1.3) ──────────────

/// Bind a `PipelineNode::Combine` node in one bottom-up traversal.
///
/// Architecture: a single pass does merged-schema construction (C.1.1),
/// where-clause typecheck + predicate decomposition, body typecheck +
/// output-row publication. Splitting these into three passes with
/// intermediate side-table handoffs is the pattern Spark SPIP-49834 is
/// actively migrating away from.
///
/// Per-input metadata is keyed by node-visible upstream NAME (not a
/// graph-layer `NodeIndex`) because `bind_schema` runs before
/// `ExecutionPlanDag` is built. See `CombineInput` rustdoc for the full
/// rationale.
///
/// Diagnostics emitted:
/// - E300 — fewer than 2 inputs
/// - E301 — reserved-namespace or dotted qualifier
/// - E303 — where-clause does not evaluate to Bool
/// - E304 — where-clause references unknown or 3+ part qualified field
/// - E305 — no cross-input equality/range extractable from predicate
/// - E308 — cxl body references unknown or 3+ part qualified field
/// - E309 — cxl body has no emit statements
///
/// E307 (undeclared upstream reference) is NOT emitted here — it fires
/// from `ExecutionPlanDag::compile` during DAG wiring.
#[allow(clippy::too_many_arguments)]
fn bind_combine(
    name: &str,
    header: &CombineHeader,
    config: &CombineBody,
    span: Span,
    diags: &mut Vec<Diagnostic>,
    artifacts: &mut CompileArtifacts,
    schema_by_name: &mut HashMap<String, Row>,
) {
    // E300: combine requires at least 2 inputs.
    if header.input.len() < 2 {
        diags.push(combine_e300(name, header.input.len(), span));
        return;
    }

    // Build merged row + collect per-input metadata. Any E301 violation
    // short-circuits this combine's downstream processing — cascading
    // E303/E304/E305/E308 errors from a partly-broken merged row are
    // pure noise. Callers who want to see multiple E301 errors can fix
    // them and re-run.
    let mut merged_declared: IndexMap<QualifiedField, Type> = IndexMap::new();
    let mut combine_inputs_entries: IndexMap<String, CombineInput> = IndexMap::new();
    let mut e301_fired = false;

    for (qualifier, node_input_spanned) in &header.input {
        if qualifier.starts_with('$') {
            diags.push(combine_e301_reserved(name, qualifier, span));
            e301_fired = true;
            continue;
        }
        if qualifier.contains('.') {
            diags.push(combine_e301_dotted(name, qualifier, span));
            e301_fired = true;
            continue;
        }
        let upstream_target = input_target(&node_input_spanned.value);
        // E307 (undeclared upstream) fires at DAG wiring time. In the
        // standalone bind_schema path missing upstreams are silently
        // skipped — DAG-level reporting owns that diagnostic.
        let upstream_row = match schema_by_name.get(upstream_target) {
            Some(r) => r,
            None => continue,
        };
        for (field, ty) in upstream_row.fields() {
            let qf = QualifiedField::qualified(qualifier.as_str(), field.name.clone());
            // Collision is structurally impossible: outer qualifier
            // uniqueness (IndexMap in CombineHeader.input) + inner field
            // uniqueness (IndexMap per upstream Row) → combined
            // `(qualifier, name)` key is always fresh. E302 was retired
            // in the C.1.0 corrective (Decisions Log #15).
            merged_declared.insert(qf, ty.clone());
        }
        combine_inputs_entries.insert(
            qualifier.clone(),
            CombineInput {
                upstream_name: Arc::from(upstream_target),
                row: upstream_row.clone(),
                estimated_cardinality: None,
            },
        );
    }

    if e301_fired {
        return;
    }

    // Select the driving (probe) input — explicit `drive:` hint, then
    // cardinality, then first-in-IndexMap. E306 fires for an unknown
    // hint; we still proceed with the rest of bind_combine so the user
    // sees any other diagnostics in one pass, but `combine_driving`
    // stays empty for this combine and the post-pass skips it.
    if let Some(driver) = select_driving_input(
        &combine_inputs_entries,
        config.drive.as_deref(),
        name,
        span,
        diags,
    ) {
        artifacts.combine_driving.insert(name.to_string(), driver);
    }

    artifacts
        .combine_inputs
        .insert(name.to_string(), combine_inputs_entries);
    artifacts
        .combine_strategy_hints
        .insert(name.to_string(), config.strategy);

    let cxl_span = cxl::lexer::Span::new(span.start as usize, span.start as usize);
    let merged_row = Row::closed(merged_declared, cxl_span);

    // ── Where-clause typecheck ─────────────────────────────────────
    let typed_where =
        match typecheck_combine_where(name, config.where_expr.as_ref(), &merged_row, span) {
            Ok(t) => Arc::new(t),
            Err(mut new_diags) => {
                diags.append(&mut new_diags);
                return;
            }
        };

    // Post-walk for E304 (unknown / 3-part qualified refs in where).
    let e304_before = diags.iter().filter(|d| d.code == "E304").count();
    if let Some(Statement::Filter { predicate, .. }) = typed_where.program.statements.first() {
        walk_for_unknown_refs(
            predicate,
            &merged_row,
            name,
            "E304",
            "where-clause",
            span,
            diags,
        );
    }
    let e304_fired = diags.iter().filter(|d| d.code == "E304").count() > e304_before;

    // Decompose predicate → DecomposedPredicate. The shared typed_where
    // Arc is threaded through so each EqualityConjunct captures the
    // typed program needed by `cxl::eval::eval_expr` at runtime — no
    // re-typecheck per side, since equality sub-`Expr`s preserve their
    // NodeIds and the where-program's regex cache covers them.
    let decomposed = match decompose_predicate(&typed_where, &merged_row, cxl_span) {
        Ok(d) => d,
        Err(type_diags) => {
            for td in type_diags {
                if !td.is_warning {
                    diags.push(Diagnostic::error(
                        "E200",
                        format!("combine {name:?} residual predicate: {}", td.message),
                        LabeledSpan::primary(span, String::new()),
                    ));
                }
            }
            return;
        }
    };

    // E305: no extractable cross-input comparisons. Fires independently
    // of E304 — a combine with an unknown field AND no cross-input
    // extraction gets both diagnostics, which is correct.
    if decomposed.equalities.is_empty() && decomposed.ranges.is_empty() {
        diags.push(combine_e305(name, span));
    }

    artifacts
        .combine_predicates
        .insert(name.to_string(), decomposed);

    // If the where-clause was structurally broken (E304), short-circuit
    // to avoid cascading body errors. The body may be fine, but without
    // a sound predicate the output isn't trustworthy.
    if e304_fired {
        return;
    }

    // ── Collect-mode short-circuit (R1) ────────────────────────────
    // Per Phase Combine R1, `match: collect` REJECTS any non-empty
    // `cxl:` body at compile time (E311). The output row is auto-
    // derived from `(driver fields ++ build_qualifier as Array)` so
    // downstream nodes can bind against it without an emit. The
    // executor synthesizes the output record at probe time; no body
    // evaluator runs for Collect.
    //
    // Auto-derivation is only meaningful for N=2. The N-ary
    // decomposition pass rewrites N>2 combines into a chain of binary
    // steps before strategy selection, so a Collect-mode N>2 fixture
    // would lose its single build_qualifier — skip the auto-derivation
    // here and let downstream passes surface diagnostics on the
    // decomposed chain.
    if matches!(config.match_mode, MatchMode::Collect) {
        if !config.cxl.source.trim().is_empty() {
            diags.push(combine_e311_collect_with_body(name, span));
            return;
        }
        let driving = match artifacts.combine_driving.get(name).cloned() {
            Some(d) => d,
            None => return,
        };
        let inputs = artifacts
            .combine_inputs
            .get(name)
            .expect("combine_inputs populated above");
        if inputs.len() != 2 {
            return;
        }
        let driver_input = inputs
            .get(&driving)
            .expect("driving qualifier present in combine_inputs");
        let build_qualifier = inputs
            .keys()
            .find(|k| k.as_str() != driving)
            .expect("N=2 guarantees a non-driver qualifier");

        // Pre-resolve qualified field references that appear in the
        // where-clause — collect mode has no body, but the residual
        // predicate (if any) still drives a `CombineResolver` at
        // probe time.
        let mut resolved_map: HashMap<QualifiedField, (crate::executor::combine::JoinSide, u32)> =
            HashMap::new();
        for stmt in &typed_where.program.statements {
            for sub in statement_exprs(stmt) {
                walk_expr_for_qualified_refs(Some(sub), inputs, Some(&driving), &mut resolved_map);
            }
        }
        artifacts
            .combine_resolved_columns
            .insert(name.to_string(), Arc::new(resolved_map));

        let mut output_decl: IndexMap<QualifiedField, Type> = IndexMap::new();
        for (qf, ty) in driver_input.row.fields() {
            output_decl.insert(qf.clone(), ty.clone());
        }
        output_decl.insert(QualifiedField::bare(build_qualifier.as_str()), Type::Array);
        // Synthetic CK rides through collect-mode combines regardless
        // of `propagate_ck`. See `combine_output_row` rustdoc for why
        // the engine-managed `$ck.aggregate.*` lineage is exempt from
        // the user-facing knob.
        for (qualifier, input) in inputs {
            if qualifier == &driving {
                continue;
            }
            for (qf, ty) in input.row.fields() {
                if qf.name.starts_with("$ck.aggregate.") {
                    let bare = QualifiedField::bare(qf.name.clone());
                    output_decl.entry(bare).or_insert_with(|| ty.clone());
                }
            }
        }
        // Build-side source-CK propagation under collect mode. The
        // matched-array column carries every per-build payload, but
        // the propagation slot itself is single-valued — the first
        // match contributes its $ck value; later matches keep their
        // own $ck inside the array's per-row Map. Mirrors the first-
        // match-wins discipline for hashable scalar slots while
        // letting the array preserve full lineage.
        if !matches!(config.propagate_ck, PropagateCkSpec::Driver) {
            for (qualifier, input) in inputs {
                if qualifier == &driving {
                    continue;
                }
                for (qf, ty) in input.row.fields() {
                    if qf.name.starts_with("$ck.aggregate.") {
                        continue;
                    }
                    let Some(field_name) = qf.name.strip_prefix("$ck.") else {
                        continue;
                    };
                    let allowed = match &config.propagate_ck {
                        PropagateCkSpec::Driver => false,
                        PropagateCkSpec::All => true,
                        PropagateCkSpec::Named(names) => names.contains(field_name),
                    };
                    if !allowed {
                        continue;
                    }
                    let bare = QualifiedField::bare(qf.name.clone());
                    output_decl.entry(bare).or_insert_with(|| ty.clone());
                }
            }
        }
        let output_row = Row::closed(output_decl, cxl_span);
        schema_by_name.insert(name.to_string(), output_row.clone());
        artifacts.typed.insert(
            name.to_string(),
            Arc::new(synthetic_typed_program(output_row)),
        );
        return;
    }

    // ── Body typecheck ─────────────────────────────────────────────
    let body_typed = match typecheck_cxl(
        name,
        &config.cxl.source,
        &merged_row,
        AggregateMode::Row,
        span,
    ) {
        Ok(t) => t,
        Err(d) => {
            diags.push(d);
            return;
        }
    };

    // Post-walk for E308 (unknown / 3-part qualified refs in body).
    for stmt in &body_typed.program.statements {
        walk_statement_exprs(stmt, &merged_row, name, "E308", "cxl body", span, diags);
    }

    // E309: combine must produce at least one non-meta emit. Unlike
    // Transform (which has pass-through semantics), combine's output
    // row is defined entirely by its emits — zero emits means zero
    // output fields.
    let has_emit = body_typed
        .program
        .statements
        .iter()
        .any(|s| matches!(s, Statement::Emit { is_meta: false, .. }));
    if !has_emit {
        diags.push(combine_e309(name, span));
    }

    let driver_row = artifacts
        .combine_driving
        .get(name)
        .and_then(|q| artifacts.combine_inputs.get(name).and_then(|m| m.get(q)))
        .map(|input| input.row.clone());
    let driver_qualifier_for_row = artifacts.combine_driving.get(name).cloned();
    let inputs_for_row = artifacts.combine_inputs.get(name);
    let output_row = combine_output_row(
        &body_typed,
        driver_row.as_ref(),
        inputs_for_row,
        driver_qualifier_for_row.as_deref(),
        &config.propagate_ck,
        cxl_span,
    );
    schema_by_name.insert(name.to_string(), output_row.clone());
    let mut body_typed = body_typed;
    body_typed.output_row = output_row;

    // Resolve every qualified field reference in the body AND in the
    // where-clause into `(JoinSide, column-index)` against each input's
    // declared schema. The executor's
    // `CombineResolverMapping::from_pre_resolved` consumes this map so
    // probe-time field resolution is a direct `Vec<Value>` index read
    // rather than a name-keyed hash lookup. `combine_driving` is
    // `None` only when driver selection failed upstream (E306); in
    // that case every qualifier lands on `JoinSide::Build`, which is
    // harmless because the post-pass skips stamping the combine's
    // runtime fields and the executor never reaches it.
    let driver_qualifier = artifacts.combine_driving.get(name).cloned();
    let inputs_for_resolve = artifacts
        .combine_inputs
        .get(name)
        .expect("combine_inputs populated above");
    let mut resolved_map =
        resolve_combine_body_columns(&body_typed, inputs_for_resolve, driver_qualifier.as_deref());
    for stmt in &typed_where.program.statements {
        for sub in statement_exprs(stmt) {
            walk_expr_for_qualified_refs(
                Some(sub),
                inputs_for_resolve,
                driver_qualifier.as_deref(),
                &mut resolved_map,
            );
        }
    }
    artifacts
        .combine_resolved_columns
        .insert(name.to_string(), Arc::new(resolved_map));

    artifacts
        .typed
        .insert(name.to_string(), Arc::new(body_typed));
}

/// Walk every `Expr::QualifiedFieldRef { parts: [qualifier, name] }` in
/// `typed.program` and emit `(QualifiedField, (JoinSide, column-index))`
/// entries resolved against the combine's per-input declared rows.
///
/// Qualifiers that match `driver_qualifier` land on `JoinSide::Probe`;
/// all others land on `JoinSide::Build`. The column index is the
/// qualified field's position within that input's declared row —
/// exactly what the executor needs to pull the value out of the
/// positional `Vec<Value>` without a name-keyed hash lookup.
fn resolve_combine_body_columns(
    typed: &TypedProgram,
    inputs: &IndexMap<String, crate::plan::combine::CombineInput>,
    driver_qualifier: Option<&str>,
) -> HashMap<QualifiedField, (crate::executor::combine::JoinSide, u32)> {
    use crate::executor::combine::JoinSide;

    let mut out: HashMap<QualifiedField, (JoinSide, u32)> = HashMap::new();
    for stmt in &typed.program.statements {
        for sub in statement_exprs(stmt) {
            walk_expr_for_qualified_refs(Some(sub), inputs, driver_qualifier, &mut out);
        }
    }
    out
}

/// Every top-level `Expr` sub-tree owned by a CXL statement. The outer
/// recursion in `walk_expr_for_qualified_refs` handles the descent;
/// this just surfaces the roots so statements with multiple expression
/// slots (trace guard + message) don't lose the guard.
fn statement_exprs(stmt: &Statement) -> Vec<&Expr> {
    match stmt {
        Statement::Let { expr, .. } => vec![expr],
        Statement::Emit { expr, .. } => vec![expr],
        Statement::Filter { predicate, .. } => vec![predicate],
        Statement::ExprStmt { expr, .. } => vec![expr],
        Statement::Trace { guard, message, .. } => {
            let mut v = vec![message];
            if let Some(g) = guard.as_deref() {
                v.push(g);
            }
            v
        }
        Statement::Distinct { .. } | Statement::UseStmt { .. } => Vec::new(),
    }
}

/// Recursive descent over `Expr` emitting `(QualifiedField,
/// (JoinSide, idx))` entries for every 2-part qualified reference.
///
/// 3-part references (namespaces like `$pipeline.x.y`) are skipped —
/// they're accepted by the typechecker only for system namespaces
/// that the executor resolves elsewhere.
fn walk_expr_for_qualified_refs(
    expr: Option<&Expr>,
    inputs: &IndexMap<String, crate::plan::combine::CombineInput>,
    driver_qualifier: Option<&str>,
    out: &mut HashMap<QualifiedField, (crate::executor::combine::JoinSide, u32)>,
) {
    use crate::executor::combine::JoinSide;

    let Some(expr) = expr else {
        return;
    };
    match expr {
        Expr::QualifiedFieldRef { parts, .. } => {
            if parts.len() == 2 {
                let qualifier = parts[0].as_ref();
                let field_name = parts[1].as_ref();
                if let Some(input) = inputs.get(qualifier)
                    && let Some(idx) = input
                        .row
                        .fields()
                        .position(|(qf, _)| qf.name.as_ref() == field_name)
                {
                    let side = match driver_qualifier {
                        Some(d) if d == qualifier => JoinSide::Probe,
                        _ => JoinSide::Build,
                    };
                    let qf = QualifiedField::qualified(qualifier, Arc::from(field_name));
                    out.insert(qf, (side, idx as u32));
                }
            }
        }
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            walk_expr_for_qualified_refs(Some(lhs), inputs, driver_qualifier, out);
            walk_expr_for_qualified_refs(Some(rhs), inputs, driver_qualifier, out);
        }
        Expr::Unary { operand, .. } => {
            walk_expr_for_qualified_refs(Some(operand), inputs, driver_qualifier, out);
        }
        Expr::MethodCall { receiver, args, .. } => {
            walk_expr_for_qualified_refs(Some(receiver), inputs, driver_qualifier, out);
            for a in args {
                walk_expr_for_qualified_refs(Some(a), inputs, driver_qualifier, out);
            }
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            walk_expr_for_qualified_refs(Some(condition), inputs, driver_qualifier, out);
            walk_expr_for_qualified_refs(Some(then_branch), inputs, driver_qualifier, out);
            if let Some(e) = else_branch.as_ref() {
                walk_expr_for_qualified_refs(Some(e), inputs, driver_qualifier, out);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject.as_deref() {
                walk_expr_for_qualified_refs(Some(s), inputs, driver_qualifier, out);
            }
            for arm in arms {
                walk_expr_for_qualified_refs(Some(&arm.pattern), inputs, driver_qualifier, out);
                walk_expr_for_qualified_refs(Some(&arm.body), inputs, driver_qualifier, out);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                walk_expr_for_qualified_refs(Some(a), inputs, driver_qualifier, out);
            }
        }
        _ => {}
    }
}

/// Typecheck a combine's `where:` source against the merged row,
/// returning a `TypedProgram` whose first statement is the Filter
/// carrying the predicate `Expr`.
///
/// Wraps the source as `"filter {where_src}"` so the CXL parser
/// produces a `Statement::Filter`. Routes diagnostics by category:
/// - Parse / name-resolution failures → E200 (general compile error)
/// - Typecheck "filter predicate must be type Bool" → E303
/// - Other typecheck errors → E200
fn typecheck_combine_where(
    combine_name: &str,
    where_src: &str,
    merged_row: &Row,
    span: Span,
) -> Result<TypedProgram, Vec<Diagnostic>> {
    let wrapped = format!("filter {where_src}");
    let parse_result = cxl::parser::Parser::parse(&wrapped);
    if !parse_result.errors.is_empty() {
        let msg = parse_result
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(vec![Diagnostic::error(
            "E200",
            format!("combine {combine_name:?} where-clause parse error: {msg}"),
            LabeledSpan::primary(span, String::new()),
        )]);
    }

    let field_refs: Vec<&str> = merged_row
        .field_names()
        .map(|qf| qf.name.as_ref())
        .collect();
    let resolved =
        match cxl::resolve::resolve_program(parse_result.ast, &field_refs, parse_result.node_count)
        {
            Ok(r) => r,
            Err(rd) => {
                let msg = rd
                    .into_iter()
                    .map(|d| d.message)
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(vec![Diagnostic::error(
                    "E200",
                    format!("combine {combine_name:?} where-clause name resolution: {msg}"),
                    LabeledSpan::primary(span, String::new()),
                )]);
            }
        };

    match cxl::typecheck::type_check(resolved, merged_row) {
        Ok(typed) => Ok(typed),
        Err(type_diags) => {
            let mut out = Vec::new();
            for td in type_diags.into_iter().filter(|d| !d.is_warning) {
                // Typechecker's Filter-statement check emits a specific
                // error message when the predicate's type is neither
                // Bool nor Any — that is exactly E303 territory.
                if td.message.starts_with("filter predicate must be type Bool") {
                    out.push(
                        Diagnostic::error(
                            "E303",
                            format!(
                                "combine {combine_name:?} where-clause is not boolean: {}",
                                td.message
                            ),
                            LabeledSpan::primary(span, String::new()),
                        )
                        .with_help(
                            "the `where:` predicate must return Bool — use a comparison \
                             (e.g. `a.id == b.id`)",
                        ),
                    );
                } else {
                    out.push(Diagnostic::error(
                        "E200",
                        format!(
                            "combine {combine_name:?} where-clause type error: {}",
                            td.message
                        ),
                        LabeledSpan::primary(span, String::new()),
                    ));
                }
            }
            Err(out)
        }
    }
}

/// Emit E304/E308 for `QualifiedFieldRef`s that don't match the merged
/// row, or that have 3+ parts (unsupported in combine context).
///
/// Bare unqualified `FieldRef`s are handled by the typechecker itself
/// (which emits a loud `Ambiguous` error for multi-match bare refs in
/// a qualified merged row). This walker only visits the qualified-ref
/// forms.
#[allow(clippy::too_many_arguments)]
fn walk_for_unknown_refs(
    expr: &Expr,
    merged_row: &Row,
    combine_name: &str,
    err_code: &str,
    context: &str,
    combine_span: Span,
    diags: &mut Vec<Diagnostic>,
) {
    match expr {
        Expr::QualifiedFieldRef { parts, .. } => {
            if parts.len() > 2 {
                let joined: Vec<&str> = parts.iter().map(|s| s.as_ref()).collect();
                diags.push(
                    Diagnostic::error(
                        err_code.to_string(),
                        format!(
                            "combine {combine_name:?} {context} references {:?} ({} parts); \
                             combine supports only 2-part qualified references \
                             (`qualifier.field`)",
                            joined.join("."),
                            parts.len()
                        ),
                        LabeledSpan::primary(combine_span, String::new()),
                    )
                    .with_help(
                        "drop the extra path segment, or move multi-record logic into a \
                         Transform before the combine",
                    ),
                );
            } else if parts.len() == 2
                && matches!(
                    merged_row.lookup_qualified(&parts[0], &parts[1]),
                    ColumnLookup::Unknown
                )
            {
                diags.push(
                    Diagnostic::error(
                        err_code.to_string(),
                        format!(
                            "combine {combine_name:?} {context} references unknown field \
                             `{}.{}`",
                            parts[0], parts[1]
                        ),
                        LabeledSpan::primary(combine_span, String::new()),
                    )
                    .with_help(
                        "declare the field in the upstream source's `schema:` block, or \
                         fix the qualifier name",
                    ),
                );
            }
        }
        Expr::Binary { lhs, rhs, .. } => {
            walk_for_unknown_refs(
                lhs,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
            walk_for_unknown_refs(
                rhs,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
        }
        Expr::Unary { operand, .. } => walk_for_unknown_refs(
            operand,
            merged_row,
            combine_name,
            err_code,
            context,
            combine_span,
            diags,
        ),
        Expr::Coalesce { lhs, rhs, .. } => {
            walk_for_unknown_refs(
                lhs,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
            walk_for_unknown_refs(
                rhs,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            walk_for_unknown_refs(
                condition,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
            walk_for_unknown_refs(
                then_branch,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
            if let Some(eb) = else_branch {
                walk_for_unknown_refs(
                    eb,
                    merged_row,
                    combine_name,
                    err_code,
                    context,
                    combine_span,
                    diags,
                );
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                walk_for_unknown_refs(
                    s,
                    merged_row,
                    combine_name,
                    err_code,
                    context,
                    combine_span,
                    diags,
                );
            }
            for arm in arms {
                walk_for_unknown_refs(
                    &arm.pattern,
                    merged_row,
                    combine_name,
                    err_code,
                    context,
                    combine_span,
                    diags,
                );
                walk_for_unknown_refs(
                    &arm.body,
                    merged_row,
                    combine_name,
                    err_code,
                    context,
                    combine_span,
                    diags,
                );
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            walk_for_unknown_refs(
                receiver,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
            for a in args {
                walk_for_unknown_refs(
                    a,
                    merged_row,
                    combine_name,
                    err_code,
                    context,
                    combine_span,
                    diags,
                );
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                walk_for_unknown_refs(
                    a,
                    merged_row,
                    combine_name,
                    err_code,
                    context,
                    combine_span,
                    diags,
                );
            }
        }
        Expr::FieldRef { .. }
        | Expr::Literal { .. }
        | Expr::PipelineAccess { .. }
        | Expr::MetaAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }
}

/// Walk every `Expr` inside a `Statement` and apply the unknown-ref
/// check. Used for the body post-walk where statements may be Emit,
/// Let, ExprStmt, Filter, or Trace.
#[allow(clippy::too_many_arguments)]
fn walk_statement_exprs(
    stmt: &Statement,
    merged_row: &Row,
    combine_name: &str,
    err_code: &str,
    context: &str,
    combine_span: Span,
    diags: &mut Vec<Diagnostic>,
) {
    match stmt {
        Statement::Emit { expr, .. }
        | Statement::Let { expr, .. }
        | Statement::ExprStmt { expr, .. } => {
            walk_for_unknown_refs(
                expr,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
        }
        Statement::Filter { predicate, .. } => {
            walk_for_unknown_refs(
                predicate,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
        }
        Statement::Trace { guard, message, .. } => {
            if let Some(g) = guard {
                walk_for_unknown_refs(
                    g,
                    merged_row,
                    combine_name,
                    err_code,
                    context,
                    combine_span,
                    diags,
                );
            }
            walk_for_unknown_refs(
                message,
                merged_row,
                combine_name,
                err_code,
                context,
                combine_span,
                diags,
            );
        }
        Statement::UseStmt { .. } | Statement::Distinct { .. } => {}
    }
}

/// Build a combine's output row from its cxl body's emit statements.
///
/// Unlike `propagate_row` (Transform), combine's output is a FRESH
/// closed row — no pass-through of the merged row's qualified fields.
/// Emit LHS names are always unqualified (`QualifiedField::bare`).
/// Meta emits (`emit $meta.x = ...`) don't contribute to the output
/// row schema; they write to per-record metadata, not the record
/// itself.
///
/// The driver's `$ck.<field>` shadow columns always land on the output
/// row so the combined record carries the driver's frozen-identity
/// snapshot through the join boundary. `propagate_ck` selects whether
/// build-side source-CK (`$ck.<field>`) columns also land:
///
/// - `Driver` — only the driver's set propagates.
/// - `All` — every non-driver input's `$ck.*` columns also land.
/// - `Named(set)` — only the listed CK fields propagate (intersected
///   with what's actually present on the upstream rows).
///
/// Driver wins on a name collision: if both the driver and a build
/// input declare `$ck.<field>`, the column appears once on the output
/// schema and the runtime CK-copy step preserves the driver's value
/// (the build record's value is not written when a non-null driver
/// value is already in place).
///
/// Synthetic CK (`$ck.aggregate.<aggregate_name>`) is engine-managed
/// lineage from a relaxed aggregate to its source rows; the user did
/// not declare it, so `propagate_ck` does not gate it. Build-side
/// synthetic CK rides through every combine regardless of the spec.
/// Without this exemption, the detect-phase fan-out from a downstream
/// failure to the aggregator's per-group source-row table would lose
/// its bridge whenever a Combine sat between the relaxed aggregate
/// and the failing node.
fn combine_output_row(
    typed: &TypedProgram,
    driver_row: Option<&Row>,
    inputs: Option<&IndexMap<String, CombineInput>>,
    driver_qualifier: Option<&str>,
    propagate_ck: &PropagateCkSpec,
    span: cxl::lexer::Span,
) -> Row {
    let mut out: IndexMap<QualifiedField, Type> = IndexMap::new();
    for stmt in &typed.program.statements {
        if let Statement::Emit {
            name,
            expr,
            is_meta,
            ..
        } = stmt
        {
            if *is_meta {
                continue;
            }
            let emit_type = typed
                .types
                .get(expr.node_id().0 as usize)
                .and_then(|t| t.clone())
                .unwrap_or(Type::Any);
            out.insert(QualifiedField::bare(name.as_ref()), emit_type);
        }
    }
    // Driver's engine-stamped tail always rides through. `widen_record_to_schema`
    // picks up the values from the driver record by name at runtime.
    if let Some(driver) = driver_row {
        for (qf, ty) in driver.fields() {
            if qf.name.starts_with("$ck.") {
                let bare = QualifiedField::bare(qf.name.clone());
                out.entry(bare).or_insert_with(|| ty.clone());
            }
        }
    }
    // Synthetic CK from the build side rides through every combine
    // unconditionally. `$ck.aggregate.<name>` carries the engine
    // lineage from a relaxed aggregate to its source rows; the user
    // did not declare it, so `propagate_ck` (a user-facing knob over
    // source CK) has no semantic meaning over it. Dropping it would
    // sever the detect-phase fan-out bridge for any
    // `Aggregate (relaxed) → Combine → …` shape.
    if let Some(inputs) = inputs {
        for (qualifier, input) in inputs {
            if Some(qualifier.as_str()) == driver_qualifier {
                continue;
            }
            for (qf, ty) in input.row.fields() {
                if qf.name.starts_with("$ck.aggregate.") {
                    let bare = QualifiedField::bare(qf.name.clone());
                    out.entry(bare).or_insert_with(|| ty.clone());
                }
            }
        }
    }
    // Build-side source-CK propagation. Without these slots, the
    // runtime CK copy has nowhere to write; downstream collateral DLQ
    // traversal would lose the build inputs' contribution to the
    // joined row's identity.
    if let Some(inputs) = inputs
        && !matches!(propagate_ck, PropagateCkSpec::Driver)
    {
        for (qualifier, input) in inputs {
            if Some(qualifier.as_str()) == driver_qualifier {
                continue;
            }
            for (qf, ty) in input.row.fields() {
                // Synthetic CK was already covered by the unconditional
                // loop above; skip to avoid double-insert.
                if qf.name.starts_with("$ck.aggregate.") {
                    continue;
                }
                let Some(field_name) = qf.name.strip_prefix("$ck.") else {
                    continue;
                };
                let allowed = match propagate_ck {
                    PropagateCkSpec::Driver => false,
                    PropagateCkSpec::All => true,
                    PropagateCkSpec::Named(names) => names.contains(field_name),
                };
                if !allowed {
                    continue;
                }
                let bare = QualifiedField::bare(qf.name.clone());
                out.entry(bare).or_insert_with(|| ty.clone());
            }
        }
    }
    Row::closed(out, span)
}

// ─── Combine diagnostics (Phase Combine C.1.1 + C.1.2 + C.1.3) ──────

/// E300 — combine requires at least 2 declared inputs.
fn combine_e300(combine_name: &str, input_count: usize, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E300",
        format!(
            "combine {combine_name:?} declares {input_count} input(s); combine requires at least 2"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "add one or more upstream inputs under `input:`, e.g.\n  input:\n    orders: orders_source\n    products: products_source",
    )
}

/// E301 — combine input qualifier starts with `$`, colliding with CXL
/// system namespaces (`$pipeline`, `$window`, `$meta`).
fn combine_e301_reserved(combine_name: &str, qualifier: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E301",
        format!(
            "combine {combine_name:?} input qualifier {qualifier:?} starts with `$`, which is \
             reserved for CXL system namespaces (`$pipeline`, `$window`, `$meta`)"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help("rename the qualifier to a plain identifier (e.g. `orders`, `products`)")
}

/// E301 — combine input qualifier contains `.`, which would alias a
/// genuine 3-part CXL reference (RESOLUTION EC-4).
fn combine_e301_dotted(combine_name: &str, qualifier: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E301",
        format!(
            "combine {combine_name:?} input qualifier {qualifier:?} contains `.`; dotted \
             qualifiers would be indistinguishable from 3-part field references in the \
             `where:` and `cxl:` bodies"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help("rename the qualifier to avoid the `.` character")
}

/// E305 — combine where-clause has no cross-input comparisons. This
/// fires for same-input-only predicates (`a.x == a.y`), OR-only
/// predicates (which are always residual), literal-only predicates
/// (`true` — V-7-1), and mixed-qualifier predicates where no conjunct
/// is cleanly cross-input.
fn combine_e305(combine_name: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E305",
        format!(
            "combine {combine_name:?} where-clause has no cross-input comparisons; combine \
             requires at least one equality or range between two different inputs"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "add a cross-input comparison (e.g. `a.id == b.id`); cross joins are not supported \
         — if all records should combine, use an explicit Merge node",
    )
}

/// E311 — `match: collect` rejects any non-empty `cxl:` body. The
/// Collect output row is auto-derived as `{ driver fields,
/// <build_qualifier>: Array }`; downstream Transform composes any
/// projection once CXL grows array primitives. Per Phase Combine R1.
fn combine_e311_collect_with_body(combine_name: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E311",
        format!(
            "combine {combine_name:?} has `match: collect` AND a non-empty `cxl:` body; \
             collect mode auto-derives the output row as \
             `{{ driver fields, <build_qualifier>: Array }}` and runs no body"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "remove the `cxl:` body, or change `match:` to `first` / `all` to evaluate per-match \
         emit statements",
    )
}

/// E309 — combine cxl body has no non-meta emit statements. Unlike
/// Transform (which has pass-through semantics on an Open row), combine
/// always produces a fresh closed row defined entirely by its emits.
fn combine_e309(combine_name: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E309",
        format!(
            "combine {combine_name:?} cxl body has no emit statements; combine requires at \
             least one emit to produce an output row"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "add `emit field_name = expr;` statements — combine has no pass-through semantics, \
         so every output field must be emitted explicitly",
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::FieldMetadata;

    /// `$ck.<field>` columns resolve to source-CK metadata; the
    /// suffix is the user-declared field name.
    #[test]
    fn schema_from_field_names_classifies_source_ck() {
        let schema = schema_from_field_names(["id", "$ck.id"]);
        assert!(schema.field_metadata_by_name("id").is_none());
        match schema.field_metadata_by_name("$ck.id") {
            Some(FieldMetadata::SourceCorrelation { source_field }) => {
                assert_eq!(source_field.as_ref(), "id");
            }
            other => panic!("expected SourceCorrelation, got {other:?}"),
        }
    }

    /// `$ck.aggregate.<aggregate_name>` columns resolve to the
    /// aggregate-group-index variant; the suffix is the aggregate node
    /// name. The aggregate prefix wins over the bare `$ck.` prefix.
    #[test]
    fn schema_from_field_names_classifies_aggregate_group_index() {
        let schema = schema_from_field_names(["region", "$ck.aggregate.dept_totals"]);
        match schema.field_metadata_by_name("$ck.aggregate.dept_totals") {
            Some(FieldMetadata::AggregateGroupIndex { aggregate_name }) => {
                assert_eq!(aggregate_name.as_ref(), "dept_totals");
            }
            other => panic!("expected AggregateGroupIndex, got {other:?}"),
        }
    }

    /// Both shadow shapes coexist in one schema without cross-talk:
    /// the `$ck.aggregate.x` column and a sibling `$ck.y` column
    /// classify independently.
    #[test]
    fn schema_from_field_names_distinguishes_both_shapes() {
        let schema =
            schema_from_field_names(["user_id", "$ck.user_id", "$ck.aggregate.daily_totals"]);
        assert!(matches!(
            schema.field_metadata_by_name("$ck.user_id"),
            Some(FieldMetadata::SourceCorrelation { .. }),
        ));
        assert!(matches!(
            schema.field_metadata_by_name("$ck.aggregate.daily_totals"),
            Some(FieldMetadata::AggregateGroupIndex { .. }),
        ));
    }
}
