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

use cxl::typecheck::{AggregateMode, Row, RowTail, Type, TypedProgram};
use indexmap::IndexMap;

use crate::config::compile_context::CompileContext;
use crate::config::composition::{
    CompositionFile, CompositionSignature, CompositionSymbolTable, OutputAlias, PortDecl,
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
    pub typed: HashMap<String, Arc<TypedProgram>>,
    /// Per-node bound row types (LD-16c-23). Populated during the
    /// `bind_schema` walk; persisted on `CompiledPlan` for downstream
    /// phases to consume.
    pub bound_schemas: BoundSchemas,
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

    // Persist all bound rows into BoundSchemas (LD-16c-23).
    for (node_name, row) in schema_by_name {
        artifacts.bound_schemas.set_output(node_name, row);
    }

    artifacts
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
                schema_by_name.insert(name, Row::closed(columns, cxl_span));
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
                        schema_by_name.insert(name.clone(), out);
                        artifacts.typed.insert(name, Arc::new(typed));
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
                        schema_by_name.insert(name.clone(), out);
                        artifacts.typed.insert(name, Arc::new(typed));
                    }
                    Err(d) => diags.push(d),
                }
            }
            PipelineNode::Route { header, config: _ } => {
                if let Some(upstream) = upstream_schema(&header.input, schema_by_name) {
                    let cloned = upstream.clone();
                    if let Ok(empty) = typecheck_cxl(&name, "", &cloned, AggregateMode::Row, span) {
                        artifacts.typed.insert(name.clone(), Arc::new(empty));
                    }
                    schema_by_name.insert(name, cloned);
                }
            }
            PipelineNode::Merge { header, .. } => {
                if let Some(first) = header.inputs.first()
                    && let Some(upstream) = schema_by_name.get(input_target(first))
                {
                    schema_by_name.insert(name, upstream.clone());
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

    // 13. Build and insert BoundBody.
    let bound_body = BoundBody {
        signature_path: resolved_path,
        nodes: vec![], // Body lowering deferred to 16c.2.3 (Stage 5 refactor).
        bound_schemas: body_bound_schemas,
        output_port_rows: output_port_rows.clone(),
        input_port_rows,
        nested_body_ids,
    };
    artifacts.insert_body(body_id, bound_body);
    artifacts
        .composition_body_assignments
        .insert(node_name.to_string(), body_id);

    // 14. Write composition's output row to parent scope.
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
        if let cxl::ast::Statement::Emit { name, .. } = stmt
            && !out.contains_key(name.as_ref())
        {
            out.insert(name.to_string(), Type::Any);
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
        if let cxl::ast::Statement::Emit { name, .. } = stmt
            && !out.contains_key(name.as_ref())
        {
            out.insert(name.to_string(), Type::Any);
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
