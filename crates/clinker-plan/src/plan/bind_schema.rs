//! Compile-time CXL typecheck + schema propagation (`bind_schema`).
//!
//! Walks the unified `nodes:` DAG in topological order, seeds each
//! source's schema from its author-declared `schema:` block, typechecks
//! every Transform/Aggregate/Route body against the upstream Row, and
//! propagates the output Row downstream. Result: a `CompileArtifacts`
//! map keyed by each node's `PlanNodeId` holding one `Arc<TypedProgram>`
//! per CXL-bearing node.
//!
//! For `PipelineNode::Composition` nodes, `bind_composition` recursively
//! binds the composition body at the call-site boundary using
//! row-polymorphic type propagation (Leijen 2005).
//!
//! A CXL body's own compilation surfaces one of three codes by failure
//! class: E202 (parse), E203 (name resolution), E200 (type check). Other
//! errors surface as E201 (missing source schema) and E102–E108 / W101
//! (composition binding).

use clinker_record::owned_storage::SharedStorage;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cxl::ast::{Expr, LiteralValue, Statement};
use cxl::typecheck::{
    AggregateMode, CarriedColumn, ColumnLookup, ColumnMark, PortAccess, PortOrigin, QualifiedField,
    Row, Type, TypedProgram,
};
use indexmap::IndexMap;

use crate::config::compile_context::{CompileContext, ConfigOverrides};
use crate::config::composition::{
    CompositionFile, CompositionSignature, CompositionSymbolTable, LayerKind, OutputAlias,
    ParamType, PortDecl, ProvenanceDb, ResolvedValue, ResourceBinding,
};
use crate::config::node_header::CombineHeader;
use crate::config::pipeline_node::{
    CombineBody, CopyFrom, CullBody, MatchMode, OnUnmapped, PipelineNode, PropagateCkSpec,
    ReshapeBody,
};
use crate::config::sort::{OrderField, OrderingSite};
use crate::config::transform::LogDirective;
use crate::plan::combine::{
    CombineInput, DecomposedPredicate, decompose_predicate, select_driving_input,
};
use crate::plan::composition_body::{BoundBody, CompositionBodyId};
use crate::plan::execution::{CompiledResourceRequirement, CompiledSourceInstance};
use crate::plan::types::JoinSide;
use crate::plan::{EntityRef, PlanNodeId};
use crate::resources::WorkspaceCatalog;
use crate::yaml::Spanned;
use clinker_core_types::diagnostic::Severity;
use clinker_core_types::span::{FileId, Span};
use clinker_core_types::{Diagnostic, LabeledSpan, QuoteName};
use clinker_format::{Column, SourceSchema};

/// Maximum composition nesting depth.
///
/// Compile-time depth violations emit E107 from `bind_composition`.
/// The runtime composition-body executor reuses this same constant as
/// its recursion-depth guard and emits a distinct E112 on overflow,
/// so log-grep on either code finds exactly one emission site.
pub const MAX_COMPOSITION_DEPTH: u32 = 50;

/// Compile artifacts produced by `bind_schema` — one entry per node
/// whose CXL body successfully type-checked, plus per-node row types.
#[derive(Debug, Default, Clone)]
pub struct CompileArtifacts {
    /// Planning-resolved CXL module closure retained by `CompiledPlan`.
    pub cxl_modules: crate::resources::CompiledModuleRegistry,
    /// Per-node typed CXL program (transform/aggregate/combine-body plus
    /// the synthetic pass-through programs for Source/Merge/Output/etc.),
    /// keyed by the node's [`PlanNodeId`]. The id is the node's stable
    /// identity, distinct for every node in the compiled plan — so a node
    /// named the same in two composition scopes (or a top-level node
    /// sharing a name with a composition-body node) gets distinct ids and
    /// cannot collide in this shared table; bare-name keying let the last
    /// writer silently win. Acts as the bind → lowering handoff (lowering
    /// reads each node's program off its id to stamp the node) and as the
    /// compile-time `$doc` path-walk source.
    pub typed: HashMap<PlanNodeId, Arc<TypedProgram>>,
    /// Output-facing fields whose array shape is authorized by an authored
    /// `multiple: true` schema declaration, keyed by stable node identity.
    ///
    /// This is a bind-to-lowering contract, not author configuration. The
    /// schema walk preserves it only through operations that retain the same
    /// value (including direct CXL aliases), so an unrelated runtime array can
    /// never become writable merely because it reaches a CSV/XML Sink.
    pub declared_multiple: HashMap<PlanNodeId, BTreeSet<String>>,
    /// Per-Combine `where:` predicate typed programs, keyed by the
    /// combine node's [`PlanNodeId`]. The node-keyed `typed` map stores
    /// only a Combine's `cxl:` body; the `where:` predicate is decomposed
    /// into `combine_predicates` and otherwise discarded as a standalone
    /// program. This side-table retains it so a `$doc` access used ONLY
    /// in a predicate (e.g. `l.amount > $doc.Head.cutoff`) is still
    /// reachable by the compile-time `$doc` path walk. Compile-time-only:
    /// the runtime executor never reads it. The per-node id keeps a
    /// combine named the same in two composition scopes from colliding.
    pub combine_where_typed: HashMap<PlanNodeId, Arc<TypedProgram>>,
    /// Per-Route branch-condition typed programs, keyed by the Route
    /// node's [`PlanNodeId`] — one program per branch, in declaration
    /// order. A Route's node-keyed `typed` entry is an empty body (the
    /// node carries no `cxl:` projection); the branch conditions live
    /// here instead. This is the bind→lowering handoff: lowering stamps
    /// these onto `PlanNode::Route.branch_programs`, and the runtime
    /// builds its branch evaluators off that on-node copy — it never
    /// reads this side-table. The table also keeps a `$doc` access used
    /// ONLY in a route condition (e.g. `amount > $doc.Head.cutoff`)
    /// reachable by the compile-time `$doc` path walk and attributed to
    /// the Route's source(s). Compile-time-only. The per-node id keeps a
    /// route named the same in two composition scopes from colliding.
    pub route_branch_typed: HashMap<PlanNodeId, Vec<Arc<TypedProgram>>>,
    /// Per-Cull typed OR-combined `drop_group_when` decision program, keyed
    /// by the Cull node's [`PlanNodeId`]. `bind_cull` builds and typechecks
    /// the `emit <decision> = (rule_1) or (rule_2) …` program (after the
    /// per-rule diagnostic typechecks pass) and stores it here. This is the
    /// bind→lowering handoff: lowering runs `extract_aggregates` over it and
    /// stamps the result onto `PlanNode::Cull.compiled` / `.typed`; the
    /// runtime builds its decision evaluator off that on-node copy and never
    /// reads this side-table. The per-node id keeps a cull named the same in
    /// two composition scopes from colliding.
    pub cull_decision_typed: HashMap<PlanNodeId, Arc<TypedProgram>>,
    /// Per-Reshape typechecked rule programs, keyed by the Reshape node's
    /// [`PlanNodeId`] — one [`CompiledReshapeRule`](crate::plan::execution::CompiledReshapeRule)
    /// per `config.rules` entry, in declaration order. `bind_reshape` builds
    /// these from the per-fragment typechecks it already runs (previously
    /// discarded after the `$doc` guard). Bind→lowering handoff: lowering
    /// stamps them onto `PlanNode::Reshape.compiled_rules`; the runtime
    /// rebuilds its per-rule evaluators off that on-node copy and never reads
    /// this side-table. The per-node id keeps a reshape named the same in two
    /// composition scopes from colliding.
    pub reshape_compiled:
        HashMap<PlanNodeId, Arc<Vec<crate::plan::execution::CompiledReshapeRule>>>,
    /// Per-Cull and per-Reshape validated `order_by`, keyed by the node's
    /// [`PlanNodeId`]: each authored entry converted to its placement-only
    /// [`OrderField`](crate::config::OrderField), in declaration order.
    /// `bind_cull` / `bind_reshape` insert it only when the node binds
    /// cleanly, so no entry holds a refused `null_order: drop`.
    /// Bind→lowering handoff: lowering stamps it onto the node's
    /// `order_by`; the runtime never reads this side-table.
    pub group_order_by: HashMap<PlanNodeId, Vec<crate::config::OrderField>>,
    /// Per-Transform validated `analytic_window.sort_by`, keyed by the
    /// Transform's [`PlanNodeId`]: each authored entry converted to its
    /// placement-only [`OrderField`](crate::config::OrderField), in
    /// declaration order. Inserted for every Transform that declares an
    /// `analytic_window` whose `sort_by` holds no refused `null_order: drop`,
    /// top-level and composition-body alike. Bind→lowering handoff: the
    /// window index requests are built from it; the runtime reads only the
    /// resulting index specs.
    pub window_sort_by: HashMap<PlanNodeId, Vec<crate::config::OrderField>>,
    /// Per-Transform typechecked log-directive gate predicates, keyed by the
    /// Transform node's [`PlanNodeId`] — one entry per `config.log` directive,
    /// in declaration order, `None` where the directive declared no
    /// `condition`. Bind→lowering handoff: lowering zips these onto
    /// `PlanTransformPayload.log_conditions`; the runtime reads only that
    /// on-node copy. The entry is inserted only when every condition on the
    /// node typechecked, so a short vec cannot reach lowering.
    pub transform_log_conditions: HashMap<PlanNodeId, Vec<Option<Arc<TypedProgram>>>>,
    /// All bound composition bodies, keyed by `CompositionBodyId`. The
    /// top-level pipeline is NOT in this map — it lives on
    /// `CompiledPlan.dag()` directly. Only body scopes are here.
    pub composition_bodies: IndexMap<CompositionBodyId, BoundBody>,
    /// Monotonic counter for allocating fresh `CompositionBodyId`s.
    next_body_id: u32,
    /// Mapping from a composition call-site node's [`PlanNodeId`] to its
    /// assigned body ID. Populated by `bind_composition`; consumed by
    /// `compile()` Stage 5 to write the body ID back onto the
    /// `PipelineNode::Composition` variant (which was borrowed immutably
    /// during bind_schema). The per-node id keeps a composition node named
    /// the same in two composition scopes from colliding — bare-name keying
    /// let the last writer silently win.
    pub composition_body_assignments: HashMap<PlanNodeId, CompositionBodyId>,
    /// Monotonic counter for allocating fresh `FileId`s for body re-parses.
    next_file_id: u32,
    /// Side-table of provenance-tracked config values. Populated by
    /// `bind_composition` for each composition node's config params.
    pub provenance: ProvenanceDb,
    /// Decomposed `where:` predicates per combine node, keyed by the
    /// combine node's [`PlanNodeId`]. Populated by the
    /// predicate-decomposition pass; consumed by combine strategy
    /// selection.
    pub combine_predicates: HashMap<PlanNodeId, DecomposedPredicate>,
    /// Per-input metadata per combine node, keyed by the combine node's
    /// [`PlanNodeId`]. The inner `IndexMap` preserves declaration order of
    /// the inputs (matches `CombineHeader.input` iteration order).
    /// Populated during schema propagation.
    pub combine_inputs: HashMap<PlanNodeId, IndexMap<String, CombineInput>>,
    /// Planner-wide column-statistics catalog. The single facility every
    /// node consults for row counts, distinct counts, heavy hitters, and
    /// membership. Plane A row counts are seeded from source file metadata
    /// before combine-strategy selection; Plane B sketch results fold back
    /// in after a run. Combine strategy selection reads its row counts
    /// instead of a per-input cardinality field.
    pub statistics: crate::plan::statistics::StatisticsCatalog,
    /// Driving-input qualifier per combine node, keyed by the combine
    /// node's [`PlanNodeId`]; the value is the chosen driving input's
    /// qualifier string. Chosen at `bind_combine` time by
    /// [`select_driving_input`] (explicit `drive:` → cardinality →
    /// first-in-IndexMap default). The `select_combine_strategies`
    /// post-pass reads this to stamp the runtime
    /// `PlanNode::Combine.driving_input` field. Combines that fail driver
    /// selection (E306) are absent from this map and their post-pass
    /// entry is skipped.
    pub combine_driving: HashMap<PlanNodeId, String>,
    /// User-supplied [`CombineStrategyHint`] per combine node, keyed by
    /// the combine node's [`PlanNodeId`]. Populated from
    /// `CombineBody.strategy` during `bind_combine`. The
    /// `select_combine_strategies` post-pass reads this to override the
    /// planner's predicate-shape default — today only `GraceHash` on
    /// pure-equi predicates produces a non-default outcome. Combines
    /// absent from this map default to [`CombineStrategyHint::Auto`].
    pub combine_strategy_hints: HashMap<PlanNodeId, crate::config::CombineStrategyHint>,
    /// Per-combine pre-resolved column map produced by the CXL
    /// typechecker's combine-body walk. Keyed by the combine node's
    /// [`PlanNodeId`]; value is the `ResolvedColumnMap` shape expected by
    /// `CombineResolverMapping::from_pre_resolved`. Combines that fail
    /// body typecheck (or the collect-mode path, which has no body)
    /// surface an empty map here.
    pub combine_resolved_columns: HashMap<PlanNodeId, crate::plan::execution::ResolvedColumnMap>,
    /// Compiled declarative header/footer synthesis per Envelope node, keyed
    /// by the node's [`PlanNodeId`]. Populated during binding (where the body
    /// input `Row` is in scope) for every Envelope that declares a
    /// `config.header:` or `config.footer:` map; consumed at lowering to
    /// attach the spec to `PlanNode::Envelope.synthesis`. An Envelope
    /// declaring neither is absent from this map and lowers with
    /// `synthesis: None`. The per-node id keeps a top-level Envelope and a
    /// composition-body Envelope sharing a name (both declaring header/footer)
    /// from colliding — bare-name keying let the last writer silently win.
    pub envelope_synthesis:
        HashMap<PlanNodeId, Arc<crate::plan::envelope_synthesis::EnvelopeSynthesis>>,
    /// Monotonic counter for fresh tail-variable IDs allocated during
    /// row-polymorphic propagation (composition input-port binding).
    /// Threaded as `&mut u32` into `build_input_port_rows`; IDs are
    /// only comparable within a single `compile()` run.
    pub next_tail_var: u32,
    /// Monotonic counter minting fresh [`PlanNodeId`]s. ONE counter feeds
    /// top-level nodes, every composition body, and every planner-
    /// synthesized node, so two instantiations of one composition body
    /// get disjoint id sets by construction. Only comparable within a
    /// single `compile()` run.
    next_node_id: u32,
    /// Minted `PlanNodeId`s for the top-level pipeline nodes, in
    /// declaration order: index `i` is the id for `nodes[i]` as passed to
    /// [`bind_schema`]. Minted before any body recursion so the top-level
    /// ids occupy the low, stable range. Stage-5 lowering stamps each
    /// lowered top-level `PlanNode.id` from this vector by the node's
    /// original declaration position, which stays correct even when
    /// lowering omits a node `bind_schema` visited (e.g. a
    /// failed-binding composition).
    pub top_level_node_ids: Vec<PlanNodeId>,
    /// Typed output row of every top-level node that bound one, keyed by
    /// node name in declaration order — the top-level counterpart of
    /// [`BoundBody::body_rows`]. A node whose upstream failed to bind has no
    /// entry. Moved onto `CompiledPlan` and read by tooling; the executor
    /// never consults it.
    pub top_level_rows: IndexMap<String, Row>,
    /// How many times a composition body was bound during this compile,
    /// counting the check of a body against its ports' declared types and
    /// the bind that produces the call's rows separately. Read by tests
    /// that pin the cost of nesting; nothing else consults it.
    pub(crate) body_binds: u32,
}

impl CompileArtifacts {
    /// Empty artifacts whose id counters continue this one's, for a bind
    /// whose outcome is read from its diagnostics alone (a composition
    /// body's check against its ports' declared types).
    ///
    /// A body bind reads nothing from the artifacts it binds into: the
    /// module closure comes from the compile context, and a node reads only
    /// the entries the same bind wrote for the nodes before it. So the check
    /// needs none of this one's contents. Its counters continue from here
    /// so an id minted during the check never names an entry this one
    /// already holds.
    pub(crate) fn scratch(&self) -> Self {
        Self {
            next_body_id: self.next_body_id,
            next_file_id: self.next_file_id,
            next_tail_var: self.next_tail_var,
            next_node_id: self.next_node_id,
            ..Self::default()
        }
    }

    /// Look up a node's typed CXL program by its stable [`PlanNodeId`].
    /// The table is per-node and only read at compile time and executor
    /// startup, so this is not hot.
    pub fn typed_get(&self, id: PlanNodeId) -> Option<&Arc<TypedProgram>> {
        self.typed.get(&id)
    }

    /// Whether a typed program is recorded for `id`.
    pub fn typed_contains(&self, id: PlanNodeId) -> bool {
        self.typed.contains_key(&id)
    }

    /// Look up the compiled multi-value declaration contract for a node.
    pub fn declared_multiple_get(&self, id: PlanNodeId) -> Option<&BTreeSet<String>> {
        self.declared_multiple.get(&id)
    }

    /// Record a node's typed CXL program under its [`PlanNodeId`].
    pub fn typed_insert(&mut self, id: PlanNodeId, prog: Arc<TypedProgram>) {
        self.typed.insert(id, prog);
    }

    /// Allocate a fresh, unique `CompositionBodyId`.
    pub fn fresh_body_id(&mut self) -> CompositionBodyId {
        let id = CompositionBodyId(self.next_body_id);
        self.next_body_id += 1;
        id
    }

    /// Mint the next fresh [`PlanNodeId`] from the single per-`compile()`
    /// counter. Every node identity in a compiled plan — top-level, body,
    /// and planner-synthesized — flows through here, so ids never collide
    /// across scopes.
    pub fn fresh_node_id(&mut self) -> PlanNodeId {
        let id = PlanNodeId::new(self.next_node_id as usize);
        self.next_node_id += 1;
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

    /// Correlate each top-level node's declared name with the
    /// [`PlanNodeId`] minted for it, by zipping `nodes` against
    /// `top_level_node_ids` (index `i` of the latter is the id for
    /// `nodes[i]`). Top-level node names are unique (config validation
    /// rejects duplicates), so the map is collision-free. The map covers
    /// only top-level nodes, never body scopes, so it cannot reintroduce
    /// the cross-scope name collision the per-node id keying removed.
    pub fn top_level_name_ids<'a>(
        &self,
        nodes: &'a [Spanned<PipelineNode>],
    ) -> HashMap<&'a str, PlanNodeId> {
        nodes
            .iter()
            .zip(self.top_level_node_ids.iter())
            .map(|(spanned, id)| (spanned.value.name(), *id))
            .collect()
    }

    /// Resolve a single top-level node's [`PlanNodeId`] by its declared
    /// name, scanning the `nodes` ↔ `top_level_node_ids` correlation. For
    /// repeated lookups prefer [`Self::top_level_name_ids`] and reuse the
    /// map; this convenience is for one-off resolutions.
    pub fn top_level_id(&self, nodes: &[Spanned<PipelineNode>], name: &str) -> Option<PlanNodeId> {
        nodes
            .iter()
            .zip(self.top_level_node_ids.iter())
            .find_map(|(spanned, id)| (spanned.value.name() == name).then_some(*id))
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
    /// User-declared `$pipeline.<key>` / `$source.<key>` / `$row.<key>`
    /// registry. Built once from `PipelineConfig.pipeline.vars` at the
    /// `bind_schema` entry and threaded into every CXL resolve / typecheck
    /// call site. Empty for compositions that don't propagate scoped vars
    ///.
    pub scoped_vars: cxl::resolve::ScopedVarsRegistry,
    /// Bounded typed workspace resources admitted before the bind walk.
    pub resource_catalog: &'a WorkspaceCatalog,
    /// Every composition body check against its ports' declared types that
    /// has already passed in this bind, so a later call with the same
    /// inputs does not run it again (see [`ContractCheck`]).
    pub passed_contract_checks: Vec<ContractCheck>,
    /// The body nodes the composition being bound names as its outputs;
    /// empty at the top level.
    pub body_outputs: HashSet<String>,
    /// The input ports the call being bound binds, in the order the
    /// composition declares them; empty at the top level.
    pub body_ports: Vec<String>,
    /// Whether this bind is a trial of an edit a help would print (see
    /// [`crate::plan::fix_offer`]). A trial only has to say whether the edit
    /// compiles, so the helps raised inside it are not checked in turn.
    pub verifying: bool,
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
    scoped_vars: cxl::resolve::ScopedVarsRegistry,
) -> CompileArtifacts {
    let mut artifacts = CompileArtifacts::default();
    artifacts.cxl_modules = ctx.cxl_modules.clone();
    // Mint one id per top-level node in declaration order BEFORE any body
    // recursion, so top-level ids occupy the low, stable id range and
    // `top_level_node_ids[i]` pairs with `nodes[i]`. Lowering stamps the
    // lowered node from this vector by original declaration position.
    artifacts.top_level_node_ids = nodes.iter().map(|_| artifacts.fresh_node_id()).collect();
    // Top-level name → id resolver, built from the just-minted ids zipped
    // with each node's declared name. Top-level node names are unique
    // (config validation rejects duplicates), so this is collision-free.
    // Threaded into the bind walk so every per-node producer keys its
    // side-table entry by the node's own id.
    let top_level_ids = top_level_name_ids(nodes, &artifacts);
    let mut schema_by_name: HashMap<String, Row> = HashMap::new();
    let mut declared_multiple_by_name: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut scoped_vars = scoped_vars;
    scoped_vars.module_exports = ctx.cxl_modules.module_exports();
    scoped_vars.runtime_modules = ctx.cxl_modules.runtime_modules();
    let resource_catalog =
        match crate::config::ClinkerToml::load_from_workspace(ctx.workspace_root())
            .map_err(|error| error.to_string())
            .and_then(|workspace| {
                WorkspaceCatalog::load(ctx.workspace_root(), &workspace.catalog)
                    .map_err(|error| error.to_string())
            }) {
            Ok(catalog) => catalog,
            Err(error) => {
                diags.push(
                    Diagnostic::error(
                        "E103",
                        format!("workspace resource catalog is invalid: {error}"),
                        LabeledSpan::primary(Span::SYNTHETIC, "invalid resource catalog"),
                    )
                    .with_help("correct `[catalog.resources]` in clinker.toml before compiling"),
                );
                return artifacts;
            }
        };
    let mut bind_ctx = BindContext {
        ctx,
        symbol_table,
        use_path_stack: Vec::new(),
        depth: 0,
        enclosing_scope_names: HashSet::new(),
        origin_dir: pipeline_dir.to_path_buf(),
        scoped_vars,
        resource_catalog: &resource_catalog,
        passed_contract_checks: Vec::new(),
        body_outputs: HashSet::new(),
        body_ports: Vec::new(),
        verifying: false,
    };
    bind_schema_inner(
        nodes,
        &top_level_ids,
        diags,
        &mut bind_ctx,
        &mut artifacts,
        &mut schema_by_name,
        &mut declared_multiple_by_name,
    );
    artifacts.top_level_rows = nodes
        .iter()
        .filter_map(|spanned| {
            let name = spanned.value.name();
            schema_by_name
                .remove(name)
                .map(|row| (name.to_string(), row))
        })
        .collect();

    // Cross-Transform duplicate `declares:` (the only path that
    // produced multi-writer for a `(scope, var)` pair) is now caught at
    // config-validation time by `validate_unique_scoped_declarations`,
    // so the previous compile-time E170 multi-writer check is dead.
    validate_init_phase_terminals(nodes, diags);
    validate_read_after_write(nodes, &top_level_ids, &artifacts, diags);
    validate_post_merge_source_reads(nodes, &top_level_ids, &artifacts, diags);
    validate_init_phase_isolation(nodes, &top_level_ids, &artifacts, diags);

    artifacts
}

/// Owned-key view of [`CompileArtifacts::top_level_name_ids`] for the
/// consumers that key by `String` — the read-after-write / post-merge /
/// init validators and `bind_schema_inner`, all of which look up by an
/// owned node name. The correlation itself lives once on the artifacts
/// method; this only re-keys the borrowed names into owned `String`s.
fn top_level_name_ids(
    nodes: &[Spanned<PipelineNode>],
    artifacts: &CompileArtifacts,
) -> HashMap<String, PlanNodeId> {
    artifacts
        .top_level_name_ids(nodes)
        .into_iter()
        .map(|(name, id)| (name.to_string(), id))
        .collect()
}

/// Compile-time check: a `phase: init` Transform may not read
/// scoped variables that are written ONLY by `phase: runtime`
/// Transforms. The init sub-DAG runs to completion before any
/// runtime node executes, so the read would silently observe the
/// declaration default — the "invisible default" hazard.
///
/// Reads of vars written by other init-phase Transforms are fine
/// (init runs as one unit). Reads of vars with no writer are fine
/// (declaration default is the intended value). Only the
/// init-reads-runtime-only case is flagged.
///
/// Reuses `iter_writers` for the writer-map shape and
/// `validate_read_after_write`'s AST walker (`collect_scope_reads_*`).
/// Iterate every (scope, var) writer in the pipeline. Each writer is
/// a Transform with one or more `config.declares:` entries; one
/// `WriterInfo` is emitted per (Transform, declare-entry) pair.
struct WriterInfo<'a> {
    scope: crate::config::VarScope,
    var: &'a str,
    node_name: &'a str,
    phase: crate::config::Phase,
}

fn iter_writers<'a>(nodes: &'a [Spanned<PipelineNode>]) -> Vec<WriterInfo<'a>> {
    let mut out = Vec::new();
    for spanned in nodes {
        if let PipelineNode::Transform { header, config } = &spanned.value {
            for entry in &config.declares {
                out.push(WriterInfo {
                    scope: entry.scope,
                    var: &entry.name,
                    node_name: &header.name,
                    phase: config.phase,
                });
            }
        }
    }
    out
}

fn validate_init_phase_isolation(
    nodes: &[Spanned<PipelineNode>],
    top_level_ids: &HashMap<String, PlanNodeId>,
    artifacts: &CompileArtifacts,
    diags: &mut Vec<Diagnostic>,
) {
    use crate::config::{Phase as ConfPhase, VarScope};

    // Build (scope, var) → (writer-name, writer-phase) map from every
    // declared writer.
    let mut writers: std::collections::HashMap<(VarScope, String), (String, ConfPhase)> =
        std::collections::HashMap::new();
    for w in iter_writers(nodes) {
        writers
            .entry((w.scope, w.var.to_string()))
            .or_insert_with(|| (w.node_name.to_string(), w.phase));
    }
    if writers.is_empty() {
        return;
    }

    // Collect (reader_name, read_span, typed_keys) for every init-phase
    // writer. Each Transform contributes one entry whose typed_keys
    // is its bare node name (one CXL block per node).
    type ReaderEntry<'a> = (&'a str, Span, Vec<String>);
    let mut readers: Vec<ReaderEntry<'_>> = Vec::new();
    for spanned in nodes {
        match &spanned.value {
            PipelineNode::Transform { header, config } if config.phase == ConfPhase::Init => {
                readers.push((
                    header.name.as_str(),
                    span_for_node(spanned),
                    vec![header.name.clone()],
                ));
            }
            _ => {}
        }
    }

    for (reader_name, read_span, typed_keys) in readers {
        for typed_key in &typed_keys {
            let Some(typed) = top_level_ids
                .get(typed_key)
                .and_then(|id| artifacts.typed_get(*id))
            else {
                continue;
            };
            let mut reads: Vec<(VarScope, String)> = Vec::new();
            for stmt in &typed.program.statements {
                collect_scope_reads_in_statement(stmt, &mut reads);
            }
            for (scope, var) in reads {
                let Some((writer_name, writer_phase)) = writers.get(&(scope, var.clone())).cloned()
                else {
                    continue;
                };
                if writer_phase == ConfPhase::Init {
                    continue;
                }
                if writer_name == reader_name {
                    continue;
                }
                let scope_str = match scope {
                    VarScope::Pipeline => "$pipeline",
                    VarScope::Source => "$source",
                    VarScope::Record => "$record",
                };
                let writer_span = nodes
                    .iter()
                    .find(|n| n.value.name() == writer_name.as_str())
                    .map(span_for_node)
                    .unwrap_or(Span::SYNTHETIC);
                diags.push(
                    Diagnostic::error(
                        "E175",
                        format!(
                            "init-phase node {reader_name:?} reads {scope_str}.{var} \
                             which is only written by runtime-phase node \
                             {writer_name:?} — init runs to completion before runtime, so \
                             the read would silently observe the declaration default. \
                             Move the writer to `phase: init` or remove the read."
                        ),
                        LabeledSpan::primary(read_span, "init-phase reader".to_string()),
                    )
                    .with_secondary(LabeledSpan::new(
                        writer_span,
                        Some("runtime-phase writer".to_string()),
                    )),
                );
            }
        }
    }
}

/// Compile-time check: nodes downstream of `Merge` / `Combine` must
/// not read user-declared `$source.<key>` (custom keys); the
/// per-source-file Arc keying that backs source-scope state means
/// each record reads its OWN origin source's value, but in cross-
/// source comparison contexts the user might intend "give me Source
/// A's batch_id and Source B's batch_id" — which is ambiguous in the
/// current syntax. Avoids Talend's `globalMap`-collapse-after-merge
/// surprise (each record sees its own source value, but the
/// cross-source intent is silently lost).
///
/// Builtin `$source.*` members (`file`, `row`, `path`, `count`,
/// `batch`, `ingestion_timestamp`) remain valid post-merge — those
/// are per-record provenance and the per-record-source semantic is
/// the desired one.
///
/// Emits E172 with primary span on the offending CXL reference and a
/// secondary span on the closest Merge/Combine ancestor.
fn validate_post_merge_source_reads(
    nodes: &[Spanned<PipelineNode>],
    top_level_ids: &HashMap<String, PlanNodeId>,
    artifacts: &CompileArtifacts,
    diags: &mut Vec<Diagnostic>,
) {
    // Compute per-node "closest merge/combine ancestor" map. A node is
    // post-merge iff this entry is `Some(_)`. The same declaration-
    // order walk used by `validate_read_after_write` works here
    // because Stage-3 already proved topological soundness.
    let mut post_merge: std::collections::HashMap<String, Option<(String, Span)>> =
        std::collections::HashMap::new();
    for spanned in nodes {
        let name = spanned.value.name().to_string();
        let span = span_for_node(spanned);
        let direct_inputs: Vec<String> = spanned
            .value
            .direct_input_names()
            .into_iter()
            .map(String::from)
            .collect();
        // A Merge or Combine node IS the merge ancestor for everything
        // downstream — including itself in the lookup so its own
        // direct descendants pick it up.
        let self_is_merge = matches!(
            &spanned.value,
            PipelineNode::Merge { .. } | PipelineNode::Combine { .. }
        );
        let inherited = direct_inputs
            .iter()
            .find_map(|inp| post_merge.get(inp).cloned().unwrap_or(None));
        let entry = if self_is_merge {
            Some((name.clone(), span))
        } else {
            inherited
        };
        post_merge.insert(name, entry);
    }

    // Walk every CXL-bearing node's typed program(s); for each
    // `$source.<custom>` read in a post-merge node, emit E172.
    for spanned in nodes {
        let reader_name = spanned.value.name().to_string();
        let Some(Some((merge_name, merge_span))) = post_merge.get(&reader_name).cloned() else {
            continue;
        };
        let reader_id = top_level_ids.get(&reader_name).copied();
        let mut typed_keys: Vec<PlanNodeId> = Vec::new();
        if spanned.value.reads_scope_vars_in_cxl()
            && let Some(id) = reader_id
            && artifacts.typed_contains(id)
        {
            typed_keys.push(id);
        }
        let span = span_for_node(spanned);
        for key in typed_keys {
            let Some(typed) = artifacts.typed_get(key) else {
                continue;
            };
            let mut reads: Vec<(crate::config::VarScope, String)> = Vec::new();
            for stmt in &typed.program.statements {
                collect_scope_reads_in_statement(stmt, &mut reads);
            }
            for (scope, var) in reads {
                if scope != crate::config::VarScope::Source {
                    continue;
                }
                if is_builtin_source_member(&var) {
                    continue;
                }
                diags.push(
                    Diagnostic::error(
                        "E172",
                        format!(
                            "node {reader_name:?} reads user-declared $source.{var} downstream \
                             of Merge/Combine node {merge_name:?}: post-merge cross-source \
                             access is ambiguous (each record carries its own source's value, \
                             but cross-source comparison requires explicit per-input \
                             qualification). Project the value into a record field before \
                             merging, or write a per-input value via a Transform with \
                             `declares:` placed upstream of the merge."
                        ),
                        LabeledSpan::primary(span, "post-merge reader".to_string()),
                    )
                    .with_secondary(LabeledSpan::new(
                        merge_span,
                        Some("merge / combine ancestor".to_string()),
                    )),
                );
            }
        }
    }
}

fn is_builtin_source_member(field: &str) -> bool {
    matches!(
        field,
        "file" | "row" | "path" | "count" | "batch" | "ingestion_timestamp" | "name"
    )
}

/// Compile-time check: every `$pipeline.<key>` / `$source.<key>` /
/// `$record.<key>` read whose `(scope, key)` pair is written by some
/// `state` node must originate at a node whose DAG ancestor set
/// includes that writer. A reader on a sibling branch (no path from
/// writer to reader) would observe the variable's default value, not
/// the written value — exactly the "hidden DAG edge" anti-pattern
/// Airflow Variables and Talend `globalMap` are infamous for.
///
/// Self-reads (a Transform referencing the same variable it writes)
/// are allowed: the read evaluates BEFORE the write within a single
/// record's processing, observing the previous value.
///
/// Reads of `(scope, key)` pairs declared in `vars:` but never written
/// by any Transform are NOT flagged here — those are valid reads of
/// the declaration default. Reads that aren't declared at all are
/// already rejected by the resolver in Phase B / C-1.
///
/// Emits E171 with primary span on the offending CXL reference and a
/// secondary span on the writer Transform.
fn validate_read_after_write(
    nodes: &[Spanned<PipelineNode>],
    top_level_ids: &HashMap<String, PlanNodeId>,
    artifacts: &CompileArtifacts,
    diags: &mut Vec<Diagnostic>,
) {
    use crate::config::{Phase as ConfPhase, VarScope};

    // Build (scope, var) → (writer-node-name, writer-phase) map. Phase
    // is needed because init-phase writers' values are visible to ALL
    // runtime readers regardless of DAG topology — init runs to
    // completion before runtime starts, so
    // descendant ancestry is irrelevant for those reads.
    let mut writers: std::collections::HashMap<(VarScope, String), (String, ConfPhase)> =
        std::collections::HashMap::new();
    for w in iter_writers(nodes) {
        writers
            .entry((w.scope, w.var.to_string()))
            .or_insert_with(|| (w.node_name.to_string(), w.phase));
    }
    if writers.is_empty() {
        return;
    }

    // Build transitive ancestor map keyed by node name. Iterates
    // `nodes` in declaration order, which is topologically sound per
    // Stage-3 validation, so each node's ancestor set can be assembled
    // from already-processed predecessors.
    let mut ancestors: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    for spanned in nodes {
        let name = spanned.value.name().to_string();
        let mut anc: std::collections::HashSet<String> = std::collections::HashSet::new();
        let direct: Vec<String> = spanned
            .value
            .direct_input_names()
            .into_iter()
            .map(String::from)
            .collect();
        for parent in direct {
            anc.insert(parent.clone());
            if let Some(parent_anc) = ancestors.get(&parent) {
                anc.extend(parent_anc.iter().cloned());
            }
        }
        ancestors.insert(name, anc);
    }

    // Walk every CXL-bearing node's typed program(s); for each scope
    // read whose key has a known writer, check the writer is in the
    // reader's ancestor set.
    for spanned in nodes {
        let reader_name = spanned.value.name().to_string();
        let reader_id = top_level_ids.get(&reader_name).copied();
        let mut typed_keys: Vec<PlanNodeId> = Vec::new();
        if spanned.value.reads_scope_vars_in_cxl()
            && let Some(id) = reader_id
            && artifacts.typed_contains(id)
        {
            typed_keys.push(id);
        }
        let span = span_for_node(spanned);
        for key in typed_keys {
            let Some(typed) = artifacts.typed_get(key) else {
                continue;
            };
            let mut reads: Vec<(VarScope, String)> = Vec::new();
            for stmt in &typed.program.statements {
                collect_scope_reads_in_statement(stmt, &mut reads);
            }
            // Determine the reader's phase — Transforms carry their
            // own `phase`; every other node kind is runtime by
            // definition. Init readers' visibility into init-only
            // writers depends on DAG ancestry within the init pass;
            // runtime readers see init writers regardless of topology
            // because init completes first.
            let reader_phase = match &spanned.value {
                PipelineNode::Transform { config, .. } => config.phase,
                _ => ConfPhase::Runtime,
            };
            for (scope, var) in reads {
                let Some((writer_name, writer_phase)) = writers.get(&(scope, var.clone())).cloned()
                else {
                    continue;
                };
                // Self-read inside the writing Transform is fine.
                if writer_name == reader_name {
                    continue;
                }
                // Init writer + runtime reader: init runs to completion
                // before runtime, so the value is visible everywhere
                // downstream regardless of DAG topology.
                if writer_phase == ConfPhase::Init && reader_phase == ConfPhase::Runtime {
                    continue;
                }
                // Init reader + runtime writer is the E175 case — skip
                // here to avoid a duplicate diagnostic; E175 fires from
                // `validate_init_phase_isolation`.
                if writer_phase == ConfPhase::Runtime && reader_phase == ConfPhase::Init {
                    continue;
                }
                // Same phase (init+init or runtime+runtime): the
                // descendant-ancestry rule applies — both run in the
                // same pass and the writer's effect must precede the
                // reader's evaluation in topo order.
                let reader_anc = ancestors.get(&reader_name);
                let is_descendant = reader_anc
                    .map(|set| set.contains(&writer_name))
                    .unwrap_or(false);
                if !is_descendant {
                    let scope_str = match scope {
                        VarScope::Pipeline => "$pipeline",
                        VarScope::Source => "$source",
                        VarScope::Record => "$record",
                    };
                    let writer_span = nodes
                        .iter()
                        .find(|n| n.value.name() == writer_name.as_str())
                        .map(span_for_node)
                        .unwrap_or(Span::SYNTHETIC);
                    diags.push(
                        Diagnostic::error(
                            "E171",
                            format!(
                                "node {reader_name:?} reads {scope_str}.{var} but is not a DAG \
                                 descendant of writer Transform {writer_name:?} — the read \
                                 would observe the declaration default, not the written value \
                                 (move the reader downstream of the writer or remove the read)"
                            ),
                            LabeledSpan::primary(span, "non-descendant reader".to_string()),
                        )
                        .with_secondary(LabeledSpan::new(
                            writer_span,
                            Some("writer Transform".to_string()),
                        )),
                    );
                }
            }
        }
    }
}

/// build a composition body's scoped-vars registry from the
/// intersection of the body's `_compose.scoped_vars` schema and the
/// parent's actual declarations.
///
/// For each `(scope, key)` declared in the schema:
/// - If the parent has the same `(scope, key)` with the same type,
///   include it in the body's registry.
/// - If the parent doesn't declare it (or declares it with a different
///   type), emit E174 and skip the entry. The body's CXL referencing
///   that name then falls back to the resolver's "unknown member"
///   diagnostic — which is the right outcome because the schema's
///   contract was unfulfillable.
///
/// Returns the body-visible registry. An empty schema produces an
/// empty registry, preserving the seal as the default.
fn build_body_scoped_vars(
    schema: &crate::config::ScopedVarsSchema,
    parent: &cxl::resolve::ScopedVarsRegistry,
    composition_name: &str,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) -> cxl::resolve::ScopedVarsRegistry {
    use cxl::resolve::ScopedVarsRegistry;

    let mut registry = ScopedVarsRegistry {
        module_exports: parent.module_exports.clone(),
        runtime_modules: Arc::clone(&parent.runtime_modules),
        ..ScopedVarsRegistry::default()
    };

    let check = |scope_str: &str,
                 schema_map: &indexmap::IndexMap<String, crate::config::ScopedVarType>,
                 parent_map: &indexmap::IndexMap<String, cxl::resolve::ScopedVarType>,
                 out_map: &mut indexmap::IndexMap<String, cxl::resolve::ScopedVarType>,
                 diags: &mut Vec<Diagnostic>| {
        for (key, schema_ty) in schema_map {
            let cxl_schema_ty: cxl::resolve::ScopedVarType = (*schema_ty).into();
            match parent_map.get(key) {
                Some(parent_ty) if *parent_ty == cxl_schema_ty => {
                    out_map.insert(key.clone(), cxl_schema_ty);
                }
                Some(parent_ty) => {
                    diags.push(Diagnostic::error(
                        "E174",
                        format!(
                            "composition {composition_name:?}: scoped_vars schema declares \
                                 ${scope_str}.{key} as {cxl_schema_ty:?}, but the parent \
                                 pipeline declares it as {parent_ty:?} — types must match"
                        ),
                        LabeledSpan::primary(span, "composition call site".to_string()),
                    ));
                }
                None => {
                    diags.push(Diagnostic::error(
                        "E174",
                        format!(
                            "composition {composition_name:?}: scoped_vars schema declares \
                                 ${scope_str}.{key} but no parent Transform declares it via \
                                 `declares:` — add a Transform with `declares: [{{ name: {key}, \
                                 scope: {scope_str}, type: ... }}]` upstream of the composition \
                                 or remove it from the composition's scoped_vars schema"
                        ),
                        LabeledSpan::primary(span, "composition call site".to_string()),
                    ));
                }
            }
        }
    };

    check(
        "pipeline",
        &schema.pipeline,
        &parent.pipeline,
        &mut registry.pipeline,
        diags,
    );
    check(
        "source",
        &schema.source,
        &parent.source,
        &mut registry.source,
        diags,
    );
    check(
        "record",
        &schema.record,
        &parent.record,
        &mut registry.record,
        diags,
    );

    // — populate the hidden tier with parent vars
    // that are NOT opted into the schema. The composition body's
    // resolver consults `hidden_*` on miss to emit E173 (composition-
    // aware "this is a parent var hidden from your body — declare in
    // _compose.scoped_vars to opt in") rather than the generic
    // "unknown member" diagnostic.
    for (key, ty) in &parent.pipeline {
        if !schema.pipeline.contains_key(key) {
            registry.hidden_pipeline.insert(key.clone(), *ty);
        }
    }
    for (key, ty) in &parent.source {
        if !schema.source.contains_key(key) {
            registry.hidden_source.insert(key.clone(), *ty);
        }
    }
    for (key, ty) in &parent.record {
        if !schema.record.contains_key(key) {
            registry.hidden_record.insert(key.clone(), *ty);
        }
    }

    registry
}

/// Populate the body registry's `$config.<param>` tiers for one composition
/// instantiation.
///
/// `config` carries the declared type of every param (so the resolver
/// accepts `$config.<param>` and rejects unknown ones, and the typechecker
/// types each read); `config_fold` carries the resolved literal the planner
/// folds each read to. Resolution precedence, highest first:
/// channel/group `config:` clobber → call-site `config:` → signature default
/// → `null`. An absent value folds to `null`, matching the `$vars.*`
/// missing-read convention.
fn populate_body_config(
    registry: &mut cxl::resolve::ScopedVarsRegistry,
    signature: &CompositionSignature,
    node_name: &str,
    call_config: &IndexMap<String, serde_json::Value>,
    config_overrides: &ConfigOverrides,
) {
    let node_overrides = config_overrides.get(node_name);
    for (param_name, decl) in &signature.config_schema {
        registry
            .config
            .insert(param_name.clone(), param_type_to_scoped(decl.param_type));

        let resolved = node_overrides
            .and_then(|m| m.get(param_name))
            .or_else(|| call_config.get(param_name))
            .or(decl.default.as_ref());
        let literal = match resolved {
            Some(value) => config_value_to_literal(value, decl.param_type),
            None => LiteralValue::Null,
        };
        registry.config_fold.insert(param_name.clone(), literal);
    }
}

/// Map a composition config [`ParamType`] to the CXL scoped-var type used by
/// the resolver / typechecker. `path` is a string at the CXL layer.
fn param_type_to_scoped(param_type: ParamType) -> cxl::resolve::ScopedVarType {
    use cxl::resolve::ScopedVarType;
    match param_type {
        ParamType::String | ParamType::Path => ScopedVarType::String,
        ParamType::Int => ScopedVarType::Int,
        ParamType::Float => ScopedVarType::Float,
        ParamType::Bool => ScopedVarType::Bool,
    }
}

/// Coerce a resolved config value to the CXL literal the fold substitutes,
/// driven by the param's declared type. Values are already type-validated
/// (signature-load and `validate_config`), so a shape mismatch here can only
/// be a `null`/absent value, which folds to [`LiteralValue::Null`].
fn config_value_to_literal(value: &serde_json::Value, param_type: ParamType) -> LiteralValue {
    if value.is_null() {
        return LiteralValue::Null;
    }
    match param_type {
        ParamType::Int => value
            .as_i64()
            .map(LiteralValue::Int)
            .or_else(|| value.as_f64().map(|f| LiteralValue::Int(f as i64)))
            .unwrap_or(LiteralValue::Null),
        ParamType::Float => value
            .as_f64()
            .map(LiteralValue::Float)
            .unwrap_or(LiteralValue::Null),
        ParamType::Bool => value
            .as_bool()
            .map(LiteralValue::Bool)
            .unwrap_or(LiteralValue::Null),
        ParamType::String | ParamType::Path => value
            .as_str()
            .map(|s| LiteralValue::String(s.into()))
            .unwrap_or(LiteralValue::Null),
    }
}

/// Constant-fold every `$config.<param>` in a freshly typechecked body
/// program to its resolved literal. A no-op outside a composition body
/// (empty `config_fold`), so a plain pipeline compiles byte-identically.
fn fold_body_config(typed: &mut TypedProgram, scoped_vars: &cxl::resolve::ScopedVarsRegistry) {
    if scoped_vars.config_fold.is_empty() {
        return;
    }
    cxl::ast::fold_config_program(&mut typed.program, &|param| {
        scoped_vars.config_fold.get(param).cloned()
    });
}

fn span_for_node(spanned: &Spanned<PipelineNode>) -> Span {
    let line = spanned.referenced.line() as u32;
    if line > 0 {
        Span::line_only(line)
    } else {
        Span::SYNTHETIC
    }
}

fn authored_value_span<T>(spanned: &Spanned<T>, fallback: Span) -> Span {
    let line = spanned.referenced.line() as u32;
    if line > 0 {
        Span::line_only(line)
    } else {
        fallback
    }
}

fn collect_scope_reads_in_statement(
    stmt: &Statement,
    out: &mut Vec<(crate::config::VarScope, String)>,
) {
    match stmt {
        Statement::Emit { expr, .. }
        | Statement::Let { expr, .. }
        | Statement::ExprStmt { expr, .. } => collect_scope_reads_in_expr(expr, out),
        Statement::Filter { predicate, .. } => collect_scope_reads_in_expr(predicate, out),
        Statement::Trace { guard, message, .. } => {
            if let Some(g) = guard {
                collect_scope_reads_in_expr(g, out);
            }
            collect_scope_reads_in_expr(message, out);
        }
        Statement::UseStmt { .. } | Statement::Distinct { .. } => {}
        Statement::EmitEach { source, body, .. } | Statement::ExplodeOuter { source, body, .. } => {
            collect_scope_reads_in_expr(source, out);
            for inner in body {
                collect_scope_reads_in_statement(inner, out);
            }
        }
    }
}

fn collect_scope_reads_in_expr(expr: &Expr, out: &mut Vec<(crate::config::VarScope, String)>) {
    use crate::config::VarScope;
    match expr {
        Expr::PipelineAccess { field, .. } => out.push((VarScope::Pipeline, field.to_string())),
        Expr::SourceAccess { field, .. } => out.push((VarScope::Source, field.to_string())),
        Expr::RecordAccess { field, .. } => out.push((VarScope::Record, field.to_string())),
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            collect_scope_reads_in_expr(lhs, out);
            collect_scope_reads_in_expr(rhs, out);
        }
        Expr::Unary { operand, .. } => collect_scope_reads_in_expr(operand, out),
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_scope_reads_in_expr(element, out);
            }
        }
        Expr::MapLiteral { entries, .. } => {
            for entry in entries {
                if let cxl::ast::MapKey::Computed(key) = &entry.key {
                    collect_scope_reads_in_expr(key, out);
                }
                collect_scope_reads_in_expr(&entry.value, out);
            }
        }
        Expr::ArrayComprehension {
            item,
            source,
            predicate,
            ..
        } => {
            collect_scope_reads_in_expr(source, out);
            collect_scope_reads_in_expr(item, out);
            if let Some(predicate) = predicate {
                collect_scope_reads_in_expr(predicate, out);
            }
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_scope_reads_in_expr(condition, out);
            collect_scope_reads_in_expr(then_branch, out);
            if let Some(eb) = else_branch {
                collect_scope_reads_in_expr(eb, out);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                collect_scope_reads_in_expr(s, out);
            }
            for arm in arms {
                collect_scope_reads_in_expr(&arm.pattern, out);
                collect_scope_reads_in_expr(&arm.body, out);
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            collect_scope_reads_in_expr(receiver, out);
            for a in args {
                collect_scope_reads_in_expr(a, out);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                collect_scope_reads_in_expr(a, out);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            collect_scope_reads_in_expr(receiver, out);
            collect_scope_reads_in_expr(index, out);
        }
        Expr::Closure { body, .. } => {
            collect_scope_reads_in_expr(body, out);
        }
        Expr::Literal { .. }
        | Expr::FieldRef { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::DocAccess { .. }
        | Expr::ConfigAccess { .. }
        | Expr::VarsAccess { .. } => {
            // $vars.* reads are global static config — no producer, no
            // DAG-descendant rule applies. $config.* is folded to a literal
            // before this walk runs. Both are skipped from the scope-read
            // accounting that drives E170/E171/E175 validation.
        }
    }
}

/// Compile-time check: an init-phase node may feed only other
/// init-phase nodes — runtime-phase consumers never see init-phase
/// nodes' record-pass-through output because the runtime walk skips
/// init-only sub-DAG nodes. Silent record-loss would be a debugging
/// nightmare; this rule surfaces the structural mismatch up front.
///
/// Walks each `phase: init` node (State or Transform) and checks
/// whether any non-init node in the topology references it as input.
/// Emits E164 with the init node's span and the offending downstream
/// runtime node's name.
fn validate_init_phase_terminals(nodes: &[Spanned<PipelineNode>], diags: &mut Vec<Diagnostic>) {
    let span_for = |spanned: &Spanned<PipelineNode>| -> Span {
        let line = spanned.referenced.line() as u32;
        if line > 0 {
            Span::line_only(line)
        } else {
            Span::SYNTHETIC
        }
    };
    let init_node_names: HashSet<&str> = nodes
        .iter()
        .filter_map(|spanned| match &spanned.value {
            PipelineNode::Transform { header, config }
                if config.phase == crate::config::Phase::Init =>
            {
                Some(header.name.as_str())
            }
            _ => None,
        })
        .collect();
    if init_node_names.is_empty() {
        return;
    }
    for spanned in nodes {
        // For every node, check its inputs against the init-node-name
        // set. Each input declared on a node implies a downstream
        // dependency on that input, so an init node showing up as an
        // input means it has a non-init descendant.
        let inputs: Vec<&str> = spanned.value.direct_input_names();
        let consumer_name = spanned.value.name();
        let consumer_span = span_for(spanned);
        // Init-phase consumers (State or Transform) are fine — the
        // two-pass walk handles them as part of the init sub-DAG.
        // Only runtime descendants are forbidden.
        let consumer_is_init = match &spanned.value {
            PipelineNode::Transform { config, .. } => config.phase == crate::config::Phase::Init,
            _ => false,
        };
        if consumer_is_init {
            continue;
        }
        for input_name in inputs {
            if init_node_names.contains(input_name) {
                let init_node_span = nodes
                    .iter()
                    .find(|n| n.value.name() == input_name)
                    .map(span_for)
                    .unwrap_or(Span::SYNTHETIC);
                diags.push(
                    Diagnostic::error(
                        "E164",
                        format!(
                            "init-phase node {input_name:?} has runtime descendant \
                             {consumer_name:?}: init nodes can feed other init-phase \
                             nodes but must not be consumed by runtime-phase nodes \
                             (their record output is not visible in the runtime walk). \
                             Either change the consumer to `phase: init` or detach it from \
                             this init node."
                        ),
                        LabeledSpan::primary(init_node_span, "init node".to_string()),
                    )
                    .with_secondary(LabeledSpan::new(
                        consumer_span,
                        Some("runtime consumer".to_string()),
                    )),
                );
            }
        }
    }
}

/// Build a placeholder `TypedProgram` carrying only `output_row`, for
/// node variants without a CXL body (Source/Merge/Output/Composition).
/// Validate `error_handling.dlq.per_source` against the declared Source
/// nodes and the DLQ-routing path set. Emits E317 when a `per_source`
/// key does not name a declared Source node, and E318 when a configured
/// `max_rate` is outside `(0.0, 1.0]` (zero is rejected because a
/// `max_rate: 0.0` config halts on the very first failure — a
/// `min_records`-bypassing footgun that should be expressed via a
/// dedicated halt-on-any-DLQ surface instead) or a configured DLQ
/// output path collides with another DLQ path (pipeline-wide or
/// per-source).
pub(crate) fn validate_dlq_per_source(
    dlq: Option<&crate::config::DlqConfig>,
    nodes: &[Spanned<PipelineNode>],
    diags: &mut Vec<Diagnostic>,
) {
    /// Accepts the half-open interval `(0.0, 1.0]` — see the function-
    /// level docstring for why zero is rejected.
    fn is_valid_max_rate(r: f64) -> bool {
        r > 0.0 && r <= 1.0 && r.is_finite()
    }

    let Some(dlq) = dlq else {
        return;
    };

    // Range-check the pipeline-wide max_rate. Accepts the half-open
    // interval `(0.0, 1.0]`: zero is rejected as an unintended
    // halt-on-first-failure footgun (the `min_records` floor would also
    // be bypassed when the numerator and denominator both start at 0).
    if let Some(r) = dlq.max_rate
        && !is_valid_max_rate(r)
    {
        diags.push(Diagnostic::error(
            "E318",
            format!("error_handling.dlq.max_rate {r} is out of range; must be in (0.0, 1.0]"),
            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
        ));
    }

    // Collect declared Source-node names so unknown per_source keys
    // can be flagged with a precise message.
    let source_names: HashSet<&str> = nodes
        .iter()
        .filter_map(|n| match &n.value {
            PipelineNode::Source { header, .. } => Some(header.name.as_ref()),
            _ => None,
        })
        .collect();

    // Track path collisions across the pipeline-wide + every per-source
    // entry. Two paths collide when they name the *same physical file*, and a
    // filesystem decides that along two independent axes: case, and Unicode
    // normalization. `errors.csv` and `Errors.csv` are one file wherever case
    // is folded; `Ärger.csv` written composed and decomposed is one file
    // wherever normalization is ignored, which includes case-*sensitive*
    // APFS. Either way the per-source and pipeline-wide writers would
    // silently overwrite each other. Each axis is therefore applied only when
    // the actual target filesystem exercises it, probed independently, so two
    // legitimately-distinct files are not falsely flagged. First insertion
    // wins; subsequent insertions onto the same key emit E318. The stored
    // value keeps the raw path for the diagnostic message; the value's `.1`
    // is the config label.
    let mut paths: HashMap<String, (String, String)> = HashMap::new();
    if let Some(p) = dlq.path.as_deref() {
        paths.insert(
            crate::config::destination_identity(std::path::Path::new(p)),
            (p.to_string(), "error_handling.dlq.path".to_string()),
        );
    }

    for (src_name, per) in &dlq.per_source {
        if !source_names.contains(src_name.as_str()) {
            diags.push(Diagnostic::error(
                "E317",
                format!(
                    "error_handling.dlq.per_source key {src_name:?} does not match \
                     any declared Source node"
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
        }
        if let Some(r) = per.max_rate
            && !is_valid_max_rate(r)
        {
            diags.push(Diagnostic::error(
                "E318",
                format!(
                    "error_handling.dlq.per_source.{src_name}.max_rate {r} \
                     is out of range; must be in (0.0, 1.0]"
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
        }
        if let Some(p) = per.path.as_deref()
            && let Some((prev_path, prev_label)) = paths.insert(
                crate::config::destination_identity(std::path::Path::new(p)),
                (
                    p.to_string(),
                    format!("error_handling.dlq.per_source.{src_name}.path"),
                ),
            )
        {
            // Surface the prior path explicitly so a case-only collision reads
            // clearly (`Errors.csv` collides with `errors.csv` …).
            let collide_note = if prev_path == p {
                format!("collides with {prev_label}")
            } else {
                format!(
                    "collides with {prev_label} ({prev_path:?}) — these paths name \
                     the same file: they may differ only in case on a \
                     case-insensitive output filesystem, differ only in being \
                     written relatively or absolutely, or reach one directory \
                     through a symlink"
                )
            };
            diags.push(Diagnostic::error(
                "E318",
                format!("error_handling.dlq.per_source.{src_name}.path {p:?} {collide_note}"),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
        }
    }
}

/// Flags E322 when two output destinations resolve to the same physical
/// file: two `Output` nodes' paths, or an `Output` node's path and a
/// configured DLQ path. The engine otherwise only checks whether a single
/// output path already exists on disk (`validate_path`), so two writers onto
/// one file are silent — and on a case-insensitive output filesystem (macOS
/// APFS / Windows NTFS) paths differing only in case (`out.csv` / `Out.csv`)
/// name one file.
///
/// Keyed through [`crate::config::destination_identity`] — the same identity
/// the runtime staging registry uses — so a collision the run would refuse is
/// refused here instead, before any record is read. Case folding and Unicode
/// normalization are each applied within it only when the target filesystem
/// exercises that axis, so two legitimately-distinct files are not flagged.
/// Collisions that only emerge after
/// path-template token resolution are still left to runtime. DLQ-vs-DLQ collisions stay owned by
/// [`validate_dlq_per_source`] (E318): this pass seeds the map with DLQ paths
/// only to catch Output-vs-DLQ overlap, and never re-reports a DLQ-only
/// collision.
pub(crate) fn validate_output_path_collisions(
    nodes: &[Spanned<PipelineNode>],
    dlq: Option<&crate::config::DlqConfig>,
    diags: &mut Vec<Diagnostic>,
) {
    // (raw path, config label) keyed by the case-folded collision key.
    let mut paths: HashMap<String, (String, String)> = HashMap::new();

    // Seed DLQ paths silently so an Output colliding with one surfaces, while
    // DLQ-vs-DLQ collisions remain E318's responsibility.
    if let Some(dlq) = dlq {
        if let Some(p) = dlq.path.as_deref() {
            paths.insert(
                crate::config::destination_identity(std::path::Path::new(p)),
                (p.to_string(), "error_handling.dlq.path".to_string()),
            );
        }
        for (src_name, per) in &dlq.per_source {
            if let Some(p) = per.path.as_deref() {
                paths.insert(
                    crate::config::destination_identity(std::path::Path::new(p)),
                    (
                        p.to_string(),
                        format!("error_handling.dlq.per_source.{src_name}.path"),
                    ),
                );
            }
        }
    }

    for n in nodes {
        let PipelineNode::Sink { config, .. } = &n.value else {
            continue;
        };
        let p = config.sink.path.as_str();
        if p.is_empty() {
            continue;
        }
        let name = config.sink.name.as_str();
        if let Some((prev_path, prev_label)) = paths.insert(
            crate::config::destination_identity(std::path::Path::new(p)),
            (p.to_string(), format!("output {name:?}")),
        ) {
            let collide_note = if prev_path == p {
                format!("collides with {prev_label}")
            } else {
                format!(
                    "collides with {prev_label} ({prev_path:?}) — these paths name the \
                     same file: they may differ only in case on a case-insensitive \
                     output filesystem, differ only in being written relatively or \
                     absolutely, or reach one directory through a symlink"
                )
            };
            diags.push(Diagnostic::error(
                "E322",
                format!("output {name:?} path {p:?} {collide_note}"),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
        }
    }
}

/// **E375** — one `log` event name carrying two different field sets anywhere
/// in the plan.
///
/// A collector groups records by the event name and holds no node identity:
/// the runtime dispatcher looks a directive up as `peek_log(&name, severity)`,
/// so two shapes filed under one name arrive as records nothing downstream can
/// separate. Several transforms emitting the *same* event with the *same*
/// fields stay legal — that is how one event is reported from several places.
///
/// The rule is plan-wide because the identity it protects is plan-wide. Run
/// per node slice it held within the top-level nodes and within each
/// composition body, but a body transform and a top-level transform were never
/// compared — so the one arrangement the collector cannot survive was the one
/// arrangement that passed. This pass runs after composition expansion, where
/// every transform is flattened and each instantiation is its own node, which
/// is also what makes a shared registry sound here and not in the per-slice
/// pass: a composition instantiated twice repeats its own events, and repeats
/// agree with themselves, so only a genuine disagreement is reported.
///
/// Reported against a synthetic span: the two transforms may live in two
/// files, and neither location alone is the offence. The message names both,
/// each qualified by the composition it was expanded from.
pub(crate) fn validate_log_event_shapes(
    nodes: &[Spanned<PipelineNode>],
    artifacts: &CompileArtifacts,
    diags: &mut Vec<Diagnostic>,
) {
    // Compared as a set: `[a, b]` and `[b, a]` request the same fields, and an
    // event is its fields rather than their order.
    fn field_set(directive: &crate::config::LogDirective) -> Vec<&str> {
        let mut fields: Vec<&str> = directive.fields.as_ref().map_or_else(Vec::new, |fields| {
            fields.iter().map(String::as_str).collect()
        });
        fields.sort_unstable();
        fields.dedup();
        fields
    }

    // event name → (label of the transform that declared it first, field set).
    // Owned rather than borrowed: the two halves of the plan are reached
    // through two independent borrows, and one registry has to outlive both.
    let mut shapes: std::collections::BTreeMap<String, (String, Vec<String>)> =
        std::collections::BTreeMap::new();
    let mut check = |label: String, directives: &[crate::config::LogDirective]| {
        for directive in directives {
            let fields: Vec<String> = field_set(directive)
                .into_iter()
                .map(str::to_owned)
                .collect();
            match shapes.get(directive.name.as_str()) {
                Some((first, first_fields)) if *first_fields != fields => {
                    diags.push(Diagnostic::error(
                        "E375",
                        format!(
                            "[E375] {label}: `log` event {:?} is also declared by {first} with \
                             different fields; one event name carries one set of fields — give \
                             both declarations the same `fields`, or give one of them its own \
                             event name",
                            directive.name
                        ),
                        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                    ));
                }
                Some(_) => {}
                None => {
                    shapes.insert(directive.name.clone(), (label.clone(), fields));
                }
            }
        }
    };

    for spanned in nodes {
        if let PipelineNode::Transform {
            header,
            config: body,
        } = &spanned.value
            && let Some(directives) = &body.log
        {
            check(format!("transform {:?}", header.name), directives);
        }
    }
    // Bodies in bind order, each walked in its own topological order, so which
    // of two conflicting transforms is named first does not depend on hashing.
    for body in artifacts.composition_bodies.values() {
        for index in &body.topo_order {
            let Some(crate::plan::execution::PlanNode::Transform {
                name,
                resolved: Some(payload),
                ..
            }) = body.graph.node_weight(*index)
            else {
                continue;
            };
            if payload.log.is_empty() {
                continue;
            }
            check(
                format!("transform {name:?} in composition {:?}", body.semantic_name),
                &payload.log,
            );
        }
    }
}

/// The two schema-dependent `mapping:` gates on a Sink node.
///
/// **E365** — an entry reads a column the graph does not supply at that point.
/// The output schema is known at plan time, so a stale or mistyped column name
/// is answerable here with a span instead of by reading the written file and
/// noticing a header that is not the one the block declared.
///
/// **E364** — an entry's OUTPUT name collides with an upstream column that
/// `include_unmapped` would append beside it. Two same-named columns on one
/// schema resolve last-write-wins through `Record::get`, so the writer would
/// serve the passthrough column's value under the renamed column's header —
/// the renamed value silently dropped.
///
/// Both judge the row bound to the Sink's input. Every bound row is closed:
/// a Source declares a typed schema, each node's row derives from it, and a
/// composition body binds against the row its call feeds it, so a body Sink
/// is judged against the caller's columns as the same Sink written inline
/// would be. The runtime carries its own guard for the `auto_widen` sidecar
/// case, which the planner cannot enumerate.
fn validate_output_mapping_columns(
    node_name: &str,
    output: &crate::config::SinkConfig,
    upstream: &Row,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) {
    let Some(mapping) = output.mapping.as_ref() else {
        return;
    };
    // A Sink's upstream row carries only bare names: a Combine publishes its
    // merged row as `QualifiedField::bare`, so the qualified spelling never
    // reaches here and matching it would accept a name the record cannot carry.
    let is_available = |wanted: &str| upstream.fields().any(|(qf, _)| qf.name.as_ref() == wanted);
    let excluded = |name: &str| {
        output
            .exclude
            .as_ref()
            .is_some_and(|ex| ex.iter().any(|x| x == name))
    };

    // A row that reserves the `auto_widen` sidecar can carry columns the
    // declared set does not name — but only where the sidecar is expanded, and
    // expansion is gated on `include_unmapped: true`. Under `false` the sidecar
    // stays packed and a mapping naming one of its columns genuinely cannot
    // resolve, so the gate still applies there.
    let sidecar_expands = upstream.has_field(crate::config::pipeline_node::WIDENED_SIDECAR_COLUMN)
        && output.include_unmapped;

    // Suggest only author-facing columns the Sink may name: the engine-stamped
    // namespaces (`$widened`, `$source.*`, `$ck.*`) are not names a pipeline
    // author types, and a caller column a composition port carries undeclared
    // would lead to E169.
    let candidates: Vec<String> = nameable_columns(upstream)
        .into_iter()
        .filter(|n| !n.starts_with('$'))
        .map(str::to_string)
        .collect();
    let candidate_refs: Vec<&str> = candidates.iter().map(String::as_str).collect();
    let available = if candidate_refs.is_empty() {
        "(none)".to_string()
    } else {
        candidate_refs
            .iter()
            .map(|c| format!("'{c}'"))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut missing: Vec<(usize, &str)> = Vec::new();
    for (index, entry) in mapping.entries().iter().enumerate() {
        let source = entry.source.as_str();
        if !is_available(source) && !missing.iter().any(|(_, name)| *name == source) {
            missing.push((index, source));
        }
    }
    for (entry_index, name) in missing {
        let entry = &mapping.entries()[entry_index];
        let near = cxl::resolve::levenshtein::best_match(name, &candidate_refs, 3);
        // Edit distance is useful help only after absence is known. A sidecar
        // can legitimately carry a drift column whose spelling happens to be
        // close to a declared column, so similarity cannot turn an unknown
        // runtime fact into a compile-time error. W365 reports the item after
        // the run if no delivered record actually carried it.
        if sidecar_expands {
            continue;
        }
        let help = match near {
            Some(near) => {
                let corrected =
                    crate::config::output_mapping::render_mapping_item(&entry.output, near);
                format!(
                    "did you mean `{corrected}`? Columns available at output \
                     {node_name:?}: {available}"
                )
            }
            None => format!(
                "columns available at output {node_name:?}: {available}. If '{name}' reaches \
                 the sink only through `on_unmapped: auto_widen`, either set \
                 `include_unmapped: true` on this output so the sidecar is expanded before \
                 the mapping reads it, or declare the column in the source's `schema:` block"
            ),
        };
        diags.push(
            Diagnostic::error(
                "E365",
                format!(
                    "output {node_name:?}: `mapping:` reads column '{name}', which does not \
                     exist at this point in the pipeline"
                ),
                LabeledSpan::primary(
                    mapping
                        .entry_line(entry_index)
                        .map(Span::line_only)
                        .unwrap_or(span),
                    String::new(),
                ),
            )
            .with_help(help),
        );
    }

    // Collision gate. Only `include_unmapped: true` appends anything, so only
    // it can collide. A column some entry READS is consumed by that entry and
    // never appended; one this output excludes is removed before the append.
    if !output.include_unmapped {
        return;
    }
    let mut collisions: Vec<(&str, usize)> = Vec::new();
    for (qf, _) in upstream.fields() {
        let column = qf.name.as_ref();
        let mapped_entry = mapping
            .entries()
            .iter()
            .enumerate()
            .find(|(_, entry)| entry.output == column);
        if column.starts_with('$')
            || mapping.claims_source(column)
            || excluded(column)
            || mapped_entry.is_none()
            || collisions.iter().any(|(name, _)| *name == column)
        {
            continue;
        }
        collisions.push((column, mapped_entry.expect("checked above").0));
    }
    for (column, entry_index) in collisions {
        let source = mapping.entries()[entry_index].source.as_str();
        diags.push(
            Diagnostic::error(
                "E364",
                format!(
                    "output {node_name:?}: `mapping:` writes column '{column}', and \
                     `include_unmapped: true` would also carry the upstream column of that name \
                     through beside it — the file would carry two '{column}' columns and readers \
                     would resolve the wrong one"
                ),
                LabeledSpan::primary(
                    mapping
                        .entry_line(entry_index)
                        .map(Span::line_only)
                        .unwrap_or(span),
                    String::new(),
                ),
            )
            .with_help(format!(
                "rename the mapped column to a name the upstream does not already use, add \
                 '{column}' to this output's `exclude:` so only the mapped column is written, \
                 or set `include_unmapped: false` so the block is the whole output. The \
                 colliding item reads '{source}'."
            )),
        );
    }
}

fn synthetic_typed_program(output_row: Row) -> TypedProgram {
    TypedProgram {
        program: cxl::ast::Program {
            statements: Vec::new(),
            span: cxl::lexer::Span::default(),
        },
        bindings: Vec::new(),
        runtime_modules: Arc::new(Default::default()),
        types: Vec::new(),
        field_types: IndexMap::new(),
        regexes: Vec::new(),
        node_count: 0,
        output_row,
        source: None,
    }
}

/// Borrowed inputs for binding one `PipelineNode::Reshape`.
///
/// Bundles the read-only node identity (name/config/upstream/span/vars)
/// so `bind_reshape` keeps only the mutable sinks (diags/artifacts/schema
/// map) as loose parameters, mirroring `CombineNodeBinding`.
struct ReshapeNodeBinding<'a> {
    name: &'a str,
    config: &'a ReshapeBody,
    upstream: &'a Row,
    span: Span,
    scoped_vars: &'a cxl::resolve::ScopedVarsRegistry,
    /// This reshape's stable [`PlanNodeId`], resolved from the bind walk's
    /// scope name→id map. Keys the `typed` table entry so a reshape named
    /// the same in two composition scopes does not collide.
    id: PlanNodeId,
}

/// Bind a `PipelineNode::Reshape`: validate `partition_by` against the
/// upstream schema, typecheck each rule's `when` / `set` / `overrides`
/// CXL against that schema, enforce the group-identity and synthesis
/// invariants, and publish the audit-widened output row.
///
/// Validation enforced here (structural checks use code E200; a rule
/// expression's own CXL compilation uses E202/E203/E200 by failure class):
/// - every `partition_by` and `order_by` field exists upstream;
/// - no `order_by` field writes `null_order: drop` (the list only orders
///   the rows of a group); the validated placement-only list goes to
///   `artifacts.group_order_by`;
/// - each rule's `when` predicate and every `set` / `overrides` value
///   expression typechecks against the upstream row;
/// - `set` targets must already exist upstream (Reshape mutates, it does
///   not widen the user schema) and must not be a `partition_by` field or
///   an engine-stamped `$`-namespaced column — group identity must
///   survive Reshape;
/// - `copy_from: none` requires every upstream column to appear in
///   `overrides`, so a synthesized row is never silently all-null;
/// - no rule fragment references `$doc` document context, which the
///   per-group spill round-trip cannot preserve.
///
/// `partition_by` carries no arity rule: an empty list is the degenerate
/// key every record shares, which groups the whole input at once.
fn bind_reshape(
    node: &ReshapeNodeBinding<'_>,
    diags: &mut Vec<Diagnostic>,
    artifacts: &mut CompileArtifacts,
    schema_by_name: &mut HashMap<String, Row>,
) {
    let &ReshapeNodeBinding {
        name,
        config,
        upstream,
        span,
        scoped_vars,
        id,
    } = node;
    let mut ok = true;

    // Every column a Reshape names outside CXL must be one the body may name.
    // A `set` target and an override name count: both require the column to
    // exist, and every row the rule does not touch keeps the caller's value,
    // so a body writing one still depends on the caller having it.
    let mut named: Vec<&str> = Vec::new();
    named.extend(config.partition_by.iter().map(String::as_str));
    named.extend(config.order_by.iter().map(|sf| sf.field.as_str()));
    for rule in &config.rules {
        if let Some(mutate) = &rule.mutate {
            named.extend(mutate.set.keys().map(String::as_str));
        }
        if let Some(synth) = &rule.synthesize {
            named.extend(synth.overrides.keys().map(String::as_str));
        }
    }
    if refuse_undeclared_names(upstream, name, named, span, diags) {
        return;
    }

    // `partition_by` fields must exist upstream.
    for pk in &config.partition_by {
        if !upstream.has_field(pk) {
            diags.push(
                Diagnostic::error(
                    "E200",
                    format!(
                        "reshape {name:?}: partition_by field {pk:?} is not present in the \
                         upstream schema"
                    ),
                    LabeledSpan::primary(span, String::new()),
                )
                .with_help(
                    "declare the column in the source's `schema:` block or emit it from an \
                     upstream transform",
                ),
            );
            ok = false;
        }
    }

    // `order_by` fields must exist upstream, and only place nulls.
    let mut order_by = Vec::with_capacity(config.order_by.len());
    for sf in &config.order_by {
        if !upstream.has_field(&sf.field) {
            diags.push(Diagnostic::error(
                "E200",
                format!(
                    "reshape {name:?}: order_by field {:?} is not present in the upstream schema",
                    sf.field
                ),
                LabeledSpan::primary(span, String::new()),
            ));
            ok = false;
        }
        match OrderField::from_authored(sf.clone(), OrderingSite::GroupOrderBy) {
            Ok(field) => order_by.push(field),
            Err(refused) => {
                diags.push(Diagnostic::error(
                    "E200",
                    format!("reshape {}: {refused}", name.quoted_name()),
                    LabeledSpan::primary(span, String::new()),
                ));
                ok = false;
            }
        }
    }

    // Per-rule typechecked CXL programs, assembled into the on-node
    // `CompiledReshapeRule` set that lowering stamps onto the node. Built
    // only when every fragment typechecks (a single failure sets `ok =
    // false`); the partially-built vec is consumed only on the `ok` path
    // below, so the node is never lowered with a short rule set.
    let mut compiled_rules: Vec<crate::plan::execution::CompiledReshapeRule> =
        Vec::with_capacity(config.rules.len());
    // The same typed programs paired with a fragment label, retained so the
    // `$doc` envelope-reference guard below can walk them once every per-rule
    // typecheck has run. `label` names the rule fragment for the diagnostic.
    let mut guard_fragments: Vec<(String, Arc<TypedProgram>)> = Vec::new();

    // Per-rule predicate + assignment typechecks and invariants.
    for rule in &config.rules {
        // `when` predicate compiles as a row filter.
        let when_src = format!("filter {}", rule.when.source);
        let when_prog = match typecheck_cxl(
            name,
            &format!("{name}:{}:when", rule.name),
            CxlSource {
                text: &when_src,

                authored: Authored::Expression {
                    key: "when",
                    text: &rule.when.source,
                    at: "filter ".len(),
                },
            },
            upstream,
            AggregateMode::Row,
            span,
            scoped_vars,
        ) {
            Ok(typed) => {
                let prog = Arc::new(typed);
                guard_fragments.push((format!("rule {:?} `when`", rule.name), Arc::clone(&prog)));
                Some(prog)
            }
            Err(d) => {
                diags.extend(d);
                ok = false;
                None
            }
        };

        let mut set: Vec<(String, Arc<TypedProgram>)> = Vec::new();
        if let Some(mutate) = &rule.mutate {
            for (field, value) in &mutate.set {
                // Group identity must survive Reshape: a `set` cannot
                // target a partition key or an engine-stamped column.
                if config.partition_by.iter().any(|p| p == field) {
                    diags.push(
                        Diagnostic::error(
                            "E200",
                            format!(
                                "reshape {name:?} rule {:?}: mutate.set may not write \
                                 partition_by field {field:?} — group identity must survive \
                                 Reshape",
                                rule.name
                            ),
                            LabeledSpan::primary(span, String::new()),
                        )
                        .with_help("drop this assignment, or partition on a different field"),
                    );
                    ok = false;
                } else if field.starts_with('$') {
                    diags.push(Diagnostic::error(
                        "E200",
                        format!(
                            "reshape {name:?} rule {:?}: mutate.set may not write the \
                             engine-stamped column {field:?}",
                            rule.name
                        ),
                        LabeledSpan::primary(span, String::new()),
                    ));
                    ok = false;
                } else if !upstream.has_field(field) {
                    diags.push(
                        Diagnostic::error(
                            "E200",
                            format!(
                                "reshape {name:?} rule {:?}: mutate.set target {field:?} is not \
                                 present in the upstream schema — Reshape mutates existing \
                                 columns, it does not add new ones",
                                rule.name
                            ),
                            LabeledSpan::primary(span, String::new()),
                        )
                        .with_help("emit the column from an upstream transform first"),
                    );
                    ok = false;
                }
                let assignment = format!("emit {field} = ");
                match typecheck_cxl(
                    name,
                    &format!("{name}:{}:set.{field}", rule.name),
                    CxlSource {
                        text: &format!("{assignment}{}", value.source),

                        authored: Authored::Expression {
                            key: field,
                            text: &value.source,
                            at: assignment.len(),
                        },
                    },
                    upstream,
                    AggregateMode::Row,
                    span,
                    scoped_vars,
                ) {
                    Ok(typed) => {
                        let prog = Arc::new(typed);
                        guard_fragments.push((
                            format!("rule {:?} `mutate.set.{field}`", rule.name),
                            Arc::clone(&prog),
                        ));
                        set.push((field.clone(), prog));
                    }
                    Err(d) => {
                        diags.extend(d);
                        ok = false;
                    }
                }
            }
        }

        let compiled_synth = if let Some(synth) = &rule.synthesize {
            // `copy_from: none` requires every upstream user column to be
            // supplied by `overrides`, so a synthesized row is never
            // silently all-null.
            if synth.copy_from == CopyFrom::None {
                for (qf, _) in upstream.fields() {
                    let col = qf.name.as_ref();
                    // Engine columns are the engine's to fill. A column a
                    // composition port carries undeclared is not the body's
                    // to name, so it cannot be overridden; a synthesized row
                    // leaves it empty, as it did before a body bound against
                    // the caller's row.
                    if col.starts_with('$') || upstream.undeclared_carried(col).is_some() {
                        continue;
                    }
                    if !synth.overrides.keys().any(|k| k == col) {
                        diags.push(
                            Diagnostic::error(
                                "E200",
                                format!(
                                    "reshape {name:?} rule {:?}: synthesize with \
                                     `copy_from: none` must override every column, but {col:?} \
                                     is missing from `overrides`",
                                    rule.name
                                ),
                                LabeledSpan::primary(span, String::new()),
                            )
                            .with_help(
                                "add the column to `overrides`, or use `copy_from: trigger` to \
                                 inherit the trigger row's values",
                            ),
                        );
                        ok = false;
                    }
                }
            }
            let mut overrides: Vec<(String, Arc<TypedProgram>)> = Vec::new();
            for (field, value) in &synth.overrides {
                let assignment = format!("emit {field} = ");
                match typecheck_cxl(
                    name,
                    &format!("{name}:{}:override.{field}", rule.name),
                    CxlSource {
                        text: &format!("{assignment}{}", value.source),

                        authored: Authored::Expression {
                            key: field,
                            text: &value.source,
                            at: assignment.len(),
                        },
                    },
                    upstream,
                    AggregateMode::Row,
                    span,
                    scoped_vars,
                ) {
                    Ok(typed) => {
                        let prog = Arc::new(typed);
                        guard_fragments.push((
                            format!("rule {:?} `synthesize.overrides.{field}`", rule.name),
                            Arc::clone(&prog),
                        ));
                        overrides.push((field.clone(), prog));
                    }
                    Err(d) => {
                        diags.extend(d);
                        ok = false;
                    }
                }
            }
            Some(crate::plan::execution::CompiledReshapeSynth {
                copy_from: synth.copy_from,
                overrides,
            })
        } else {
            None
        };

        // Assemble the rule only when its `when` typechecked. On any fragment
        // failure `ok` is already false, so a partial `compiled_rules` is
        // never consumed — the node is not lowered.
        if let Some(when) = when_prog {
            compiled_rules.push(crate::plan::execution::CompiledReshapeRule {
                name: rule.name.clone(),
                when,
                set,
                synth: compiled_synth,
            });
        }
    }

    // Reject `$doc` envelope references in any rule fragment. Reshape spills
    // its per-group input records to disk under memory pressure and re-runs
    // the rules on reload, but the spill envelope does not carry document
    // context — a reloaded record resolves `$doc.*` to null, so a rule that
    // reads `$doc` would produce output that depends on whether the group
    // spilled (i.e. on the memory budget). Rather than silently differ, fail
    // loud here until envelope context survives the spill round-trip.
    if ok {
        // Reshape rule fragments carry no scope ambiguity (they are
        // labels within one node's rule set), so attribute by the bare
        // label `String`.
        let programs: Vec<(String, &TypedProgram)> = guard_fragments
            .iter()
            .map(|(label, typed)| (label.clone(), typed.as_ref()))
            .collect();
        let doc_paths = cxl::analyzer::doc_paths::collect_doc_paths(&programs);
        // Any `$doc` reference disqualifies the rule — both statically-resolved
        // paths (`by_node`) and accesses whose index could not be resolved
        // (`unresolvable`, e.g. a computed index). The first fragment label, in
        // deterministic order, names the offender.
        let referencing_fragment = doc_paths
            .by_node
            .values()
            .flat_map(|labels| labels.iter())
            .map(String::as_str)
            .chain(
                doc_paths
                    .unresolvable
                    .iter()
                    .map(|(label, _)| label.as_str()),
            )
            .min();
        if let Some(first_label) = referencing_fragment {
            diags.push(
                Diagnostic::error(
                    "E200",
                    format!(
                        "reshape {name:?}: {first_label} references `$doc` document context, \
                         which Reshape rules cannot use yet — Reshape spills per-group state to \
                         disk under memory pressure and the spill round-trip does not preserve \
                         envelope context, so a `$doc` reference would resolve differently for a \
                         spilled group. This is a current limitation."
                    ),
                    LabeledSpan::primary(span, String::new()),
                )
                .with_help(
                    "move the `$doc` lookup into an upstream Transform that copies the value into \
                     a record column, then reference that column in the Reshape rule",
                ),
            );
            ok = false;
        }
    }

    if !ok {
        return;
    }

    // Output row = upstream columns ++ the three `$meta.*` audit columns
    // Reshape stamps on every output record.
    let mut declared = upstream.declared_map().clone();
    use crate::config::pipeline_node::{
        RESHAPE_MUTATED_BY_COLUMN, RESHAPE_SYNTHESIZED_BY_COLUMN, RESHAPE_SYNTHETIC_COLUMN,
    };
    declared.insert(QualifiedField::bare(RESHAPE_SYNTHETIC_COLUMN), Type::Bool);
    declared.insert(
        QualifiedField::bare(RESHAPE_SYNTHESIZED_BY_COLUMN),
        Type::String,
    );
    declared.insert(
        QualifiedField::bare(RESHAPE_MUTATED_BY_COLUMN),
        Type::String,
    );
    let mut out = Row::from_parts(declared, upstream.declared_span, upstream.tail.clone());
    // A `set` or override keeps the caller's value on every row its rule does
    // not touch, so a carried column stays the caller's through Reshape. The
    // audit columns are the engine's.
    out.inherit_marks(upstream, |_| true);
    for audit in [
        RESHAPE_SYNTHETIC_COLUMN,
        RESHAPE_SYNTHESIZED_BY_COLUMN,
        RESHAPE_MUTATED_BY_COLUMN,
    ] {
        out.mark(QualifiedField::bare(audit), ColumnMark::Own);
    }
    schema_by_name.insert(name.to_string(), out.clone());
    // Bind→lowering handoff: lowering stamps these onto
    // `PlanNode::Reshape.compiled_rules`. Every rule is present here (each
    // rule's `when` typechecked, else `ok` was false and we returned above).
    artifacts
        .reshape_compiled
        .insert(id, Arc::new(compiled_rules));
    artifacts.group_order_by.insert(id, order_by);
    artifacts.typed_insert(id, Arc::new(synthetic_typed_program(out)));
}

/// Borrowed inputs for binding one `PipelineNode::Cull`.
///
/// Bundles the read-only node identity (name/config/upstream/span/vars)
/// so `bind_cull` keeps only the mutable sinks (diags/artifacts/schema
/// map) as loose parameters, mirroring `ReshapeNodeBinding`.
struct CullNodeBinding<'a> {
    name: &'a str,
    config: &'a CullBody,
    upstream: &'a Row,
    span: Span,
    scoped_vars: &'a cxl::resolve::ScopedVarsRegistry,
    /// This cull's stable [`PlanNodeId`], resolved from the bind walk's
    /// scope name→id map. Keys the `typed` table entry so a cull named the
    /// same in two composition scopes does not collide.
    id: PlanNodeId,
}

/// Bind a `PipelineNode::Cull`: validate `partition_by` / `order_by`
/// against the upstream schema, typecheck each rule's `drop_group_when`
/// predicate in aggregate context, validate the `removed_to` port name,
/// and publish the (unchanged) upstream row as the node's output.
///
/// Validation enforced here (structural checks use code E200; a rule
/// predicate's own CXL compilation uses E202/E203/E200 by failure class):
/// - every `partition_by` and `order_by` field exists upstream;
/// - no `order_by` field writes `null_order: drop` (the list only orders
///   the rows of a group); the validated placement-only list goes to
///   `artifacts.group_order_by`;
/// - at least one removal rule is declared;
/// - each rule's `drop_group_when` predicate typechecks against the
///   upstream row in aggregate context (group-by = `partition_by`), so a
///   group-level predicate like `count(*) > 100` is well-formed;
/// - `removed_to` is non-empty and is not the node's own name (the main
///   output port), so the two output ports are distinguishable.
///
/// `partition_by` carries no arity rule: an empty list is the degenerate
/// key every record shares, which groups the whole input at once.
///
/// Cull does not widen: both the main and `removed_to` output ports carry
/// the unchanged upstream row, so the bound output schema equals upstream.
fn bind_cull(
    node: &CullNodeBinding<'_>,
    diags: &mut Vec<Diagnostic>,
    artifacts: &mut CompileArtifacts,
    schema_by_name: &mut HashMap<String, Row>,
) {
    let &CullNodeBinding {
        name,
        config,
        upstream,
        span,
        scoped_vars,
        id,
    } = node;
    let mut ok = true;

    let named = config
        .partition_by
        .iter()
        .map(String::as_str)
        .chain(config.order_by.iter().map(|sf| sf.field.as_str()));
    if refuse_undeclared_names(upstream, name, named, span, diags) {
        return;
    }

    // `partition_by` fields must exist upstream.
    for pk in &config.partition_by {
        if !upstream.has_field(pk) {
            diags.push(
                Diagnostic::error(
                    "E200",
                    format!(
                        "cull {name:?}: partition_by field {pk:?} is not present in the \
                         upstream schema"
                    ),
                    LabeledSpan::primary(span, String::new()),
                )
                .with_help(
                    "declare the column in the source's `schema:` block or emit it from an \
                     upstream transform",
                ),
            );
            ok = false;
        }
    }

    // `order_by` fields must exist upstream, and only place nulls.
    let mut order_by = Vec::with_capacity(config.order_by.len());
    for sf in &config.order_by {
        if !upstream.has_field(&sf.field) {
            diags.push(Diagnostic::error(
                "E200",
                format!(
                    "cull {name:?}: order_by field {:?} is not present in the upstream schema",
                    sf.field
                ),
                LabeledSpan::primary(span, String::new()),
            ));
            ok = false;
        }
        match OrderField::from_authored(sf.clone(), OrderingSite::GroupOrderBy) {
            Ok(field) => order_by.push(field),
            Err(refused) => {
                diags.push(Diagnostic::error(
                    "E200",
                    format!("cull {}: {refused}", name.quoted_name()),
                    LabeledSpan::primary(span, String::new()),
                ));
                ok = false;
            }
        }
    }

    // `removed_to` names a distinct side-output port. An empty name has
    // no resolvable edge, and reusing the node's own name would collide
    // the side-output port with the main output, so both are rejected.
    if config.removed_to.trim().is_empty() {
        diags.push(
            Diagnostic::error(
                "E200",
                format!("cull {name:?}: removed_to must name a non-empty side-output port"),
                LabeledSpan::primary(span, String::new()),
            )
            .with_help(
                "set `removed_to:` to the port name downstream nodes draw removed rows from",
            ),
        );
        ok = false;
    } else if config.removed_to == name {
        diags.push(
            Diagnostic::error(
                "E200",
                format!(
                    "cull {name:?}: removed_to {:?} collides with the node's own name (the main \
                     output port) — the two output ports must be distinguishable",
                    config.removed_to
                ),
                LabeledSpan::primary(span, String::new()),
            )
            .with_help("rename `removed_to:` to a distinct port name"),
        );
        ok = false;
    }

    // A Cull with no removal rule never removes anything — it is a no-op
    // operator with two ports, almost certainly an authoring mistake. Reject
    // it rather than silently route every record to the main port.
    if config.rules.is_empty() {
        diags.push(
            Diagnostic::error(
                "E200",
                format!("cull {name:?}: rules must contain at least one removal rule"),
                LabeledSpan::primary(span, String::new()),
            )
            .with_help(
                "add a rule with a `drop_group_when:` predicate, or remove the Cull node entirely",
            ),
        );
        ok = false;
    }

    if !ok {
        return;
    }

    // Unlike `bind_reshape`, Cull has no `$doc` envelope-reference guard, and
    // that asymmetry is intentional: Cull folds the decision aggregate over
    // the live drained input at the START of dispatch — before any group-buffer
    // spill — with each record's intact document context, and never
    // re-evaluates the predicate after a spill. So a `$doc` reference in
    // `drop_group_when` resolves deterministically. (Reshape needs the guard
    // because it re-runs its rules at finalize, after a spill that drops `$doc`
    // context.) If Cull's decision aggregate is ever allowed to spill, mirror
    // Reshape's guard here.

    // Build the decision program the runtime evaluates per group: a group is
    // removed when ANY rule's `drop_group_when` holds, so the decision is the
    // OR of every rule's predicate emitted to `CULL_DROP_DECISION_COLUMN`. The
    // aggregates (`count`, `sum`, …) sit directly in the emit body, where
    // `extract_aggregates` folds them at lowering.
    //
    // Each rule is parsed on its own — as an `emit __cull_pred = <predicate>`
    // program — and the parsed predicate expressions are OR-combined at the
    // AST level rather than by joining rule source strings into one `(r1) or
    // (r2)` line. Per-rule parsing keeps a trailing `#` line-comment (which
    // runs to end-of-line) scoped to its own rule instead of swallowing the
    // following disjuncts and the closing paren, which a single joined source
    // line cannot do. Each rule's node ids are relocated into a dense,
    // collision-free span so the resolver/typechecker side-tables (indexed by
    // `NodeId`) stay consistent; the OR-chain and wrapping emit mint fresh ids
    // above every relocated rule. Each rule's byte-offset spans are likewise
    // rebased from its parse-wrapper coordinates into `decision_src` — the
    // reconstructed one-line source stamped on the typed program — so a
    // runtime eval-error caret (whose offsets are relative to that stamped
    // source) underlines the failing rule's text rather than an off-by-prefix
    // slice. Lowering runs `extract_aggregates` over the result and stamps it
    // onto `PlanNode::Cull.compiled`/`.typed`. `rules` is non-empty (rejected
    // above).
    let agg_mode = AggregateMode::GroupBy {
        group_by_fields: config.partition_by.iter().cloned().collect(),
    };

    // Build the decision source as the rules are parsed, so it is the single
    // coordinate space every predicate's spans rebase into. It is stamped as
    // the typed program's display source (eval-error / explain rendering) and
    // is never re-parsed, so a rule's `#` comment inside it is inert. Each
    // rule is parenthesized and joined by ` or `, matching the AST-level
    // OR-fold below.
    let mut decision_src = format!(
        "emit {} = ",
        crate::config::pipeline_node::CULL_DROP_DECISION_COLUMN
    );
    let mut disjuncts: Vec<Expr> = Vec::with_capacity(config.rules.len());
    let mut next_id: u32 = 0;
    let mut parse_failed = false;
    for (i, rule) in config.rules.iter().enumerate() {
        if i > 0 {
            decision_src.push_str(" or ");
        }
        decision_src.push('(');
        // Byte offset where this rule's source text begins in `decision_src`
        // (just past the opening paren) — the target coordinate the parsed
        // predicate's spans rebase onto.
        let source_start = decision_src.len() as u32;
        decision_src.push_str(&rule.drop_group_when.source);
        decision_src.push(')');

        match parse_cull_rule_predicate(
            &format!("{name}:{}:drop_group_when", rule.name),
            &rule.drop_group_when.source,
            next_id,
            span,
        ) {
            Ok((mut predicate, node_count)) => {
                // The predicate's spans are relative to a source beginning
                // right after `CULL_RULE_PARSE_PREFIX`; shift them so they
                // index `decision_src` instead. `source_start` is always past
                // that prefix (the decision prefix is strictly longer), so the
                // delta is non-negative.
                let delta = source_start - CULL_RULE_PARSE_PREFIX.len() as u32;
                cxl::ast::rebase_expr_spans(&mut predicate, delta);
                disjuncts.push(predicate);
                next_id += node_count;
            }
            Err(d) => {
                diags.push(d);
                parse_failed = true;
            }
        }
    }
    if parse_failed {
        return;
    }

    // OR-fold the parsed rule predicates left-to-right, minting one fresh
    // `NodeId` per inserted `or`. A single-rule Cull needs no `or`: its lone
    // predicate is the decision body.
    let body = disjuncts
        .into_iter()
        .reduce(|lhs, rhs| {
            let node_id = cxl::ast::NodeId(next_id);
            next_id += 1;
            let or_span = lhs.span();
            Expr::Binary {
                node_id,
                op: cxl::ast::BinOp::Or,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                span: or_span,
            }
        })
        .expect("cull rules are non-empty");

    let body_span = body.span();
    let emit_id = cxl::ast::NodeId(next_id);
    next_id += 1;
    let node_count = next_id;
    let decision_program = cxl::ast::Program {
        statements: vec![Statement::Emit {
            node_id: emit_id,
            name: crate::config::pipeline_node::CULL_DROP_DECISION_COLUMN.into(),
            expr: body,
            target: cxl::ast::EmitTarget::Field,
            span: body_span,
        }],
        span: cxl::lexer::Span::default(),
    };

    // Typecheck the combined program first — the happy path needs only this
    // one pass. On failure, re-typecheck each rule on its own (same aggregate
    // mode, with `or false` forcing a boolean operand) purely to attribute the
    // diagnostic to the offending rule; fall back to the combined diagnostic
    // if no single rule reproduces it. Either way, skip lowering (the node is
    // absent from `cull_decision_typed`).
    let decision_label = format!("{name}:drop_group_when");
    let decision_ctx = CxlTypecheckCtx {
        node: name,
        node_name: &decision_label,
        schema: upstream,
        mode: &agg_mode,
        span,
        scoped_vars,
        authored: Authored::Synthesized,
    };
    match typecheck_parsed_program(
        &decision_ctx,
        decision_program,
        node_count,
        std::sync::Arc::from(decision_src.as_str()),
    ) {
        Ok(typed) => {
            artifacts.cull_decision_typed.insert(id, Arc::new(typed));
        }
        Err(combined) => {
            let mut attributed = false;
            for rule in &config.rules {
                let rule_label = format!("{name}:{}:drop_group_when", rule.name);
                let rule_ctx = CxlTypecheckCtx {
                    node: name,
                    node_name: &rule_label,
                    schema: upstream,
                    mode: &agg_mode,
                    span,
                    scoped_vars,
                    authored: Authored::Expression {
                        key: "drop_group_when",
                        text: &rule.drop_group_when.source,
                        at: CULL_RULE_PARSE_PREFIX.len(),
                    },
                };
                if let Err(rule_diags) =
                    typecheck_cull_rule(&rule_ctx, &rule.drop_group_when.source)
                {
                    // Two rules naming one undeclared column are one fault.
                    for d in rule_diags {
                        if !diags
                            .iter()
                            .any(|seen| seen.code == d.code && seen.message == d.message)
                        {
                            diags.push(d);
                        }
                    }
                    attributed = true;
                }
            }
            if !attributed {
                diags.extend(combined);
            }
            return;
        }
    }

    // Cull does not widen: both output ports carry the unchanged upstream
    // row. Publish it as the node's output so downstream binding sees the
    // identical schema on the main and `removed_to` ports.
    //
    // This synthetic upstream-row program is kept in `typed` (distinct from
    // the decision program in `cull_decision_typed`) deliberately: lowering's
    // `schema_from_bound()` reads it to build the node's port `output_schema`
    // (the unchanged upstream row), whereas the decision program's own
    // output_row is the narrow `[partition_by, __cull_drop_decision__]` shape
    // — wrong for the ports. The two `id`-keyed entries serve different
    // consumers and must stay separate.
    let out = upstream.clone();
    schema_by_name.insert(name.to_string(), out.clone());
    artifacts.typed_insert(id, Arc::new(synthetic_typed_program(out)));
    artifacts.group_order_by.insert(id, order_by);
}

// ─── Internal recursive bind_schema ─────────────────────────────────

/// Internal recursive bind_schema. Walks nodes in topological order,
/// seeds source schemas, typechecks CXL-bearing nodes, and recurses
/// into composition bodies via `bind_composition`.
///
/// `node_ids` maps each node name in THIS scope to its [`PlanNodeId`] —
/// for the top-level walk it is built from `top_level_node_ids`; for a
/// composition body it is the per-body map pre-minted by
/// `bind_composition` before this recursion. Names are unique within one
/// scope (config validation rejects duplicates), so the resolution is
/// unambiguous. Every per-node producer keys its side-table entry by the
/// id resolved here, so a node named the same across two scopes gets
/// distinct ids and cannot collide.
fn bind_schema_inner(
    nodes: &[Spanned<PipelineNode>],
    node_ids: &HashMap<String, PlanNodeId>,
    diags: &mut Vec<Diagnostic>,
    bind_ctx: &mut BindContext<'_>,
    artifacts: &mut CompileArtifacts,
    schema_by_name: &mut HashMap<String, Row>,
    declared_multiple_by_name: &mut HashMap<String, BTreeSet<String>>,
) -> HashSet<String> {
    let span_for = |spanned: &Spanned<PipelineNode>| -> Span {
        let line = spanned.referenced.line() as u32;
        if line > 0 {
            Span::line_only(line)
        } else {
            Span::SYNTHETIC
        }
    };

    // Per-node set of upstream-reachable Source names, plus a flag per
    // Source recording whether it declared `watermark.column`. Populated
    // as we walk in topological order; consumed by the time-windowed
    // aggregate guard (E156) below. A node's set is the union of its
    // input nodes' sets, so the check at an Aggregate sees every Source
    // that can deliver records to it through any path.
    //
    // `crate::config::pipeline::build_node_source_sets` is a parallel
    // source-attribution walk over the same DAG, sharing this walk's shape
    // (Source-seeds-self, declaration-order topo order, union of inputs).
    // It intentionally diverges where document context — not mere
    // reachability — matters: it narrows a `Combine` to its driving input
    // and resolves a `Composition` through its full `inputs:` port set.
    // Keep the shared shape in sync across both when either changes.
    let mut source_has_watermark: HashMap<String, bool> = HashMap::new();
    let mut upstream_sources: HashMap<String, HashSet<String>> = HashMap::new();
    // This scope's Source nodes, which a composition call's E102 points at.
    let source_nodes: HashSet<String> = nodes
        .iter()
        .filter(|spanned| matches!(spanned.value, PipelineNode::Source { .. }))
        .map(|spanned| spanned.value.name().to_string())
        .collect();
    // Bind each node after the nodes of this scope it reads. An author may
    // list a node before its input; binding in list order left such a node
    // unbound, so it escaped every check and wrote no rows.
    let (order, unplaced) = bind_order(nodes);
    // The nodes of this scope that failed to bind: those on a cycle, each
    // whose own bind raised an error, and each left without a row because a
    // node it reads failed. A call reading one of them is not told a second
    // time that it reads a node with no row.
    let mut failed_nodes: HashSet<String> = unplaced
        .iter()
        .map(|&i| nodes[i].value.name().to_string())
        .collect();
    // The node being bound (its index) and where its diagnostics start.
    let mut binding: Option<(usize, usize)> = None;
    fn settle(
        binding: &mut Option<(usize, usize)>,
        nodes: &[Spanned<PipelineNode>],
        diags: &[Diagnostic],
        schema_by_name: &HashMap<String, Row>,
        failed_nodes: &mut HashSet<String>,
    ) {
        let Some((i, from)) = binding.take() else {
            return;
        };
        let node = &nodes[i].value;
        let name = node.name();
        let own_error = diags[from..].iter().any(|d| d.severity == Severity::Error);
        let upstream_failed = !schema_by_name.contains_key(name)
            && node_input_names(node)
                .iter()
                .any(|input| failed_nodes.contains(*input));
        if own_error || upstream_failed {
            failed_nodes.insert(name.to_string());
        }
    }
    // In a body, the inputs no node or port names, the nodes that read
    // themselves and the cycles; each such node has failed before it binds.
    let mut body_fixes: Vec<BodyInputFix> = Vec::new();
    if bind_ctx.depth > 0 {
        let ports: HashSet<&str> = schema_by_name.keys().map(String::as_str).collect();
        let faults = body_input_diagnostics(
            &BodyInputs {
                nodes,
                unplaced: &unplaced,
                ports: &ports,
                declared_ports: &bind_ctx.body_ports,
                enclosing: &bind_ctx.enclosing_scope_names,
                outputs: &bind_ctx.body_outputs,
            },
            diags,
        );
        failed_nodes.extend(faults.failed);
        body_fixes = faults.fixes;
    }

    for &index in &order {
        settle(
            &mut binding,
            nodes,
            diags,
            schema_by_name,
            &mut failed_nodes,
        );
        let spanned = &nodes[index];
        let node = &spanned.value;
        let name = node.name().to_string();
        let span = span_for(spanned);
        // A body node that cannot receive a row (it reads itself, a name
        // that is nothing in the body, or sits on or after a cycle) was
        // reported above; binding it would only raise followers.
        if bind_ctx.depth > 0 && failed_nodes.contains(&name) {
            continue;
        }
        binding = Some((index, diags.len()));
        // The node's stable id for this scope. Pre-minted before the walk
        // (top-level vector zip, or body pre-mint), so every visited node
        // is present. A missing entry can only mean an internal pre-mint /
        // walk desync — fail loudly with an internal-error diagnostic
        // rather than mint a colliding fresh id or silently drop the node.
        let node_id = match node_ids.get(&name) {
            Some(&id) => id,
            None => {
                debug_assert!(
                    false,
                    "bind_schema_inner: node {name:?} has no pre-minted PlanNodeId \
                     (pre-mint/walk desync)"
                );
                diags.push(Diagnostic::error(
                    "E000",
                    format!(
                        "internal error: node {name:?} has no pre-minted PlanNodeId \
                         during schema binding"
                    ),
                    LabeledSpan::primary(span, String::new()),
                ));
                continue;
            }
        };

        // Compute the upstream-source set for this node BEFORE the
        // typecheck match — Aggregate's E156 guard reads it inside the
        // match arm. Source nodes seed the set with themselves; every
        // other variant unions its direct inputs' sets.
        let node_sources: HashSet<String> = match node {
            PipelineNode::Source { .. } => {
                let mut s = HashSet::new();
                s.insert(name.clone());
                s
            }
            // Every non-Source variant inherits the union of its direct
            // inputs' upstream-source sets.
            _ => node
                .direct_input_names()
                .into_iter()
                .filter_map(|n| upstream_sources.get(n))
                .flat_map(|s| s.iter().cloned())
                .collect(),
        };
        upstream_sources.insert(name.clone(), node_sources);

        match node {
            PipelineNode::Source { config, .. } => {
                if bind_ctx.depth == 0
                    && let Some(resource) = config.resource.as_ref()
                {
                    diags.push(
                        Diagnostic::error(
                            "E103",
                            format!(
                                "top-level source {name:?} declares `resource: {}` but top-level resource binding is not available",
                                resource.value
                            ),
                            LabeledSpan::primary(
                                authored_value_span(resource, span),
                                "resource slots are composition-body scoped",
                            ),
                        )
                        .with_help(
                            "remove `resource:` and set exactly one of `path`, `glob`, `regex`, or `paths`; use a composition body plus `_compose.resources_schema` and call-site `resources:` for a catalog-backed Source",
                        ),
                    );
                    continue;
                }
                // A `sort_order` only states the order records arrive in,
                // so `null_order: drop` is refused here, at the Source, under
                // the code every ordering-only field uses. The rest of the
                // ordering policy is checked when the source order compiles.
                for spec in config.source.sort_order.iter().flatten() {
                    if let Err(refused) = OrderField::from_authored(
                        spec.clone().into_sort_field(),
                        OrderingSite::SourceSortOrder,
                    ) {
                        diags.push(Diagnostic::error(
                            "E200",
                            crate::config::source::source_drop_message(&name, &refused),
                            LabeledSpan::primary(span, String::new()),
                        ));
                    }
                }
                // Resolve the unified `schema:` (single-record column list,
                // multi-record superset, external file, or engine-generated)
                // to the effective column list this source seeds its row from.
                let resolved_columns = resolve_source_columns(
                    &config.schema,
                    &config.source.format,
                    name.as_str(),
                    span,
                    diags,
                );
                let declared_multiple: BTreeSet<String> = resolved_columns
                    .iter()
                    .filter(|column| column.multiple.unwrap_or(false))
                    .map(|column| column.name.clone())
                    .collect();
                declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                artifacts
                    .declared_multiple
                    .insert(node_id, declared_multiple);
                let (columns, missing) = columns_from_decl(
                    &resolved_columns,
                    config.correlation_key.as_ref(),
                    &config.on_unmapped,
                );
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
                // Record whether this source declared `watermark.column`
                // so the downstream time-windowed aggregate guard (E156)
                // can verify every upstream-reachable Source has one.
                source_has_watermark.insert(name.clone(), config.source.watermark.is_some());
                // Watermark declaration: column must exist on the
                // source's declared schema AND have an event-time-
                // coercible CXL type (DateTime or Date).
                if let Some(wm) = config.source.watermark.as_ref() {
                    let declared = resolved_columns.iter().find(|c| c.name == wm.column);
                    match declared {
                        None => diags.push(
                            Diagnostic::error(
                                "E154",
                                format!(
                                    "source {name:?} declares watermark.column = {col:?} \
                                     but the column is not present in this source's \
                                     `schema:` block.",
                                    col = wm.column,
                                ),
                                LabeledSpan::primary(span, String::new()),
                            )
                            .with_help(
                                "add the watermark column to the source's `schema:` \
                                 block, or remove the `watermark:` declaration",
                            ),
                        ),
                        // `bound_type()`, not `ty`: on a `multiple: true`
                        // column `ty` describes each ELEMENT, and the column
                        // binds — and reaches the watermark extractor — as an
                        // array. Reading `ty` here would accept a multi-value
                        // date column and then hand the extractor an array,
                        // leaving event time frozen with no diagnostic.
                        Some(col)
                            if !matches!(
                                col.bound_type(),
                                cxl::typecheck::Type::DateTime | cxl::typecheck::Type::Date,
                            ) =>
                        {
                            diags.push(
                                Diagnostic::error(
                                    "E155",
                                    format!(
                                        "source {name:?} declares watermark.column = {col_name:?} \
                                         but its declared type {ty:?} is not event-time-coercible; \
                                         watermark columns must be `date_time` or `date`.",
                                        col_name = wm.column,
                                        ty = col.bound_type().display_name(),
                                    ),
                                    LabeledSpan::primary(span, String::new()),
                                )
                                .with_help(
                                    "change the column's declared `type:` to `date_time` \
                                     or `date`, or point `watermark.column` at a column \
                                     that already has one of those types; a `multiple: true` \
                                     column holds an array and cannot carry event time, so \
                                     drop the declaration if the watermark column is the one \
                                     that repeats",
                                ),
                            );
                        }
                        Some(_) => {}
                    }
                }
                let cxl_span = cxl::lexer::Span::new(span.start as usize, span.start as usize);
                let mut row = Row::closed(columns, cxl_span);
                if bind_ctx.depth > 0 {
                    // A Source inside a composition body reads the body's own
                    // data: every column is the body's.
                    row.enter_body_scope();
                    let own: Vec<QualifiedField> = row.field_names().cloned().collect();
                    for field in own {
                        row.mark(field, ColumnMark::Own);
                    }
                }
                schema_by_name.insert(name.clone(), row.clone());
                artifacts.typed_insert(node_id, Arc::new(synthetic_typed_program(row)));
            }
            PipelineNode::Transform { header, config } => {
                // A window `sort_by` only orders the rows of one partition,
                // so each field converts to its placement-only form and a
                // `null_order: drop` is refused at this Transform. The index
                // requests are built from the validated list.
                if let Some(window) = &config.analytic_window {
                    let mut sort_by = Vec::with_capacity(window.sort_by.len());
                    let mut refused_any = false;
                    for sf in &window.sort_by {
                        match OrderField::from_authored(sf.clone(), OrderingSite::WindowSortBy) {
                            Ok(field) => sort_by.push(field),
                            Err(refused) => {
                                diags.push(Diagnostic::error(
                                    "E200",
                                    format!("transform {}: {refused}", name.quoted_name()),
                                    LabeledSpan::primary(span, String::new()),
                                ));
                                refused_any = true;
                            }
                        }
                    }
                    if !refused_any {
                        artifacts.window_sort_by.insert(node_id, sort_by);
                    }
                }
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
                // Column names the Transform uses outside CXL: a window's
                // partition and order keys, and a validation's field.
                let mut named: Vec<&str> = Vec::new();
                if let Some(window) = &config.analytic_window {
                    named.extend(window.group_by.iter().map(String::as_str));
                    named.extend(window.sort_by.iter().map(|sf| sf.field.as_str()));
                }
                for validation in config.validations.iter().flatten() {
                    named.extend(validation.field.iter().map(String::as_str));
                }
                if refuse_undeclared_names(&upstream, &name, named, span, diags) {
                    continue;
                }
                match typecheck_cxl(
                    &name,
                    &name,
                    CxlSource {
                        text: &config.cxl.source,

                        authored: Authored::Program,
                    },
                    &upstream,
                    AggregateMode::Row,
                    span,
                    &bind_ctx.scoped_vars,
                ) {
                    Ok(mut typed) => {
                        let out = propagate_row(&upstream, &typed);
                        let incoming = declared_multiple_by_name
                            .get(input_target(&header.input.value))
                            .cloned()
                            .unwrap_or_default();
                        let declared_multiple =
                            propagate_transform_declared_multiple(&incoming, &typed);
                        declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                        artifacts
                            .declared_multiple
                            .insert(node_id, declared_multiple);
                        schema_by_name.insert(name.clone(), out.clone());
                        typed.output_row = out;
                        artifacts.typed_insert(node_id, Arc::new(typed));
                    }
                    Err(d) => diags.extend(d),
                }
                // Log-directive gates typecheck against the INPUT row, not the
                // projected output: dispatch fires before the transform's own
                // program runs, so an authored condition sees the record as it
                // arrived. Wrapping in `filter` reuses the row-predicate
                // lowering Reshape's `when` uses, which is what makes a
                // non-boolean gate a normal type error instead of a runtime
                // surprise.
                if let Some(directives) = &config.log {
                    diags.extend(log_field_diagnostics(&name, directives, &upstream, span));
                    // A gate is compiled as its own program, so a module alias
                    // the transform declared is not in scope for it. Module-root
                    // discovery already walks conditions, so the closure is
                    // compiled and the alias is available — only the gate's own
                    // program was missing the declaration that binds it.
                    let use_prelude = module_use_prelude(&config.cxl.source);
                    let mut compiled: Vec<Option<Arc<TypedProgram>>> =
                        Vec::with_capacity(directives.len());
                    let mut every_condition_bound = true;
                    for (index, directive) in directives.iter().enumerate() {
                        let Some(condition) = &directive.condition else {
                            compiled.push(None);
                            continue;
                        };
                        match typecheck_cxl(
                            &name,
                            &format!("{name}:log[{index}]:condition"),
                            CxlSource {
                                text: &format!("{}filter {}", use_prelude.source, condition.source),

                                authored: Authored::Expression {
                                    key: "condition",
                                    text: &condition.source,
                                    at: use_prelude.source.len() + "filter ".len(),
                                },
                            },
                            &upstream,
                            AggregateMode::Row,
                            span,
                            &bind_ctx.scoped_vars,
                        ) {
                            // The condition is spliced into `filter <source>`, so a
                            // source carrying a statement separator compiles into
                            // statements the author never wrote into a gate — including
                            // ones the gate's evaluator is not allocated to run. A gate
                            // is one predicate; anything else is refused here rather
                            // than reaching the first record.
                            Ok(typed)
                                if authored_gate_statements(
                                    &typed.program.statements,
                                    &use_prelude,
                                ) == 1 =>
                            {
                                compiled.push(Some(Arc::new(typed)));
                            }
                            Ok(typed) => {
                                diags.push(Diagnostic::error(
                                    "E373",
                                    format!(
                                        "transform `{name}` log[{index}].condition must be one predicate, but it parses as {} statements; write the gate as a single expression, for example `condition: \"amount > 1 and region == 'eu'\"`",
                                        authored_gate_statements(
                                            &typed.program.statements,
                                            &use_prelude
                                        )
                                    ),
                                    LabeledSpan::primary(span, "log condition".to_string()),
                                ));
                                every_condition_bound = false;
                            }
                            Err(d) => {
                                diags.extend(d);
                                every_condition_bound = false;
                            }
                        }
                    }
                    // Insert only on a complete set, so lowering's length guard
                    // can treat a missing entry as "bind rejected this node".
                    if every_condition_bound {
                        artifacts.transform_log_conditions.insert(node_id, compiled);
                    }
                }
            }
            PipelineNode::Aggregate { header, config } => {
                let upstream = match upstream_schema(&header.input.value, schema_by_name) {
                    Some(s) => s.clone(),
                    None => continue,
                };
                // E156 — a time-windowed aggregate's close decision
                // reads `min_across_sources` over the upstream-reachable
                // Source set. Every Source on every path into this
                // aggregate MUST declare `watermark.column` for the
                // close to ever fire; a single watermark-less Source on
                // any path holds the min at `None` forever, silently
                // suppressing all window emissions. Flink TVF / Spark
                // window do the same plan-time check.
                if config.time_window.is_some() {
                    let upstream_set = upstream_sources.get(&name).cloned().unwrap_or_default();
                    let mut missing_wm: Vec<&str> = upstream_set
                        .iter()
                        .filter(|s| !source_has_watermark.get(*s).copied().unwrap_or(false))
                        .map(|s| s.as_str())
                        .collect();
                    missing_wm.sort();
                    if !missing_wm.is_empty() {
                        let missing_list: Vec<String> =
                            missing_wm.iter().map(|s| format!("{s:?}")).collect();
                        diags.push(
                            Diagnostic::error(
                                "E156",
                                format!(
                                    "aggregate {name:?} declares `time_window:` but the \
                                     following upstream-reachable source(s) do not declare \
                                     `watermark.column`: {missing}. Without a watermark on \
                                     every upstream source, `min_across_sources` stays at \
                                     `None` and the window can never close.",
                                    missing = missing_list.join(", "),
                                ),
                                LabeledSpan::primary(span, String::new()),
                            )
                            .with_help(
                                "add `watermark: { column: <event-time-column> }` to each \
                                 listed source's config, or remove `time_window:` from this \
                                 aggregate",
                            ),
                        );
                        continue;
                    }
                }
                // A group_by naming a column a composition port carries
                // undeclared is E169; any other absent field is E200.
                let refused: Vec<Diagnostic> = config
                    .group_by
                    .iter()
                    .filter_map(|gb| refuse_undeclared(&upstream, &name, gb, span))
                    .collect();
                if !refused.is_empty() {
                    diags.extend(refused);
                    continue;
                }
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
                match typecheck_cxl(
                    &name,
                    &name,
                    CxlSource {
                        text: &config.cxl.source,

                        authored: Authored::Program,
                    },
                    &upstream,
                    agg_mode,
                    span,
                    &bind_ctx.scoped_vars,
                ) {
                    Ok(mut typed) => {
                        let out = propagate_aggregate(&name, &config.group_by, &upstream, &typed);
                        let incoming = declared_multiple_by_name
                            .get(input_target(&header.input.value))
                            .cloned()
                            .unwrap_or_default();
                        let declared_multiple: BTreeSet<String> = config
                            .group_by
                            .iter()
                            .filter(|field| incoming.contains(field.as_str()))
                            .cloned()
                            .collect();
                        declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                        artifacts
                            .declared_multiple
                            .insert(node_id, declared_multiple);
                        schema_by_name.insert(name.clone(), out.clone());
                        typed.output_row = out;
                        artifacts.typed_insert(node_id, Arc::new(typed));
                    }
                    Err(d) => diags.extend(d),
                }
            }
            PipelineNode::Route { header, config } => {
                if let Some(upstream) = upstream_schema(&header.input.value, schema_by_name) {
                    let cloned = upstream.clone();
                    if let Ok(mut empty) = typecheck_cxl(
                        &name,
                        &name,
                        CxlSource {
                            text: "",

                            authored: Authored::Synthesized,
                        },
                        &cloned,
                        AggregateMode::Row,
                        span,
                        &bind_ctx.scoped_vars,
                    ) {
                        empty.output_row = cloned.clone();
                        artifacts.typed_insert(node_id, Arc::new(empty));
                    }
                    // Type-check each branch condition into its own program,
                    // in `conditions` declaration order. Each program is carried
                    // onto `PlanNode::Route.branch_programs` at lowering so route
                    // dispatch evaluates it off the node, and it also feeds the
                    // compile-time `$doc` walk (which reaches an envelope path
                    // referenced ONLY in a route predicate). The condition is a
                    // boolean expression; wrapping it in `filter` mirrors how the
                    // runtime evaluates it, giving a `Statement::Filter` whose
                    // predicate the `$doc` walk descends. A branch that fails to
                    // type-check is a hard compile error here — the executor does
                    // not re-compile route conditions at startup, so this is the
                    // sole place they are diagnosed — and on any failure the node
                    // is left without an entry so it is not lowered. A clean
                    // bind therefore records a full-length vec aligned 1:1 with
                    // `branches`.
                    let mut branch_programs: Vec<Arc<TypedProgram>> =
                        Vec::with_capacity(config.conditions.len());
                    for (branch_name, cond) in &config.conditions {
                        // Qualify the diagnostic node name with the branch so a
                        // type error in a multi-branch route points at the
                        // offending branch, not just the route node.
                        match typecheck_cxl(
                            &name,
                            &format!("{name} (branch {branch_name})"),
                            CxlSource {
                                text: &format!("filter {}", cond.source),

                                authored: Authored::Expression {
                                    key: branch_name,
                                    text: &cond.source,
                                    at: "filter ".len(),
                                },
                            },
                            &cloned,
                            AggregateMode::Row,
                            span,
                            &bind_ctx.scoped_vars,
                        ) {
                            Ok(typed) => branch_programs.push(Arc::new(typed)),
                            Err(d) => diags.extend(d),
                        }
                    }
                    // Record the programs only when every branch typechecked: a
                    // short vec would not align 1:1 with `branches` at lowering.
                    // A failed branch has already pushed its diagnostic, which
                    // aborts the compile before lowering runs.
                    if branch_programs.len() == config.conditions.len() {
                        artifacts
                            .route_branch_typed
                            .insert(node_id, branch_programs);
                    }
                    let declared_multiple = declared_multiple_by_name
                        .get(input_target(&header.input.value))
                        .cloned()
                        .unwrap_or_default();
                    declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                    artifacts
                        .declared_multiple
                        .insert(node_id, declared_multiple);
                    schema_by_name.insert(name, cloned);
                }
            }
            PipelineNode::Merge { header, .. } => {
                // E315 — Merge inputs must agree on `$widened` sidecar
                // presence. The runtime concatenates streams positionally
                // against the merge's `output_schema` (taken from the
                // first input); if upstream A has `$widened` but
                // upstream B does not, the dispatcher's
                // `check_input_schema` raises E314 with a message that
                // names neither `$widened` nor the policy mismatch,
                // leaving the user to debug a column-count discrepancy
                // by hand. Detecting the disagreement at plan time
                // surfaces it with a focused message that names the
                // disagreeing inputs and points at each source's
                // `on_unmapped` policy.
                let widened_col = crate::config::pipeline_node::WIDENED_SIDECAR_COLUMN;
                let mut presence: Vec<(String, bool)> = Vec::new();
                for input in &header.inputs {
                    let upstream_name = input_target(&input.value).to_string();
                    if let Some(row) = schema_by_name.get(upstream_name.as_str()) {
                        presence.push((upstream_name, row.has_field(widened_col)));
                    }
                }
                if !presence.is_empty()
                    && presence.iter().any(|(_, has)| *has)
                    && presence.iter().any(|(_, has)| !*has)
                {
                    let with_sidecar: Vec<&str> = presence
                        .iter()
                        .filter(|(_, has)| *has)
                        .map(|(n, _)| n.as_str())
                        .collect();
                    let without_sidecar: Vec<&str> = presence
                        .iter()
                        .filter(|(_, has)| !*has)
                        .map(|(n, _)| n.as_str())
                        .collect();
                    diags.push(
                        Diagnostic::error(
                            "E315",
                            format!(
                                "merge {name:?}: input schemas disagree on the `$widened` \
                                 auto_widen sidecar column. Inputs with sidecar: {with_sidecar:?}; \
                                 inputs without sidecar: {without_sidecar:?}. The runtime \
                                 concatenates streams positionally against the merge's output \
                                 schema (taken from the first input); a sidecar-present record \
                                 cannot be merged with sidecar-absent records without losing \
                                 either the unmapped fields or column-count alignment."
                            ),
                            LabeledSpan::primary(span, String::new()),
                        )
                        .with_help(
                            "set every merge upstream source to the same `on_unmapped` policy \
                             (all `auto_widen` to keep the sidecar, or all `drop`/`reject` to \
                             omit it). Per-source policy lives in the source's `config.on_unmapped` \
                             block; the engine-wide default is `auto_widen`.",
                        ),
                    );
                }
                if let Some(first) = header.inputs.first()
                    && let Some(upstream) = schema_by_name.get(input_target(&first.value))
                {
                    let mut inputs = header.inputs.iter().map(|input| {
                        declared_multiple_by_name
                            .get(input_target(&input.value))
                            .cloned()
                            .unwrap_or_default()
                    });
                    let declared_multiple = inputs.next().map_or_else(BTreeSet::new, |first| {
                        inputs.fold(first, |intersection, next| {
                            intersection.intersection(&next).cloned().collect()
                        })
                    });
                    declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                    artifacts
                        .declared_multiple
                        .insert(node_id, declared_multiple);
                    let mut row = upstream.clone();
                    // A column any input carries in undeclared stays unnamed
                    // after the Merge: its records may come from that input.
                    for input in header.inputs.iter().skip(1) {
                        if let Some(other) = schema_by_name.get(input_target(&input.value)) {
                            let undeclared: Vec<(QualifiedField, CarriedColumn)> = other
                                .carried_columns()
                                .filter(|(_, _, column)| column.access == PortAccess::Undeclared)
                                .map(|(field, _, column)| (field.clone(), column.clone()))
                                .collect();
                            for (field, column) in undeclared {
                                row.mark(field, ColumnMark::Carried(column));
                            }
                        }
                    }
                    schema_by_name.insert(name.clone(), row.clone());
                    artifacts.typed_insert(node_id, Arc::new(synthetic_typed_program(row)));
                }
            }
            PipelineNode::Sink { header, config } => {
                if let Some(upstream) = upstream_schema(&header.input.value, schema_by_name) {
                    let incoming = declared_multiple_by_name
                        .get(input_target(&header.input.value))
                        .cloned()
                        .unwrap_or_default();
                    let declared_multiple =
                        crate::config::multi_value::sink_output_multiple_columns(
                            &config.sink,
                            incoming,
                        );
                    declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                    artifacts
                        .declared_multiple
                        .insert(node_id, declared_multiple);
                    let row = upstream.clone();
                    // Every column the Sink names: mapping sources, exclusions,
                    // the split group key and the sort keys.
                    let sink = &config.sink;
                    let mut named: Vec<String> = Vec::new();
                    if let Some(mapping) = &sink.mapping {
                        named.extend(mapping.entries().iter().map(|e| e.source.clone()));
                    }
                    named.extend(sink.exclude.iter().flatten().cloned());
                    named.extend(
                        sink.split
                            .iter()
                            .filter_map(|split| split.group_key.clone()),
                    );
                    named.extend(
                        sink.sort_order
                            .iter()
                            .flatten()
                            .map(|spec| spec.clone().into_sort_field().field),
                    );
                    if refuse_undeclared_names(
                        &row,
                        &name,
                        named.iter().map(String::as_str),
                        span,
                        diags,
                    ) {
                        continue;
                    }
                    validate_output_mapping_columns(&name, &config.sink, &row, span, diags);
                    schema_by_name.insert(name.clone(), row.clone());
                    artifacts.typed_insert(node_id, Arc::new(synthetic_typed_program(row)));
                }
            }
            PipelineNode::Reshape { header, config } => {
                if let Some(upstream) = upstream_schema(&header.input.value, schema_by_name) {
                    let declared_multiple = declared_multiple_by_name
                        .get(input_target(&header.input.value))
                        .cloned()
                        .unwrap_or_default();
                    declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                    artifacts
                        .declared_multiple
                        .insert(node_id, declared_multiple);
                    let upstream = upstream.clone();
                    bind_reshape(
                        &ReshapeNodeBinding {
                            name: &name,
                            config,
                            upstream: &upstream,
                            span,
                            scoped_vars: &bind_ctx.scoped_vars,
                            id: node_id,
                        },
                        diags,
                        artifacts,
                        schema_by_name,
                    );
                }
            }
            PipelineNode::Cull { header, config } => {
                if let Some(upstream) = upstream_schema(&header.input.value, schema_by_name) {
                    let declared_multiple = declared_multiple_by_name
                        .get(input_target(&header.input.value))
                        .cloned()
                        .unwrap_or_default();
                    declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                    artifacts
                        .declared_multiple
                        .insert(node_id, declared_multiple);
                    let upstream = upstream.clone();
                    bind_cull(
                        &CullNodeBinding {
                            name: &name,
                            config,
                            upstream: &upstream,
                            span,
                            scoped_vars: &bind_ctx.scoped_vars,
                            id: node_id,
                        },
                        diags,
                        artifacts,
                        schema_by_name,
                    );
                }
            }
            // Envelope adopts the body input's schema verbatim — neither
            // strategy widens it: `preserve` passes body records through
            // unchanged, and `concat` changes only the framing grain and the
            // ambient `$doc` view, not the record schema. A wired `header:` port
            // replaces the framing header (not the record schema), so binding
            // reads only the body input for the output row here.
            PipelineNode::Envelope { header, config } => {
                if let Some(upstream) = upstream_schema(&header.body.value, schema_by_name) {
                    let declared_multiple = declared_multiple_by_name
                        .get(input_target(&header.body.value))
                        .cloned()
                        .unwrap_or_default();
                    declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                    artifacts
                        .declared_multiple
                        .insert(node_id, declared_multiple);
                    let row = upstream.clone();
                    schema_by_name.insert(name.clone(), row.clone());
                    // Compile declarative header/footer synthesis against the
                    // body input `Row` (the Envelope's output row, since it does
                    // not widen). Header scalars typecheck in `Row` mode and may
                    // not reference a body column (E353); footer aggregates
                    // typecheck in `GroupBy` mode with an empty group_by and must
                    // be in the streaming allow-list (E354). Errors push onto
                    // `diags` and the node lowers without a synthesis spec.
                    if (!config.header.is_empty() || !config.footer.is_empty())
                        && let Some(synthesis) = compile_envelope_synthesis(
                            &name,
                            config,
                            &row,
                            span,
                            &bind_ctx.scoped_vars,
                            diags,
                        )
                    {
                        artifacts
                            .envelope_synthesis
                            .insert(node_id, Arc::new(synthesis));
                    }
                    artifacts.typed_insert(node_id, Arc::new(synthetic_typed_program(row)));
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
                    &CombineNodeBinding {
                        name: &name,
                        header,
                        config,
                        span,
                        scoped_vars: &bind_ctx.scoped_vars,
                        id: node_id,
                    },
                    diags,
                    artifacts,
                    schema_by_name,
                );
                let declared_multiple = artifacts
                    .typed_get(node_id)
                    .map(|typed| {
                        propagate_combine_declared_multiple(
                            header,
                            declared_multiple_by_name,
                            typed,
                        )
                    })
                    .unwrap_or_default();
                declared_multiple_by_name.insert(name.clone(), declared_multiple.clone());
                artifacts
                    .declared_multiple
                    .insert(node_id, declared_multiple);
            }
            PipelineNode::Composition {
                r#use,
                inputs,
                config,
                resources,
                ..
            } => {
                bind_composition(
                    &CompositionBindParams {
                        node_name: &name,
                        node_id,
                        use_path: r#use,
                        call_inputs: inputs,
                        call_config: config,
                        call_resources: resources,
                        source_nodes: &source_nodes,
                        scope: nodes,
                        failed_nodes: &failed_nodes,
                        span,
                    },
                    diags,
                    bind_ctx,
                    artifacts,
                    schema_by_name,
                    declared_multiple_by_name,
                );
            }
        }
    }
    settle(
        &mut binding,
        nodes,
        diags,
        schema_by_name,
        &mut failed_nodes,
    );

    // Each fix a body input diagnostic would print is shown only once the
    // body, edited as it says, binds without the error.
    if !bind_ctx.verifying {
        for fix in body_fixes {
            check_body_input_fix(
                fix,
                nodes,
                diags,
                bind_ctx,
                artifacts,
                schema_by_name,
                declared_multiple_by_name,
            );
        }
    }

    // Inside a composition body every bound row must carry its marks: a
    // row built without them would let a node name every column. A node
    // kind that forgets to pass the marks on is caught here, loudly, instead
    // of silently widening what the body may name.
    if bind_ctx.depth > 0 {
        for spanned in nodes {
            let name = spanned.value.name();
            if schema_by_name
                .get(name)
                .is_some_and(|row| !row.in_body_scope())
            {
                diags.push(Diagnostic::error(
                    "E000",
                    format!(
                        "internal error: composition node {name:?} was bound without the \
                         marks that say which columns the composition may use"
                    ),
                    LabeledSpan::primary(span_for(spanned), String::new()),
                ));
            }
        }
    }
    failed_nodes
}

// ─── Composition binding ────────────────────────────────────────────

/// The binding state a composition body binds in: its `use:` path (for
/// cycle detection), the directory its own `use:` paths resolve from, its
/// sealed scoped variables, and the node names of every enclosing scope.
struct BodyScope {
    use_path: PathBuf,
    origin_dir: PathBuf,
    scoped_vars: cxl::resolve::ScopedVarsRegistry,
    enclosing_scope_names: HashSet<String>,
    /// The body nodes the composition names as its outputs.
    outputs: HashSet<String>,
    /// The input ports the call binds, in declaration order.
    ports: Vec<String>,
}

/// Run `bind` with `bind_ctx` one composition level deeper, in `scope`, and
/// put the calling scope's state back when it returns.
///
/// Entering and leaving happen here and nowhere else, so no exit from
/// `bind` (or before it) can leave the calling scope bound as if it were
/// inside a body.
fn in_body_scope<'a, R>(
    bind_ctx: &mut BindContext<'a>,
    scope: BodyScope,
    bind: impl FnOnce(&mut BindContext<'a>) -> R,
) -> R {
    let BodyScope {
        use_path,
        origin_dir,
        scoped_vars,
        enclosing_scope_names,
        outputs,
        ports,
    } = scope;
    let saved_enclosing =
        std::mem::replace(&mut bind_ctx.enclosing_scope_names, enclosing_scope_names);
    let saved_outputs = std::mem::replace(&mut bind_ctx.body_outputs, outputs);
    let saved_ports = std::mem::replace(&mut bind_ctx.body_ports, ports);
    let saved_origin_dir = std::mem::replace(&mut bind_ctx.origin_dir, origin_dir);
    let saved_scoped_vars = std::mem::replace(&mut bind_ctx.scoped_vars, scoped_vars);
    bind_ctx.use_path_stack.push(use_path);
    bind_ctx.depth += 1;
    let out = bind(bind_ctx);
    bind_ctx.depth -= 1;
    bind_ctx.use_path_stack.pop();
    bind_ctx.enclosing_scope_names = saved_enclosing;
    bind_ctx.body_outputs = saved_outputs;
    bind_ctx.body_ports = saved_ports;
    bind_ctx.scoped_vars = saved_scoped_vars;
    bind_ctx.origin_dir = saved_origin_dir;
    out
}

/// Immutable call-site inputs to [`bind_composition`]: the node's name and
/// `use:` path, plus the three call-site maps (`inputs:`, `config:`,
/// `resources:`) read off the
/// `PipelineNode::Composition` variant. Grouped so the mutable binding
/// accumulators (`diags`, `bind_ctx`, `artifacts`, parent scope) stay
/// visually distinct from the read-only call-site data the function
/// validates against the resolved signature.
struct CompositionBindParams<'a> {
    node_name: &'a str,
    /// The call-site composition node's stable identity. Keys the body
    /// assignment so two compositions named the same in disjoint scopes
    /// (or sharing a name with a top-level node) get distinct entries.
    node_id: PlanNodeId,
    use_path: &'a Path,
    call_inputs: &'a IndexMap<String, String>,
    call_config: &'a IndexMap<String, serde_json::Value>,
    call_resources: &'a IndexMap<String, ResourceBinding>,
    /// The calling scope's Source nodes, so an E102 can point at the
    /// `schema:` line that declares a column's type.
    source_nodes: &'a HashSet<String>,
    /// Every node of the calling scope, so an E102 about a `node.port`
    /// upstream can say what kind of node it names.
    scope: &'a [Spanned<PipelineNode>],
    /// The nodes of the calling scope whose bind failed with an error.
    failed_nodes: &'a HashSet<String>,
    span: Span,
}

/// Bind a composition body at its call-site boundary.
///
/// This is the `PipelineNode::Composition` arm of `bind_schema_inner`.
/// It recursively binds the body as a sub-problem within a nested scope
/// (preserve-and-recurse, not flatten-and-splice).
fn bind_composition(
    params: &CompositionBindParams<'_>,
    diags: &mut Vec<Diagnostic>,
    bind_ctx: &mut BindContext<'_>,
    artifacts: &mut CompileArtifacts,
    parent_schema_by_name: &mut HashMap<String, Row>,
    parent_declared_multiple_by_name: &mut HashMap<String, BTreeSet<String>>,
) {
    let &CompositionBindParams {
        node_name,
        node_id,
        use_path,
        call_inputs,
        call_config,
        call_resources,
        source_nodes,
        scope,
        failed_nodes,
        span,
    } = params;
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

    // 5. Validate every logical resource binding against the declared slot.
    if !validate_resources(
        call_resources,
        signature,
        bind_ctx.resource_catalog,
        node_name,
        span,
        diags,
    ) {
        has_errors = true;
    }

    if has_errors {
        return;
    }

    // 5b. Populate provenance entries for each resolved config param.
    populate_config_provenance(
        call_config,
        signature,
        node_id,
        node_name,
        span,
        &mut artifacts.provenance,
    );

    // 6. Check each bound port against its upstream (E102) and record the
    //    port's declared columns, the vocabulary the body may name.
    let input_port_rows = match build_input_port_rows(
        &PortAdmission {
            call: node_name,
            call_inputs,
            signature,
            parent_schemas: parent_schema_by_name,
            source_nodes,
            scope,
            failed_nodes,
            body_file: &resolved_path,
            span,
        },
        &mut artifacts.next_tail_var,
        diags,
    ) {
        Some(rows) => rows,
        None => return, // E102 errors already pushed
    };

    // 7. Re-read body from disk and verify it is the same snapshot module
    // closure discovery admitted earlier in this compile invocation.
    let mut body_file = match read_and_parse_body(
        &signature.source_path,
        &resolved_path,
        bind_ctx.ctx,
        artifacts,
        node_name,
        span,
        diags,
    ) {
        Some(f) => f,
        None => return,
    };

    // 7a. Apply any channel `sources:` patch addressed at a source declared
    // inside this composition body (qualified key `<this-node>.<inner-source>`).
    // These could not be applied at patch time because the body is expanded
    // only here; apply them to the freshly re-read body so its source nodes
    // carry the channel's schema / options / array changes before the body
    // binds, exactly as a top-level source patch shapes a top-level source
    // before it binds. An unknown inner source or a failed op abandons the
    // body with a spanned diagnostic naming the call site and body file.
    //
    // Gate on `depth == 0`: a qualified key names a TOP-LEVEL composition node
    // (`apply_source_patches` validates the alias against `config.nodes` only,
    // and a two-dot key naming a source inside a *nested* body is rejected).
    // The patch map is keyed by bare node name, so without this gate a patch
    // keyed for a top-level composition would also match a same-named
    // composition node nested inside another body and mutate the wrong body.
    // `depth` is 0 here only for top-level call sites — it is incremented by
    // `in_body_scope` while the body binds — so this restricts application to
    // the top-level composition the qualified key actually addresses.
    if bind_ctx.depth == 0
        && let Some(inner_patches) = bind_ctx.ctx.body_source_patches.get(node_name)
    {
        for (inner_source, patch) in inner_patches {
            let Some(source_body) =
                crate::config::patch::source_body_in_nodes(&mut body_file.nodes, inner_source)
            else {
                // Match the channel-patch family: the E-code is embedded in the
                // message so it survives the messages-only compile-error render,
                // as the top-level `[E230]` unknown-source error does.
                diags.push(Diagnostic::error(
                    "E230",
                    format!(
                        "[E230] composition node {node_name:?}: body file {}: channel source \
                         patch targets unknown source {inner_source:?}: no source node by that \
                         name in the composition body",
                        resolved_path.display()
                    ),
                    LabeledSpan::primary(span, String::new()),
                ));
                return;
            };
            if let Err(err) =
                crate::config::patch::apply_patch_to_source_body(source_body, patch, inner_source)
            {
                let raw = match err {
                    crate::config::ConfigError::Validation(m) => m,
                    other => other.to_string(),
                };
                let (code, detail) = crate::config::patch::split_diagnostic_code(&raw);
                diags.push(Diagnostic::error(
                    code,
                    format!(
                        "[{code}] composition node {node_name:?}: body file {}: {detail}",
                        resolved_path.display()
                    ),
                    LabeledSpan::primary(span, String::new()),
                ));
                return;
            }
        }
    }

    // 7b. Resolve every authored body Source through one explicit resource
    // slot before the ordinary body bind. Synthetic input-port Sources are
    // introduced later and deliberately do not appear in this inventory:
    // they receive parent records and do not open an external dataset.
    let pending_source_requirements = match compile_body_source_requirements(
        &body_file.nodes,
        signature,
        call_resources,
        bind_ctx.resource_catalog,
        node_name,
        &resolved_path,
        diags,
    ) {
        Some(requirements) => requirements,
        None => return,
    };

    // 7c. Body nodes never flow through top-level config validation
    // (`validate_config` sees only the call-site pipeline's own nodes),
    // so run the shared node-scoped checks here. Without this pass a
    // body Envelope with a wired `trailer:` — or a Transform with a
    // reserved `declares:` name, a mistyped declare default, a zero
    // `batch_size`, or an invalid log directive — reaches the executor
    // and dies as an internal error instead of a compile diagnostic.
    // Emit one E115 per violation and abandon the body, mirroring the
    // other early-outs.
    {
        let violations = crate::config::pipeline::validate_node_configs(&body_file.nodes);
        if !violations.is_empty() {
            for violation in violations {
                diags.push(Diagnostic::error(
                    "E115",
                    format!(
                        "composition node {node_name:?}: body file {}: {}",
                        resolved_path.display(),
                        violation.message
                    ),
                    LabeledSpan::primary(
                        span_for_node(&body_file.nodes[violation.node_index]),
                        String::new(),
                    ),
                ));
            }
            return;
        }
    }

    // 7d. A body Source/Output CSV `delimiter` / `quote_char` is only ever
    // rejected at runtime by the writer's single-byte lowering guard, because
    // top-level `validate_config` (which runs the same check) sees only the
    // call-site pipeline's own nodes. Run the shared byte-option check here so
    // an empty, multi-character, or non-ASCII value on a body node fails at
    // compile / `--explain` — the same stage a top-level node is rejected at —
    // rather than after the run has already started producing output. Collect
    // every violation, then abandon the body, mirroring the E115 pass above.
    {
        let mut has_csv_violation = false;
        for spanned in &body_file.nodes {
            let node_span = span_for_node(spanned);
            let mut check = |result: Result<(), crate::config::ConfigError>| {
                if let Err(err) = result {
                    let raw = match err {
                        crate::config::ConfigError::Validation(m) => m,
                        other => other.to_string(),
                    };
                    diags.push(Diagnostic::error(
                        "E115",
                        format!(
                            "composition node {node_name:?}: body file {}: {raw}",
                            resolved_path.display()
                        ),
                        LabeledSpan::primary(node_span, String::new()),
                    ));
                    has_csv_violation = true;
                }
            };
            match &spanned.value {
                PipelineNode::Source { config, .. } => {
                    if let crate::config::InputFormat::Csv(Some(opts)) = &config.source.format {
                        if let Some(delimiter) = opts.delimiter.as_deref() {
                            check(crate::config::pipeline::validate_csv_byte_option(
                                "source",
                                &config.source.name,
                                "delimiter",
                                delimiter,
                            ));
                        }
                        if let Some(quote_char) = opts.quote_char.as_deref() {
                            check(crate::config::pipeline::validate_csv_byte_option(
                                "source",
                                &config.source.name,
                                "quote_char",
                                quote_char,
                            ));
                        }
                    }
                }
                PipelineNode::Sink { config, .. } => {
                    if let crate::config::OutputFormat::Csv(Some(opts)) = &config.sink.format
                        && let Some(delimiter) = opts.delimiter.as_deref()
                    {
                        check(crate::config::pipeline::validate_csv_byte_option(
                            "output",
                            &config.sink.name,
                            "delimiter",
                            delimiter,
                        ));
                    }
                }
                _ => {}
            }
        }
        if has_csv_violation {
            return;
        }
    }

    // 7e. Resolve external `.schema.yaml` (`SourceSchema::File`) references on
    // body Source and Sink nodes to their inline column form, each path
    // resolved relative to the composition file's own directory. Top-level
    // nodes have their externals resolved at parse time by
    // `resolve_and_hash_external_schemas`; body files are re-read here and
    // never pass through that fold. Two consequences motivate resolving them:
    // a body Output's declared-decimal `scale` and fixed-width column widths
    // both read through `SourceSchema::as_columns`, which yields `None` for an
    // unresolved `File`, so the write boundary silently skips scale rounding;
    // and a body Source's `File` would otherwise resolve against the process
    // working directory (`resolve_source_columns`) instead of the composition
    // directory. Resolving in place, before the body binds and lowers, fixes
    // both. The pipeline-identity fold of the referenced content is
    // intentionally not mirrored here.
    {
        let comp_dir = signature
            .source_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        // Pass 1: immutable walk loading each external file once.
        let mut resolved: Vec<(usize, SourceSchema)> = Vec::new();
        for (idx, spanned) in body_file.nodes.iter().enumerate() {
            let (rel, node_label) = match &spanned.value {
                PipelineNode::Source { config, .. } => match &config.schema {
                    SourceSchema::File(rel) => (rel, format!("source {:?}", config.source.name)),
                    _ => continue,
                },
                PipelineNode::Sink { config, .. } => match &config.sink.schema {
                    Some(SourceSchema::File(rel)) => {
                        (rel, format!("output {:?}", config.sink.name))
                    }
                    _ => continue,
                },
                _ => continue,
            };
            let path = comp_dir.join(rel);
            match crate::schema::load_source_schema(&path) {
                Ok(inline) => resolved.push((idx, inline)),
                Err(e) => {
                    diags.push(Diagnostic::error(
                        "E157",
                        format!(
                            "composition node {node_name:?}: body file {}: {node_label} declares \
                             an external schema file {rel:?} that failed to load: {e}",
                            resolved_path.display()
                        ),
                        LabeledSpan::primary(span_for_node(spanned), String::new()),
                    ));
                    return;
                }
            }
        }
        // Pass 2: write each resolved inline schema back onto its body node.
        for (idx, inline) in resolved {
            match &mut body_file.nodes[idx].value {
                PipelineNode::Source { config, .. } => config.schema = inline,
                PipelineNode::Sink { config, .. } => config.sink.schema = Some(inline),
                _ => {}
            }
        }
    }

    // 7f. The multi-value gates (E358 / E359 / E360 / E361) and the E363
    // `record_path` grammar gate, which the call-site pipeline runs over its
    // OWN `nodes:` — a list that never contains a body's nodes. Without this
    // pass a body source declaring the retired `array_paths:`, a `multiple:
    // true` column its format cannot produce, a malformed `split_to_rows`
    // block, or an XPath-shaped `record_path` reaches the executor ungated and
    // emits one record per document — or none at all — where the author asked
    // for one per element. The XML reader's construction-time
    // nesting/duplicate rejection used to catch part of that for body sources
    // and was replaced by the plan-time gate, so running the gate here is what
    // keeps the replacement whole. Runs after 7d so an external `.schema.yaml`
    // is already folded inline and the gates see its columns. Collect every
    // finding, then abandon the body, mirroring the passes above.
    {
        let faults = crate::config::multi_value::source_node_faults(&body_file.nodes)
            .into_iter()
            .chain(crate::config::multi_value::sink_node_faults(
                &body_file.nodes,
            ))
            .chain(crate::config::record_path::record_path_faults(
                &body_file.nodes,
            ));
        let mut has_node_config_violation = false;
        for fault in faults {
            diags.push(
                Diagnostic::error(
                    fault.code,
                    format!(
                        "composition node {node_name:?}: body file {}: {}",
                        resolved_path.display(),
                        fault.message
                    ),
                    LabeledSpan::primary(
                        span_for_node(&body_file.nodes[fault.node_index]),
                        String::new(),
                    ),
                )
                .with_help(fault.help),
            );
            has_node_config_violation = true;
        }
        for fault in crate::config::output_mapping::output_mapping_faults_spanned(&body_file.nodes)
        {
            diags.push(
                Diagnostic::error(
                    fault.code,
                    format!(
                        "composition node {node_name:?}: body file {}: {}",
                        resolved_path.display(),
                        fault.message
                    ),
                    LabeledSpan::primary(
                        fault
                            .item_line
                            .map(Span::line_only)
                            .unwrap_or_else(|| span_for_node(&body_file.nodes[fault.node_index])),
                        String::new(),
                    ),
                )
                .with_help(fault.help),
            );
            has_node_config_violation = true;
        }
        if has_node_config_violation {
            return;
        }
    }

    // 7g. A body node that reads an input port this call leaves unbound (a
    // port declared `required: false`), or an output port that is such a
    // port, could receive no row. Left alone it would bind against nothing
    // and escape every check, and the call would run writing nothing; the
    // call is refused instead, with the binding to add at the call site.
    {
        let unbound =
            |port: &str| signature.inputs.contains_key(port) && !call_inputs.contains_key(port);
        let mut refused: Vec<(usize, String, UnboundPortReader<'_>)> = Vec::new();
        for spanned in &body_file.nodes {
            for port in node_input_names(&spanned.value) {
                if unbound(port) {
                    refused.push((
                        diags.len(),
                        port.to_string(),
                        UnboundPortReader::Node(spanned.value.name()),
                    ));
                    diags.push(unbound_port_read(&UnboundPortRead {
                        call: node_name,
                        reader: UnboundPortReader::Node(spanned.value.name()),
                        port,
                        body_file: &resolved_path,
                        span,
                    }));
                }
            }
        }
        for (output, alias) in &signature.outputs {
            let internal = alias.internal_ref.value.as_str();
            let port = internal.split('.').next().unwrap_or(internal);
            if unbound(port) {
                refused.push((
                    diags.len(),
                    port.to_string(),
                    UnboundPortReader::Output(output),
                ));
                diags.push(unbound_port_read(&UnboundPortRead {
                    call: node_name,
                    reader: UnboundPortReader::Output(output),
                    port,
                    body_file: &resolved_path,
                    span,
                }));
            }
        }
        if !refused.is_empty() {
            if !bind_ctx.verifying {
                offer_port_bindings(
                    &refused,
                    &CallFeeders {
                        call: node_name,
                        call_inputs,
                        scope,
                        failed_nodes,
                    },
                    diags,
                    bind_ctx,
                    artifacts,
                    parent_schema_by_name,
                    parent_declared_multiple_by_name,
                );
            }
            return;
        }
    }

    // 8. Prepare the nested scope. Nothing in `bind_ctx` changes until the
    //    body binds inside it (`in_body_scope`), so every early return below
    //    leaves the calling scope as it found it.
    let body_id = artifacts.fresh_body_id();
    // Composition bodies are sealed from parent scoped vars by default
    // — the body sees an empty `ScopedVarsRegistry` and the
    // resolver rejects every `$pipeline.<custom>` / `$source.<custom>` /
    // `$record.<custom>` read with the standard "unknown member"
    // diagnostic.
    //
    // Opt-in inheritance via the composition's
    // `_compose.scoped_vars` schema: for each `(scope, key)` declared
    // in `signature.scoped_vars_schema`, if the parent has a matching
    // declaration with the same type, the body's registry includes it.
    // Mismatches (parent missing the var, or type doesn't match) emit
    // E174.
    let mut body_scoped_vars = build_body_scoped_vars(
        &signature.scoped_vars_schema,
        &bind_ctx.scoped_vars,
        &signature.name,
        span,
        diags,
    );
    // Wire `$config.<param>` for this instantiation's body: the declared
    // types drive resolve/typecheck, and the resolved values drive the
    // compile-time fold (see `fold_body_config`). Two call sites of the same
    // composition with different config therefore compile to different bodies.
    populate_body_config(
        &mut body_scoped_vars,
        signature,
        node_name,
        call_config,
        &bind_ctx.ctx.config_overrides,
    );
    // The body's enclosing scope is the parent's node names (for E108),
    // plus the names of every scope enclosing the parent, so nested
    // compositions can detect references to any ancestor scope.
    let mut enclosing_scope_names: HashSet<String> =
        parent_schema_by_name.keys().cloned().collect();
    enclosing_scope_names.extend(bind_ctx.enclosing_scope_names.iter().cloned());
    let body_scope = BodyScope {
        use_path: resolved_path.clone(),
        // The composition file's directory (workspace-relative), so nested
        // `use:` paths resolve correctly.
        origin_dir: resolved_path
            .parent()
            .unwrap_or(Path::new(""))
            .to_path_buf(),
        scoped_vars: body_scoped_vars,
        enclosing_scope_names,
        outputs: body_output_nodes(signature),
        ports: input_port_rows.keys().cloned().collect(),
    };

    // Seed each port with the row its upstream carries: the records reach
    // the body untouched, in the upstream's column order with the
    // upstream's engine columns, so the body binds against exactly what it
    // will read, as the same nodes written inline would. Every position a
    // body node compiles (an Aggregate's group key and field arguments, a
    // Cull's decision aggregate) then addresses the record's real layout.
    // The port's declaration decides what the body may name and the types
    // it sees (see `seed_port_row`). The body binds against the types this
    // call provides; when those differ from the declared types, it is also
    // checked against the declared ones (see `bind_body`).
    let mut body_schema_by_name: HashMap<String, Row> = HashMap::new();
    let mut body_declared_multiple_by_name: HashMap<String, BTreeSet<String>> = HashMap::new();
    let body_file_label: Arc<str> = Arc::from(resolved_path.display().to_string().as_str());
    let mut port_seeds: IndexMap<String, Row> = IndexMap::new();
    let mut contract_seeds: IndexMap<String, Row> = IndexMap::new();
    for port_name in input_port_rows.keys() {
        let Some(upstream_row) = call_inputs
            .get(port_name)
            .and_then(|upstream| parent_schema_by_name.get(upstream))
        else {
            continue; // build_input_port_rows already diagnosed it
        };
        let origin = Arc::new(PortOrigin {
            call: Arc::from(node_name),
            port: Arc::from(port_name.as_str()),
            body_file: Arc::clone(&body_file_label),
        });
        let declared = signature
            .inputs
            .get(port_name)
            .map(port_columns)
            .unwrap_or_default();
        let contract = seed_port_row(
            upstream_row,
            &declared,
            Arc::clone(&origin),
            PortTypes::Declared,
        );
        contract_seeds.insert(port_name.clone(), contract);
        let seed = seed_port_row(upstream_row, &declared, origin, PortTypes::Provided);
        body_schema_by_name.insert(port_name.clone(), seed.clone());
        port_seeds.insert(port_name.clone(), seed);
        let declared_multiple = call_inputs
            .get(port_name)
            .and_then(|upstream| parent_declared_multiple_by_name.get(upstream))
            .cloned()
            .unwrap_or_default();
        body_declared_multiple_by_name.insert(port_name.clone(), declared_multiple);
    }

    // Reject duplicate body node names (E001) before minting. The
    // top-level E001 pass in `validate_acyclic` never sees body files, so a
    // body with two same-named nodes would otherwise collapse to ONE
    // `PlanNodeId` at the pre-mint below (`entry().or_insert_with`), which
    // both trips the `rebuild_id_index` coverage assert in debug and
    // silently aliases the two nodes' compiled programs in release. Emit
    // one diagnostic per offending node and abandon this body — leaving it
    // unbound mirrors the other `bind_composition` early-outs.
    {
        let mut seen: HashSet<&str> = HashSet::new();
        let mut has_duplicate = false;
        for spanned in &body_file.nodes {
            let n_name = spanned.value.name();
            if !seen.insert(n_name) {
                has_duplicate = true;
                diags.push(Diagnostic::error(
                    "E001",
                    format!(
                        "composition node {node_name:?}: body file {} declares \
                         duplicate node name: {n_name:?}",
                        resolved_path.display()
                    ),
                    LabeledSpan::primary(span_for_node(spanned), String::new()),
                ));
            }
        }
        if has_duplicate {
            return;
        }
    }

    // Reject a dotted body node name (E010) on the same grounds and with
    // the same message as the top-level pass, which never sees body files.
    // A body node's key is the call-site path joined to its name, so a `.`
    // in the name is a second join the reader cannot tell from the first.
    {
        let mut has_dotted = false;
        for spanned in &body_file.nodes {
            if let Some(diag) = crate::config::pipeline::dotted_node_name_diagnostic(
                spanned.value.name(),
                LabeledSpan::primary(span_for_node(spanned), String::new()),
            ) {
                has_dotted = true;
                diags.push(diag);
            }
        }
        if has_dotted {
            return;
        }
    }

    // Pre-mint a stable id per body node and per synthetic input-port
    // source BEFORE binding, so the bind walk can key this body's typed /
    // combine side-table entries by id — body node ids are not available
    // at lowering time, which runs after binding. Step-14 lowering CARRIES
    // these ids onto the body `PlanNode`s rather than minting fresh, so an
    // id is minted exactly once per node and two instantiations of one
    // composition body still get disjoint id sets (the shared counter
    // advances on each pre-mint). Body names are unique here (the E001 pass
    // above rejected duplicates), so each `or_insert_with` mints exactly
    // once. Port-source ids are minted into a separate map because a port
    // name and a body-internal node name can legally collide; each is
    // consumed in its own lowering context.
    let mut body_node_ids: HashMap<String, PlanNodeId> = HashMap::new();
    for spanned in &body_file.nodes {
        let n_name = spanned.value.name().to_string();
        body_node_ids
            .entry(n_name)
            .or_insert_with(|| artifacts.fresh_node_id());
    }
    let mut body_port_source_ids: HashMap<String, PlanNodeId> = HashMap::new();
    for (port_name, _row) in &input_port_rows {
        body_port_source_ids
            .entry(port_name.clone())
            .or_insert_with(|| artifacts.fresh_node_id());
    }

    let source_instances: Vec<CompiledSourceInstance> = pending_source_requirements
        .into_iter()
        .map(|(source_name, resource)| {
            CompiledSourceInstance::composition_body(
                body_id.into(),
                body_node_ids[&source_name],
                source_name,
                resource,
            )
        })
        .collect();

    // A channel `sources:` patch rewrites this call's body (step 7a), so
    // its check stands for no other call.
    let shared_body =
        !(bind_ctx.depth == 0 && bind_ctx.ctx.body_source_patches.contains_key(node_name));

    // Recursive bind_schema on body nodes, inside the body's scope.
    in_body_scope(bind_ctx, body_scope, |bind_ctx| {
        bind_body(
            &BodyBind {
                call: node_name,
                body_file: &resolved_path,
                shared_body,
                nodes: &body_file.nodes,
                node_ids: &body_node_ids,
                port_seeds: &port_seeds,
                contract_seeds: &contract_seeds,
                span,
            },
            diags,
            bind_ctx,
            artifacts,
            &mut body_schema_by_name,
            &mut body_declared_multiple_by_name,
        )
    });

    // 9. Compute output rows from signature.outputs → body schemas.
    let output_port_rows = compute_output_rows(&signature.outputs, &body_schema_by_name);

    // 10. A body node that named a caller column its port does not declare
    // was refused where it named it (E169). The body is still recorded, as
    // a body with any other refused node is, so a nested call it holds is
    // not left without an owner.
    //
    // W101: a body column written under the name of a caller column one of
    // the input ports passes through undeclared.
    for (port_name, row) in &output_port_rows {
        let output_node = signature
            .outputs
            .get(port_name)
            .map(|alias| {
                let internal = alias.internal_ref.value.as_str();
                internal.split('.').next().unwrap_or(internal)
            })
            .unwrap_or_default();
        for (field, _) in row.fields() {
            let column = field.name.as_ref();
            if column.starts_with('$') || !matches!(row.mark_of(field), Some(ColumnMark::Own)) {
                continue; // an engine column, or the caller's own value
            }
            // The caller's type for the declaration in the fix: the type
            // the port seed gives the column it carries undeclared, which is
            // the type the caller passes.
            let Some((input, caller_type)) = port_seeds.iter().find_map(|(input, seed)| {
                seed.undeclared_carried(column)
                    .map(|(ty, _)| (input, port_column_type(ty)))
            }) else {
                continue;
            };
            let writer = body_writer_of(
                &body_file.nodes,
                &body_node_ids,
                artifacts,
                output_node,
                column,
            );
            let (writer_span, writer_label) = match writer {
                Some(spanned) => (
                    span_for_node(spanned),
                    format!("node {:?} writes `{column}`", spanned.value.name()),
                ),
                None => (span, "body creates this column".to_string()),
            };
            diags.push(
                Diagnostic::warning(
                    "W101",
                    format!(
                        "composition {node_name:?} writes column `{column}` to output port \
                         {port_name:?}, replacing the caller's `{column}`, which input port \
                         {input:?} passes through undeclared"
                    ),
                    LabeledSpan::primary(writer_span, writer_label),
                )
                .with_help(format!(
                    "declare `{column}` on input port `{input}` in `{}` so replacing the \
                     caller's value is part of the port's contract:\n_compose:\n  inputs:\n    \
                     {input}:\n      schema:\n        - {{ name: {column}, type: {caller_type} }}",
                    resolved_path.display()
                )),
            );
        }
    }

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

    // A body with a node on a cycle or reading itself was refused when it
    // bound (E107, see `body_input_diagnostics`); its graph has no order to
    // lower in, and the same cycle is not reported twice.
    let reads_itself = body_file
        .nodes
        .iter()
        .any(|spanned| node_input_names(&spanned.value).contains(&spanned.value.name()));
    if reads_itself || !bind_order(&body_file.nodes).1.is_empty() {
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
    // The synthetic node's `output_schema` is the upstream's columns: the
    // records flowing in carry every upstream column, and the body bound
    // against that same row (step 8), so the port Source and every body
    // node agree on the layout by construction.
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
        // Carry the port-source id pre-minted before binding (never
        // re-mint), so it never collides with a top-level or body node id.
        let port_source = crate::plan::execution::PlanNode::Source {
            name: port_name.clone(),
            id: body_port_source_ids[port_name],
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
            clinker_core_types::span::Span::line_only(saphyr_line as u32)
        } else {
            clinker_core_types::span::Span::SYNTHETIC
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
        // Carry the body node's id pre-minted before binding (never
        // re-mint), so the typed/combine entries the bind walk keyed by
        // this id resolve to the node lowered here.
        let node_id = body_node_ids[&n_name];
        if let Some(mut plan_node) = crate::config::lower_node_to_plan_node(
            n, node_id, body_span, artifacts, &lower_ctx, diags,
        ) {
            if let crate::plan::execution::PlanNode::Source {
                resolved: Some(payload),
                ..
            } = &mut plan_node
            {
                payload.resource = source_instances
                    .iter()
                    .find(|instance| instance.id().source_node == node_id)
                    .and_then(CompiledSourceInstance::resource)
                    .cloned();
            }
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
        let consumer_name = n.name().to_string();
        let Some(&consumer_idx) = body_name_to_idx.get(consumer_name.as_str()) else {
            continue;
        };
        let mut wire = |producer_full: &str, port: Option<String>| {
            let producer_key = body_strip_port(producer_full);
            // Body-internal references resolve through name_to_idx;
            // references to a signature input port don't produce a
            // graph edge — port seeding is handled at composition
            // entry by the executor through the live edge graph.
            if let Some(&producer_idx) = body_name_to_idx.get(producer_key) {
                // Tag the producer output port a `<route>.<branch>` (or
                // bare-name) reference draws from, mirroring the top-level
                // wiring so the body executor routes Route branches off the
                // live edge graph too.
                let producer_port = crate::config::resolve_producer_port(
                    &body_graph[producer_idx],
                    producer_full,
                    producer_key,
                    &consumer_name,
                );
                body_graph.add_edge(
                    producer_idx,
                    consumer_idx,
                    PlanEdge {
                        dependency_type: DependencyType::Data,
                        port,
                        producer_port,
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
            | PipelineNode::Reshape { header, .. }
            | PipelineNode::Cull { header, .. }
            | PipelineNode::Sink { header, .. } => {
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
            PipelineNode::Envelope { header, .. } => {
                let refs = std::iter::once(&header.body)
                    .chain(header.header.iter())
                    .chain(header.trailer.iter());
                for ni in refs {
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

    // Populate `node_input_refs` from the body file so the body executor
    // can resolve `<route>.<branch>` references without re-parsing the
    // .comp.yaml at runtime.
    let mut node_input_refs: HashMap<String, Vec<String>> = HashMap::new();
    for spanned in &body_file.nodes {
        let n = &spanned.value;
        let n_name = n.name().to_string();
        let inputs: Vec<String> = match n {
            PipelineNode::Source { .. } => Vec::new(),
            PipelineNode::Composition { header, .. }
            | PipelineNode::Transform { header, .. }
            | PipelineNode::Aggregate { header, .. }
            | PipelineNode::Route { header, .. }
            | PipelineNode::Reshape { header, .. }
            | PipelineNode::Cull { header, .. }
            | PipelineNode::Sink { header, .. } => {
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
            PipelineNode::Envelope { header, .. } => std::iter::once(&header.body)
                .chain(header.header.iter())
                .chain(header.trailer.iter())
                .map(|ni| match &ni.value {
                    crate::config::node_header::NodeInput::Single(s) => s.clone(),
                    crate::config::node_header::NodeInput::Port { node, port } => {
                        format!("{node}.{port}")
                    }
                })
                .collect(),
        };
        node_input_refs.insert(n_name, inputs);
    }

    // Capture analytic-window configs from body Transform nodes so
    // the post-parent-DAG-build pass can build `body_indices_to_build`
    // without re-reading the body file. Body lowering does not stamp
    // `window_index` on the PlanNode::Transform — that backfill is
    // owned by the body-window pass once parent NodeIndex space is
    // allocated.
    let mut body_window_configs: HashMap<String, crate::plan::index::AnalyticWindowSpec> =
        HashMap::new();
    for spanned in &body_file.nodes {
        if let PipelineNode::Transform { header, config } = &spanned.value
            && let Some(spec) = config.analytic_window.as_ref()
        {
            body_window_configs.insert(header.name.clone(), spec.clone());
        }
    }

    // 15. Build and insert BoundBody. `body_indices_to_build` is
    // empty here — the post-parent-DAG-build pass populates it once
    // the parent's NodeIndex space exists, threading the parent's
    // body call tree is available and every window can receive a stable,
    // scope-local runtime key.
    let mut bound_body = BoundBody::empty(resolved_path);
    bound_body.body_scope = body_id.into();
    bound_body.semantic_name = body_file.signature.name.clone();
    // Digested here rather than at parse: steps 7a and 7d rewrote the body's
    // nodes with this call site's channel `sources:` patches and any external
    // schema file they name, and the identity has to be of the body that
    // binds, not the body the file spelled.
    bound_body.semantic_digest = body_file.semantic_digest();
    bound_body.resource_bindings = call_resources.clone();
    bound_body.source_instances = source_instances;
    bound_body.graph = body_graph;
    bound_body.topo_order = body_topo;
    bound_body.name_to_idx = body_name_to_idx;
    bound_body.port_name_to_node_idx = port_name_to_node_idx;
    bound_body.body_rows = body_rows;
    bound_body.node_input_refs = node_input_refs;
    bound_body.output_port_rows = output_port_rows.clone();
    bound_body.output_port_to_node_idx = output_port_to_node_idx;
    bound_body.input_port_rows = input_port_rows;
    bound_body.nested_body_ids = nested_body_ids;
    bound_body.body_window_configs = body_window_configs;
    // The body mini-DAG is structurally final here (its graph is built
    // once and toposorted above; later body passes only stamp fields).
    // Build the id→index bridge so every body node id resolves.
    bound_body.rebuild_id_index();
    artifacts.insert_body(body_id, bound_body);
    artifacts
        .composition_body_assignments
        .insert(node_id, body_id);

    // 16. Write composition's output row to parent scope.
    // Use the first output port's row as the composition node's output.
    if let Some((port_name, row)) = output_port_rows.into_iter().next() {
        let declared_multiple = signature
            .outputs
            .get(&port_name)
            .and_then(|output| {
                let internal_name = output
                    .internal_ref
                    .value
                    .split('.')
                    .next()
                    .unwrap_or(output.internal_ref.value.as_str());
                body_declared_multiple_by_name.get(internal_name)
            })
            .cloned()
            .unwrap_or_default();
        parent_declared_multiple_by_name.insert(node_name.to_string(), declared_multiple.clone());
        artifacts
            .declared_multiple
            .insert(node_id, declared_multiple);
        let row = carry_into_caller_scope(
            row,
            &CallerScope {
                call: node_name,
                in_body: bind_ctx.depth > 0,
                call_inputs,
                parent_schema_by_name,
                span,
            },
            diags,
        );
        parent_schema_by_name.insert(node_name.to_string(), row);
    }
}

// ─── Validation helpers ─────────────────────────────────────────────

fn compile_body_source_requirements(
    nodes: &[Spanned<PipelineNode>],
    signature: &CompositionSignature,
    call_resources: &IndexMap<String, ResourceBinding>,
    catalog: &WorkspaceCatalog,
    call_name: &str,
    body_path: &Path,
    diags: &mut Vec<Diagnostic>,
) -> Option<Vec<(String, CompiledResourceRequirement)>> {
    let mut requirements = Vec::new();
    let mut valid = true;

    for spanned in nodes {
        let PipelineNode::Source { config, .. } = &spanned.value else {
            continue;
        };
        let source_name = config.source.name.as_str();
        let node_span = span_for_node(spanned);
        let Some(authored_slot) = config.resource.as_ref() else {
            diags.push(
                Diagnostic::error(
                    "E103",
                    format!(
                        "composition node {call_name:?}: body file {}: source {source_name:?} declares no resource slot; authored body Sources cannot use an inert direct file target",
                        body_path.display()
                    ),
                    LabeledSpan::primary(node_span, "missing body Source resource slot"),
                )
                .with_help(
                    "add `resource: <slot>` to this Source, declare that slot under `_compose.resources_schema`, and bind it at the composition call; if rows arrive from the caller, remove this Source and consume the declared `_compose.inputs` port directly",
                ),
            );
            valid = false;
            continue;
        };
        let slot = authored_slot.value.as_str();
        let resource_span = authored_value_span(authored_slot, node_span);
        let matchers: Vec<&str> = [
            ("path", config.source.path.is_some()),
            ("glob", config.source.glob.is_some()),
            ("regex", config.source.regex.is_some()),
            ("paths", config.source.paths.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, present)| present.then_some(name))
        .collect();
        if !matchers.is_empty() {
            let remove = matchers
                .iter()
                .map(|matcher| format!("`{matcher}`"))
                .collect::<Vec<_>>()
                .join(", ");
            diags.push(
                Diagnostic::error(
                    "E103",
                    format!(
                        "composition node {call_name:?}: body file {}: source {source_name:?} declares `resource: {slot}` together with direct matcher(s) {}; a resource-backed Source has exactly one external target",
                        body_path.display(),
                        matchers.join(", ")
                    ),
                    LabeledSpan::primary(resource_span, "resource conflicts with direct matcher"),
                )
                .with_help(format!(
                    "remove {remove}; the catalog resource bound through `resource: {slot}` supplies the Source target"
                )),
            );
            valid = false;
            continue;
        }
        if !config.source.transport.is_file() {
            diags.push(
                Diagnostic::error(
                    "E103",
                    format!(
                        "composition node {call_name:?}: body file {}: source {source_name:?} binds file resource slot {slot:?} but declares `{}` transport",
                        body_path.display(),
                        config.source.transport.transport_name()
                    ),
                    LabeledSpan::primary(resource_span, "file resource requires file transport"),
                )
                .with_help("remove `transport:` so the Source uses the default file transport"),
            );
            valid = false;
            continue;
        }
        let Some(declaration) = signature.resources_schema.get(slot) else {
            diags.push(
                Diagnostic::error(
                    "E103",
                    format!(
                        "composition node {call_name:?}: body file {}: source {source_name:?} names resource slot {slot:?}, but `_compose.resources_schema` does not declare it",
                        body_path.display()
                    ),
                    LabeledSpan::primary(resource_span, "undeclared body Source resource slot"),
                )
                .with_help(format!(
                    "add `{slot}: {{ kind: file, required: true }}` under `_compose.resources_schema`"
                )),
            );
            valid = false;
            continue;
        };
        let Some(binding) = call_resources.get(slot) else {
            diags.push(
                Diagnostic::error(
                    "E103",
                    format!(
                        "composition node {call_name:?}: body source {source_name:?} uses resource slot {slot:?}, but this call does not bind it"
                    ),
                    LabeledSpan::primary(resource_span, "unbound body Source resource slot"),
                )
                .with_help(format!(
                    "add `resources: {{ {slot}: <catalog-resource-id> }}` to composition node {call_name:?}"
                )),
            );
            valid = false;
            continue;
        };
        let resource = match catalog.resolve_resource(binding.logical_id()) {
            Ok(resource) => resource,
            Err(error) => {
                diags.push(
                    Diagnostic::error(
                        "E103",
                        format!(
                            "composition node {call_name:?}: body source {source_name:?} cannot resolve resource slot {slot:?}: {error}"
                        ),
                        LabeledSpan::primary(resource_span, "unresolved body Source resource"),
                    )
                    .with_help("bind the slot to an existing compatible catalog resource"),
                );
                valid = false;
                continue;
            }
        };
        requirements.push((
            source_name.to_owned(),
            CompiledResourceRequirement {
                slot: slot.to_owned(),
                binding: binding.clone(),
                kind: declaration.kind,
                required_capabilities: declaration
                    .kind
                    .required_capabilities()
                    .to_vec()
                    .into_boxed_slice(),
                opener: declaration.kind.opener_kind(),
                lifetime: declaration.kind.lifetime(),
                dataset_identity: resource.dataset_identity(),
            },
        ));
    }

    valid.then_some(requirements)
}

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

fn validate_resources(
    call_site: &IndexMap<String, ResourceBinding>,
    signature: &CompositionSignature,
    catalog: &WorkspaceCatalog,
    node_name: &str,
    fallback_span: Span,
    diags: &mut Vec<Diagnostic>,
) -> bool {
    let mut valid = true;
    for (slot, declaration) in &signature.resources_schema {
        let Some(binding) = call_site.get(slot) else {
            if declaration.required {
                diags.push(
                    Diagnostic::error(
                        "E103",
                        format!(
                            "composition node {node_name:?}: required resource slot {slot:?} has no catalog binding"
                        ),
                        LabeledSpan::primary(fallback_span, "missing required resource binding"),
                    )
                    .with_help(format!(
                        "add `resources: {{ {slot}: <catalog-resource-id> }}` to this composition call"
                    )),
                );
                valid = false;
            }
            continue;
        };
        let span = binding
            .provenance()
            .winning_layer()
            .map_or(fallback_span, |layer| layer.span);
        let resource = match catalog.resolve_resource(binding.logical_id()) {
            Ok(resource) => resource,
            Err(error) => {
                diags.push(
                    Diagnostic::error(
                        "E103",
                        format!(
                            "composition node {node_name:?}: resource slot {slot:?} cannot bind `{}`: {error}",
                            binding.logical_id()
                        ),
                        LabeledSpan::primary(span, "unknown catalog resource"),
                    )
                    .with_help(format!(
                        "add `[catalog.resources.{}]` to clinker.toml or bind {slot:?} to an existing resource",
                        binding.logical_id()
                    )),
                );
                valid = false;
                continue;
            }
        };
        if resource.descriptor().kind() != declaration.kind {
            diags.push(
                Diagnostic::error(
                    "E103",
                    format!(
                        "composition node {node_name:?}: resource slot {slot:?} requires kind `{}`, but `{}` has kind `{}`",
                        declaration.kind.label(),
                        binding.logical_id(),
                        resource.descriptor().kind().label()
                    ),
                    LabeledSpan::primary(span, "incompatible resource kind"),
                )
                .with_help(format!(
                    "bind {slot:?} to a catalog resource with `kind = {:?}`",
                    declaration.kind.label()
                )),
            );
            valid = false;
            continue;
        }
        let missing = declaration
            .kind
            .required_capabilities()
            .iter()
            .find(|required| !resource.descriptor().capabilities().contains(required));
        if let Some(missing) = missing {
            diags.push(
                Diagnostic::error(
                    "E103",
                    format!(
                        "composition node {node_name:?}: resource slot {slot:?} requires capability {missing:?}, but `{}` does not provide it",
                        binding.logical_id()
                    ),
                    LabeledSpan::primary(span, "incompatible resource capability"),
                )
                .with_help("choose a compatible catalog resource or correct its `access` declaration"),
            );
            valid = false;
        }
    }
    for (slot, binding) in call_site {
        if !signature.resources_schema.contains_key(slot) {
            let span = binding
                .provenance()
                .winning_layer()
                .map_or(fallback_span, |layer| layer.span);
            diags.push(
                Diagnostic::error(
                    "E103",
                    format!(
                        "composition node {node_name:?}: resource slot {slot:?} is not declared in `_compose.resources_schema`"
                    ),
                    LabeledSpan::primary(span, "undeclared resource slot"),
                )
                .with_help("remove the binding or declare the slot in the composition signature"),
            );
            valid = false;
        }
    }
    valid
}

/// Populate [`ProvenanceDb`] entries for each resolved config param.
///
/// For each param in the signature's `config_schema`, the resolved value is:
/// - The call-site override if present, or
/// - The signature's default if present.
///
/// All values at this stage are tagged as [`LayerKind::PipelineDefault`]
/// since channel/group-level resolution is layered on afterwards.
fn populate_config_provenance(
    call_site: &IndexMap<String, serde_json::Value>,
    signature: &CompositionSignature,
    node_id: PlanNodeId,
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

        provenance.insert_unscoped(
            node_id,
            node_name.to_owned(),
            param_name.clone(),
            ResolvedValue::new(resolved_value, LayerKind::PipelineDefault, value_span),
        );
    }
}

// ─── Input port row building ────────────────────────────────────────

/// One call site's input ports, as [`build_input_port_rows`] checks them.
struct PortAdmission<'a> {
    /// The composition node's name at the call site.
    call: &'a str,
    /// The call site's `inputs:` map: port name to upstream node.
    call_inputs: &'a IndexMap<String, String>,
    signature: &'a CompositionSignature,
    /// The calling scope's bound rows.
    parent_schemas: &'a HashMap<String, Row>,
    /// The calling scope's Source nodes, whose `schema:` declares a type.
    source_nodes: &'a HashSet<String>,
    /// Every node of the calling scope.
    scope: &'a [Spanned<PipelineNode>],
    /// The nodes of the calling scope whose bind failed with an error.
    failed_nodes: &'a HashSet<String>,
    /// The composition file, as the author refers to it.
    body_file: &'a Path,
    span: Span,
}

/// Check a call site's bound input ports and return each port's vocabulary.
///
/// For each bound port, every declared column must reach the call with a
/// type the declaration admits (E102): the same type; `T` into
/// `{ nullable: T }`; anything into `any`. Nothing is converted at the port,
/// so there is no numeric widening, and a column the caller has no type for
/// (`any`) is admitted only by an `any` declaration: the body is typed by
/// the port, and a value the caller never typed is not known to be one.
///
/// Each port's declared columns are recorded as `Row::open(declared,
/// fresh_tail)`, the port's vocabulary. Nothing positional is compiled from
/// it: the body binds against the caller's row (see `seed_port_row`).
/// Returns `None` if any port was refused.
fn build_input_port_rows(
    admission: &PortAdmission<'_>,
    next_tail_var: &mut u32,
    diags: &mut Vec<Diagnostic>,
) -> Option<IndexMap<String, Row>> {
    let &PortAdmission {
        call,
        call_inputs,
        signature,
        parent_schemas,
        source_nodes,
        scope,
        failed_nodes,
        span,
        ..
    } = admission;
    let mut rows = IndexMap::new();
    let mut has_errors = false;

    for (port_name, port_decl) in &signature.inputs {
        // An unbound port is optional or already diagnosed by validate_inputs.
        let Some(upstream_name) = call_inputs.get(port_name) else {
            continue;
        };
        let Some(upstream_row) = parent_schemas.get(upstream_name.as_str()) else {
            // A node of this scope whose own bind failed said why where it
            // failed; a second error here would only say that the call
            // cannot read it.
            has_errors = true;
            if !failed_nodes.contains(upstream_name.as_str()) {
                diags.push(unbound_port_upstream(
                    call,
                    port_name,
                    upstream_name,
                    scope,
                    span,
                ));
            }
            continue;
        };
        let from_source = source_nodes.contains(upstream_name.as_str());

        let declared_columns = port_columns(port_decl);
        for (col_name, col_type) in &declared_columns {
            let col_str = col_name.name.as_ref();
            // A nested call inside a body names each column its port
            // declares: one the enclosing port carries undeclared is E169.
            if let Some(refused) = refuse_undeclared(upstream_row, call, col_str, span) {
                diags.push(refused);
                has_errors = true;
                continue;
            }
            let refusal = match upstream_row.lookup(col_str) {
                ColumnLookup::Declared(provided) if admits(col_type, provided) => None,
                ColumnLookup::Declared(provided) => Some(port_type_refusal(
                    admission, port_name, col_str, col_type, provided,
                )),
                // Every bound row is closed: a column it does not declare is
                // not there.
                ColumnLookup::Unknown | ColumnLookup::PassThrough(_) => Some(
                    Diagnostic::error(
                        "E102",
                        format!(
                            "composition {call:?}: input port `{port_name}` needs column \
                             `{col_str}`, which {upstream_name:?} does not provide"
                        ),
                        LabeledSpan::primary(span, String::new()),
                    )
                    .with_help(if from_source {
                        format!(
                            "add the column to the `schema:` of source {upstream_name:?}: \
                             `- {{ name: {col_str}, type: {} }}`",
                            port_column_type(col_type)
                        )
                    } else {
                        format!(
                            "emit `{col_str}` in {upstream_name:?} or a node before it, so the \
                             call receives it"
                        )
                    }),
                ),
                ColumnLookup::Ambiguous(_) => Some(Diagnostic::error(
                    "E102",
                    format!(
                        "composition {call:?}: input port `{port_name}` needs column \
                         `{col_str}`, and {upstream_name:?} provides more than one column of \
                         that name"
                    ),
                    LabeledSpan::primary(span, String::new()),
                )),
            };
            if let Some(refusal) = refusal {
                diags.push(refusal);
                has_errors = true;
            }
        }

        // Record the port's vocabulary: its declared columns, open to the
        // columns that pass through.
        let tail_var = cxl::typecheck::row::TailVarId(*next_tail_var);
        *next_tail_var += 1;
        let cxl_span = cxl::lexer::Span::new(span.start as usize, span.start as usize);
        let row = Row::open(declared_columns, cxl_span, tail_var);
        rows.insert(port_name.clone(), row);
    }

    if has_errors { None } else { Some(rows) }
}

/// The body nodes a composition names as its outputs (`out: node.port`
/// names `node`).
fn body_output_nodes(signature: &CompositionSignature) -> HashSet<String> {
    signature
        .outputs
        .values()
        .map(|alias| {
            let internal_ref = &alias.internal_ref.value;
            internal_ref
                .split('.')
                .next()
                .unwrap_or(internal_ref)
                .to_string()
        })
        .collect()
}

/// What reads an input port a call leaves unbound: a body node, or one of
/// the composition's output ports.
#[derive(Clone, Copy)]
enum UnboundPortReader<'a> {
    Node(&'a str),
    Output(&'a str),
}

/// A reader `reader` in the composition of the call `call` of input port
/// `port`, which the call does not bind.
struct UnboundPortRead<'a> {
    call: &'a str,
    reader: UnboundPortReader<'a>,
    port: &'a str,
    body_file: &'a Path,
    span: Span,
}

/// E104 for a composition call whose composition reads an input port the
/// call does not bind: a body node reads it, or an output port is it. The
/// port is optional, but what reads it needs it. The help is the sentence
/// to bind the port at the call; [`offer_port_bindings`] puts the line to
/// paste in its place once one checks out.
fn unbound_port_read(at: &UnboundPortRead<'_>) -> Diagnostic {
    let &UnboundPortRead {
        call,
        reader,
        port,
        body_file,
        span,
    } = at;
    let file = body_file.display();
    let message = match reader {
        UnboundPortReader::Node(node) => format!(
            "composition {call:?}: node {node:?} in `{file}` reads input port `{port}`, which \
             this call does not bind, so the node could receive no row"
        ),
        UnboundPortReader::Output(output) => format!(
            "composition {call:?}: output port `{output}` in `{file}` carries input port \
             `{port}`, which this call does not bind, so the call could write no row"
        ),
    };
    Diagnostic::error(
        "E104",
        message,
        LabeledSpan::primary(span, format!("does not bind `{port}`")),
    )
    .with_help(format!(
        "bind input port `{port}` under `inputs:` of composition {call:?} to a node whose rows \
         have the columns `{port}` declares"
    ))
}

/// The calling scope of a composition call, as a fix that binds one of
/// its ports chooses the node to bind it to.
struct CallFeeders<'a> {
    call: &'a str,
    call_inputs: &'a IndexMap<String, String>,
    scope: &'a [Spanned<PipelineNode>],
    failed_nodes: &'a HashSet<String>,
}

impl CallFeeders<'_> {
    /// The names a port of the call could be bound to, in the order a fix
    /// tries them: the nodes feeding the call's other ports, the calling
    /// body's own input ports, then every other node of the calling scope
    /// in declaration order. A node is left out when it has no row to
    /// give, failed, is the call, writes rows out (a Sink), or is a Route
    /// or a Cull, whose rows a call reads through one of its output ports.
    fn candidates(&self, bind_ctx: &BindContext<'_>, rows: &HashMap<String, Row>) -> Vec<String> {
        let feeds = |name: &str| {
            name != self.call
                && rows.contains_key(name)
                && !self.failed_nodes.contains(name)
                && !self.scope.iter().any(|spanned| {
                    spanned.value.name() == name
                        && matches!(
                            spanned.value,
                            PipelineNode::Sink { .. }
                                | PipelineNode::Route { .. }
                                | PipelineNode::Cull { .. }
                        )
                })
        };
        let mut out: Vec<String> = Vec::new();
        let named = self
            .call_inputs
            .values()
            .map(String::as_str)
            .chain(bind_ctx.body_ports.iter().map(String::as_str))
            .chain(self.scope.iter().map(|spanned| spanned.value.name()));
        for name in named {
            if feeds(name) && !out.iter().any(|seen| seen == name) {
                out.push(name.to_string());
            }
        }
        out
    }

    /// The call node, as the calling scope declares it, with `port` bound
    /// to `upstream`.
    fn binding(&self, port: &str, upstream: &str) -> Option<Spanned<PipelineNode>> {
        let mut edited = self
            .scope
            .iter()
            .find(|spanned| spanned.value.name() == self.call)?
            .clone();
        match &mut edited.value {
            PipelineNode::Composition { inputs, .. } => {
                inputs.insert(port.to_string(), upstream.to_string());
            }
            _ => return None,
        }
        Some(edited)
    }
}

/// Put in each E104 of `refused` (diagnostic index, port, reader) the line
/// that binds its port, naming the first node of the calling scope that,
/// bound to the port, lets the call compile without the E104 and without
/// an error the compile did not already raise (see
/// [`crate::plan::fix_offer`]). With none, the E104 keeps its sentence.
fn offer_port_bindings(
    refused: &[(usize, String, UnboundPortReader<'_>)],
    feeders: &CallFeeders<'_>,
    diags: &mut [Diagnostic],
    bind_ctx: &mut BindContext<'_>,
    artifacts: &CompileArtifacts,
    rows: &HashMap<String, Row>,
    declared_multiple: &HashMap<String, BTreeSet<String>>,
) {
    let call = feeders.call;
    let candidates = feeders.candidates(bind_ctx, rows);
    // The node each port binds to, decided by the port's first E104.
    let mut chosen: HashMap<&str, Option<String>> = HashMap::new();
    for (diag, port, reader) in refused {
        let lead = match reader {
            UnboundPortReader::Node(node) => format!(
                "bind input port `{port}` at the call, so node {node:?} receives rows: add under \
                 `inputs:` of composition {call:?}"
            ),
            UnboundPortReader::Output(output) => format!(
                "bind input port `{port}` at the call, so output port `{output}` has rows to \
                 carry: add under `inputs:` of composition {call:?}"
            ),
        };
        let offered = candidates.iter().take(MAX_FIX_TRIALS).map(|upstream| {
            (
                crate::plan::fix_offer::PasteFix::new(lead.clone(), format!("{port}: {upstream}")),
                upstream.clone(),
            )
        });
        let Some(plain) = diags[*diag].help.clone() else {
            continue;
        };
        let help = match chosen.get(port.as_str()) {
            Some(decided) => crate::plan::fix_offer::offer_fix(plain, offered, |upstream| {
                decided.as_ref() == Some(upstream)
            }),
            None => {
                let mut passed = None;
                let help = crate::plan::fix_offer::offer_fix(plain, offered, |upstream| {
                    let Some(edited) = feeders.binding(port, upstream) else {
                        return false;
                    };
                    let tried = trial_scope(
                        &ScopeTrial {
                            scope: feeders.scope,
                            edits: std::slice::from_ref(&edited),
                            targets: &[call],
                        },
                        bind_ctx,
                        artifacts,
                        rows,
                        declared_multiple,
                    );
                    let resolves =
                        crate::plan::fix_offer::fix_resolves(&diags[*diag], diags, &tried);
                    if resolves {
                        passed = Some(upstream.clone());
                    }
                    resolves
                });
                chosen.insert(port, passed);
                help
            }
        };
        diags[*diag].help = Some(help);
    }
}

/// E102 for a port whose upstream has no bound row. A `node.port`
/// reference names an output port of a node of the scope: of a Route or a
/// Cull, which a composition input port cannot read yet (#888), so the fix
/// routes the rows through a Transform that passes them on; of a
/// composition, whose call carries only the rows of its first output port
/// (#1315); or of a node that has no output ports at all.
fn unbound_port_upstream(
    call: &str,
    port: &str,
    upstream: &str,
    scope: &[Spanned<PipelineNode>],
    span: Span,
) -> Diagnostic {
    let kind_of = |name: &str| {
        scope
            .iter()
            .find(|spanned| spanned.value.name() == name)
            .map(|spanned| spanned.value.type_tag())
    };
    let named = upstream
        .split_once('.')
        .and_then(|(node, branch)| kind_of(node).map(|kind| (node, branch, kind)));
    match named {
        Some((node, branch, "route" | "cull")) => {
            let relay = format!("{node}_{branch}");
            Diagnostic::error(
                "E102",
                format!(
                    "composition {call:?}: input port `{port}` cannot read {upstream:?}, an \
                     output port of {node:?}; a composition input port cannot read the output \
                     port of a Route or a Cull yet"
                ),
                LabeledSpan::primary(span, String::new()),
            )
            .with_help(format!(
                "pass the rows through a Transform and feed the port from it: add\n  \
                 - type: transform\n    name: {relay}\n    input: {upstream}\n    config:\n      \
                 cxl: |\n        filter true\nthen write `{relay}` for `{upstream}` in the call's \
                 `input:` and under `inputs:` as `{port}: {relay}` (issue #888 tracks reading the \
                 branch directly)"
            ))
        }
        Some((node, branch, "composition")) => Diagnostic::error(
            "E102",
            format!(
                "composition {call:?}: input port `{port}` cannot read {upstream:?}, output port \
                 `{branch}` of composition {node:?}; a call carries only the rows of its first \
                 output port"
            ),
            LabeledSpan::primary(span, String::new()),
        )
        .with_help(format!(
            "write `{port}: {node}` under `inputs:` to feed the port from the first output \
             port of {node:?}; to read `{branch}`, move the nodes that read it into \
             `{node}`'s composition (issue #1315 tracks reading another output port)"
        )),
        Some((node, branch, kind)) => Diagnostic::error(
            "E102",
            format!(
                "composition {call:?}: input port `{port}` reads {upstream:?}, but {node:?} is a \
                 {kind} node, which has no output port `{branch}`"
            ),
            LabeledSpan::primary(span, String::new()),
        )
        .with_help(format!(
            "write `{port}: {node}` under `inputs:` to feed the port from {node:?}"
        )),
        _ => Diagnostic::error(
            "E102",
            format!(
                "composition {call:?}: input port `{port}` reads {upstream:?}, which is not a \
                 node this pipeline binds"
            ),
            LabeledSpan::primary(span, String::new()),
        ),
    }
}

/// How E102 says what type a caller provides a column with, in the author's
/// words: the port spelling where there is one, and a description for a
/// value a port cannot declare.
fn provided_type_words(ty: &Type) -> String {
    match ty {
        Type::Null => "always empty (every value is null)".to_string(),
        Type::Array => "as a list".to_string(),
        Type::Map => "as a map".to_string(),
        Type::Numeric => "as a number".to_string(),
        Type::Any => "with no declared type (`any`)".to_string(),
        other => format!("as `{}`", port_column_type(other)),
    }
}

/// Whether a port column declared `declared` admits a caller column of type
/// `provided`: the same type, `T` into `{ nullable: T }`, or anything into
/// `any`. A caller column typed `any` is admitted only by `any`.
fn admits(declared: &Type, provided: &Type) -> bool {
    match declared {
        Type::Any => true,
        // `{ nullable: any }` is `any`: an empty value is already one.
        Type::Nullable(inner) if matches!(inner.as_ref(), Type::Any) => true,
        Type::Nullable(inner) => provided == declared || provided == inner.as_ref(),
        _ => provided == declared,
    }
}

/// E102 for a caller column whose type the port's declaration does not
/// admit, with the one change that makes the call compile.
fn port_type_refusal(
    admission: &PortAdmission<'_>,
    port: &str,
    column: &str,
    declared: &Type,
    provided: &Type,
) -> Diagnostic {
    let &PortAdmission {
        call,
        call_inputs,
        source_nodes,
        body_file,
        span,
        ..
    } = admission;
    if let Some(refusal) = enclosing_port_may_be_empty(admission, port, column, declared, provided)
    {
        return refusal;
    }
    let upstream = call_inputs
        .get(port)
        .map(String::as_str)
        .unwrap_or_default();
    let from_source = source_nodes.contains(upstream);
    let declared_spelling = port_column_type(declared);
    let provided_spelling = port_column_type(provided);
    let base = declared.unwrap_nullable();
    let conversion =
        conversion_method(base).map(|method| format!("`emit {column} = {column}.{method}()`"));
    let message = if matches!(provided, Type::Any) {
        format!(
            "composition {call:?}: input port `{port}` declares `{column}` as \
             `{declared_spelling}`, but {upstream:?} provides `{column}` with no declared type \
             (`any`), so nothing guarantees its values are `{declared_spelling}`"
        )
    } else {
        format!(
            "composition {call:?}: input port `{port}` declares `{column}` as \
             `{declared_spelling}`, but {upstream:?} provides `{column}` {}",
            provided_type_words(provided)
        )
    };
    let help = match (provided, &conversion) {
        (Type::Any, _) if from_source => format!(
            "declare the type where source {upstream:?} declares the column: \
             `- {{ name: {column}, type: {} }}`",
            port_column_type(base)
        ),
        (Type::Nullable(inner), _) if inner.as_ref() == declared => format!(
            "{upstream:?} can provide an empty `{column}`; declare the port column as nullable \
             in `{}`: `- {{ name: {column}, type: {{ nullable: {declared_spelling} }} }}`",
            body_file.display()
        ),
        (_, Some(conversion)) => {
            format!("convert the column in a transform before the call: {conversion}")
        }
        (_, None) => format!(
            "give `{column}` the type `{declared_spelling}` before the call, or declare it as \
             `{provided_spelling}` on input port `{port}` in `{}`",
            body_file.display()
        ),
    };
    Diagnostic::error("E102", message, LabeledSpan::primary(span, String::new())).with_help(
        format!(
            "{help}. A port admits the type it declares, that type's non-empty form into a \
         `{{ nullable: ... }}` declaration, and anything into `any`; nothing is converted at \
         the port"
        ),
    )
}

/// E102 for a call inside a composition body whose port needs `column` never
/// empty, when the column reaches it unchanged from the enclosing
/// composition's own input port, which declares it `{ nullable: T }`.
///
/// The enclosing body is checked at every call as if the column could be
/// empty, so the cause is that declaration, not the node feeding the call:
/// the error names the enclosing port, and the fix to paste is that port's
/// line with the never-empty type. `None` for every other refusal.
fn enclosing_port_may_be_empty(
    admission: &PortAdmission<'_>,
    port: &str,
    column: &str,
    declared: &Type,
    provided: &Type,
) -> Option<Diagnostic> {
    let &PortAdmission {
        call,
        call_inputs,
        parent_schemas,
        body_file,
        span,
        ..
    } = admission;
    let Type::Nullable(inner) = provided else {
        return None;
    };
    if inner.as_ref() != declared {
        return None;
    }
    let carried = parent_schemas
        .get(call_inputs.get(port)?.as_str())?
        .carried(&QualifiedField::bare(column))?;
    if carried.access != PortAccess::Declared {
        return None;
    }
    let PortOrigin {
        call: outer,
        port: outer_port,
        body_file: outer_file,
    } = carried.port.as_ref();
    let needed = port_column_type(declared);
    let possibly_empty = port_column_type(provided);
    Some(
        Diagnostic::error(
            "E102",
            format!(
                "composition {outer:?} passes a `{column}` that may be empty to composition \
                 {call:?}, whose input port `{port}` needs `{column}` never empty (`{needed}`): \
                 input port `{outer_port}` of {outer:?} declares `{column}` as \
                 `{possibly_empty}` in `{outer_file}`, so {outer:?} is checked at every call as \
                 if `{column}` could be empty"
            ),
            LabeledSpan::primary(span, String::new()),
        )
        .with_help(format!(
            "declare `{column}` as never empty on input port `{outer_port}` in `{outer_file}`: \
             `- {{ name: {column}, type: {needed} }}`, so {outer:?} accepts only a `{column}` \
             that is never empty, as {call:?} needs. If {outer:?} must accept an empty \
             `{column}`, declare it nullable on input port `{port}` in `{}` instead: \
             `- {{ name: {column}, type: {possibly_empty} }}`",
            body_file.display()
        )),
    )
}

/// The CXL method that converts a value to `ty`, where one exists.
fn conversion_method(ty: &Type) -> Option<&'static str> {
    Some(match ty {
        Type::Int => "to_int",
        Type::Float => "to_float",
        Type::Decimal => "to_decimal",
        Type::String => "to_string",
        Type::Bool => "to_bool",
        Type::Date => "to_date",
        Type::DateTime => "to_datetime",
        _ => return None,
    })
}

/// Extract declared columns from a `PortDecl` as bare
/// `IndexMap<QualifiedField, Type>`. Composition ports never carry
/// qualifiers — qualification is a Combine-only concept.
fn port_columns(decl: &PortDecl) -> IndexMap<QualifiedField, Type> {
    match &decl.schema {
        Some(columns) => columns
            .iter()
            .map(|c| (QualifiedField::bare(c.name.as_str()), c.bound_type()))
            .collect(),
        None => IndexMap::new(),
    }
}

// ─── Body re-parse ──────────────────────────────────────────────────

/// Re-read and parse a composition body from disk.
fn read_and_parse_body(
    source_path: &Path,
    resolved_path: &Path,
    ctx: &CompileContext,
    artifacts: &mut CompileArtifacts,
    node_name: &str,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) -> Option<CompositionFile> {
    let bytes = match std::fs::read(source_path) {
        Ok(bytes) => bytes,
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
    if let Some(expected) = ctx.composition_body_identities.get(resolved_path)
        && blake3::hash(&bytes).as_bytes() != expected
    {
        diags.push(Diagnostic::error(
            "E103",
            format!(
                "composition node {node_name:?}: body {} changed after module-closure discovery; retry compilation against a stable workspace snapshot",
                source_path.display()
            ),
            LabeledSpan::primary(span, String::new()),
        ));
        return None;
    }
    let yaml = match String::from_utf8(bytes) {
        Ok(yaml) => yaml,
        Err(e) => {
            diags.push(Diagnostic::error(
                "E101",
                format!(
                    "composition node {node_name:?}: body parse failed for {}: body is not UTF-8: {e}",
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

// ─── Port vocabulary ────────────────────────────────────────────────

/// The body node nearest `output` (walking its inputs back) whose emits
/// write `column`: the line W101 points at.
fn body_writer_of<'a>(
    nodes: &'a [Spanned<PipelineNode>],
    node_ids: &HashMap<String, PlanNodeId>,
    artifacts: &CompileArtifacts,
    output: &str,
    column: &str,
) -> Option<&'a Spanned<PipelineNode>> {
    let mut queue: std::collections::VecDeque<&str> = std::collections::VecDeque::from([output]);
    let mut seen: HashSet<&str> = HashSet::new();
    while let Some(name) = queue.pop_front() {
        if !seen.insert(name) {
            continue;
        }
        let Some(spanned) = nodes.iter().find(|spanned| spanned.value.name() == name) else {
            continue;
        };
        let mut writes = false;
        if let Some(typed) = node_ids.get(name).and_then(|id| artifacts.typed_get(*id)) {
            cxl::ast::for_each_field_emit(&typed.program.statements, &mut |emitted, _| {
                writes |= emitted == column;
            });
        }
        if writes {
            return Some(spanned);
        }
        queue.extend(node_input_names(&spanned.value));
    }
    None
}

/// The names of the nodes `node` reads, with a port suffix stripped: its
/// `input:` (or inputs), and for a composition call each `inputs:` port's
/// upstream.
fn node_input_names(node: &PipelineNode) -> Vec<&str> {
    let mut inputs: Vec<&str> = node.direct_input_names();
    if let PipelineNode::Composition { inputs: ports, .. } = node {
        inputs.extend(
            ports
                .values()
                .map(|upstream| upstream.split('.').next().unwrap_or(upstream)),
        );
    }
    inputs
}

/// The order the bind walk binds `nodes` in: each node after every node of
/// the same scope it reads, keeping the authored order wherever it already
/// holds. Returns the order and the nodes that could not be placed because
/// they are on, or read from, a cycle; those come last in the order.
fn bind_order(nodes: &[Spanned<PipelineNode>]) -> (Vec<usize>, Vec<usize>) {
    let names: HashSet<&str> = nodes.iter().map(|spanned| spanned.value.name()).collect();
    let reads: Vec<Vec<&str>> = nodes
        .iter()
        .map(|spanned| {
            let own = spanned.value.name();
            node_input_names(&spanned.value)
                .into_iter()
                .filter(|input| names.contains(input) && *input != own)
                .collect()
        })
        .collect();
    let mut placed: HashSet<&str> = HashSet::new();
    let mut order: Vec<usize> = Vec::with_capacity(nodes.len());
    let mut pending: Vec<usize> = (0..nodes.len()).collect();
    loop {
        let Some(at) = pending
            .iter()
            .position(|&i| reads[i].iter().all(|input| placed.contains(input)))
        else {
            break;
        };
        let i = pending.remove(at);
        placed.insert(nodes[i].value.name());
        order.push(i);
    }
    order.extend(pending.iter().copied());
    (order, pending)
}

/// Whether `node` reads one node through a single `input:` line, so a fix
/// can rewrite that line.
fn reads_through_one_input_line(node: &PipelineNode) -> bool {
    matches!(
        node,
        PipelineNode::Transform { .. }
            | PipelineNode::Aggregate { .. }
            | PipelineNode::Route { .. }
            | PipelineNode::Sink { .. }
            | PipelineNode::Reshape { .. }
            | PipelineNode::Cull { .. }
    )
}

/// `node` reading `to` through its single `input:` line, for a node that
/// has one ([`reads_through_one_input_line`]).
fn with_single_input(node: &Spanned<PipelineNode>, to: &str) -> Option<Spanned<PipelineNode>> {
    let mut edited = node.clone();
    match &mut edited.value {
        PipelineNode::Transform { header, .. }
        | PipelineNode::Aggregate { header, .. }
        | PipelineNode::Route { header, .. }
        | PipelineNode::Sink { header, .. }
        | PipelineNode::Reshape { header, .. }
        | PipelineNode::Cull { header, .. } => {
            header.input.value = crate::config::node_header::NodeInput::Single(to.to_string());
        }
        _ => return None,
    }
    Some(edited)
}

/// What [`body_input_diagnostics`] reads of a composition body.
struct BodyInputs<'a> {
    nodes: &'a [Spanned<PipelineNode>],
    /// The nodes [`bind_order`] could not place.
    unplaced: &'a [usize],
    /// Every name a body node may read besides the body's own nodes.
    ports: &'a HashSet<&'a str>,
    /// The input ports the call binds, in declaration order.
    declared_ports: &'a [String],
    enclosing: &'a HashSet<String>,
    /// The body nodes the composition names as its outputs.
    outputs: &'a HashSet<String>,
}

/// The nodes [`body_input_diagnostics`] reported, which cannot receive a
/// row, and the fixes its diagnostics would print once checked.
struct BodyInputFaults {
    failed: Vec<String>,
    fixes: Vec<BodyInputFix>,
}

/// A fix one of [`body_input_diagnostics`]' errors would print: node
/// `node` reading one of `candidates` through its `input:` line. It is
/// printed only when the body, edited so, binds `targets` without the error
/// (see [`check_body_input_fix`]).
struct BodyInputFix {
    /// The diagnostic's index in the diagnostics list.
    diag: usize,
    node: usize,
    /// The names the node could read instead, in the order to try them.
    candidates: Vec<String>,
    /// The nodes the edit is for: the node, and every other node of its
    /// cycle.
    targets: Vec<String>,
    /// The sentence leading to the `input:` line.
    lead: String,
}

/// E004 and E107 for a composition body's nodes whose input the bind walk
/// cannot bind: an input that names no node and no input port of the body,
/// a node that reads itself, and a cycle. At pipeline level the same shapes
/// are refused before binding (E004, E002, E003); a body is first read here.
///
/// Each diagnostic's help is one plain sentence. Where the fix is one
/// node's `input:` line, the line is offered as well (see
/// [`BodyInputFix`]), and replaces the sentence once checked. A cycle
/// through a Merge, a Combine or a composition call keeps the sentence:
/// such a node reads several inputs, and the line to change is the
/// author's choice. A fix never makes a node read an output node, since a
/// body node that reads an output node fails when the call runs (#1315).
fn body_input_diagnostics(body: &BodyInputs<'_>, diags: &mut Vec<Diagnostic>) -> BodyInputFaults {
    let &BodyInputs {
        nodes,
        unplaced,
        ports,
        declared_ports,
        enclosing,
        outputs,
    } = body;
    let names: HashSet<&str> = nodes.iter().map(|spanned| spanned.value.name()).collect();
    let mut faults = BodyInputFaults {
        failed: Vec::new(),
        fixes: Vec::new(),
    };
    for (index, spanned) in nodes.iter().enumerate() {
        let node = spanned.value.name();
        let span = span_for_node(spanned);
        for input in node_input_names(&spanned.value) {
            let (diag, candidates, lead) = if input == node {
                (
                    Diagnostic::error(
                        "E107",
                        format!(
                            "composition node {node:?} reads itself, so it can never \
                             receive a row"
                        ),
                        LabeledSpan::primary(span, "reads itself".to_string()),
                    )
                    .with_help(format!(
                        "make node {node:?} read an input port of the composition instead of \
                         itself"
                    )),
                    declared_ports.to_vec(),
                    format!("set `input:` of node {node:?} to an input port of the composition"),
                )
            } else if !names.contains(input) && !ports.contains(input) && !enclosing.contains(input)
            {
                // A misspelt name may have meant a port or a node; any node
                // but an output node, which a body node cannot read.
                let candidates = declared_ports
                    .iter()
                    .cloned()
                    .chain(
                        nodes
                            .iter()
                            .map(|other| other.value.name())
                            .filter(|other| *other != node && !outputs.contains(*other))
                            .map(str::to_string),
                    )
                    .collect();
                (
                    Diagnostic::error(
                        "E004",
                        format!(
                            "composition node {node:?} input {input:?} references no node and \
                             no input port of the composition"
                        ),
                        LabeledSpan::primary(span, format!("reads {input:?}")),
                    )
                    .with_help(format!(
                        "change `input:` of node {node:?}, which names nothing in the \
                         composition, to a node of the composition or one of its input ports"
                    )),
                    candidates,
                    format!("set `input:` of node {node:?} to a name the composition has"),
                )
            } else {
                continue;
            };
            faults.failed.push(node.to_string());
            if reads_through_one_input_line(&spanned.value) {
                faults.fixes.push(BodyInputFix {
                    diag: diags.len(),
                    node: index,
                    candidates,
                    targets: vec![node.to_string()],
                    lead,
                });
            }
            diags.push(diag);
        }
    }
    // Of the unplaced nodes, those on a cycle: each reaches itself through
    // the unplaced nodes it reads. The nodes that reach each other are one
    // cycle, reported once, naming every node on it in declaration order.
    let unplaced_names: HashSet<&str> = unplaced.iter().map(|&i| nodes[i].value.name()).collect();
    faults
        .failed
        .extend(unplaced_names.iter().map(|name| name.to_string()));
    let reads_of = |name: &str| -> Vec<&str> {
        nodes
            .iter()
            .find(|spanned| spanned.value.name() == name)
            .map(|spanned| {
                node_input_names(&spanned.value)
                    .into_iter()
                    .filter(|input| unplaced_names.contains(input) && *input != name)
                    .collect()
            })
            .unwrap_or_default()
    };
    // Every unplaced node `from` reaches by reading, `from` itself included
    // exactly when it is on a cycle.
    let reaches = |from: &str| -> HashSet<&str> {
        let mut seen: HashSet<&str> = HashSet::new();
        let mut stack: Vec<&str> = reads_of(from);
        while let Some(next) = stack.pop() {
            if seen.insert(next) {
                stack.extend(reads_of(next));
            }
        }
        seen
    };
    let mut reported: HashSet<&str> = HashSet::new();
    for &i in unplaced {
        let node = nodes[i].value.name();
        if reported.contains(node) {
            continue;
        }
        let reach = reaches(node);
        if !reach.contains(node) {
            continue;
        }
        let members: Vec<&str> = unplaced
            .iter()
            .map(|&j| nodes[j].value.name())
            .filter(|member| {
                *member == node || (reach.contains(member) && reaches(member).contains(node))
            })
            .collect();
        reported.extend(members.iter().copied());
        let named: Vec<String> = members.iter().map(|name| format!("{name:?}")).collect();
        let (listed, none) = match named.as_slice() {
            [first, second] => (format!("{first} and {second}"), "neither"),
            [rest @ .., last] => (format!("{} and {last}", rest.join(", ")), "none of them"),
            [] => unreachable!("a node on a cycle is one of its members"),
        };
        // Changing one member to read a port leaves every other member
        // reading the member before it. A member reading an output node
        // would then be a node that reads the node the call's rows leave
        // by, which fails when the call runs; so the member to change is the
        // one that reads an output node on the cycle, or, when none does,
        // the first.
        let reading_an_output: Vec<&str> = members
            .iter()
            .copied()
            .filter(|member| {
                reads_of(member)
                    .iter()
                    .any(|input| members.contains(input) && outputs.contains(*input))
            })
            .collect();
        let to_change = match reading_an_output.as_slice() {
            [] => Some(members[0]),
            [only] => Some(*only),
            _ => None,
        };
        let change = to_change.and_then(|member| {
            let index = nodes
                .iter()
                .position(|spanned| spanned.value.name() == member)?;
            let from = reads_of(member)
                .into_iter()
                .find(|input| members.contains(input))?;
            Some((index, member, from))
        });
        let plain = match (change, reading_an_output.as_slice()) {
            (Some((_, member, from)), _) => format!(
                "change what node {member:?} reads so that it no longer reads node {from:?}, \
                 which ends the cycle"
            ),
            (None, several) => format!(
                "change what nodes {} read so that none of them reads a node the composition \
                 names as an output, which ends the cycle",
                several
                    .iter()
                    .map(|name| format!("{name:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };
        if let Some((index, member, _)) = change
            && reads_through_one_input_line(&nodes[index].value)
            && members
                .iter()
                .all(|other| reads_through_one_input_line_or_is(nodes, other, member))
        {
            faults.fixes.push(BodyInputFix {
                diag: diags.len(),
                node: index,
                candidates: declared_ports.to_vec(),
                targets: members.iter().map(|name| name.to_string()).collect(),
                lead: format!(
                    "set `input:` of node {member:?} to an input port of the composition"
                ),
            });
        }
        diags.push(
            Diagnostic::error(
                "E107",
                format!(
                    "composition nodes {listed} read each other in a cycle, so {none} can receive \
                     a row"
                ),
                LabeledSpan::primary(span_for_node(&nodes[i]), "on a cycle".to_string()),
            )
            .with_help(plain),
        );
    }
    faults
}

/// Whether body node `name` reads through a single `input:` line, or is
/// `member`. A cycle through a node that reads several inputs (a Merge, a
/// Combine, a composition call) gets no generated fix.
fn reads_through_one_input_line_or_is(
    nodes: &[Spanned<PipelineNode>],
    name: &str,
    member: &str,
) -> bool {
    name == member
        || nodes
            .iter()
            .find(|spanned| spanned.value.name() == name)
            .is_some_and(|spanned| reads_through_one_input_line(&spanned.value))
}

/// Show the `input:` line of `fix` in place of its diagnostic's sentence
/// when the body, edited so, binds the nodes the fix is for without the
/// error and without an error the bind did not already raise.
fn check_body_input_fix(
    fix: BodyInputFix,
    nodes: &[Spanned<PipelineNode>],
    diags: &mut [Diagnostic],
    bind_ctx: &mut BindContext<'_>,
    artifacts: &CompileArtifacts,
    schema_by_name: &HashMap<String, Row>,
    declared_multiple_by_name: &HashMap<String, BTreeSet<String>>,
) {
    let BodyInputFix {
        diag,
        node,
        candidates,
        targets,
        lead,
    } = fix;
    let targets: Vec<&str> = targets.iter().map(String::as_str).collect();
    let Some(plain) = diags[diag].help.clone() else {
        return;
    };
    let offered = candidates
        .into_iter()
        .take(MAX_FIX_TRIALS)
        .filter_map(|candidate| {
            let edited = with_single_input(&nodes[node], &candidate)?;
            Some((
                crate::plan::fix_offer::PasteFix::new(lead.clone(), format!("input: {candidate}")),
                edited,
            ))
        });
    let help = crate::plan::fix_offer::offer_fix(plain, offered, |edited| {
        let tried = trial_scope(
            &ScopeTrial {
                scope: nodes,
                edits: std::slice::from_ref(edited),
                targets: &targets,
            },
            bind_ctx,
            artifacts,
            schema_by_name,
            declared_multiple_by_name,
        );
        crate::plan::fix_offer::fix_resolves(&diags[diag], diags, &tried)
    });
    diags[diag].help = Some(help);
}

/// The most edits one help tries before it settles for its sentence.
const MAX_FIX_TRIALS: usize = 16;

/// An edit to a scope's nodes, to try in scratch state (see
/// [`trial_scope`]).
struct ScopeTrial<'a> {
    scope: &'a [Spanned<PipelineNode>],
    /// Nodes that replace the scope's node of the same name, or join the
    /// scope when it has none.
    edits: &'a [Spanned<PipelineNode>],
    /// The nodes the edit is for. The trial binds them and every node of
    /// the edited scope they read, directly or not.
    targets: &'a [&'a str],
}

/// The diagnostics of binding `trial`'s edited scope, in scratch state.
///
/// Only the nodes the targets read, and the targets, bind; every other
/// node keeps the row it has in `rows`. The bind runs as a trial
/// ([`BindContext::verifying`]): nothing it does reaches `artifacts`,
/// `rows` or the record of passed contract checks.
fn trial_scope(
    trial: &ScopeTrial<'_>,
    bind_ctx: &mut BindContext<'_>,
    artifacts: &CompileArtifacts,
    rows: &HashMap<String, Row>,
    declared_multiple: &HashMap<String, BTreeSet<String>>,
) -> Vec<Diagnostic> {
    let edit_of = |name: &str| trial.edits.iter().find(|edit| edit.value.name() == name);
    let mut edited: Vec<Spanned<PipelineNode>> = trial
        .scope
        .iter()
        .map(|spanned| edit_of(spanned.value.name()).unwrap_or(spanned).clone())
        .collect();
    edited.extend(
        trial
            .edits
            .iter()
            .filter(|edit| {
                !trial
                    .scope
                    .iter()
                    .any(|spanned| spanned.value.name() == edit.value.name())
            })
            .cloned(),
    );
    let mut needed: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = trial.targets.iter().map(|name| name.to_string()).collect();
    while let Some(name) = stack.pop() {
        if !needed.insert(name.clone()) {
            continue;
        }
        if let Some(spanned) = edited.iter().find(|spanned| spanned.value.name() == name) {
            stack.extend(
                node_input_names(&spanned.value)
                    .into_iter()
                    .map(str::to_string),
            );
        }
    }
    edited.retain(|spanned| needed.contains(spanned.value.name()));
    let mut scratch = artifacts.scratch();
    let ids: HashMap<String, PlanNodeId> = edited
        .iter()
        .map(|spanned| (spanned.value.name().to_string(), scratch.fresh_node_id()))
        .collect();
    let mut rows = rows.clone();
    let mut multiple = declared_multiple.clone();
    for name in ids.keys() {
        rows.remove(name);
        multiple.remove(name);
    }
    let was_verifying = std::mem::replace(&mut bind_ctx.verifying, true);
    let checks = bind_ctx.passed_contract_checks.len();
    let mut tried = Vec::new();
    bind_schema_inner(
        &edited,
        &ids,
        &mut tried,
        bind_ctx,
        &mut scratch,
        &mut rows,
        &mut multiple,
    );
    bind_ctx.passed_contract_checks.truncate(checks);
    bind_ctx.verifying = was_verifying;
    tried
}

/// Which types a body's port rows give the columns the port declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortTypes {
    /// The declared types: the composition's contract, which every call is
    /// checked against. A `{ nullable: T }` column is seen as possibly empty
    /// whatever the caller passes.
    Declared,
    /// The types this call provides, where the declaration admits them as
    /// they are: a `{ nullable: T }` column fed a never-empty `T` is `T`.
    /// The rows a call carries out, and so the types after the call, follow
    /// from these, as they would for the same nodes written inline.
    Provided,
}

/// One composition body to bind at one call site.
struct BodyBind<'a> {
    /// The composition node's name at the call site.
    call: &'a str,
    /// The composition file, workspace-relative: which body this is.
    body_file: &'a Path,
    /// Whether `nodes` is the composition file as every call reads it, so a
    /// check of it can stand for another call's (no channel patch rewrote
    /// it for this call).
    shared_body: bool,
    nodes: &'a [Spanned<PipelineNode>],
    node_ids: &'a HashMap<String, PlanNodeId>,
    /// Each port's row with the types this call provides
    /// ([`PortTypes::Provided`]); already in the body's row map.
    port_seeds: &'a IndexMap<String, Row>,
    /// Each port's row with the declared types ([`PortTypes::Declared`]).
    contract_seeds: &'a IndexMap<String, Row>,
    span: Span,
}

/// Everything a composition body's check against its ports' declared types
/// depends on, except the call's name.
///
/// The check is a pure function of these: the body (its file; a channel
/// patch makes a call's body its own, see [`BodyBind::shared_body`]), the
/// rows its ports are seeded with at their declared types, which columns
/// hold several values, the scoped variables and `$config` values the body
/// sees, where the body sits in the `use:` chain, and which of the names
/// its nodes read from outside the body belong to an enclosing scope. The
/// call's name only words the check's diagnostics, so a check that raised
/// none stands for every call with the same inputs; one that raised any is
/// run again at each call, so each call's diagnostics name that call.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ContractCheck {
    body_file: PathBuf,
    /// Each port's seed row, in port order: its tail, and its columns in
    /// order with their types and what the body may do with each (see
    /// [`SeedRow`]).
    seeds: Vec<(String, SeedRow)>,
    declared_multiple: HashMap<String, BTreeSet<String>>,
    scoped_vars: ScopedVarsSeen,
    use_path_stack: Vec<PathBuf>,
    depth: u32,
    /// Each name a body node reads that is neither a body node nor an input
    /// port, and whether an enclosing scope has a node of that name.
    outside_inputs: Vec<(String, bool)>,
}

/// What a body's check reads of a port's seed row: its tail, which decides
/// what a name the row does not declare resolves to ([`Row::lookup`]: an
/// unknown column on a closed row, a pass-through on an open one), and its
/// columns in order ([`SeedColumn`]). The row's declaration span only words
/// diagnostics, and so does the call's name in a mark, so neither is in the
/// key.
#[derive(Debug, Clone, PartialEq)]
struct SeedRow {
    tail: cxl::typecheck::RowTail,
    columns: Vec<SeedColumn>,
}

impl SeedRow {
    fn of_row(row: &Row) -> Self {
        Self {
            tail: row.tail.clone(),
            columns: SeedColumn::of_row(row),
        }
    }
}

/// What a body's check reads of one column of a port's seed row: its
/// position (the column's place in the row), its name, its type and what
/// the port's declaration lets the body do with it.
#[derive(Debug, Clone, PartialEq)]
struct SeedColumn {
    field: QualifiedField,
    ty: Type,
    access: Option<SeedAccess>,
}

/// A seed column's mark, as a check reads it: the body's own column, or one
/// carried in through the port being seeded with the port's access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeedAccess {
    Own,
    Carried(PortAccess),
}

impl SeedColumn {
    /// The columns of a seed row, in order.
    fn of_row(row: &Row) -> Vec<Self> {
        row.fields()
            .map(|(field, ty)| Self {
                field: field.clone(),
                ty: ty.clone(),
                access: row.mark_of(field).map(|mark| match mark {
                    ColumnMark::Own => SeedAccess::Own,
                    ColumnMark::Carried(column) => SeedAccess::Carried(column.access),
                }),
            })
            .collect()
    }
}

/// The parts of a body's [`cxl::resolve::ScopedVarsRegistry`] a check can
/// see differ between calls. The module exports and runtime modules are
/// the compile's own, the same in every body.
#[derive(Debug, Clone, PartialEq)]
struct ScopedVarsSeen {
    pipeline: IndexMap<String, cxl::resolve::ScopedVarType>,
    source: IndexMap<String, cxl::resolve::ScopedVarType>,
    record: IndexMap<String, cxl::resolve::ScopedVarType>,
    hidden_pipeline: IndexMap<String, cxl::resolve::ScopedVarType>,
    hidden_source: IndexMap<String, cxl::resolve::ScopedVarType>,
    hidden_record: IndexMap<String, cxl::resolve::ScopedVarType>,
    static_vars: IndexMap<String, cxl::resolve::ScopedVarType>,
    config: IndexMap<String, cxl::resolve::ScopedVarType>,
    config_fold: IndexMap<String, LiteralValue>,
}

impl ContractCheck {
    /// The check `bind` runs inside the body scope `bind_ctx` is in, or
    /// `None` when its body is this call's own.
    fn of(
        bind: &BodyBind<'_>,
        bind_ctx: &BindContext<'_>,
        body_declared_multiple_by_name: &HashMap<String, BTreeSet<String>>,
    ) -> Option<Self> {
        if !bind.shared_body {
            return None;
        }
        let names: HashSet<&str> = bind.nodes.iter().map(|n| n.value.name()).collect();
        let mut outside_inputs: Vec<(String, bool)> = bind
            .nodes
            .iter()
            .flat_map(|n| node_input_names(&n.value))
            .filter(|input| !names.contains(input) && !bind.port_seeds.contains_key(*input))
            .map(|input| {
                (
                    input.to_string(),
                    bind_ctx.enclosing_scope_names.contains(input),
                )
            })
            .collect();
        outside_inputs.sort();
        outside_inputs.dedup();
        let vars = &bind_ctx.scoped_vars;
        Some(Self {
            body_file: bind.body_file.to_path_buf(),
            seeds: bind
                .contract_seeds
                .iter()
                .map(|(port, row)| (port.clone(), SeedRow::of_row(row)))
                .collect(),
            declared_multiple: body_declared_multiple_by_name.clone(),
            scoped_vars: ScopedVarsSeen {
                pipeline: vars.pipeline.clone(),
                source: vars.source.clone(),
                record: vars.record.clone(),
                hidden_pipeline: vars.hidden_pipeline.clone(),
                hidden_source: vars.hidden_source.clone(),
                hidden_record: vars.hidden_record.clone(),
                static_vars: vars.static_vars.clone(),
                config: vars.config.clone(),
                config_fold: vars.config_fold.clone(),
            },
            use_path_stack: bind_ctx.use_path_stack.clone(),
            depth: bind_ctx.depth,
            outside_inputs,
        })
    }
}

/// Bind a composition body's nodes at one call site.
///
/// The body is bound against the types this call provides, and those rows,
/// programs and output types are what the call carries out: a column the
/// body does not write leaves with the type it arrived with, and a column it
/// writes has the type the body computes from what the caller passed, as
/// the same nodes written inline would.
///
/// The composition's contract is its ports' declared types. Where this call
/// provides a narrower type (a never-empty `T` into `{ nullable: T }`), the
/// body is first bound against the declared types as well, into scratch
/// artifacts that are then dropped ([`CompileArtifacts::scratch`]), and only
/// that check's diagnostics are kept: a body that does not cope with an empty value its port admits fails
/// at every call, whoever the caller is. Every CXL and planner rule is
/// monotone in nullability, so a body that passes the declared-type check
/// passes the provided-type bind too; a failure there is reported as an
/// internal error (E000) rather than dropped.
///
/// A check that passed with no diagnostics is not run again for a later
/// call with the same [`ContractCheck`] inputs: its outcome is the same, and
/// a passing check has nothing to report. A check that raised anything runs
/// at every call, so every call reports its own diagnostics.
fn bind_body(
    bind: &BodyBind<'_>,
    diags: &mut Vec<Diagnostic>,
    bind_ctx: &mut BindContext<'_>,
    artifacts: &mut CompileArtifacts,
    body_schema_by_name: &mut HashMap<String, Row>,
    body_declared_multiple_by_name: &mut HashMap<String, BTreeSet<String>>,
) {
    let check = ContractCheck::of(bind, bind_ctx, body_declared_multiple_by_name);
    let record_passed = |bind_ctx: &mut BindContext<'_>, check: Option<ContractCheck>| {
        if let Some(check) = check
            && !bind_ctx.passed_contract_checks.contains(&check)
        {
            bind_ctx.passed_contract_checks.push(check);
        }
    };
    if bind.contract_seeds == bind.port_seeds {
        let checked_from = diags.len();
        artifacts.body_binds += 1;
        bind_schema_inner(
            bind.nodes,
            bind.node_ids,
            diags,
            bind_ctx,
            artifacts,
            body_schema_by_name,
            body_declared_multiple_by_name,
        );
        if diags.len() == checked_from {
            record_passed(bind_ctx, check);
        }
        return;
    }
    let passed_before = check
        .as_ref()
        .is_some_and(|check| bind_ctx.passed_contract_checks.contains(check));
    let contract_failed = !passed_before && {
        let mut scratch = artifacts.scratch();
        let mut contract_rows = body_schema_by_name.clone();
        for (port, seed) in bind.contract_seeds {
            contract_rows.insert(port.clone(), seed.clone());
        }
        let mut contract_multiple = body_declared_multiple_by_name.clone();
        let checked_from = diags.len();
        scratch.body_binds += 1;
        bind_schema_inner(
            bind.nodes,
            bind.node_ids,
            diags,
            bind_ctx,
            &mut scratch,
            &mut contract_rows,
            &mut contract_multiple,
        );
        artifacts.body_binds += scratch.body_binds;
        if diags.len() == checked_from {
            record_passed(bind_ctx, check);
        }
        diags[checked_from..]
            .iter()
            .any(|d| d.severity == Severity::Error)
    };
    let mut produced: Vec<Diagnostic> = Vec::new();
    artifacts.body_binds += 1;
    bind_schema_inner(
        bind.nodes,
        bind.node_ids,
        &mut produced,
        bind_ctx,
        artifacts,
        body_schema_by_name,
        body_declared_multiple_by_name,
    );
    if let Some(internal) =
        unchecked_instantiation_error(bind.call, contract_failed, &produced, bind.span)
    {
        diags.push(internal);
    }
}

/// E000 when a composition body that passed the check against its ports'
/// declared types fails to bind against the narrower types a call provides.
/// That cannot happen while every typing rule is monotone in nullability;
/// if it does, the call must not compile on the strength of a check whose
/// result the compiled body no longer matches.
fn unchecked_instantiation_error(
    call: &str,
    contract_failed: bool,
    produced: &[Diagnostic],
    span: Span,
) -> Option<Diagnostic> {
    if contract_failed {
        return None;
    }
    let errors: Vec<String> = produced
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .map(|d| format!("{}: {}", d.code, d.message))
        .collect();
    if errors.is_empty() {
        return None;
    }
    Some(Diagnostic::error(
        "E000",
        format!(
            "internal error: composition {call:?} passed the check against its input ports' \
             declared types but failed to bind against the types this call provides: {}",
            errors.join("; ")
        ),
        LabeledSpan::primary(span, String::new()),
    ))
}

/// The row a composition body binds input port `port` against: the row the
/// call site feeds it, with the port's declaration applied.
///
/// Positions, engine columns and every other column are the caller's, so
/// each position a body node compiles addresses the records the body
/// receives. A column the port declares with a concrete type is seen with
/// that type (E102 has admitted the caller's type into it); under
/// [`PortTypes::Provided`] a `{ nullable: T }` column the caller provides as
/// `T` is seen as `T`. A column the port declares `any` keeps the type this
/// call provides. Every author column is marked as carried in through the
/// port, and one the port does not declare cannot be named by the body
/// ([`Row::lookup`] refuses it), which every name check in the body reports
/// as E169. The marks are the same under either [`PortTypes`].
fn seed_port_row(
    upstream: &Row,
    declared: &IndexMap<QualifiedField, Type>,
    origin: Arc<PortOrigin>,
    types: PortTypes,
) -> Row {
    // A declaration with a concrete type retypes the column; `any` and
    // `{ nullable: any }` keep the type this call provides.
    let typed_by_port = |ty: &Type| !matches!(ty.unwrap_nullable(), Type::Any);
    let columns: IndexMap<QualifiedField, Type> = upstream
        .fields()
        .map(|(field, ty)| match declared.get(field) {
            Some(Type::Nullable(inner)) if types == PortTypes::Provided && inner.as_ref() == ty => {
                (field.clone(), ty.clone())
            }
            Some(port_ty) if !field.name.starts_with('$') && typed_by_port(port_ty) => {
                (field.clone(), port_ty.clone())
            }
            _ => (field.clone(), ty.clone()),
        })
        .collect();
    let mut seed = Row::from_parts(columns, upstream.declared_span, upstream.tail.clone());
    seed.enter_body_scope();
    let fields: Vec<QualifiedField> = seed.field_names().cloned().collect();
    for field in fields {
        if field.name.starts_with('$') {
            // Engine columns stay nameable where authors may name them.
            seed.mark(field, ColumnMark::Own);
            continue;
        }
        let access = match declared.get(&field) {
            None => PortAccess::Undeclared,
            Some(ty) if typed_by_port(ty) => PortAccess::Declared,
            Some(_) => PortAccess::DeclaredAny,
        };
        seed.mark(
            field,
            ColumnMark::Carried(CarriedColumn {
                port: Arc::clone(&origin),
                access,
            }),
        );
    }
    seed
}

/// The scope a composition call returns its output row into, for
/// [`carry_into_caller_scope`].
struct CallerScope<'a> {
    /// The composition node's name at the call site.
    call: &'a str,
    /// Whether the calling scope is itself a composition body. Decided by
    /// the bind depth, not by the rows feeding the call's ports: a call
    /// that binds no input port (a composition reading only its own
    /// Sources) still returns into a body when it sits in one.
    in_body: bool,
    /// The call site's `inputs:` map: port name to upstream node.
    call_inputs: &'a IndexMap<String, String>,
    /// The calling scope's bound rows.
    parent_schema_by_name: &'a HashMap<String, Row>,
    span: Span,
}

/// A composition call's output row as the scope that called it sees it.
///
/// Inside the body, a carried mark means the column is the caller's,
/// unchanged. Seen from the calling scope, such a column is whatever it was
/// in the row that fed the port: the mark that row gave it (from an enclosing
/// body's own port). A column the body wrote is the calling scope's own, as
/// any column a node there writes is. When the calling scope is the pipeline
/// itself, the row leaves the body scope and every column may be named.
///
/// Inside an enclosing body every column must leave with a mark: a carried
/// column takes the mark of the row that fed its port, which was seeded from
/// that row. A carried column whose port has no fed row, or whose fed row
/// does not mark it, or a column with no mark at all, is a state no bind
/// produces; it is reported as an internal error (E000) naming the call, the
/// port and the column, and the column is left unmarked, so not nameable.
fn carry_into_caller_scope(row: Row, scope: &CallerScope<'_>, diags: &mut Vec<Diagnostic>) -> Row {
    let &CallerScope {
        call,
        in_body,
        call_inputs,
        parent_schema_by_name,
        span,
    } = scope;
    let fed = |port: &str| {
        call_inputs
            .get(port)
            .and_then(|upstream| parent_schema_by_name.get(upstream))
    };
    let mut out = row.clone();
    out.leave_body_scope();
    if !in_body {
        return out;
    }
    out.enter_body_scope();
    for field in row.field_names() {
        let mark = match row.mark_of(field) {
            Some(ColumnMark::Carried(column)) => match fed(&column.port.port) {
                Some(fed) => fed.mark_of(field).cloned().ok_or_else(|| {
                    format!(
                        "the row that fed input port `{}` does not mark column `{}`",
                        column.port.port, field.name
                    )
                }),
                None => Err(format!(
                    "column `{}` came in through input port `{}`, which no row fed",
                    field.name, column.port.port
                )),
            },
            Some(ColumnMark::Own) => Ok(ColumnMark::Own),
            None => Err(format!(
                "column `{}` left the body without a mark",
                field.name
            )),
        };
        match mark {
            Ok(mark) => out.mark(field.clone(), mark),
            Err(reason) => diags.push(Diagnostic::error(
                "E000",
                format!(
                    "internal error: composition {call:?} returned a row to the composition \
                     that called it, but {reason}"
                ),
                LabeledSpan::primary(span, String::new()),
            )),
        }
    }
    out
}

/// E169 for `column` when `row` carries it through a composition port that
/// does not declare it; `None` for any other name.
///
/// Every name a body node uses outside CXL (a `group_by`, a `partition_by`,
/// a Sink `mapping:` source, ...) goes through this before its own presence
/// check, so the author reads which port to edit rather than "not present".
fn refuse_undeclared(row: &Row, node: &str, column: &str, span: Span) -> Option<Diagnostic> {
    let (caller_type, carried) = row.undeclared_carried(column)?;
    Some(undeclared_column_diagnostic(
        node,
        column,
        carried,
        caller_type,
        span,
    ))
}

/// [`refuse_undeclared`] for each of `names`, once per column. Returns
/// whether any was refused.
fn refuse_undeclared_names<'a>(
    row: &Row,
    node: &str,
    names: impl IntoIterator<Item = &'a str>,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) -> bool {
    let mut refused: Vec<&str> = Vec::new();
    for column in names {
        if refused.contains(&column) {
            continue;
        }
        if let Some(d) = refuse_undeclared(row, node, column, span) {
            refused.push(column);
            diags.push(d);
        }
    }
    !refused.is_empty()
}

/// E169: a composition body names a caller column its input port does not
/// declare.
///
/// The help is the declaration to add to the port, in the composition file,
/// with the type the caller's column has at this call site.
fn undeclared_column_diagnostic(
    node: &str,
    column: &str,
    carried: &CarriedColumn,
    caller_type: &Type,
    span: Span,
) -> Diagnostic {
    let PortOrigin {
        call,
        port,
        body_file,
    } = carried.port.as_ref();
    Diagnostic::error(
        "E169",
        format!(
            "composition {call:?} uses column `{column}` in node {node:?}, but its input \
             port `{port}` does not declare `{column}`. A composition may use only the \
             columns its input ports declare; every other column passes through the call \
             unchanged."
        ),
        LabeledSpan::primary(span, format!("uses `{column}`")),
    )
    .with_help(format!(
        "declare `{column}` on input port `{port}` in `{body_file}`, so every call of the \
         composition has to provide it:\n\
         _compose:\n  inputs:\n    {port}:\n      schema:\n        \
         - {{ name: {column}, type: {} }}",
        port_column_type(caller_type),
    ))
}

/// The names a CXL program bound against `row` may resolve as columns:
/// every column except one a composition port carries undeclared.
fn nameable_columns(row: &Row) -> Vec<&str> {
    row.field_names()
        .filter(|field| row.is_nameable(field))
        .map(|field| field.name.as_ref())
        .collect()
}

/// Where a program [`resolve_against_row`] resolves sits: its node, the
/// context its resolver failures are reported under, and the node's span.
#[derive(Clone, Copy)]
struct ResolveSite<'a> {
    node: &'a str,
    context: &'a str,
    span: Span,
}

/// Resolve `program`'s names against `row`, as [`cxl::resolve`] does, with
/// the columns a composition port carries undeclared left out of the names
/// it may resolve.
///
/// A field reference that fails only because it names such a column is
/// E169, one per column; every other failure is the resolver's own, worded
/// by [`resolve_failure_diagnostic`] under `context`. Leaving those columns
/// out is also what keeps them out of a "did you mean" suggestion.
fn resolve_against_row(
    site: ResolveSite<'_>,
    program: cxl::ast::Program,
    node_count: u32,
    row: &Row,
    module_exports: &HashMap<String, cxl::resolve::ModuleExports>,
    scoped_vars: &cxl::resolve::ScopedVarsRegistry,
) -> Result<cxl::resolve::ResolvedProgram, Vec<Diagnostic>> {
    let ResolveSite {
        node,
        context,
        span,
    } = site;
    struct FieldRefs(Vec<(cxl::lexer::Span, Box<str>)>);
    impl cxl::analyzer::visitor::Visitor for FieldRefs {
        fn visit_expr(&mut self, expr: &Expr) {
            if let Expr::FieldRef { name, span, .. } = expr {
                self.0.push((*span, name.clone()));
            }
            cxl::analyzer::visitor::walk_expr(self, expr);
        }
    }
    // Field references are collected only where a refusal can need them:
    // in a composition body, whose rows mark the columns the body may use.
    let mut refs = FieldRefs(Vec::new());
    if row.in_body_scope() {
        for statement in &program.statements {
            cxl::analyzer::visitor::Visitor::visit_statement(&mut refs, statement);
        }
    }
    let names = nameable_columns(row);
    cxl::resolve::resolve_program_with_modules_and_vars(
        program,
        &names,
        node_count,
        module_exports,
        scoped_vars,
    )
    .map_err(|failures| {
        let mut out: Vec<Diagnostic> = Vec::new();
        let mut refused: Vec<&str> = Vec::new();
        let mut other = Vec::new();
        for failure in failures {
            let undeclared = refs
                .0
                .iter()
                .find(|(at, name)| *at == failure.span && row.undeclared_carried(name).is_some())
                .map(|(_, name)| name);
            match undeclared {
                Some(name) => {
                    if !refused.contains(&name.as_ref()) {
                        refused.push(name);
                        out.extend(refuse_undeclared(row, node, name, span));
                    }
                }
                None => other.push(failure),
            }
        }
        if !other.is_empty() {
            out.push(resolve_failure_diagnostic(context, other, span));
        }
        out
    })
}

/// The column names a resolved program reads, gathered in one walk for the
/// checks a composition body adds: the names the resolver does not check
/// (`distinct by` fields, positional window fields, qualified references),
/// and each field reference with its span.
#[derive(Default)]
struct BodyReads {
    /// `(qualifier, name)` for a name outside the resolver's reach; the
    /// qualifier is set for a qualified reference.
    unresolved: Vec<(Option<String>, Box<str>)>,
    /// Each column the program reads, bare (`v`) or qualified (`a.v`).
    fields: Vec<FieldRead>,
}

/// One read of a column in a program: where it is, and the column it names.
struct FieldRead {
    at: cxl::lexer::Span,
    /// The input a qualified reference (`a.v`) names; `None` for a bare one.
    qualifier: Option<Box<str>>,
    name: Box<str>,
}

impl FieldRead {
    /// Whether this read names `field`: the same column name, of the same
    /// input when both the read and the column say which.
    fn reads(&self, field: &QualifiedField) -> bool {
        self.name.as_ref() == field.name.as_ref()
            && match (&self.qualifier, &field.qualifier) {
                (Some(read), Some(column)) => read.as_ref() == column.as_ref(),
                _ => true,
            }
    }

    /// The reference as the author wrote it: `a.v` or `v`.
    fn written(&self) -> String {
        match &self.qualifier {
            Some(qualifier) => format!("{qualifier}.{}", self.name),
            None => self.name.to_string(),
        }
    }
}

impl BodyReads {
    fn collect(resolved: &cxl::resolve::ResolvedProgram) -> Self {
        struct Walk<'a> {
            bindings: &'a [Option<cxl::resolve::ResolvedBinding>],
            reads: BodyReads,
        }
        impl cxl::analyzer::visitor::Visitor for Walk<'_> {
            fn visit_statement(&mut self, statement: &Statement) {
                if let Statement::Distinct {
                    field: Some(field), ..
                } = statement
                {
                    self.reads.unresolved.push((None, field.clone()));
                }
                cxl::analyzer::visitor::walk_statement(self, statement);
            }
            fn visit_expr(&mut self, expr: &Expr) {
                let binding = |node_id: &cxl::ast::NodeId| {
                    self.bindings
                        .get(node_id.0 as usize)
                        .and_then(|binding| binding.as_ref())
                };
                match expr {
                    Expr::FieldRef {
                        node_id,
                        name,
                        span,
                    } if matches!(
                        binding(node_id),
                        Some(cxl::resolve::ResolvedBinding::Field(_))
                    ) =>
                    {
                        self.reads.fields.push(FieldRead {
                            at: *span,
                            qualifier: None,
                            name: name.clone(),
                        });
                    }
                    // A qualified reference reads its last part; outside a
                    // Combine the qualifier names no input.
                    Expr::QualifiedFieldRef {
                        node_id,
                        parts,
                        span,
                    } if !matches!(
                        binding(node_id),
                        Some(cxl::resolve::ResolvedBinding::ModuleConstant(..))
                    ) =>
                    {
                        if let Some((name, qualifier)) = parts.split_last() {
                            let qualifier = qualifier
                                .iter()
                                .map(|part| part.as_ref())
                                .collect::<Vec<&str>>()
                                .join(".");
                            self.reads.fields.push(FieldRead {
                                at: *span,
                                qualifier: Some(qualifier.clone().into()),
                                name: name.clone(),
                            });
                            self.reads.unresolved.push((Some(qualifier), name.clone()));
                        }
                    }
                    Expr::MethodCall {
                        receiver,
                        method,
                        args,
                        ..
                    } if args.is_empty()
                        && matches!(
                            receiver.as_ref(),
                            Expr::WindowCall { function, .. }
                                if matches!(function.as_ref(), "first" | "last" | "lag" | "lead")
                        ) =>
                    {
                        self.reads.unresolved.push((None, method.clone()));
                    }
                    _ => {}
                }
                cxl::analyzer::visitor::walk_expr(self, expr);
            }
        }
        let mut walk = Walk {
            bindings: &resolved.bindings,
            reads: BodyReads::default(),
        };
        for statement in &resolved.program.statements {
            cxl::analyzer::visitor::Visitor::visit_statement(&mut walk, statement);
        }
        walk.reads
    }

    /// E169 for each name outside the resolver's reach that `row` carries
    /// undeclared. A qualified reference is checked against the input its
    /// qualifier names when the row has one, and by its bare name otherwise,
    /// which is the column the runtime reads.
    fn refused(&self, row: &Row, node: &str, span: Span) -> Vec<Diagnostic> {
        let mut seen: Vec<&str> = Vec::new();
        let mut out = Vec::new();
        for (qualifier, name) in &self.unresolved {
            if seen.contains(&name.as_ref()) {
                continue;
            }
            let qualified_input = qualifier.as_deref().filter(|qualifier| {
                row.field_names()
                    .any(|field| field.qualifier.as_deref() == Some(*qualifier))
            });
            let refused = match qualified_input {
                Some(qualifier) => {
                    row.undeclared_carried_qualified(qualifier, name)
                        .map(|(ty, carried)| {
                            undeclared_column_diagnostic(node, name, carried, ty, span)
                        })
                }
                None => refuse_undeclared(row, node, name, span),
            };
            if let Some(diag) = refused {
                seen.push(name);
                out.push(diag);
            }
        }
        out
    }

    /// The columns of `empty_by_port` a type error spanning one of `within`
    /// reads, or every one the program reads when no error span holds one.
    fn possibly_empty_in<'e>(
        &self,
        empty_by_port: &'e [MayBeEmpty],
        within: &[cxl::lexer::Span],
    ) -> Vec<&'e MayBeEmpty> {
        let read = |inside: &dyn Fn(cxl::lexer::Span) -> bool| {
            empty_by_port
                .iter()
                .filter(|empty| {
                    self.fields
                        .iter()
                        .any(|read| inside(read.at) && read.reads(&empty.field))
                })
                .collect::<Vec<_>>()
        };
        let in_errors = read(&|at| {
            within
                .iter()
                .any(|span| span.start <= at.start && at.end <= span.end)
        });
        if in_errors.is_empty() {
            read(&|_| true)
        } else {
            in_errors
        }
    }

    /// The author's line that reads `field` inside one of the failing
    /// expressions (`line.within`), or the first line reading it when none
    /// does, with every read of `field` on it written `(v ?? stand_in)`
    /// (`(a.v ?? stand_in)` for a qualified read). For an expression the
    /// author wrote under a YAML key, the rewritten entry
    /// (`key: '<expression>'`). `None` when the program is the planner's
    /// own, the expression spans several lines, or the reads' spans do not
    /// address the text.
    fn line_with_stand_in(
        &self,
        line: &AuthoredLine<'_>,
        field: &QualifiedField,
        stand_in: &str,
    ) -> Option<String> {
        self.line_rewriting(line, field, &|written| format!("({written} ?? {stand_in})"))
    }

    /// The author's line that reads `field` inside one of the failing
    /// expressions (`line.within`), or the first line reading it when none
    /// does, with every read of `field` on it, written `written`, replaced
    /// by `rewrite(written)`. For an expression the author wrote under a
    /// YAML key, the rewritten entry (`key: '<expression>'`). `None` when the
    /// program is the planner's own, the expression spans several lines, or
    /// the reads' spans do not address the text.
    fn line_rewriting(
        &self,
        line: &AuthoredLine<'_>,
        field: &QualifiedField,
        rewrite: &dyn Fn(&str) -> String,
    ) -> Option<String> {
        let reads_of_name = || {
            self.fields
                .iter()
                .filter(|read| read.reads(field))
                .map(|read| (read.at.start as usize, read.at.end as usize, read.written()))
        };
        let anchor = reads_of_name()
            .filter(|(start, end, _)| {
                line.within
                    .iter()
                    .any(|span| span.start as usize <= *start && *end <= span.end as usize)
            })
            .map(|(start, ..)| start)
            .min()
            .or_else(|| reads_of_name().map(|(start, ..)| start).min())?;
        // The text to rewrite, the offset of its first byte in the program
        // source, and the bounds of the line within the source.
        let (text, base, start, end) = match line.authored {
            Authored::Program => {
                let source = line.source;
                let start = source.get(..anchor)?.rfind('\n').map_or(0, |at| at + 1);
                let end = source
                    .get(anchor..)?
                    .find('\n')
                    .map_or(source.len(), |at| anchor + at);
                (source, 0, start, end)
            }
            Authored::Expression { text, at, .. } => {
                if text.contains('\n') {
                    return None;
                }
                (text, at, at, at + text.len())
            }
            Authored::Synthesized => return None,
        };
        let mut reads: Vec<(usize, usize, String)> = reads_of_name()
            .filter(|(read_start, read_end, _)| start <= *read_start && *read_end <= end)
            .collect();
        reads.sort_unstable();
        reads.dedup();
        let mut rewritten = String::new();
        let mut from = start;
        for (read_start, read_end, written) in reads {
            if text.get(read_start - base..read_end - base)? != written {
                return None;
            }
            rewritten.push_str(text.get(from - base..read_start - base)?);
            rewritten.push_str(&rewrite(&written));
            from = read_end;
        }
        rewritten.push_str(text.get(from - base..end - base)?);
        let rewritten = rewritten.trim().to_string();
        match line.authored {
            Authored::Expression { key, .. } => yaml_entry(key, &rewritten),
            _ => Some(rewritten),
        }
    }

    /// The `any`-declared column the field reference inside `within` reads,
    /// if one does: the column a type error spanning `within` is about.
    fn caller_typed_in<'r>(
        &self,
        row: &'r Row,
        within: cxl::lexer::Span,
    ) -> Option<(&'r QualifiedField, &'r Type, &'r CarriedColumn)> {
        self.fields
            .iter()
            .filter(|read| within.start <= read.at.start && read.at.end <= within.end)
            .find_map(|read| {
                row.carried_columns().find(|(field, _, column)| {
                    column.access == PortAccess::DeclaredAny && read.reads(field)
                })
            })
    }
}

/// How a port schema spells `ty`: the spelling a `- { name: ..., type: ... }`
/// line takes, so a declaration built from it is admitted by E102 when the
/// caller provides `ty`. A type a port column cannot declare directly (an
/// array, a map, or a type the caller's row leaves open) is written `any`,
/// which admits whatever the caller provides.
fn port_column_type(ty: &Type) -> String {
    match ty {
        Type::Nullable(inner) => match inner.as_ref() {
            Type::Null | Type::Array | Type::Map | Type::Numeric | Type::Any => "any".to_string(),
            inner => format!("{{ nullable: {} }}", port_column_type(inner)),
        },
        Type::Bool => "bool".to_string(),
        Type::Int => "int".to_string(),
        Type::Float => "float".to_string(),
        Type::Decimal => "decimal".to_string(),
        Type::String => "string".to_string(),
        Type::Date => "date".to_string(),
        Type::DateTime => "date_time".to_string(),
        Type::Null | Type::Array | Type::Map | Type::Numeric | Type::Any => "any".to_string(),
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
pub(crate) fn resolve_use_path(
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

/// Build a runtime `SharedStorage<Schema>` from an iterator of column names,
/// stamping engine-stamp metadata on every column whose name begins
/// with a recognized engine prefix:
///
/// - `$ck.aggregate.<aggregate_name>` — synthetic group-index column
///   emitted by a relaxed aggregate. Stamped
///   [`FieldMetadata::AggregateGroupIndex`].
/// - `$ck.<source_field>` — source-CK shadow column. Stamped
///   [`FieldMetadata::SourceCorrelation`].
/// - `$widened` — `auto_widen` sidecar absorber. Stamped
///   [`FieldMetadata::WidenedSidecar`].
/// - `$source.file` — per-record source-file lineage stamp. Stamped
///   [`FieldMetadata::SourceFile`].
/// - `$source.name` — per-record Source-node identity stamp. Stamped
///   [`FieldMetadata::SourceName`].
///
/// The aggregate prefix is checked first because `$ck.aggregate.x`
/// also matches the generic `$ck.` prefix; misordering would mis-
/// classify aggregate columns as source-CK shadows.
///
/// Mirrors the deduction in `lower_node_to_plan_node`'s
/// `schema_from_bound` closure so a composition body's port-synthetic
/// Source carries the same marker.
pub(crate) fn schema_from_field_names<'a, I>(names: I) -> SharedStorage<clinker_record::Schema>
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
        } else if name == crate::config::pipeline_node::WIDENED_SIDECAR_COLUMN {
            builder.with_field_meta(name, FieldMetadata::widened_sidecar())
        } else if name == crate::config::pipeline_node::SOURCE_FILE_COLUMN {
            builder.with_field_meta(name, FieldMetadata::source_file())
        } else if name == crate::config::pipeline_node::SOURCE_NAME_COLUMN {
            builder.with_field_meta(name, FieldMetadata::source_name())
        } else if name == crate::config::pipeline_node::SOURCE_EVENT_TIME_COLUMN {
            builder.with_field_meta(name, FieldMetadata::source_event_time())
        } else {
            builder.with_field(name)
        };
    }
    builder.build()
}

/// Resolve a source's unified [`SourceSchema`] to the effective ordered column
/// list its runtime row is seeded from:
///
/// - `Columns` — the declared single-record columns directly.
/// - `MultiRecord` — the discriminator-led superset over every record type
///   (matching the multi-record reader's static superset schema).
/// - `File` — the external `.schema.yaml`, loaded and resolved at compile time
///   (an I/O or parse failure emits E157 and seeds no columns).
/// - `Generated` — engine-synthesized positional columns (EDI/HL7/SWIFT),
///   synthesized here at compile time from the source's declared format and its
///   count options via [`generated_source_columns`], so a Generated source's
///   typechecked row carries exactly the columns the runtime reader emits.
fn resolve_source_columns(
    schema: &SourceSchema,
    format: &crate::config::InputFormat,
    name: &str,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) -> Vec<Column> {
    match schema {
        SourceSchema::Columns(cols) => cols.clone(),
        SourceSchema::MultiRecord { record_types, .. } => {
            clinker_format::multi_record_superset(record_types)
        }
        SourceSchema::Generated(_) => generated_source_columns(format),
        SourceSchema::File(path) => {
            match crate::schema::load_source_schema(std::path::Path::new(path)) {
                Ok(inner) => resolve_source_columns(&inner, format, name, span, diags),
                Err(e) => {
                    diags.push(Diagnostic::error(
                        "E157",
                        format!(
                            "source {name:?} declares an external schema file {path:?} that \
                             failed to load: {e}"
                        ),
                        LabeledSpan::primary(span, String::new()),
                    ));
                    Vec::new()
                }
            }
        }
    }
}

/// Synthesize the engine-generated positional columns for a
/// [`SourceSchema::Generated`] source from its declared EDI-family format and
/// count options. This is the compile-time twin of the runtime reader's own
/// schema synthesis — both draw the layout from the same
/// `clinker_format::*_generated_columns` functions — so a Generated source's
/// typechecked row carries exactly the columns the reader emits at runtime
/// (`seg_id`/`e01…` for EDIFACT/X12, `f01…`/split leaves for HL7,
/// `block,tag,value` for SWIFT). A non-EDI format cannot carry a `generated`
/// schema (config validation rejects that with E159 before this runs), so its
/// arm synthesizes nothing.
fn generated_source_columns(format: &crate::config::InputFormat) -> Vec<Column> {
    use crate::config::InputFormat;
    match format {
        InputFormat::Edifact(opts) => {
            let max = opts
                .as_ref()
                .and_then(|o| o.max_elements)
                .unwrap_or(clinker_format::EDIFACT_DEFAULT_MAX_ELEMENTS);
            clinker_format::edifact_generated_columns(max)
        }
        InputFormat::X12(opts) => {
            let max = opts
                .as_ref()
                .and_then(|o| o.max_elements)
                .unwrap_or(clinker_format::X12_DEFAULT_MAX_ELEMENTS);
            clinker_format::x12_generated_columns(max)
        }
        InputFormat::Hl7(opts) => {
            let max = opts
                .as_ref()
                .and_then(|o| o.max_fields)
                .unwrap_or(clinker_format::HL7_DEFAULT_MAX_FIELDS);
            let splits = opts
                .as_ref()
                .map(|o| o.resolved_splits())
                .unwrap_or_default();
            clinker_format::hl7_generated_columns(max, &splits)
        }
        InputFormat::Swift(_) => clinker_format::swift_generated_columns(),
        InputFormat::Csv(_)
        | InputFormat::Json(_)
        | InputFormat::Xml(_)
        | InputFormat::FixedWidth(_) => Vec::new(),
    }
}

/// Build the `Row.declared` map for a Source from its author-declared
/// `schema:` block, tail-appending engine-stamped columns:
///
/// - One `$ck.<field>` shadow column per field listed in the source's
///   own `correlation_key:`. Each shadow column is typed identically
///   to the user-declared field of the same name.
/// - One `$widened` sidecar absorber (typed `Any`, carries
///   `Value::Map`) when `on_unmapped: auto_widen` is the source's
///   policy. Tail-append preserves user-declared positional indices
///   while keeping the planner's view aligned with the runtime
///   reader's emitted schema — the dispatch canonicalize invariant
///   holds because `$widened` is engine-stamped.
///
/// Returns the column map paired with a list of `correlation_key`
/// field names that did NOT appear in `decl.columns`. The caller emits
/// E153 for each such name — every CK field a source declares MUST be
/// in that source's own schema.
fn columns_from_decl(
    columns: &[Column],
    correlation_key: Option<&crate::config::CorrelationKey>,
    on_unmapped: &OnUnmapped,
) -> (IndexMap<QualifiedField, Type>, Vec<String>) {
    let mut cols: IndexMap<QualifiedField, Type> = columns
        .iter()
        .map(|c| (QualifiedField::bare(c.name.as_str()), c.bound_type()))
        .collect();
    // Engine-stamped tail order: `$ck.<field>` shadow columns first,
    // then the `$widened` sidecar, then `$source.file`, then
    // `$source.name`, then `$source.event_time`. The order is load-
    // bearing — CK-aligned aggregate / combine propagation walks the
    // schema expecting `$ck.*` immediately after declared columns;
    // pushing `$widened` between them would break that propagation.
    // Sources with `auto_widen` get `$widened` after every `$ck.<field>`
    // slot; the `$source.*` lineage columns live outside the CK
    // lattice and tail-append after `$widened`. The same order is
    // replayed by `executor::mod::ingest_source_into_stream` when it
    // widens the runtime reader schema, so the planner's view stays
    // positionally aligned with the actual records flowing through
    // dispatch.
    let mut missing: Vec<String> = Vec::new();
    if let Some(ck) = correlation_key {
        for field in ck.fields() {
            match columns
                .iter()
                .find(|c| c.name.as_str() == field)
                .map(|c| c.bound_type())
            {
                Some(ty) => {
                    let shadow_name = format!("$ck.{field}");
                    cols.insert(QualifiedField::bare(shadow_name), ty);
                }
                None => missing.push(field.to_string()),
            }
        }
    }
    if on_unmapped.reserves_widened_sidecar() {
        cols.insert(
            QualifiedField::bare(crate::config::pipeline_node::WIDENED_SIDECAR_COLUMN),
            Type::Any,
        );
    }
    cols.insert(
        QualifiedField::bare(crate::config::pipeline_node::SOURCE_FILE_COLUMN),
        Type::String,
    );
    cols.insert(
        QualifiedField::bare(crate::config::pipeline_node::SOURCE_NAME_COLUMN),
        Type::String,
    );
    cols.insert(
        QualifiedField::bare(crate::config::pipeline_node::SOURCE_EVENT_TIME_COLUMN),
        Type::Int,
    );
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

/// The invariant context a CXL program is typechecked against: node label
/// (diagnostic attribution), upstream `schema`, aggregate `mode`, diagnostic
/// `span`, and the `scoped_vars` registry. Bundled so the synthesizing entry
/// points ([`typecheck_parsed_program`], [`typecheck_cull_rule`]) stay below
/// clippy's argument threshold and share one context value across a node's
/// several rule-level checks; the program-specific inputs (AST, node count,
/// display source) stay direct arguments.
struct CxlTypecheckCtx<'a> {
    /// The node the program belongs to, as the author named it.
    node: &'a str,
    /// The node and the part of it the program compiles, for diagnostics
    /// (`"t1"`, `"r (branch high)"`, `"t1:log[0]:condition"`).
    node_name: &'a str,
    schema: &'a Row,
    mode: &'a AggregateMode,
    span: Span,
    scoped_vars: &'a cxl::resolve::ScopedVarsRegistry,
    /// How the author wrote the program, so a help that rewrites one of
    /// its lines prints the author's YAML.
    authored: Authored<'a>,
}

/// A CXL program to typecheck: its text, and how the author wrote it.
#[derive(Clone, Copy)]
struct CxlSource<'a> {
    text: &'a str,
    authored: Authored<'a>,
}

/// How the author wrote the program a [`CxlTypecheckCtx`] typechecks. A
/// help that prints one of its lines rewritten (the E200 for a column that
/// may be empty) prints what the author wrote, under the key they wrote it
/// under, and never the program the planner synthesized around it.
#[derive(Clone, Copy)]
enum Authored<'a> {
    /// The program is the author's `cxl:` block as written.
    Program,
    /// The program wraps one authored expression, `text`, written under the
    /// YAML key `key` (a Route branch, a Reshape `when:`, a log
    /// `condition:`, a Cull rule, a Reshape `set` or `overrides` entry);
    /// `text` starts at byte `at` of the program's source.
    Expression {
        key: &'a str,
        text: &'a str,
        at: usize,
    },
    /// The program is the planner's own; no line of it is the author's.
    Synthesized,
}

/// The engine-supplied opening of a gate's program: the transform's `use`
/// declarations, plus how many statements they contribute.
struct GatePrelude {
    /// CXL source prepended to the authored predicate.
    source: String,
    /// Statements the prelude itself contributes, counted by parsing the
    /// rendered prelude so the count cannot drift from what is prepended.
    statements: usize,
}

/// The `use` declarations of a program, rendered back as CXL source.
///
/// Reconstructed from the parsed statements rather than sliced out of the
/// authored text, so an alias reaches a gate in the one canonical spelling
/// regardless of how it was written.
fn module_use_prelude(source: &str) -> GatePrelude {
    let parsed = cxl::parser::Parser::parse(source);
    let mut prelude = String::new();
    for statement in &parsed.ast.statements {
        if let Statement::UseStmt { path, alias, .. } = statement {
            prelude.push_str("use ");
            let segments: Vec<&str> = path.iter().map(AsRef::as_ref).collect();
            prelude.push_str(&segments.join("."));
            if let Some(alias) = alias {
                prelude.push_str(" as ");
                prelude.push_str(alias);
            }
            prelude.push('\n');
        }
    }
    let statements = cxl::parser::Parser::parse(&prelude).ast.statements.len();
    GatePrelude {
        source: prelude,
        statements,
    }
}

/// The statements a gate's author actually wrote.
///
/// The compiled program opens with the transform's `use` declarations, and
/// those are the engine's doing, not the author's — counting them would reject
/// every gate in a transform that imports a module. Only that leading prelude
/// is discounted, by length: a `use` written into the gate itself lands past
/// the predicate, is a statement the gate's evaluator was never allocated to
/// run, and counting it is what lets the guard refuse it.
fn authored_gate_statements(statements: &[Statement], prelude: &GatePrelude) -> usize {
    statements.len().saturating_sub(prelude.statements)
}

/// Columns named in the E374 message before it stops listing them.
///
/// A wide row would otherwise put hundreds of names between the author and the
/// correction at the end of the sentence.
const LOG_FIELD_ROW_LISTING_LIMIT: usize = 12;

/// Refuse a requested log field the transform's input record cannot carry.
///
/// `fields` is the only channel by which record data reaches an event, so a
/// selector matching no column publishes an event with the attribute missing
/// rather than the value the author asked for — which reads exactly like a run
/// where nothing had that value. Dispatch fires before the transform's own
/// program runs, for `per_record` and `on_error` alike, so the row a selector
/// must name is the input row, not the projected output.
///
/// Only a closed row decides this. Every row the bind walk produces is closed
/// (a composition body binds against its caller's row), so an open row reaches
/// here only from a caller that builds one; a selector against it is not
/// decidable and stays with the runtime miss counter this check backs up.
fn log_field_diagnostics(
    node_name: &str,
    directives: &[LogDirective],
    upstream: &Row,
    span: Span,
) -> Vec<Diagnostic> {
    let mut diags = Vec::new();
    for (index, directive) in directives.iter().enumerate() {
        let Some(fields) = &directive.fields else {
            continue;
        };
        for field in fields {
            if let Some(refused) = refuse_undeclared(upstream, node_name, field, span) {
                diags.push(refused);
                continue;
            }
            if !matches!(upstream.lookup(field), ColumnLookup::Unknown) {
                continue;
            }
            // Only columns this node may name: a caller column a composition
            // port carries undeclared would lead to E169 if requested.
            let mut names = nameable_columns(upstream)
                .into_iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let elided = names.len().saturating_sub(LOG_FIELD_ROW_LISTING_LIMIT);
            names.truncate(LOG_FIELD_ROW_LISTING_LIMIT);
            let mut listing = names
                .iter()
                .map(|name| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(", ");
            if listing.is_empty() {
                listing = "no columns at all".to_string();
            } else if elided > 0 {
                listing.push_str(&format!(" and {elided} more"));
            }
            let correction = match nearest_column(field, upstream) {
                Some(suggestion) => format!("write `fields: [{suggestion}]`"),
                None => "request a column the input row declares".to_string(),
            };
            diags.push(Diagnostic::error(
                "E374",
                format!(
                    "transform `{node_name}` log[{index}].fields requests `{field}`, which the \
                     input record does not carry; the upstream row has {listing} — {correction}"
                ),
                LabeledSpan::primary(span, "log fields".to_string()),
            ));
        }
    }
    diags
}

/// The declared column an unmatched selector most likely meant, if any,
/// among the columns the node may name.
///
/// Folding case and separators out before measuring is what lets `orderId`
/// reach `order_id`: the two differ by an underscore and a capital, which is
/// two edits on the raw spelling and none on the folded one.
fn nearest_column(field: &str, upstream: &Row) -> Option<String> {
    fn fold(value: &str) -> String {
        value
            .to_lowercase()
            .chars()
            .filter(|c| *c != '_' && *c != '-')
            .collect()
    }
    let folded = fold(field);
    let threshold = 1 + folded.len() / 8;
    nameable_columns(upstream)
        .into_iter()
        .filter_map(|column| {
            let name = column.to_string();
            let distance =
                cxl::resolve::levenshtein::levenshtein_bounded(&folded, &fold(&name), threshold)?;
            Some((distance, name))
        })
        .min_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)))
        .map(|(_, name)| name)
}

/// Parse, resolve and typecheck one CXL program of node `node` against
/// `schema`. `node_name` labels the program in diagnostics. A failure is one
/// or more diagnostics: E169 once per column a composition port carries
/// undeclared, otherwise the one parse, resolve or type error.
fn typecheck_cxl(
    node: &str,
    node_name: &str,
    source: CxlSource<'_>,
    schema: &Row,
    mode: AggregateMode,
    span: Span,
    scoped_vars: &cxl::resolve::ScopedVarsRegistry,
) -> Result<TypedProgram, Vec<Diagnostic>> {
    let CxlSource {
        text: source,
        authored,
    } = source;
    let parse_result = cxl::parser::Parser::parse(source);
    if !parse_result.errors.is_empty() {
        let messages: Vec<String> = parse_result
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect();
        return Err(vec![Diagnostic::error(
            "E202",
            format!("CXL parse error in {node_name:?}: {}", messages.join("; ")),
            LabeledSpan::primary(span, String::new()),
        )]);
    }
    let ctx = CxlTypecheckCtx {
        node,
        node_name,
        schema,
        mode: &mode,
        span,
        scoped_vars,
        authored,
    };
    typecheck_parsed_program(
        &ctx,
        parse_result.ast,
        parse_result.node_count,
        std::sync::Arc::from(source),
    )
}

/// Turn CXL name-resolution failures into one plan diagnostic, keeping the
/// resolver's own code and help when it carries them.
///
/// A resolver diagnostic may name its own code as a `[CODE]` message prefix —
/// E173 does, for a composition body reading a parent-scope var its
/// `_compose.scoped_vars` schema does not opt into. Wrapping every failure as
/// E203 published two contradicting codes in one report (an E203 header over
/// an `[E173]` message, with a pointer to the E203 page) and dropped the
/// resolver's `help`, which for E173 is the line naming the exact field to add.
///
/// A code heads the report only when every failure names it. A mixed batch
/// stays E203, and each sub-failure that named a code has it restated after
/// the message rather than bracketed in front of it: the bracketed form is how
/// a code announces itself as *the* code of a report, so leaving one there
/// under an E203 header reproduced the same contradiction one layer down.
///
/// `context` names the failing node and is kept on both paths — a pipeline
/// with more than one composition body is otherwise unactionable, since a
/// lifted diagnostic carries no span that would identify which body to edit.
fn resolve_failure_diagnostic(
    context: &str,
    diags: Vec<cxl::resolve::ResolveDiagnostic>,
    span: Span,
) -> Diagnostic {
    /// Split a resolver message into the registered `[CODE]` it names for
    /// itself, if any, and the message body without that prefix.
    fn split_own_code(message: &str) -> (Option<&str>, &str) {
        let Some((code, body)) = message
            .strip_prefix('[')
            .and_then(|rest| rest.split_once(']'))
        else {
            return (None, message);
        };
        if clinker_core_types::diagnostic::is_registered(code) {
            (Some(code), body.trim_start())
        } else {
            (None, message)
        }
    }

    let split: Vec<(Option<&str>, &str)> =
        diags.iter().map(|d| split_own_code(&d.message)).collect();
    let shared_code = split
        .first()
        .and_then(|(code, _)| *code)
        .filter(|code| split.iter().all(|(c, _)| *c == Some(*code)));

    let joined = split
        .iter()
        .map(|(code, body)| match code {
            // Restated only where it is not already the report's own code,
            // which would say it twice.
            Some(c) if shared_code.is_none() => format!("{body} ({c})"),
            _ => (*body).to_owned(),
        })
        .collect::<Vec<_>>()
        .join("; ");

    let helps: Vec<String> = diags.iter().filter_map(|d| d.help.clone()).collect();
    let diag = Diagnostic::error(
        shared_code.unwrap_or("E203"),
        format!("{context}: {joined}"),
        LabeledSpan::primary(span, String::new()),
    );
    if helps.is_empty() {
        diag
    } else {
        diag.with_help(helps.join("; "))
    }
}

/// Resolve + typecheck an already-parsed CXL `program` against `schema`,
/// attaching `source` as the typed program's display text.
///
/// Split out from [`typecheck_cxl`] so callers that synthesize a program
/// directly (rather than from one source string) can enter after parse —
/// notably `bind_cull`, which OR-combines each rule's independently parsed
/// predicate at the AST level. `node_count` must exceed every `NodeId` in
/// `program` (the resolver/typechecker size their per-node side-tables by
/// it); a synthesizing caller is responsible for keeping ids dense and
/// below it.
fn typecheck_parsed_program(
    ctx: &CxlTypecheckCtx<'_>,
    program: cxl::ast::Program,
    node_count: u32,
    source: Arc<str>,
) -> Result<TypedProgram, Vec<Diagnostic>> {
    let CxlTypecheckCtx {
        node,
        node_name,
        schema,
        mode,
        span,
        scoped_vars,
        authored,
    } = *ctx;
    // Field names for CXL resolve. In the non-combine case all fields are
    // bare; in the combine case qualified fields collapse to their `.name` —
    // the resolver only needs to know which identifiers are "field-like",
    // and combine bodies address qualified fields via
    // `Expr::QualifiedFieldRef` (handled in typecheck/pass.rs, not resolve).
    let context = format!("CXL name resolution failed in {node_name:?}");
    // A body whose port declares a column `{ nullable: T }` is checked as if
    // the column could be empty. Kept to tell, if the check fails, whether
    // the column's possible emptiness is what failed it.
    // A body that reads a column its port declares `any` sees the type this
    // call provides; kept too, to find the type a failing node needs.
    let empty_by_port = may_be_empty_by_port(schema);
    let reads_caller_typed = schema.in_body_scope()
        && schema
            .carried_columns()
            .any(|(_, _, column)| column.access == PortAccess::DeclaredAny);
    let recheck = (!empty_by_port.is_empty() || reads_caller_typed).then(|| program.clone());
    let display_source = Arc::clone(&source);
    let resolved = resolve_against_row(
        ResolveSite {
            node,
            context: &context,
            span,
        },
        program,
        node_count,
        schema,
        &scoped_vars.module_exports,
        scoped_vars,
    )?;
    // The composition-body checks walk the program once, and only inside a
    // body: outside one no column carries a mark.
    let reads = if schema.in_body_scope() {
        BodyReads::collect(&resolved)
    } else {
        BodyReads::default()
    };
    let refused = reads.refused(schema, node, span);
    if !refused.is_empty() {
        return Err(refused);
    }
    cxl::typecheck::pass::type_check_with_mode_and_vars(resolved, schema, mode.clone(), scoped_vars)
        .map(|mut typed| {
            fold_body_config(&mut typed, scoped_vars);
            typed.with_source(source)
        })
        .map_err(|diags| {
            let errors: Vec<&cxl::typecheck::TypeDiagnostic> =
                diags.iter().filter(|d| !d.is_warning).collect();
            let errors = if errors.is_empty() {
                diags.iter().collect()
            } else {
                errors
            };
            // An error inside an expression that reads a column the port
            // declares `any` is this call's: the node sees the type the call
            // provides, so the author has to learn that first. Every other
            // error is the node's own.
            let mut out: Vec<Diagnostic> = Vec::new();
            let mut by_column: Vec<CallerTypedErrors<'_>> = Vec::new();
            let mut plain: Vec<&cxl::typecheck::TypeDiagnostic> = Vec::new();
            for error in errors {
                match reads.caller_typed_in(schema, error.span) {
                    Some((field, provided, column)) => {
                        match by_column.iter_mut().find(|seen| seen.field == field) {
                            Some(seen) => {
                                seen.details.push(&error.message);
                                seen.within.push(error.span);
                            }
                            None => by_column.push(CallerTypedErrors {
                                field,
                                provided,
                                column,
                                details: vec![&error.message],
                                within: vec![error.span],
                            }),
                        }
                    }
                    None => plain.push(error),
                }
            }
            let recheck = recheck.as_ref().map(|program| Recheck {
                site: ResolveSite {
                    node,
                    context: &context,
                    span,
                },
                program,
                node_count,
                row: schema,
                mode,
                module_exports: &scoped_vars.module_exports,
                scoped_vars,
            });
            for errors in &by_column {
                let fix = recheck.as_ref().and_then(|recheck| {
                    converted_line(
                        recheck,
                        &reads,
                        errors,
                        &AuthoredLine {
                            authored,
                            source: &display_source,
                            within: &errors.within,
                        },
                    )
                });
                out.push(caller_typed_type_error(node, errors, fix.as_ref(), span));
            }
            if let Some(recheck) = &recheck
                && let Some(blamed) = possibly_empty_errors(
                    recheck,
                    &empty_by_port,
                    &reads,
                    &plain,
                    authored,
                    &display_source,
                )
            {
                out.extend(blamed);
                return out;
            }
            if !plain.is_empty() {
                let joined = plain
                    .iter()
                    .map(|d| d.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ");
                let helps: Vec<&str> = plain.iter().filter_map(|d| d.help.as_deref()).collect();
                let diag = Diagnostic::error(
                    "E200",
                    format!("CXL type error in {node_name:?}: {joined}"),
                    LabeledSpan::primary(span, String::new()),
                );
                out.push(if helps.is_empty() {
                    diag
                } else {
                    diag.with_help(helps.join("; "))
                });
            }
            out
        })
}

/// A body row column whose input port declares it `{ nullable: T }` with a
/// concrete `T`.
struct MayBeEmpty {
    field: QualifiedField,
    /// The port's declaration, `{ nullable: T }`: the column's type in the
    /// row the body is checked against.
    declared: Type,
    column: CarriedColumn,
}

/// The columns of a body row that a port declares `{ nullable: T }` with a
/// concrete `T`: a body is checked as if each could be empty.
fn may_be_empty_by_port(row: &Row) -> Vec<MayBeEmpty> {
    if !row.in_body_scope() {
        return Vec::new();
    }
    row.carried_columns()
        .filter(|(_, ty, column)| {
            column.access == PortAccess::Declared
                && matches!(ty, Type::Nullable(inner) if !matches!(inner.as_ref(), Type::Any))
        })
        .map(|(field, ty, column)| MayBeEmpty {
            field: field.clone(),
            declared: ty.clone(),
            column: column.clone(),
        })
        .collect()
}

/// A body program that failed to typecheck against `row`, kept to
/// typecheck again with some of `row`'s columns retyped: whether it then
/// passes tells which column's type failed it.
struct Recheck<'a> {
    site: ResolveSite<'a>,
    /// The program as parsed, before name resolution.
    program: &'a cxl::ast::Program,
    node_count: u32,
    row: &'a Row,
    mode: &'a AggregateMode,
    module_exports: &'a HashMap<String, cxl::resolve::ModuleExports>,
    scoped_vars: &'a cxl::resolve::ScopedVarsRegistry,
}

impl Recheck<'_> {
    /// Whether the program typechecks once each column of `retyped` has the
    /// type given with it, every other column keeping its type.
    fn passes_with(&self, retyped: &[(&QualifiedField, Type)]) -> bool {
        let columns: IndexMap<QualifiedField, Type> = self
            .row
            .fields()
            .map(|(field, ty)| {
                let ty = retyped
                    .iter()
                    .find(|(column, _)| *column == field)
                    .map_or_else(|| ty.clone(), |(_, retyped)| retyped.clone());
                (field.clone(), ty)
            })
            .collect();
        // Built outside a body scope: the program's names were already
        // checked against `row`, and this check is about types only.
        let row = Row::from_parts(columns, self.row.declared_span, self.row.tail.clone());
        let Ok(resolved) = resolve_against_row(
            self.site,
            self.program.clone(),
            self.node_count,
            &row,
            self.module_exports,
            self.scoped_vars,
        ) else {
            return false;
        };
        cxl::typecheck::pass::type_check_with_mode_and_vars(
            resolved,
            &row,
            self.mode.clone(),
            self.scoped_vars,
        )
        .is_ok()
    }

    /// Whether the program typechecks once every column in `never_empty` is
    /// known never to be empty: then the column's possible emptiness is
    /// what the node does not handle.
    fn passes_when_never_empty(&self, never_empty: &[&MayBeEmpty]) -> bool {
        let retyped: Vec<(&QualifiedField, Type)> = never_empty
            .iter()
            .map(|empty| (&empty.field, empty.declared.unwrap_nullable().clone()))
            .collect();
        self.passes_with(&retyped)
    }
}

/// The E200s for a body program whose type errors `plain` come from a
/// column its input port declares `{ nullable: T }` (one of
/// `empty_by_port`) that it does not handle being empty: one per column
/// whose possible emptiness fails it. `None` when the errors are not about
/// an empty value, or when no column the program reads can be blamed; the
/// caller then reports the errors as they are.
fn possibly_empty_errors(
    recheck: &Recheck<'_>,
    empty_by_port: &[MayBeEmpty],
    reads: &BodyReads,
    plain: &[&cxl::typecheck::TypeDiagnostic],
    authored: Authored<'_>,
    source: &str,
) -> Option<Vec<Diagnostic>> {
    if plain.is_empty()
        || empty_by_port.is_empty()
        || !recheck.passes_when_never_empty(&empty_by_port.iter().collect::<Vec<_>>())
    {
        return None;
    }
    let detail = plain
        .iter()
        .map(|d| d.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    let within: Vec<cxl::lexer::Span> = plain.iter().map(|d| d.span).collect();
    // Blame a column only when its possible emptiness is what fails the
    // node: one that, never empty on its own, lets the node compile. When
    // no single column does, the columns the failing expressions read are
    // needed together.
    let candidates = reads.possibly_empty_in(empty_by_port, &within);
    let on_their_own: Vec<&MayBeEmpty> = candidates
        .iter()
        .copied()
        .filter(|empty| candidates.len() == 1 || recheck.passes_when_never_empty(&[empty]))
        .collect();
    let blamed = if on_their_own.is_empty() {
        candidates
    } else {
        on_their_own
    };
    if blamed.is_empty() {
        return None;
    }
    let ResolveSite { node, span, .. } = recheck.site;
    Some(
        blamed
            .into_iter()
            .map(|empty| {
                possibly_empty_type_error(
                    &PossiblyEmpty {
                        node,
                        field: &empty.field,
                        column: &empty.column,
                        declared: &empty.declared,
                        span,
                    },
                    reads,
                    &AuthoredLine {
                        authored,
                        source,
                        within: &within,
                    },
                    &detail,
                )
            })
            .collect(),
    )
}

/// Where a help finds the author's line to rewrite: how the program was
/// written, its source, and the spans of the failing expressions.
struct AuthoredLine<'a> {
    authored: Authored<'a>,
    source: &'a str,
    within: &'a [cxl::lexer::Span],
}

/// `key: '<expr>'` as a YAML entry the author can paste: single-quoted
/// unless the expression holds a single quote, then double-quoted unless
/// that needs escaping too.
fn yaml_entry(key: &str, expr: &str) -> Option<String> {
    if !expr.contains('\'') {
        Some(format!("{key}: '{expr}'"))
    } else if !expr.contains('"') && !expr.contains('\\') {
        Some(format!("{key}: \"{expr}\""))
    } else {
        None
    }
}

/// `detail`, a CXL type checker message, with each type spelled as a port
/// schema spells it: `Nullable(Int)` as `{ nullable: int }`, `Bool` as
/// `bool`. Every other word is kept.
fn author_spelling(detail: &str) -> String {
    fn spelled(word: &str) -> Option<&'static str> {
        Some(match word {
            "Bool" => "bool",
            "Int" => "int",
            "Float" => "float",
            "Decimal" => "decimal",
            "String" => "string",
            "Date" => "date",
            "DateTime" => "date_time",
            _ => return None,
        })
    }
    let mut out = String::with_capacity(detail.len());
    let mut rest = detail;
    while !rest.is_empty() {
        let word_end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        if word_end == 0 {
            let mut chars = rest.chars();
            out.push(chars.next().expect("non-empty"));
            rest = chars.as_str();
            continue;
        }
        let word = &rest[..word_end];
        rest = &rest[word_end..];
        if word == "Nullable"
            && let Some(inner) = rest.strip_prefix('(')
            && let Some(close) = inner.find(')')
            && let Some(inner_spelling) = spelled(&inner[..close])
        {
            out.push_str(&format!("{{ nullable: {inner_spelling} }}"));
            rest = &inner[close + 1..];
            continue;
        }
        out.push_str(spelled(word).unwrap_or(word));
    }
    out
}

/// A body node that cannot use a column while it may be empty.
struct PossiblyEmpty<'a> {
    node: &'a str,
    field: &'a QualifiedField,
    column: &'a CarriedColumn,
    /// The port's declaration, `{ nullable: T }`.
    declared: &'a Type,
    span: Span,
}

/// A value of `ty` a `??` can stand in for an empty one with, where CXL has
/// a literal for it.
fn empty_stand_in(ty: &Type) -> Option<&'static str> {
    Some(match ty {
        Type::Int => "0",
        Type::Float => "0.0",
        Type::String => "\"\"",
        Type::Bool => "false",
        _ => return None,
    })
}

/// E200 for a body node that fails because a column its input port declares
/// `{ nullable: T }` may be empty. Every call checks the body as if the
/// column could be empty, so this fails at every call, whoever the caller
/// is; the fix is in the composition.
fn possibly_empty_type_error(
    at: &PossiblyEmpty<'_>,
    reads: &BodyReads,
    line: &AuthoredLine<'_>,
    detail: &str,
) -> Diagnostic {
    let PossiblyEmpty {
        node,
        field,
        column,
        span,
        ..
    } = at;
    let PortOrigin {
        call,
        port,
        body_file,
    } = column.port.as_ref();
    let name = field.name.as_ref();
    let declared = port_column_type(at.declared);
    let inner = at.declared.unwrap_nullable();
    let never_empty = port_column_type(inner);
    let declare = format!(
        "declare `{name}` as never empty on input port `{port}` in `{body_file}`: \
         `- {{ name: {name}, type: {never_empty} }}`"
    );
    let fix = match empty_stand_in(inner)
        .and_then(|stand_in| reads.line_with_stand_in(line, field, stand_in))
    {
        Some(line) => format!(
            "handle an empty `{name}` in node {node:?} in `{body_file}`: `{line}`, so the \
             composition works for callers whose `{name}` can be empty. If it should not accept \
             an empty `{name}`, {declare}"
        ),
        None => format!(
            "give node {node:?} in `{body_file}` a value to use when `{name}` is empty \
             (`{name} ?? <value>`), or {declare}"
        ),
    };
    Diagnostic::error(
        "E200",
        format!(
            "node {node:?} in composition {call:?} cannot use `{name}` while it may be empty: \
             input port `{port}` declares `{name}` as `{declared}`, so the composition is \
             checked at every call as if `{name}` could be empty"
        ),
        LabeledSpan::primary(*span, String::new()),
    )
    .with_help(format!(
        "{fix}. Technical detail: {}",
        author_spelling(detail)
    ))
}

/// The type errors of one body node inside expressions that read one
/// column its input port declares `any`.
struct CallerTypedErrors<'a> {
    field: &'a QualifiedField,
    /// The type this call provides for the column.
    provided: &'a Type,
    column: &'a CarriedColumn,
    details: Vec<&'a str>,
    /// The failing expressions' spans.
    within: Vec<cxl::lexer::Span>,
}

/// The types a node can need a caller-typed column to have, in the order
/// a fix tries them: each has a CXL conversion method.
const CONVERTIBLE_TYPES: [Type; 7] = [
    Type::String,
    Type::Int,
    Type::Float,
    Type::Decimal,
    Type::Bool,
    Type::Date,
    Type::DateTime,
];

/// The type a node needs a column its port declares `any` to have, and the
/// node's failing line with every read of the column on it converted to
/// that type (`v.to_string()`): the line compiles whatever type a call
/// provides. `None` when no single conversion makes the node compile or no
/// authored line can be rewritten.
fn converted_line(
    recheck: &Recheck<'_>,
    reads: &BodyReads,
    errors: &CallerTypedErrors<'_>,
    line: &AuthoredLine<'_>,
) -> Option<(Type, String)> {
    let needed = CONVERTIBLE_TYPES
        .iter()
        .find(|ty| recheck.passes_with(&[(errors.field, (*ty).clone())]))?;
    let method = conversion_method(needed)?;
    let rewritten = reads.line_rewriting(line, errors.field, &|written| {
        format!("{written}.{method}()")
    })?;
    Some((needed.clone(), rewritten))
}

/// E200 for a type error in a composition body node that reads a column
/// its input port declares `any`. The body sees the type this call
/// provides, so the composition and the port are what the author has to
/// read first. With `fix`, the type the node needs and its line converting
/// the column to it, the help is that line; the composition then works for
/// every call whose values convert.
fn caller_typed_type_error(
    node: &str,
    errors: &CallerTypedErrors<'_>,
    fix: Option<&(Type, String)>,
    span: Span,
) -> Diagnostic {
    let PortOrigin {
        call,
        port,
        body_file,
    } = errors.column.port.as_ref();
    let name = errors.field.name.as_ref();
    let detail = errors.details.join("; ");
    let help = match fix {
        Some((needed, line)) => format!(
            "convert `{name}` where node {node:?} in `{body_file}` uses it, so the node gets the \
             `{}` it needs whatever type a call provides, as long as its values convert: `{line}`",
            port_column_type(needed)
        ),
        None => format!(
            "declare on input port `{port}` in `{body_file}` the type node {node:?} needs for \
             `{name}`, so every call has to provide that type — replace `type: any` in \
             `- {{ name: {name}, type: any }}` — or convert `{name}` in this pipeline before the \
             call"
        ),
    };
    Diagnostic::error(
        "E200",
        format!(
            "composition {call:?} passes column `{name}` to node {node:?} as `{}`, the type \
             this call provides, because its input port `{port}` declares `{name}` as `any`; \
             node {node:?} cannot use it that way: {detail}",
            port_column_type(errors.provided)
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(help)
}

/// Wrapper a single Cull rule predicate is parsed inside. The rule source is
/// appended verbatim, so a parsed predicate's byte-offset spans are relative
/// to a source that begins immediately after this prefix. `bind_cull` shifts
/// those spans by the distance to where the rule's text lands in the
/// reconstructed decision source, so keep the two uses in lockstep.
const CULL_RULE_PARSE_PREFIX: &str = "emit __cull_pred = ";

/// Parse one Cull rule's `drop_group_when` predicate into a stand-alone
/// expression, relocating its node ids past `base_id`.
///
/// The predicate is parsed as its own [`CULL_RULE_PARSE_PREFIX`]`<source>`
/// program, so a trailing `#` line-comment terminates at end-of-line
/// within this rule instead of swallowing later disjuncts. The single
/// emit's right-hand side is extracted and its node ids shifted by
/// `base_id`, so the caller can OR several rule predicates into one
/// dense-id-space decision program. The predicate's byte-offset spans
/// stay in this wrapper's coordinate space; the caller rebases them into
/// the reconstructed decision source via [`cxl::ast::rebase_expr_spans`].
/// Returns the relocated predicate and the rule program's node count (the
/// id span the caller must skip). `Err` is an E202 parse diagnostic
/// already attributed to `node_name`.
#[allow(clippy::result_large_err)]
fn parse_cull_rule_predicate(
    node_name: &str,
    source: &str,
    base_id: u32,
    span: Span,
) -> Result<(Expr, u32), Diagnostic> {
    let parsed = cxl::parser::Parser::parse(&format!("{CULL_RULE_PARSE_PREFIX}{source}"));
    if !parsed.errors.is_empty() {
        let messages: Vec<String> = parsed.errors.iter().map(|e| e.message.clone()).collect();
        return Err(Diagnostic::error(
            "E202",
            format!("CXL parse error in {node_name:?}: {}", messages.join("; ")),
            LabeledSpan::primary(span, String::new()),
        ));
    }
    // The wrapper is exactly one field emit of a single expression; a rule
    // source that parsed into anything else (extra statements, a scoped-state
    // emit) is not a group predicate.
    let mut statements = parsed.ast.statements;
    let single = (statements.len() == 1).then(|| statements.pop()).flatten();
    let mut predicate = match single {
        Some(Statement::Emit {
            expr,
            target: cxl::ast::EmitTarget::Field,
            ..
        }) => expr,
        _ => {
            return Err(Diagnostic::error(
                "E202",
                format!(
                    "CXL parse error in {node_name:?}: drop_group_when must be a single \
                     expression"
                ),
                LabeledSpan::primary(span, String::new()),
            ));
        }
    };
    cxl::ast::offset_node_ids(&mut predicate, base_id);
    Ok((predicate, parsed.node_count))
}

/// Typecheck one Cull rule's predicate in isolation, to attribute a
/// diagnostic to the offending rule when the OR-combined program failed.
///
/// The predicate is wrapped as `<predicate> or false` (built on the AST, so
/// a `#` comment stays scoped to the predicate) to force a boolean operand,
/// surfacing a non-boolean predicate as the same type error the combined
/// program raises. Returns `Ok` if this rule alone typechecks cleanly,
/// `Err` with the attributed diagnostic otherwise.
fn typecheck_cull_rule(
    ctx: &CxlTypecheckCtx<'_>,
    source: &str,
) -> Result<TypedProgram, Vec<Diagnostic>> {
    let (predicate, mut next_id) =
        parse_cull_rule_predicate(ctx.node_name, source, 0, ctx.span).map_err(|d| vec![d])?;
    let body_span = predicate.span();
    let false_id = cxl::ast::NodeId(next_id);
    next_id += 1;
    let or_id = cxl::ast::NodeId(next_id);
    next_id += 1;
    let emit_id = cxl::ast::NodeId(next_id);
    next_id += 1;
    let body = Expr::Binary {
        node_id: or_id,
        op: cxl::ast::BinOp::Or,
        lhs: Box::new(predicate),
        rhs: Box::new(Expr::Literal {
            node_id: false_id,
            value: cxl::ast::LiteralValue::Bool(false),
            span: body_span,
        }),
        span: body_span,
    };
    let program = cxl::ast::Program {
        statements: vec![Statement::Emit {
            node_id: emit_id,
            name: "__cull_pred".into(),
            expr: body,
            target: cxl::ast::EmitTarget::Field,
            span: body_span,
        }],
        span: cxl::lexer::Span::default(),
    };
    let display = format!("emit __cull_pred = ({source}) or false");
    typecheck_parsed_program(
        ctx,
        program,
        next_id,
        std::sync::Arc::from(display.as_str()),
    )
}

/// Compile an Envelope node's declarative header/footer synthesis against the
/// body input `Row` (the Envelope's output row, since it does not widen).
///
/// Returns `Some(spec)` when at least one section compiled cleanly; `None` when
/// every declared section failed compilation (all errors already pushed onto
/// `diags`). A section that fails to compile is dropped and its diagnostic is
/// emitted, so a present spec carries only the sections the executor can run.
///
/// # Header sections
///
/// Each field is typechecked in [`AggregateMode::Row`] as a synthetic
/// `emit <field> = <expr>` program (all of a section's fields in one program,
/// so they share the typed side-tables). The header is emitted before any body
/// record streams, so a body-column reference is meaningless — any
/// [`Expr::FieldRef`] / [`Expr::QualifiedFieldRef`] in a header expression is
/// rejected with **E353**.
///
/// # Footer sections
///
/// Each field is typechecked in [`AggregateMode::GroupBy`] with an empty
/// group_by (the per-document grouping is external) and extracted via
/// [`cxl::plan::extract_aggregates`]. Every binding must be in the streaming
/// allow-list — `count` / `sum` / `avg` / `min` / `max`, the O(1)
/// distributive/algebraic set — with a bare-field or `*` argument; a holistic
/// aggregate (`collect`, `any`, `weighted_avg`) or a composed/two-arg argument
/// is rejected with **E354**. Holistic functions that are not CXL functions at
/// all (`median`, `mode`) already fail typechecking as unknown functions.
#[allow(clippy::result_large_err)]
fn compile_envelope_synthesis(
    name: &str,
    config: &crate::config::pipeline_node::EnvelopeBody,
    body_row: &Row,
    span: Span,
    scoped_vars: &cxl::resolve::ScopedVarsRegistry,
    diags: &mut Vec<Diagnostic>,
) -> Option<crate::plan::envelope_synthesis::EnvelopeSynthesis> {
    use crate::plan::envelope_synthesis::{
        EnvelopeSynthesis, SynthesizedFooterSection, SynthesizedHeaderSection,
    };

    let mut header_sections: Vec<SynthesizedHeaderSection> = Vec::new();
    for (section, fields) in &config.header {
        if let Some(compiled) =
            compile_header_section(name, section, fields, body_row, span, scoped_vars, diags)
        {
            header_sections.push(compiled);
        }
    }

    let mut footer_sections: Vec<SynthesizedFooterSection> = Vec::new();
    for (section, fields) in &config.footer {
        if let Some(compiled) =
            compile_footer_section(name, section, fields, body_row, span, scoped_vars, diags)
        {
            footer_sections.push(compiled);
        }
    }

    let synthesis = EnvelopeSynthesis {
        header: header_sections,
        footer: footer_sections,
    };
    if synthesis.is_empty() {
        None
    } else {
        Some(synthesis)
    }
}

/// Compile one header section: typecheck all its fields together as a `Row`-mode
/// program, then reject any body-column reference with E353. Returns `None` (and
/// pushes a diagnostic) on a typecheck failure or an E353 rejection.
#[allow(clippy::result_large_err)]
fn compile_header_section(
    name: &str,
    section: &str,
    fields: &IndexMap<String, crate::yaml::CxlSource>,
    body_row: &Row,
    span: Span,
    scoped_vars: &cxl::resolve::ScopedVarsRegistry,
    diags: &mut Vec<Diagnostic>,
) -> Option<crate::plan::envelope_synthesis::SynthesizedHeaderSection> {
    use crate::plan::envelope_synthesis::SynthesizedHeaderSection;

    // One synthetic program per section: `emit <field> = <expr>` per field, in
    // declaration order, so they typecheck together and share side-tables.
    let mut program = String::new();
    for (field, expr) in fields {
        program.push_str("emit ");
        program.push_str(field);
        program.push_str(" = ");
        program.push_str(expr.as_ref());
        program.push('\n');
    }

    let typed = match typecheck_cxl(
        name,
        &format!("{name}:header.{section}"),
        CxlSource {
            text: &program,

            authored: Authored::Synthesized,
        },
        body_row,
        AggregateMode::Row,
        span,
        scoped_vars,
    ) {
        Ok(t) => t,
        Err(d) => {
            diags.extend(d);
            return None;
        }
    };

    // Pair each field's typed emit expression by output name. A header field
    // expression that references a body column is rejected: the header is
    // emitted before the body streams, so a body value is not yet knowable.
    let mut field_exprs: Vec<(Box<str>, Expr)> = Vec::new();
    let mut rejected = false;
    cxl::ast::for_each_field_emit(&typed.program.statements, &mut |field, expr| {
        if let Some(bad) = first_body_field_ref(expr) {
            diags.push(
                Diagnostic::error(
                    "E353",
                    format!(
                        "envelope {name:?} header section {section:?} field {field:?} references \
                         body column {bad:?} — the header is emitted before the body streams, so \
                         it may read only $vars / $source / $pipeline / $doc, never a body field"
                    ),
                    LabeledSpan::primary(span, String::new()),
                )
                .with_help(
                    "compute the value from a document-open-knowable input ($vars / $source / \
                     $pipeline / $doc), or move a body-derived value into a footer aggregate",
                ),
            );
            rejected = true;
        }
        field_exprs.push((Box::from(field), expr.clone()));
    });

    if rejected {
        return None;
    }

    Some(SynthesizedHeaderSection {
        section: section.to_string(),
        typed: Arc::new(typed),
        fields: field_exprs,
    })
}

/// The name of the first body-column reference reachable from `expr`, or `None`
/// when the expression reads only document-open-knowable inputs ($-namespaces).
/// A bare [`Expr::FieldRef`] is a body column; a [`Expr::QualifiedFieldRef`]'s
/// dotted path is reported joined. `$source` / `$doc` / `$vars` / `$pipeline`
/// accesses are NOT field references and pass.
fn first_body_field_ref(expr: &Expr) -> Option<String> {
    match expr {
        Expr::FieldRef { name, .. } => Some(name.to_string()),
        Expr::QualifiedFieldRef { parts, .. } => Some(
            parts
                .iter()
                .map(|p| p.as_ref())
                .collect::<Vec<_>>()
                .join("."),
        ),
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            first_body_field_ref(lhs).or_else(|| first_body_field_ref(rhs))
        }
        Expr::Unary { operand, .. } => first_body_field_ref(operand),
        Expr::ArrayLiteral { elements, .. } => elements.iter().find_map(first_body_field_ref),
        Expr::MapLiteral { entries, .. } => entries.iter().find_map(|entry| {
            match &entry.key {
                cxl::ast::MapKey::Computed(key) => first_body_field_ref(key),
                cxl::ast::MapKey::Static(_) => None,
            }
            .or_else(|| first_body_field_ref(&entry.value))
        }),
        Expr::ArrayComprehension {
            item,
            source,
            predicate,
            ..
        } => first_body_field_ref(source)
            .or_else(|| first_body_field_ref(item))
            .or_else(|| predicate.as_deref().and_then(first_body_field_ref)),
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => first_body_field_ref(condition)
            .or_else(|| first_body_field_ref(then_branch))
            .or_else(|| else_branch.as_deref().and_then(first_body_field_ref)),
        Expr::IndexAccess {
            receiver, index, ..
        } => first_body_field_ref(receiver).or_else(|| first_body_field_ref(index)),
        Expr::MethodCall { receiver, args, .. } => {
            first_body_field_ref(receiver).or_else(|| args.iter().find_map(first_body_field_ref))
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            args.iter().find_map(first_body_field_ref)
        }
        Expr::Match { subject, arms, .. } => subject
            .as_deref()
            .and_then(first_body_field_ref)
            .or_else(|| {
                arms.iter().find_map(|arm| {
                    first_body_field_ref(&arm.pattern).or_else(|| first_body_field_ref(&arm.body))
                })
            }),
        Expr::Closure { body, .. } => first_body_field_ref(body),
        Expr::Literal { .. }
        | Expr::PipelineAccess { .. }
        | Expr::VarsAccess { .. }
        | Expr::ConfigAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::RecordAccess { .. }
        | Expr::DocAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => None,
    }
}

/// Compile one footer section: typecheck its fields as a `GroupBy`-mode program
/// with an empty group_by, extract the aggregates, then reject any binding
/// outside the streaming allow-list (or with a composed argument) with E354.
/// Returns `None` (and pushes a diagnostic) on any failure.
#[allow(clippy::result_large_err)]
fn compile_footer_section(
    name: &str,
    section: &str,
    fields: &IndexMap<String, crate::yaml::CxlSource>,
    body_row: &Row,
    span: Span,
    scoped_vars: &cxl::resolve::ScopedVarsRegistry,
    diags: &mut Vec<Diagnostic>,
) -> Option<crate::plan::envelope_synthesis::SynthesizedFooterSection> {
    use clinker_record::accumulator::AggregateType;
    use cxl::plan::BindingArg;

    use crate::plan::envelope_synthesis::SynthesizedFooterSection;

    let mut program = String::new();
    for (field, expr) in fields {
        program.push_str("emit ");
        program.push_str(field);
        program.push_str(" = ");
        program.push_str(expr.as_ref());
        program.push('\n');
    }

    let typed = match typecheck_cxl(
        name,
        &format!("{name}:footer.{section}"),
        CxlSource {
            text: &program,

            authored: Authored::Synthesized,
        },
        body_row,
        AggregateMode::GroupBy {
            group_by_fields: HashSet::new(),
        },
        span,
        scoped_vars,
    ) {
        Ok(t) => t,
        Err(d) => {
            diags.extend(d);
            return None;
        }
    };

    let input_schema: Vec<String> = body_row
        .field_names()
        .map(|qf| qf.name.to_string())
        .collect();
    let compiled = match cxl::plan::extract_aggregates(&typed, &[], &input_schema) {
        Ok(c) => c,
        Err(errs) => {
            for e in errs {
                diags.push(Diagnostic::error(
                    "E354",
                    format!(
                        "envelope {name:?} footer section {section:?}: aggregate extraction \
                         failed: {}",
                        e.message
                    ),
                    LabeledSpan::primary(span, String::new()),
                ));
            }
            return None;
        }
    };

    // Every binding must be a streaming distributive/algebraic aggregate over a
    // bare field or `*`. Holistic / non-O(1) aggregates and composed (multi-arg
    // or expression) arguments are not supported by the streaming footer fold.
    let mut rejected = false;
    for binding in &compiled.bindings {
        let allowed_type = matches!(
            binding.acc_type,
            AggregateType::Count { .. }
                | AggregateType::Sum
                | AggregateType::Avg
                | AggregateType::Min
                | AggregateType::Max
        );
        if !allowed_type {
            diags.push(
                Diagnostic::error(
                    "E354",
                    format!(
                        "envelope {name:?} footer section {section:?} uses aggregate {} \
                         ({:?}), which is not a streaming footer aggregate — a footer fold \
                         supports only count / sum / avg / min / max",
                        binding.output_name, binding.acc_type
                    ),
                    LabeledSpan::primary(span, String::new()),
                )
                .with_help(
                    "rewrite the footer with a count / sum / avg / min / max aggregate, or \
                     compute a holistic value in an upstream Aggregate node",
                ),
            );
            rejected = true;
            continue;
        }
        if !matches!(binding.arg, BindingArg::Field(_) | BindingArg::Wildcard) {
            diags.push(
                Diagnostic::error(
                    "E354",
                    format!(
                        "envelope {name:?} footer section {section:?} aggregate {} takes a \
                         composed or multi-argument expression — a footer fold supports only a \
                         bare field or `*` argument (count() / count(*) / sum(amount) / min(x))",
                        binding.output_name
                    ),
                    LabeledSpan::primary(span, String::new()),
                )
                .with_help(
                    "project the composed value in an upstream Transform, then aggregate the \
                     resulting bare column in the footer",
                ),
            );
            rejected = true;
        }
    }

    if rejected {
        return None;
    }

    Some(SynthesizedFooterSection {
        section: section.to_string(),
        compiled: Arc::new(compiled),
    })
}

/// Propagate an upstream row through a Transform node: start with all
/// upstream fields, then append (or overwrite) one entry per `emit`
/// statement. Output field names come from the `emit name = expr` LHS
/// and are always bare (`QualifiedField::bare`). Preserves the upstream
/// tail (Closed / Open) so pass-through semantics propagate across
/// Transform boundaries. A column a composition port carried in keeps its
/// mark unless the Transform writes it, which makes it the body's own.
fn propagate_row(upstream: &Row, typed: &TypedProgram) -> Row {
    let mut out = upstream.declared_map().clone();
    let mut written: Vec<QualifiedField> = Vec::new();
    cxl::ast::for_each_field_emit(&typed.program.statements, &mut |name, expr| {
        let emit_type = typed
            .types
            .get(expr.node_id().0 as usize)
            .and_then(|t| t.clone())
            .unwrap_or(Type::Any);
        out.insert(QualifiedField::bare(name), emit_type);
        written.push(QualifiedField::bare(name));
    });
    let mut row = Row::from_parts(out, upstream.declared_span, upstream.tail.clone());
    row.inherit_marks(upstream, |field| !written.contains(field));
    for field in written {
        row.mark(field, ColumnMark::Own);
    }
    row
}

/// Propagate the authored multi-value declaration through one Transform.
///
/// Transform evaluation starts from the incoming record and overwrites fields
/// named by `emit`. Only a direct field reference (or a chain of top-level
/// `let` aliases ending at one) preserves the declaration. Every expression
/// that constructs or computes a value revokes it, even when its inferred CXL
/// type is `array`: runtime type alone cannot distinguish an intentional
/// schema-declared repeated field from `match: collect`, an array literal, or
/// a value routed into the wrong column.
fn propagate_transform_declared_multiple(
    incoming: &BTreeSet<String>,
    typed: &TypedProgram,
) -> BTreeSet<String> {
    fn is_direct_alias(
        expr: &Expr,
        incoming: &BTreeSet<String>,
        let_aliases: &[bool],
        typed: &TypedProgram,
    ) -> bool {
        let Expr::FieldRef { node_id, name, .. } = expr else {
            return false;
        };
        match typed
            .bindings
            .get(node_id.0 as usize)
            .and_then(Option::as_ref)
        {
            Some(cxl::resolve::ResolvedBinding::Field(_)) => incoming.contains(name.as_ref()),
            Some(cxl::resolve::ResolvedBinding::LetVar(index)) => {
                let_aliases.get(*index).copied().unwrap_or(false)
            }
            _ => false,
        }
    }

    fn revoke_nested_emits(statements: &[Statement], output: &mut BTreeSet<String>) {
        cxl::ast::for_each_field_emit(statements, &mut |name, _| {
            output.remove(name);
        });
    }

    let mut output = incoming.clone();
    let mut let_aliases = Vec::new();
    for statement in &typed.program.statements {
        match statement {
            Statement::Let { expr, .. } => {
                let_aliases.push(is_direct_alias(expr, incoming, &let_aliases, typed));
            }
            Statement::Emit {
                name,
                expr,
                target: cxl::ast::EmitTarget::Field,
                ..
            } => {
                if is_direct_alias(expr, incoming, &let_aliases, typed) {
                    output.insert(name.to_string());
                } else {
                    output.remove(name.as_ref());
                }
            }
            Statement::EmitEach { body, .. } | Statement::ExplodeOuter { body, .. } => {
                // Fan-out bindings change value identity and may overwrite an
                // otherwise pass-through field. Revoke every nested emit
                // rather than authorizing one from its inferred array type.
                revoke_nested_emits(body, &mut output);
            }
            _ => {}
        }
    }
    output
}

/// Propagate declarations through a Combine body without treating the
/// `match: collect` result or any computed array as schema authorization.
/// Combine outputs are projection-only, so unlike Transform this starts empty
/// and admits only direct qualified aliases from an input whose contract names
/// the referenced field.
fn propagate_combine_declared_multiple(
    header: &CombineHeader,
    declared_multiple_by_name: &HashMap<String, BTreeSet<String>>,
    typed: &TypedProgram,
) -> BTreeSet<String> {
    fn is_direct_alias(
        expr: &Expr,
        header: &CombineHeader,
        declared_multiple_by_name: &HashMap<String, BTreeSet<String>>,
        let_aliases: &[bool],
        typed: &TypedProgram,
    ) -> bool {
        match expr {
            Expr::QualifiedFieldRef { parts, .. } if parts.len() == 2 => {
                let Some(input) = header.input.get(parts[0].as_ref()) else {
                    return false;
                };
                declared_multiple_by_name
                    .get(input_target(&input.value))
                    .is_some_and(|fields| fields.contains(parts[1].as_ref()))
            }
            Expr::FieldRef { node_id, .. } => match typed
                .bindings
                .get(node_id.0 as usize)
                .and_then(Option::as_ref)
            {
                Some(cxl::resolve::ResolvedBinding::LetVar(index)) => {
                    let_aliases.get(*index).copied().unwrap_or(false)
                }
                _ => false,
            },
            _ => false,
        }
    }

    let mut output = BTreeSet::new();
    let mut let_aliases = Vec::new();
    for statement in &typed.program.statements {
        match statement {
            Statement::Let { expr, .. } => let_aliases.push(is_direct_alias(
                expr,
                header,
                declared_multiple_by_name,
                &let_aliases,
                typed,
            )),
            Statement::Emit {
                name,
                expr,
                target: cxl::ast::EmitTarget::Field,
                ..
            } => {
                if is_direct_alias(expr, header, declared_multiple_by_name, &let_aliases, typed) {
                    output.insert(name.to_string());
                }
            }
            // Fan-out emits are not direct Combine aliases; they construct a
            // different record stream and therefore cannot authorize arrays.
            Statement::EmitEach { .. } | Statement::ExplodeOuter { .. } => {}
            _ => {}
        }
    }
    output
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
    // `extract_aggregates` rejects `emit each` inside an aggregate body
    // because finalize emits one record per group and a fan-out's
    // cardinality cannot collapse onto a single group key. The walker
    // here still recurses so the typed `output_row` stays coherent with
    // every other emit-name collector — the rejection diagnostic
    // supersedes whatever schema this produces.
    let mut written: Vec<QualifiedField> = Vec::new();
    cxl::ast::for_each_field_emit(&typed.program.statements, &mut |name, expr| {
        let emit_type = typed
            .types
            .get(expr.node_id().0 as usize)
            .and_then(|t| t.clone())
            .unwrap_or(Type::Any);
        out.insert(QualifiedField::bare(name), emit_type);
        written.push(QualifiedField::bare(name));
    });
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
    // Carry the `$widened` sidecar column through aggregation when the
    // upstream row has it. The aggregator's `finalize_group_inner`
    // initializes values to `Value::Null` for any slot not covered by
    // group_by stamping or an emit, so the post-aggregate `$widened`
    // is always `Value::Null` — the per-row map payload has no
    // canonical reduction semantic, and unioning maps across N input
    // rows would produce arbitrary key collisions. Users who need an
    // unmapped field at the aggregate output should add it to
    // `group_by` or emit it explicitly via an aggregate function.
    // The slot itself stays on the schema (engine-stamped) so
    // downstream `include_unmapped: true` projections, dispatch
    // canonicalize invariants, and composition body propagation all
    // see a stable column shape across the aggregate boundary.
    if upstream.has_field(crate::config::pipeline_node::WIDENED_SIDECAR_COLUMN) {
        out.insert(
            QualifiedField::bare(crate::config::pipeline_node::WIDENED_SIDECAR_COLUMN),
            Type::Any,
        );
    }
    let mut row = Row::from_parts(out, upstream.declared_span, upstream.tail.clone());
    // A group key holds the caller's value; an emitted column and the
    // engine's own columns are the body's.
    row.inherit_marks(upstream, |field| {
        group_by.iter().any(|gb| gb == field.name.as_ref()) && !written.contains(field)
    });
    let own: Vec<QualifiedField> = row
        .field_names()
        .filter(|field| written.contains(field) || field.name.starts_with('$'))
        .cloned()
        .collect();
    for field in own {
        row.mark(field, ColumnMark::Own);
    }
    row
}

// ─── Combine arm (Phase Combine C.1.1 + C.1.2 + C.1.3) ──────────────

/// Immutable inputs describing the `PipelineNode::Combine` being bound:
/// its node name, header (input qualifiers), body config, call-site
/// span, and the enclosing scoped-vars registry. Grouped so
/// [`bind_combine`]'s read-only node description stays separate from its
/// mutable accumulators (`diags`, `artifacts`, the parent schema map).
struct CombineNodeBinding<'a> {
    name: &'a str,
    header: &'a CombineHeader,
    config: &'a CombineBody,
    span: Span,
    scoped_vars: &'a cxl::resolve::ScopedVarsRegistry,
    /// This combine's stable [`PlanNodeId`], resolved from the bind walk's
    /// scope name→id map. Keys every per-combine side-table entry
    /// (`combine_where_typed`, `combine_inputs`, `combine_driving`,
    /// `combine_strategy_hints`, `combine_predicates`,
    /// `combine_resolved_columns`, `typed`) so a combine named the same in
    /// two composition scopes does not collide.
    id: PlanNodeId,
}

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
fn bind_combine(
    node: &CombineNodeBinding<'_>,
    diags: &mut Vec<Diagnostic>,
    artifacts: &mut CompileArtifacts,
    schema_by_name: &mut HashMap<String, Row>,
) {
    let &CombineNodeBinding {
        name,
        header,
        config,
        span,
        scoped_vars,
        id,
    } = node;
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
        let producer_port = match &node_input_spanned.value {
            crate::config::node_header::NodeInput::Port { port, .. } => Some(port.clone()),
            crate::config::node_header::NodeInput::Single(_) => None,
        };
        combine_inputs_entries.insert(
            qualifier.clone(),
            CombineInput {
                upstream_name: Arc::from(upstream_target),
                producer_port,
                row: upstream_row.clone(),
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
        &artifacts.statistics,
        config.drive.as_deref(),
        name,
        span,
        diags,
    ) {
        artifacts.combine_driving.insert(id, driver);
    }

    artifacts.combine_inputs.insert(id, combine_inputs_entries);
    artifacts.combine_strategy_hints.insert(id, config.strategy);

    let cxl_span = cxl::lexer::Span::new(span.start as usize, span.start as usize);
    let mut merged_row = Row::closed(merged_declared, cxl_span);
    // Each input's marks stay on its columns under their qualifier, so
    // `b.extra` is refused where `b`'s port does not declare `extra`.
    let inputs_in_body = artifacts
        .combine_inputs
        .get(&id)
        .is_some_and(|inputs| inputs.values().any(|input| input.row.in_body_scope()));
    if inputs_in_body {
        merged_row.enter_body_scope();
    }
    for (qualifier, input) in artifacts
        .combine_inputs
        .get(&id)
        .into_iter()
        .flat_map(|inputs| inputs.iter())
    {
        for field in input.row.field_names() {
            if let Some(mark) = input.row.mark_of(field) {
                merged_row.mark(
                    QualifiedField::qualified(qualifier.as_str(), field.name.clone()),
                    mark.clone(),
                );
            }
        }
    }

    // ── Where-clause typecheck ─────────────────────────────────────
    let typed_where = match typecheck_combine_where(
        name,
        config.where_expr.as_ref(),
        &merged_row,
        span,
        scoped_vars,
    ) {
        Ok(t) => Arc::new(t),
        Err(mut new_diags) => {
            diags.append(&mut new_diags);
            return;
        }
    };

    artifacts
        .combine_where_typed
        .insert(id, Arc::clone(&typed_where));

    // Post-walk for E304 (unknown / 3-part qualified refs in where).
    let refused_where = |d: &Diagnostic| d.code == "E304" || d.code == "E169";
    let e304_before = diags.iter().filter(|d| refused_where(d)).count();
    if let Some(Statement::Filter { predicate, .. }) = typed_where.program.statements.first() {
        walk_for_unknown_refs(
            predicate,
            CombineWalkContext {
                merged_row: &merged_row,
                combine_name: name,
                err_code: "E304",
                context: "where-clause",
                combine_span: span,
            },
            diags,
        );
    }
    let e304_fired = diags.iter().filter(|d| refused_where(d)).count() > e304_before;

    // Decompose predicate → DecomposedPredicate. The shared typed_where
    // Arc is threaded through so each EqualityConjunct captures the
    // typed program the runtime key compilation needs — no re-typecheck
    // per side, since equality sub-`Expr`s preserve their NodeIds and the
    // where-program's regex cache covers them.
    let decomposed = match decompose_predicate(&typed_where, &merged_row, cxl_span, scoped_vars) {
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

    artifacts.combine_predicates.insert(id, decomposed);

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
        let driving = match artifacts.combine_driving.get(&id).cloned() {
            Some(d) => d,
            None => return,
        };
        let inputs = artifacts
            .combine_inputs
            .get(&id)
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
        let mut resolved_map: HashMap<QualifiedField, (JoinSide, u32)> = HashMap::new();
        for stmt in &typed_where.program.statements {
            for sub in statement_exprs(stmt) {
                walk_expr_for_qualified_refs(Some(sub), inputs, Some(&driving), &mut resolved_map);
            }
        }
        artifacts
            .combine_resolved_columns
            .insert(id, Arc::new(resolved_map));

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
        let mut output_row = Row::closed(output_decl, cxl_span);
        // The driver's columns pass through collect mode unchanged; the
        // matched-array column and the propagated keys are the engine's.
        output_row.inherit_marks(&driver_input.row, |_| true);
        let own: Vec<QualifiedField> = output_row
            .field_names()
            .filter(|field| {
                field.name.as_ref() == build_qualifier.as_str()
                    || (field.name.starts_with('$') && driver_input.row.mark_of(field).is_none())
            })
            .cloned()
            .collect();
        for field in own {
            output_row.mark(field, ColumnMark::Own);
        }
        schema_by_name.insert(name.to_string(), output_row.clone());
        artifacts.typed_insert(id, Arc::new(synthetic_typed_program(output_row)));
        return;
    }

    // ── Body typecheck ─────────────────────────────────────────────
    let body_typed = match typecheck_cxl(
        name,
        name,
        CxlSource {
            text: &config.cxl.source,

            authored: Authored::Program,
        },
        &merged_row,
        AggregateMode::Row,
        span,
        scoped_vars,
    ) {
        Ok(t) => t,
        Err(d) => {
            diags.extend(d);
            return;
        }
    };

    // Post-walk for E308 (unknown / 3-part qualified refs in body).
    let body_walk_cx = CombineWalkContext {
        merged_row: &merged_row,
        combine_name: name,
        err_code: "E308",
        context: "cxl body",
        combine_span: span,
    };
    for stmt in &body_typed.program.statements {
        walk_statement_exprs(stmt, body_walk_cx, diags);
    }

    // E309: combine must produce at least one non-meta emit. Unlike
    // Transform (which has pass-through semantics), combine's output
    // row is defined entirely by its emits — zero emits means zero
    // output fields.
    let mut has_emit = false;
    cxl::ast::for_each_field_emit(&body_typed.program.statements, &mut |_, _| {
        has_emit = true;
    });
    if !has_emit {
        diags.push(combine_e309(name, span));
    }

    let driver_row = artifacts
        .combine_driving
        .get(&id)
        .and_then(|q| artifacts.combine_inputs.get(&id).and_then(|m| m.get(q)))
        .map(|input| input.row.clone());
    let driver_qualifier_for_row = artifacts.combine_driving.get(&id).cloned();
    let inputs_for_row = artifacts.combine_inputs.get(&id);
    let mut output_row = combine_output_row(
        &body_typed,
        driver_row.as_ref(),
        inputs_for_row,
        driver_qualifier_for_row.as_deref(),
        &config.propagate_ck,
        cxl_span,
    );
    // The output holds only the body's emits and the engine's columns: in a
    // composition body every one of them is the body's own.
    if inputs_in_body {
        output_row.enter_body_scope();
        let own: Vec<QualifiedField> = output_row.field_names().cloned().collect();
        for field in own {
            output_row.mark(field, ColumnMark::Own);
        }
    }
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
    let driver_qualifier = artifacts.combine_driving.get(&id).cloned();
    let inputs_for_resolve = artifacts
        .combine_inputs
        .get(&id)
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
        .insert(id, Arc::new(resolved_map));

    artifacts.typed_insert(id, Arc::new(body_typed));
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
) -> HashMap<QualifiedField, (JoinSide, u32)> {
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
        Statement::EmitEach { source, body, .. } | Statement::ExplodeOuter { source, body, .. } => {
            // Surface the source expression plus the expressions from
            // each body statement so qualified-ref walking covers the
            // entire fan-out block, not just the outer driver.
            let mut v = vec![source];
            for inner in body {
                v.extend(statement_exprs(inner));
            }
            v
        }
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
    out: &mut HashMap<QualifiedField, (JoinSide, u32)>,
) {
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
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                walk_expr_for_qualified_refs(Some(element), inputs, driver_qualifier, out);
            }
        }
        Expr::MapLiteral { entries, .. } => {
            for entry in entries {
                if let cxl::ast::MapKey::Computed(key) = &entry.key {
                    walk_expr_for_qualified_refs(Some(key), inputs, driver_qualifier, out);
                }
                walk_expr_for_qualified_refs(Some(&entry.value), inputs, driver_qualifier, out);
            }
        }
        Expr::ArrayComprehension {
            item,
            source,
            predicate,
            ..
        } => {
            walk_expr_for_qualified_refs(Some(source), inputs, driver_qualifier, out);
            walk_expr_for_qualified_refs(Some(item), inputs, driver_qualifier, out);
            if let Some(predicate) = predicate {
                walk_expr_for_qualified_refs(Some(predicate), inputs, driver_qualifier, out);
            }
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
/// - Parse failures → E202
/// - Name-resolution failures → the resolver's own code when every failure
///   names the same one, else E203; see [`resolve_failure_diagnostic`]
/// - Typecheck errors in a composition body that come from a column an
///   input port declares `{ nullable: T }` and the predicate does not
///   handle being empty → the E200 naming the composition, the port and
///   the column, as for every other body program (see
///   [`possibly_empty_errors`])
/// - Typecheck "filter predicate must be type Bool" → E303
/// - Other typecheck errors → E200
fn typecheck_combine_where(
    combine_name: &str,
    where_src: &str,
    merged_row: &Row,
    span: Span,
    scoped_vars: &cxl::resolve::ScopedVarsRegistry,
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
            "E202",
            format!("combine {combine_name:?} where-clause parse error: {msg}"),
            LabeledSpan::primary(span, String::new()),
        )]);
    }

    let context = format!("combine {combine_name:?} where-clause name resolution");
    let site = ResolveSite {
        node: combine_name,
        context: &context,
        span,
    };
    let no_modules = std::collections::HashMap::new();
    // Inside a composition body the predicate is checked as if each column
    // a port declares `{ nullable: T }` could be empty, as every body
    // program is; kept to tell whether that is what fails it.
    let empty_by_port = may_be_empty_by_port(merged_row);
    let recheck = (!empty_by_port.is_empty()).then(|| parse_result.ast.clone());
    let resolved = resolve_against_row(
        site,
        parse_result.ast,
        parse_result.node_count,
        merged_row,
        &no_modules,
        scoped_vars,
    )?;
    let reads = if merged_row.in_body_scope() {
        BodyReads::collect(&resolved)
    } else {
        BodyReads::default()
    };
    let mode = cxl::typecheck::pass::AggregateMode::Row;

    match cxl::typecheck::pass::type_check_with_mode_and_vars(
        resolved,
        merged_row,
        mode.clone(),
        scoped_vars,
    ) {
        Ok(mut typed) => {
            fold_body_config(&mut typed, scoped_vars);
            Ok(typed.with_source(std::sync::Arc::from(wrapped.as_str())))
        }
        Err(type_diags) => {
            let errors: Vec<&cxl::typecheck::TypeDiagnostic> =
                type_diags.iter().filter(|d| !d.is_warning).collect();
            if let Some(program) = &recheck
                && let Some(blamed) = possibly_empty_errors(
                    &Recheck {
                        site,
                        program,
                        node_count: parse_result.node_count,
                        row: merged_row,
                        mode: &mode,
                        module_exports: &no_modules,
                        scoped_vars,
                    },
                    &empty_by_port,
                    &reads,
                    &errors,
                    Authored::Expression {
                        key: "where",
                        text: where_src,
                        at: "filter ".len(),
                    },
                    &wrapped,
                )
            {
                return Err(blamed);
            }
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

/// Immutable per-walk context shared by [`walk_for_unknown_refs`] and
/// [`walk_statement_exprs`]: the merged combine row to resolve qualified
/// refs against, plus the diagnostic-shaping fields (`combine_name`,
/// `err_code`, `context`, `combine_span`) that every emitted E304/E308
/// carries. `Copy` so the recursive walk threads it by value without a
/// per-node borrow; the mutable `diags` accumulator stays a separate
/// parameter because it cannot live in a `Copy` value.
#[derive(Clone, Copy)]
struct CombineWalkContext<'a> {
    merged_row: &'a Row,
    combine_name: &'a str,
    err_code: &'a str,
    context: &'a str,
    combine_span: Span,
}

/// Emit E304/E308 for `QualifiedFieldRef`s that don't match the merged
/// row, or that have 3+ parts (unsupported in combine context).
///
/// Bare unqualified `FieldRef`s are handled by the typechecker itself
/// (which emits a loud `Ambiguous` error for multi-match bare refs in
/// a qualified merged row). This walker only visits the qualified-ref
/// forms.
fn walk_for_unknown_refs(expr: &Expr, cx: CombineWalkContext<'_>, diags: &mut Vec<Diagnostic>) {
    match expr {
        Expr::QualifiedFieldRef { parts, .. } => {
            if parts.len() > 2 {
                let joined: Vec<&str> = parts.iter().map(|s| s.as_ref()).collect();
                diags.push(
                    Diagnostic::error(
                        cx.err_code.to_string(),
                        format!(
                            "combine {:?} {} references {:?} ({} parts); \
                             combine supports only 2-part qualified references \
                             (`qualifier.field`)",
                            cx.combine_name,
                            cx.context,
                            joined.join("."),
                            parts.len()
                        ),
                        LabeledSpan::primary(cx.combine_span, String::new()),
                    )
                    .with_help(
                        "drop the extra path segment, or move multi-record logic into a \
                         Transform before the combine",
                    ),
                );
            } else if parts.len() == 2
                && let Some((caller_type, carried)) = cx
                    .merged_row
                    .undeclared_carried_qualified(&parts[0], &parts[1])
            {
                // The input's composition port carries the column undeclared.
                let diag = undeclared_column_diagnostic(
                    cx.combine_name,
                    &parts[1],
                    carried,
                    caller_type,
                    cx.combine_span,
                );
                if !diags
                    .iter()
                    .any(|seen| seen.code == diag.code && seen.message == diag.message)
                {
                    diags.push(diag);
                }
            } else if parts.len() == 2
                && matches!(
                    cx.merged_row.lookup_qualified(&parts[0], &parts[1]),
                    ColumnLookup::Unknown
                )
            {
                diags.push(
                    Diagnostic::error(
                        cx.err_code.to_string(),
                        format!(
                            "combine {:?} {} references unknown field \
                             `{}.{}`",
                            cx.combine_name, cx.context, parts[0], parts[1]
                        ),
                        LabeledSpan::primary(cx.combine_span, String::new()),
                    )
                    .with_help(
                        "declare the field in the upstream source's `schema:` block, or \
                         fix the qualifier name",
                    ),
                );
            }
        }
        Expr::Binary { lhs, rhs, .. } => {
            walk_for_unknown_refs(lhs, cx, diags);
            walk_for_unknown_refs(rhs, cx, diags);
        }
        Expr::Unary { operand, .. } => walk_for_unknown_refs(operand, cx, diags),
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                walk_for_unknown_refs(element, cx, diags);
            }
        }
        Expr::MapLiteral { entries, .. } => {
            for entry in entries {
                if let cxl::ast::MapKey::Computed(key) = &entry.key {
                    walk_for_unknown_refs(key, cx, diags);
                }
                walk_for_unknown_refs(&entry.value, cx, diags);
            }
        }
        Expr::ArrayComprehension {
            item,
            source,
            predicate,
            ..
        } => {
            walk_for_unknown_refs(source, cx, diags);
            walk_for_unknown_refs(item, cx, diags);
            if let Some(predicate) = predicate {
                walk_for_unknown_refs(predicate, cx, diags);
            }
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            walk_for_unknown_refs(lhs, cx, diags);
            walk_for_unknown_refs(rhs, cx, diags);
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            walk_for_unknown_refs(condition, cx, diags);
            walk_for_unknown_refs(then_branch, cx, diags);
            if let Some(eb) = else_branch {
                walk_for_unknown_refs(eb, cx, diags);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                walk_for_unknown_refs(s, cx, diags);
            }
            for arm in arms {
                walk_for_unknown_refs(&arm.pattern, cx, diags);
                walk_for_unknown_refs(&arm.body, cx, diags);
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            walk_for_unknown_refs(receiver, cx, diags);
            for a in args {
                walk_for_unknown_refs(a, cx, diags);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                walk_for_unknown_refs(a, cx, diags);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            walk_for_unknown_refs(receiver, cx, diags);
            walk_for_unknown_refs(index, cx, diags);
        }
        Expr::Closure { body, .. } => {
            walk_for_unknown_refs(body, cx, diags);
        }
        Expr::FieldRef { .. }
        | Expr::Literal { .. }
        | Expr::PipelineAccess { .. }
        | Expr::VarsAccess { .. }
        | Expr::ConfigAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::RecordAccess { .. }
        | Expr::DocAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }
}

/// Walk every `Expr` inside a `Statement` and apply the unknown-ref
/// check. Used for the body post-walk where statements may be Emit,
/// Let, ExprStmt, Filter, or Trace.
fn walk_statement_exprs(stmt: &Statement, cx: CombineWalkContext<'_>, diags: &mut Vec<Diagnostic>) {
    match stmt {
        Statement::Emit { expr, .. }
        | Statement::Let { expr, .. }
        | Statement::ExprStmt { expr, .. } => {
            walk_for_unknown_refs(expr, cx, diags);
        }
        Statement::Filter { predicate, .. } => {
            walk_for_unknown_refs(predicate, cx, diags);
        }
        Statement::Trace { guard, message, .. } => {
            if let Some(g) = guard {
                walk_for_unknown_refs(g, cx, diags);
            }
            walk_for_unknown_refs(message, cx, diags);
        }
        Statement::UseStmt { .. } | Statement::Distinct { .. } => {}
        Statement::EmitEach { source, body, .. } | Statement::ExplodeOuter { source, body, .. } => {
            walk_for_unknown_refs(source, cx, diags);
            for inner in body {
                walk_statement_exprs(inner, cx, diags);
            }
        }
    }
}

/// Build a combine's output row from its cxl body's emit statements.
///
/// Unlike `propagate_row` (Transform), combine's output is a FRESH
/// closed row — no pass-through of the merged row's qualified fields.
/// Emit LHS names are always unqualified (`QualifiedField::bare`).
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
    // The executor's combine residual-filter eval rejects `EvalResult::EmitMany`
    // with `PipelineError::Internal` — `emit each` fan-out in a combine
    // body has no well-defined join semantic. The walker still recurses
    // so this typed output row stays coherent with every other
    // emit-name collector.
    cxl::ast::for_each_field_emit(&typed.program.statements, &mut |name, expr| {
        let emit_type = typed
            .types
            .get(expr.node_id().0 as usize)
            .and_then(|t| t.clone())
            .unwrap_or(Type::Any);
        out.insert(QualifiedField::bare(name), emit_type);
    });
    // Driver's engine-stamped tail always rides through. `widen_record_to_schema`
    // picks up the values from the driver record by name at runtime.
    //
    // Two driver-side engine-stamp shapes propagate:
    //
    // - `$ck.<field>` source-CK shadow columns. The driver carries the
    //   authoritative correlation identity for the joined output; the
    //   `widen_record_to_schema` name-lookup at runtime picks the
    //   driver's CK value for the output's CK slot.
    // - `$widened` `auto_widen` sidecar absorber. The driver's per-row
    //   undeclared-fields map carries through to the output's
    //   `$widened` slot. Build-side `$widened` is intentionally
    //   dropped — build records' undeclared fields are passthrough
    //   payload that the user did not ask the join to surface; users
    //   who need a build-side unmapped field on the joined output
    //   must declare it in the build source's `schema:` block (which
    //   makes it referenceable via `<build_qualifier>.<field>` in the
    //   combine body) or explicitly emit it via the body's CXL.
    //   Mirrors `propagate_ck: Driver` (the default) which suppresses
    //   build-side user-declared CK by the same rationale.
    if let Some(driver) = driver_row {
        for (qf, ty) in driver.fields() {
            if qf.name.starts_with("$ck.")
                || qf.name.as_ref() == crate::config::pipeline_node::WIDENED_SIDECAR_COLUMN
            {
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

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::FieldMetadata;

    /// Only the engine's own prelude is discounted from a gate's statement
    /// count.
    ///
    /// The prelude is made of `use` declarations, so discounting `use`
    /// statements by kind discounted an author's too — and a gate carrying a
    /// second statement then counted as the one predicate the guard requires.
    /// Counting the prelude by length refuses it, and still admits a gate in a
    /// transform that imports a module.
    #[test]
    fn a_gate_discounts_the_prelude_and_nothing_else() {
        let no_modules = module_use_prelude("emit amount = amount");
        assert_eq!(no_modules.source, "");
        assert_eq!(no_modules.statements, 0);

        let one_module = module_use_prelude("use other.module as m\nemit amount = amount");
        assert_eq!(one_module.source, "use other.module as m\n");
        assert_eq!(one_module.statements, 1);

        let statements = |source: &str| cxl::parser::Parser::parse(source).ast.statements;

        // The prelude's own declaration is the engine's doing.
        assert_eq!(
            authored_gate_statements(
                &statements("use other.module as m\nfilter amount > 1"),
                &one_module
            ),
            1
        );
        // A declaration the prelude did not put there is a second statement.
        assert_eq!(
            authored_gate_statements(
                &statements("use other.module as m\nfilter amount > 1"),
                &no_modules
            ),
            2
        );
        assert_eq!(
            authored_gate_statements(&statements("filter amount > 1\ndistinct"), &no_modules),
            2
        );
    }

    /// The field check decides what a declared schema can decide, and stops
    /// there.
    ///
    /// An open row carries columns declared outside this walk's view, so an
    /// undeclared selector against one is not a mistake the planner has
    /// grounds to refuse. Refusing it anyway would
    /// reject working pipelines; this is also the case that keeps the runtime
    /// miss counter a backstop rather than dead surface.
    #[test]
    fn a_field_request_is_refused_against_a_closed_row_and_left_to_run_time_on_an_open_one() {
        let directives = vec![LogDirective {
            name: "transform.seen".to_string(),
            level: crate::config::LogLevel::Info,
            when: crate::config::LogTiming::PerRecord,
            message: "seen".to_string(),
            fields: Some(vec!["customerId".to_string()]),
            every: Some(1),
            condition: None,
        }];
        let columns: IndexMap<QualifiedField, Type> = [
            (QualifiedField::bare("customer_id"), Type::String),
            (QualifiedField::bare("amount"), Type::Int),
        ]
        .into_iter()
        .collect();
        let cxl_span = cxl::lexer::Span::new(0, 0);

        let closed = Row::closed(columns.clone(), cxl_span);
        let refused = log_field_diagnostics("observe", &directives, &closed, Span::line_only(1));
        assert_eq!(refused.len(), 1, "{refused:?}");
        assert_eq!(refused[0].code, "E374");
        assert!(
            refused[0].message.contains("write `fields: [customer_id]`"),
            "the near miss must be named: {}",
            refused[0].message
        );

        let open = Row::open(columns, cxl_span, cxl::typecheck::row::TailVarId(0));
        assert!(
            log_field_diagnostics("observe", &directives, &open, Span::line_only(1)).is_empty(),
            "a column arriving through an open port is not the planner's to refuse"
        );
    }

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

    /// A call whose body passed the check against its ports' declared types
    /// must not compile when binding against the types the call provides
    /// fails: that is an internal error, never a silent success.
    #[test]
    fn a_bind_that_fails_after_its_check_passed_is_an_internal_error() {
        let failed = Diagnostic::error(
            "E200",
            "CXL type error in \"t\": something".to_string(),
            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
        );
        let warned = Diagnostic::warning(
            "W101",
            "a warning".to_string(),
            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
        );
        let internal = unchecked_instantiation_error(
            "call",
            false,
            std::slice::from_ref(&failed),
            Span::SYNTHETIC,
        )
        .expect("the check passed and the bind failed");
        assert_eq!(internal.code, "E000");
        assert!(
            internal.message.contains("composition \"call\"")
                && internal.message.contains("E200: CXL type error in \"t\""),
            "{}",
            internal.message
        );
        // The check already failed, and said why: nothing to add.
        assert!(unchecked_instantiation_error("call", true, &[failed], Span::SYNTHETIC).is_none());
        assert!(unchecked_instantiation_error("call", false, &[warned], Span::SYNTHETIC).is_none());
    }

    /// A body row's columns are nameable only where marked. Every row the
    /// planner binds inside a composition body marks every column, so no
    /// pipeline reaches an unmarked one; this takes a real body row from a
    /// compiled call, drops its marks, and checks that the planner's own
    /// typecheck refuses its columns rather than admitting them.
    #[test]
    fn a_body_row_without_its_marks_names_no_column() {
        let workspace = tempfile::tempdir().expect("tempdir");
        let files = [
            (
                "pipelines/p.yaml",
                r#"pipeline:
  name: p
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: v, type: int }
        - { name: wt, type: int }
  - type: composition
    name: call
    input: src
    use: ../compositions/c.comp.yaml
    inputs:
      inp: src
  - type: sink
    name: out
    input: call
    config:
      name: out
      type: csv
      path: out.csv
"#,
            ),
            (
                "compositions/c.comp.yaml",
                r#"_compose:
  name: c
  inputs:
    inp:
      schema:
        - { name: v, type: int }
  outputs:
    out: last
  config_schema: {}

nodes:
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit z = v
"#,
            ),
        ];
        for (relative, contents) in files {
            let path = workspace.path().join(relative);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(path, contents).expect("write");
        }
        let config = crate::config::parse_config(files[0].1).expect("parse");
        let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
        let plan = config.compile(&ctx).expect("the call compiles");
        let body = plan.composition_bodies().values().next().expect("one body");
        let marked = &body.body_rows["last"];
        let typecheck = |row: &Row| {
            typecheck_cxl(
                "last",
                "last",
                CxlSource {
                    text: "emit q = v",

                    authored: Authored::Program,
                },
                row,
                AggregateMode::Row,
                Span::SYNTHETIC,
                &cxl::resolve::ScopedVarsRegistry::default(),
            )
        };
        assert!(
            typecheck(marked).is_ok(),
            "the port declares `v`: the marked row names it"
        );
        let mut unmarked = Row::from_parts(
            marked.declared_map().clone(),
            marked.declared_span,
            marked.tail.clone(),
        );
        unmarked.enter_body_scope();
        assert!(
            typecheck(&unmarked).is_err(),
            "a body row that lost its marks must not name `v`"
        );
    }

    /// A row a nested call returns into its enclosing body whose carried
    /// column names a port no row fed is an internal error, not a column
    /// silently left without its mark.
    #[test]
    fn a_contract_check_key_tells_an_open_seed_row_from_a_closed_one() {
        // An open row resolves a name it does not declare to a pass-through
        // column, a closed one to an unknown column: a check that passed
        // against one does not stand for the other.
        let columns: IndexMap<QualifiedField, Type> = [(QualifiedField::bare("v"), Type::Int)]
            .into_iter()
            .collect();
        let span = cxl::lexer::Span::new(0, 0);
        let closed = Row::from_parts(columns.clone(), span, cxl::typecheck::RowTail::Closed);
        let open = Row::from_parts(
            columns,
            span,
            cxl::typecheck::RowTail::Open(cxl::typecheck::row::TailVarId(0)),
        );
        assert!(matches!(
            closed.lookup("w"),
            cxl::typecheck::ColumnLookup::Unknown
        ));
        assert!(matches!(
            open.lookup("w"),
            cxl::typecheck::ColumnLookup::PassThrough(_)
        ));
        assert_ne!(SeedRow::of_row(&closed), SeedRow::of_row(&open));
        assert_eq!(SeedRow::of_row(&closed), SeedRow::of_row(&closed.clone()));
    }

    #[test]
    fn a_carried_column_whose_port_was_not_fed_is_an_internal_error() {
        let origin = Arc::new(PortOrigin {
            call: Arc::from("inner"),
            port: Arc::from("pb"),
            body_file: Arc::from("inner.comp.yaml"),
        });
        let columns: IndexMap<QualifiedField, Type> = [
            (QualifiedField::bare("v"), Type::Int),
            (QualifiedField::bare("w"), Type::Int),
        ]
        .into_iter()
        .collect();
        let mut returned = Row::from_parts(
            columns.clone(),
            cxl::lexer::Span::new(0, 0),
            cxl::typecheck::RowTail::Closed,
        );
        returned.enter_body_scope();
        returned.mark(QualifiedField::bare("v"), ColumnMark::Own);
        returned.mark(
            QualifiedField::bare("w"),
            ColumnMark::Carried(CarriedColumn {
                port: origin,
                access: PortAccess::Declared,
            }),
        );
        // `pa` is fed by a body row; `pb` is bound to a node with no row.
        let mut fed = Row::from_parts(
            columns,
            cxl::lexer::Span::new(0, 0),
            cxl::typecheck::RowTail::Closed,
        );
        fed.enter_body_scope();
        let parent: HashMap<String, Row> = [("up".to_string(), fed)].into_iter().collect();
        let inputs: IndexMap<String, String> = [
            ("pa".to_string(), "up".to_string()),
            ("pb".to_string(), "gone".to_string()),
        ]
        .into_iter()
        .collect();
        let mut diags = Vec::new();
        let out = carry_into_caller_scope(
            returned,
            &CallerScope {
                call: "inner",
                in_body: true,
                call_inputs: &inputs,
                parent_schema_by_name: &parent,
                span: Span::SYNTHETIC,
            },
            &mut diags,
        );
        assert_eq!(diags.len(), 1, "{diags:#?}");
        assert_eq!(diags[0].code, "E000");
        assert!(
            diags[0].message.contains("composition \"inner\"")
                && diags[0].message.contains("`w`")
                && diags[0].message.contains("`pb`"),
            "{}",
            diags[0].message
        );
        assert!(out.is_nameable(&QualifiedField::bare("v")));
        assert!(!out.is_nameable(&QualifiedField::bare("w")));
    }

    /// A call reading a node with no row is told so unless that node's own
    /// bind failed; an error elsewhere in the compile does not stand in
    /// for the upstream's failure.
    #[test]
    fn a_port_reading_a_rowless_node_that_did_not_fail_is_still_refused() {
        let config = crate::config::parse_config(
            "pipeline:\n  name: p\nnodes:\n  - type: transform\n    name: up\n    input: src\n    \
             config:\n      cxl: |\n        emit a = 1\n",
        )
        .expect("parse");
        let body = CompositionFile::parse(
            "_compose:\n  name: c\n  inputs:\n    inp:\n      schema:\n        - { name: a, type: \
             int }\n  outputs:\n    out: last\n  config_schema: {}\nnodes:\n  - type: transform\n    \
             name: last\n    input: inp\n    config:\n      cxl: |\n        emit b = a\n",
            FileId::new(NonZeroU32::new(1).expect("non-zero")),
            PathBuf::from("compositions/c.comp.yaml"),
        )
        .expect("parse body");
        let call_inputs: IndexMap<String, String> = [("inp".to_string(), "up".to_string())]
            .into_iter()
            .collect();
        let parent_schemas: HashMap<String, Row> = HashMap::new();
        let source_nodes: HashSet<String> = HashSet::new();
        let mut diags = vec![Diagnostic::error(
            "E200",
            "an unrelated node failed".to_string(),
            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
        )];
        let mut next_tail_var = 0;
        let mut admission = |failed_nodes: &HashSet<String>, diags: &mut Vec<Diagnostic>| {
            build_input_port_rows(
                &PortAdmission {
                    call: "call",
                    call_inputs: &call_inputs,
                    signature: &body.signature,
                    parent_schemas: &parent_schemas,
                    source_nodes: &source_nodes,
                    scope: &config.nodes,
                    failed_nodes,
                    body_file: Path::new("compositions/c.comp.yaml"),
                    span: Span::SYNTHETIC,
                },
                &mut next_tail_var,
                diags,
            )
        };
        assert!(admission(&HashSet::new(), &mut diags).is_none());
        assert_eq!(diags.len(), 2, "{diags:#?}");
        assert_eq!(diags[1].code, "E102");
        assert!(
            diags[1].message.contains("reads \"up\""),
            "{}",
            diags[1].message
        );

        let failed: HashSet<String> = ["up".to_string()].into_iter().collect();
        let mut diags_after_failure = vec![diags[0].clone()];
        assert!(admission(&failed, &mut diags_after_failure).is_none());
        assert_eq!(diags_after_failure.len(), 1, "{diags_after_failure:#?}");
    }

    #[test]
    fn a_type_checker_detail_is_respelled_as_a_port_schema_spells_types() {
        assert_eq!(
            author_spelling("filter predicate must be type Bool, got Nullable(Bool). Use it."),
            "filter predicate must be type bool, got { nullable: bool }. Use it."
        );
        assert_eq!(
            author_spelling("method 'upper' expects a String receiver, but got Int"),
            "method 'upper' expects a string receiver, but got int"
        );
        assert_eq!(
            author_spelling("Nullable(DateTime) and Nullable(Map)"),
            "{ nullable: date_time } and Nullable(Map)"
        );
        assert_eq!(
            author_spelling("Integer_count Boolean"),
            "Integer_count Boolean"
        );
    }

    #[test]
    fn a_yaml_entry_quotes_the_expression_so_it_pastes() {
        assert_eq!(
            yaml_entry("high", "(v ?? 0) > 6").as_deref(),
            Some("high: '(v ?? 0) > 6'")
        );
        assert_eq!(
            yaml_entry("when", "(s ?? \"\") == 'x'").as_deref(),
            None,
            "both quote styles would need escaping"
        );
        assert_eq!(
            yaml_entry("when", "(s ?? 'a') == x").as_deref(),
            Some("when: \"(s ?? 'a') == x\"")
        );
    }
}
