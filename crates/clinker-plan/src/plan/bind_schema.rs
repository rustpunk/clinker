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
    CombineBody, CopyFrom, CullBody, MatchMode, OnUnmapped, PipelineNode, PropagateCkSpec,
    ReshapeBody, SchemaDecl,
};
use crate::plan::combine::{
    CombineInput, DecomposedPredicate, decompose_predicate, select_driving_input,
};
use crate::plan::composition_body::{BoundBody, CompositionBodyId};
use crate::plan::types::JoinSide;
use crate::yaml::Spanned;
use clinker_core_types::span::{FileId, Span};
use clinker_core_types::{Diagnostic, LabeledSpan};

/// Maximum composition nesting depth.
///
/// Compile-time depth violations emit E107 from `bind_composition`.
/// The runtime composition-body executor reuses this same constant as
/// its recursion-depth guard and emits a distinct E112 on overflow,
/// so log-grep on either code finds exactly one emission site.
pub const MAX_COMPOSITION_DEPTH: u32 = 50;

/// Compile artifacts produced by `bind_schema` — one entry per node name
/// whose CXL body successfully type-checked, plus per-node row types.
#[derive(Debug, Default, Clone)]
pub struct CompileArtifacts {
    pub typed: HashMap<String, Arc<TypedProgram>>,
    /// Per-Combine `where:` predicate typed programs, keyed by combine
    /// node name. The node-keyed `typed` map stores only a Combine's
    /// `cxl:` body; the `where:` predicate is decomposed into
    /// `combine_predicates` and otherwise discarded as a standalone
    /// program. This side-table retains it so a `$doc` access used ONLY
    /// in a predicate (e.g. `l.amount > $doc.Head.cutoff`) is still
    /// reachable by the compile-time `$doc` path walk.
    pub combine_where_typed: HashMap<String, Arc<TypedProgram>>,
    /// Per-Route branch-condition typed programs, keyed by Route node
    /// name — one program per branch, in declaration order. A Route's
    /// node-keyed `typed` entry is an empty body (the node carries no
    /// `cxl:` projection); the branch conditions are otherwise compiled
    /// straight to runtime evaluators and never land in `typed`. This
    /// side-table retains them so a `$doc` access used ONLY in a route
    /// condition (e.g. `amount > $doc.Head.cutoff`) is still reachable by
    /// the compile-time `$doc` path walk and attributed to the Route's
    /// source(s).
    pub route_branch_typed: HashMap<String, Vec<Arc<TypedProgram>>>,
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
    /// Planner-wide column-statistics catalog. The single facility every
    /// node consults for row counts, distinct counts, heavy hitters, and
    /// membership. Plane A row counts are seeded from source file metadata
    /// before combine-strategy selection; Plane B sketch results fold back
    /// in after a run. Combine strategy selection reads its row counts
    /// instead of a per-input cardinality field.
    pub statistics: crate::plan::statistics::StatisticsCatalog,
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
    /// Compiled declarative header/footer synthesis per Envelope node, keyed
    /// by node name. Populated during binding (where the body input `Row` is
    /// in scope) for every Envelope that declares a `config.header:` or
    /// `config.footer:` map; consumed at lowering to attach the spec to
    /// `PlanNode::Envelope.synthesis`. An Envelope declaring neither is absent
    /// from this map and lowers with `synthesis: None`.
    pub envelope_synthesis:
        HashMap<String, Arc<crate::plan::envelope_synthesis::EnvelopeSynthesis>>,
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
    /// User-declared `$pipeline.<key>` / `$source.<key>` / `$row.<key>`
    /// registry. Built once from `PipelineConfig.pipeline.vars` at the
    /// `bind_schema` entry and threaded into every CXL resolve / typecheck
    /// call site. Empty for compositions that don't propagate scoped vars
    ///.
    pub scoped_vars: cxl::resolve::ScopedVarsRegistry,
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
    let mut schema_by_name: HashMap<String, Row> = HashMap::new();
    let mut bind_ctx = BindContext {
        ctx,
        symbol_table,
        use_path_stack: Vec::new(),
        depth: 0,
        enclosing_scope_names: HashSet::new(),
        origin_dir: pipeline_dir.to_path_buf(),
        scoped_vars,
    };
    bind_schema_inner(
        nodes,
        diags,
        &mut bind_ctx,
        &mut artifacts,
        &mut schema_by_name,
    );

    // Cross-Transform duplicate `declares:` (the only path that
    // produced multi-writer for a `(scope, var)` pair) is now caught at
    // config-validation time by `validate_unique_scoped_declarations`,
    // so the previous compile-time E170 multi-writer check is dead.
    validate_init_phase_terminals(nodes, diags);
    validate_read_after_write(nodes, &artifacts, diags);
    validate_post_merge_source_reads(nodes, &artifacts, diags);
    validate_init_phase_isolation(nodes, &artifacts, diags);

    artifacts
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
            let Some(typed) = artifacts.typed.get(typed_key) else {
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
        let mut typed_keys: Vec<String> = Vec::new();
        if spanned.value.reads_scope_vars_in_cxl() && artifacts.typed.contains_key(&reader_name) {
            typed_keys.push(reader_name.clone());
        }
        let span = span_for_node(spanned);
        for key in typed_keys {
            let Some(typed) = artifacts.typed.get(&key) else {
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
        let mut typed_keys: Vec<String> = Vec::new();
        if spanned.value.reads_scope_vars_in_cxl() && artifacts.typed.contains_key(&reader_name) {
            typed_keys.push(reader_name.clone());
        }
        let span = span_for_node(spanned);
        for key in typed_keys {
            let Some(typed) = artifacts.typed.get(&key) else {
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

    let mut registry = ScopedVarsRegistry::default();

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

fn span_for_node(spanned: &Spanned<PipelineNode>) -> Span {
    let line = spanned.referenced.line() as u32;
    if line > 0 {
        Span::line_only(line)
    } else {
        Span::SYNTHETIC
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
        | Expr::VarsAccess { .. } => {
            // $vars.* reads are global static config — no producer, no
            // DAG-descendant rule applies. Skipped from the scope-read
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
    // entry. Two paths collide when they name the *same physical file*, which
    // on a case-insensitive output filesystem (macOS APFS / Windows NTFS
    // default) includes paths differing only in case — `errors.csv` and
    // `Errors.csv` resolve to one file there, so the per-source and
    // pipeline-wide writers would silently overwrite each other. The
    // collision key is therefore folded *conditionally*: only when the actual
    // target filesystem folds case (probed via `case_sensitive_dir`), never
    // unconditionally, so two legitimately-distinct files on case-sensitive
    // Linux are not falsely flagged. First insertion wins; subsequent
    // insertions onto the same key emit E318. The stored value keeps the raw
    // path for the diagnostic message; the value's `.1` is the config label.
    let mut paths: HashMap<String, (String, String)> = HashMap::new();
    if let Some(p) = dlq.path.as_deref() {
        paths.insert(
            crate::config::collision_key(p),
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
                crate::config::collision_key(p),
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
                     the same file on a case-insensitive output filesystem"
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
/// Case is folded conditionally via [`crate::config::collision_key`] — the
/// same primitive the DLQ collision check uses — so two legitimately-distinct
/// files on case-sensitive Linux are not flagged. Paths are compared
/// as-authored; collisions that only emerge after path-template token
/// resolution are left to runtime. DLQ-vs-DLQ collisions stay owned by
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
                crate::config::collision_key(p),
                (p.to_string(), "error_handling.dlq.path".to_string()),
            );
        }
        for (src_name, per) in &dlq.per_source {
            if let Some(p) = per.path.as_deref() {
                paths.insert(
                    crate::config::collision_key(p),
                    (
                        p.to_string(),
                        format!("error_handling.dlq.per_source.{src_name}.path"),
                    ),
                );
            }
        }
    }

    for n in nodes {
        let PipelineNode::Output { config, .. } = &n.value else {
            continue;
        };
        let p = config.output.path.as_str();
        if p.is_empty() {
            continue;
        }
        let name = config.output.name.as_str();
        if let Some((prev_path, prev_label)) = paths.insert(
            crate::config::collision_key(p),
            (p.to_string(), format!("output {name:?}")),
        ) {
            let collide_note = if prev_path == p {
                format!("collides with {prev_label}")
            } else {
                format!(
                    "collides with {prev_label} ({prev_path:?}) — these paths name the \
                     same file on a case-insensitive output filesystem"
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
}

/// Bind a `PipelineNode::Reshape`: validate `partition_by` against the
/// upstream schema, typecheck each rule's `when` / `set` / `overrides`
/// CXL against that schema, enforce the group-identity and synthesis
/// invariants, and publish the audit-widened output row.
///
/// Validation enforced here (compile errors, code E200):
/// - every `partition_by` field exists upstream;
/// - each rule's `when` predicate and every `set` / `overrides` value
///   expression typechecks against the upstream row;
/// - `set` targets must already exist upstream (Reshape mutates, it does
///   not widen the user schema) and must not be a `partition_by` field or
///   an engine-stamped `$`-namespaced column — group identity must
///   survive Reshape;
/// - `copy_from: none` requires every upstream column to appear in
///   `overrides`, so a synthesized row is never silently all-null.
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
    } = node;
    let mut ok = true;

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

    // `order_by` fields must exist upstream.
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
    }

    // Typed programs for every rule fragment, retained so the `$doc`
    // envelope-reference guard below can walk them once the per-rule
    // typechecks have all run. Each is `(label, typed)` where `label` names
    // the rule fragment for the guard's diagnostic.
    let mut typed_fragments: Vec<(String, TypedProgram)> = Vec::new();

    // Per-rule predicate + assignment typechecks and invariants.
    for rule in &config.rules {
        // `when` predicate compiles as a row filter.
        let when_src = format!("filter {}", rule.when.source);
        match typecheck_cxl(
            &format!("{name}:{}:when", rule.name),
            &when_src,
            upstream,
            AggregateMode::Row,
            span,
            scoped_vars,
        ) {
            Ok(typed) => typed_fragments.push((format!("rule {:?} `when`", rule.name), typed)),
            Err(d) => {
                diags.push(d);
                ok = false;
            }
        }

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
                match typecheck_cxl(
                    &format!("{name}:{}:set.{field}", rule.name),
                    &format!("emit {field} = {}", value.source),
                    upstream,
                    AggregateMode::Row,
                    span,
                    scoped_vars,
                ) {
                    Ok(typed) => typed_fragments
                        .push((format!("rule {:?} `mutate.set.{field}`", rule.name), typed)),
                    Err(d) => {
                        diags.push(d);
                        ok = false;
                    }
                }
            }
        }

        if let Some(synth) = &rule.synthesize {
            // `copy_from: none` requires every upstream user column to be
            // supplied by `overrides`, so a synthesized row is never
            // silently all-null.
            if synth.copy_from == CopyFrom::None {
                for (qf, _) in upstream.fields() {
                    let col = qf.name.as_ref();
                    if col.starts_with('$') {
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
            for (field, value) in &synth.overrides {
                match typecheck_cxl(
                    &format!("{name}:{}:override.{field}", rule.name),
                    &format!("emit {field} = {}", value.source),
                    upstream,
                    AggregateMode::Row,
                    span,
                    scoped_vars,
                ) {
                    Ok(typed) => typed_fragments.push((
                        format!("rule {:?} `synthesize.overrides.{field}`", rule.name),
                        typed,
                    )),
                    Err(d) => {
                        diags.push(d);
                        ok = false;
                    }
                }
            }
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
        let programs: Vec<(&str, &TypedProgram)> = typed_fragments
            .iter()
            .map(|(label, typed)| (label.as_str(), typed))
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
    let out = Row::from_parts(declared, upstream.declared_span, upstream.tail.clone());
    schema_by_name.insert(name.to_string(), out.clone());
    artifacts
        .typed
        .insert(name.to_string(), Arc::new(synthetic_typed_program(out)));
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
}

/// Bind a `PipelineNode::Cull`: validate `partition_by` / `order_by`
/// against the upstream schema, typecheck each rule's `drop_group_when`
/// predicate in aggregate context, validate the `removed_to` port name,
/// and publish the (unchanged) upstream row as the node's output.
///
/// Validation enforced here (compile errors, code E200):
/// - every `partition_by` and `order_by` field exists upstream;
/// - at least one removal rule is declared;
/// - each rule's `drop_group_when` predicate typechecks against the
///   upstream row in aggregate context (group-by = `partition_by`), so a
///   group-level predicate like `count(*) > 100` is well-formed;
/// - `removed_to` is non-empty and is not the node's own name (the main
///   output port), so the two output ports are distinguishable.
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
    } = node;
    let mut ok = true;

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

    // `order_by` fields must exist upstream.
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

    // Each rule's `drop_group_when` typechecks in aggregate context over the
    // whole group (group-by = `partition_by`), so an aggregate predicate such
    // as `count(*) > 100` or `sum(if status == 'error' then 1 else 0) > 0` is
    // well-formed. The predicate is wrapped in a single `emit` so it
    // typechecks through the same aggregate-mode path the Aggregate node uses;
    // the bound result is discarded here (the executor recompiles per node at
    // dispatch time, the Route condition-compile seam).
    let agg_mode = AggregateMode::GroupBy {
        group_by_fields: config.partition_by.iter().cloned().collect(),
    };
    for rule in &config.rules {
        let drop_src = format!("emit __drop = {}", rule.drop_group_when.source);
        if let Err(d) = typecheck_cxl(
            &format!("{name}:{}:drop_group_when", rule.name),
            &drop_src,
            upstream,
            agg_mode.clone(),
            span,
            scoped_vars,
        ) {
            diags.push(d);
            ok = false;
        }
    }

    if !ok {
        return;
    }

    // Cull does not widen: both output ports carry the unchanged upstream
    // row. Publish it as the node's output so downstream binding sees the
    // identical schema on the main and `removed_to` ports.
    let out = upstream.clone();
    schema_by_name.insert(name.to_string(), out.clone());
    artifacts
        .typed
        .insert(name.to_string(), Arc::new(synthetic_typed_program(out)));
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

    for spanned in nodes {
        let node = &spanned.value;
        let name = node.name().to_string();
        let span = span_for(spanned);

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
                let schema_decl: &SchemaDecl = &config.schema;
                let (columns, missing) = columns_from_decl(
                    schema_decl,
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
                    let declared = schema_decl.columns.iter().find(|c| c.name == wm.column);
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
                        Some(col)
                            if !matches!(
                                col.ty,
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
                                        ty = col.ty.display_name(),
                                    ),
                                    LabeledSpan::primary(span, String::new()),
                                )
                                .with_help(
                                    "change the column's declared `type:` to `date_time` \
                                     or `date`, or point `watermark.column` at a column \
                                     that already has one of those types",
                                ),
                            );
                        }
                        Some(_) => {}
                    }
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
                    &bind_ctx.scoped_vars,
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
                    &config.cxl.source,
                    &upstream,
                    agg_mode,
                    span,
                    &bind_ctx.scoped_vars,
                ) {
                    Ok(mut typed) => {
                        let out = propagate_aggregate(&name, &config.group_by, &upstream, &typed);
                        schema_by_name.insert(name.clone(), out.clone());
                        typed.output_row = out;
                        artifacts.typed.insert(name, Arc::new(typed));
                    }
                    Err(d) => diags.push(d),
                }
            }
            PipelineNode::Route { header, config } => {
                if let Some(upstream) = upstream_schema(&header.input.value, schema_by_name) {
                    let cloned = upstream.clone();
                    if let Ok(mut empty) = typecheck_cxl(
                        &name,
                        "",
                        &cloned,
                        AggregateMode::Row,
                        span,
                        &bind_ctx.scoped_vars,
                    ) {
                        empty.output_row = cloned.clone();
                        artifacts.typed.insert(name.clone(), Arc::new(empty));
                    }
                    // Type-check each branch condition into its own program
                    // so the compile-time `$doc` walk can reach an envelope
                    // path referenced ONLY in a route predicate. The
                    // condition is a boolean expression; wrapping it in
                    // `filter` mirrors how the runtime compiles it, giving a
                    // `Statement::Filter` whose predicate the `$doc` walk
                    // descends. A condition that fails to type-check is not
                    // diagnosed here — that surfaces through the existing
                    // route-compilation path — so only well-typed branches
                    // contribute paths.
                    let branch_programs: Vec<Arc<TypedProgram>> = config
                        .conditions
                        .values()
                        .filter_map(|cond| {
                            typecheck_cxl(
                                &name,
                                &format!("filter {}", cond.source),
                                &cloned,
                                AggregateMode::Row,
                                span,
                                &bind_ctx.scoped_vars,
                            )
                            .ok()
                            .map(Arc::new)
                        })
                        .collect();
                    if !branch_programs.is_empty() {
                        artifacts
                            .route_branch_typed
                            .insert(name.clone(), branch_programs);
                    }
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
            PipelineNode::Reshape { header, config } => {
                if let Some(upstream) = upstream_schema(&header.input.value, schema_by_name) {
                    let upstream = upstream.clone();
                    bind_reshape(
                        &ReshapeNodeBinding {
                            name: &name,
                            config,
                            upstream: &upstream,
                            span,
                            scoped_vars: &bind_ctx.scoped_vars,
                        },
                        diags,
                        artifacts,
                        schema_by_name,
                    );
                }
            }
            PipelineNode::Cull { header, config } => {
                if let Some(upstream) = upstream_schema(&header.input.value, schema_by_name) {
                    let upstream = upstream.clone();
                    bind_cull(
                        &CullNodeBinding {
                            name: &name,
                            config,
                            upstream: &upstream,
                            span,
                            scoped_vars: &bind_ctx.scoped_vars,
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
                            .insert(name.clone(), Arc::new(synthesis));
                    }
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
                    &CombineNodeBinding {
                        name: &name,
                        header,
                        config,
                        span,
                        scoped_vars: &bind_ctx.scoped_vars,
                    },
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
                    &CompositionBindParams {
                        node_name: &name,
                        use_path: r#use,
                        call_inputs: inputs,
                        call_outputs: outputs,
                        call_config: config,
                        call_resources: resources,
                        span,
                    },
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

/// Immutable call-site inputs to [`bind_composition`]: the node's name and
/// `use:` path, plus the four call-site override maps (`inputs:`,
/// `outputs:`, `config:`, `resources:`) read off the
/// `PipelineNode::Composition` variant. Grouped so the mutable binding
/// accumulators (`diags`, `bind_ctx`, `artifacts`, parent scope) stay
/// visually distinct from the read-only call-site data the function
/// validates against the resolved signature.
struct CompositionBindParams<'a> {
    node_name: &'a str,
    use_path: &'a Path,
    call_inputs: &'a IndexMap<String, String>,
    call_outputs: &'a IndexMap<String, String>,
    call_config: &'a IndexMap<String, serde_json::Value>,
    call_resources: &'a IndexMap<String, serde_json::Value>,
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
) {
    let &CompositionBindParams {
        node_name,
        use_path,
        call_inputs,
        call_outputs,
        call_config,
        call_resources,
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
    let saved_scoped_vars = std::mem::take(&mut bind_ctx.scoped_vars);
    bind_ctx.scoped_vars = build_body_scoped_vars(
        &signature.scoped_vars_schema,
        &saved_scoped_vars,
        &signature.name,
        span,
        diags,
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
    bind_ctx.scoped_vars = saved_scoped_vars;
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
            | PipelineNode::Reshape { header, .. }
            | PipelineNode::Cull { header, .. }
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
    // `name_to_idx` so body-internal port-input references can resolve
    // to a parent-DAG `NodeIndex` and emit
    // `PlanIndexRoot::ParentNode { upstream, .. }`.
    let mut bound_body = BoundBody::empty(resolved_path);
    bound_body.graph = body_graph;
    bound_body.topo_order = body_topo;
    bound_body.name_to_idx = body_name_to_idx;
    bound_body.port_name_to_node_idx = port_name_to_node_idx;
    bound_body.body_rows = body_rows;
    bound_body.node_input_refs = node_input_refs;
    bound_body.route_bodies = route_bodies;
    bound_body.output_port_rows = output_port_rows.clone();
    bound_body.output_port_to_node_idx = output_port_to_node_idx;
    bound_body.input_port_rows = input_port_rows;
    bound_body.nested_body_ids = nested_body_ids;
    bound_body.body_window_configs = body_window_configs;
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

        // Append the parent's engine-stamped tail columns to the
        // body's port declared set. Three engine-stamp shapes
        // propagate from parent to body:
        //
        // - `$ck.<field>` source-CK shadow columns from each parent
        //   source's `correlation_key:` widening.
        // - `$widened` sidecar absorber column from
        //   `on_unmapped: auto_widen`.
        // - `$source.file` per-record source-file lineage stamp
        //   every Source carries.
        //
        // The runtime port-synthetic Source built at body entry adopts
        // every parent column, so the body sees these at runtime;
        // declaring them here keeps the compile-time Row aligned with
        // that runtime shape — without this step the open-tail
        // mechanism would silently drop engine-stamped columns from
        // the body's compile-time view and surface as a schema
        // mismatch when records flow back to the parent (the
        // composition's `output_schema` is derived from the body's
        // terminal Row).
        let mut declared_columns = declared_columns;
        for (qf, ty) in upstream_row.fields() {
            let is_engine_stamped = qf.name.starts_with("$ck.")
                || qf.name.as_ref() == crate::config::pipeline_node::WIDENED_SIDECAR_COLUMN
                || qf.name.as_ref() == crate::config::pipeline_node::SOURCE_FILE_COLUMN
                || qf.name.as_ref() == crate::config::pipeline_node::SOURCE_NAME_COLUMN
                || qf.name.as_ref() == crate::config::pipeline_node::SOURCE_EVENT_TIME_COLUMN;
            if is_engine_stamped && !declared_columns.contains_key(qf) {
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
    decl: &SchemaDecl,
    correlation_key: Option<&crate::config::CorrelationKey>,
    on_unmapped: &OnUnmapped,
) -> (IndexMap<QualifiedField, Type>, Vec<String>) {
    let mut cols: IndexMap<QualifiedField, Type> = decl
        .columns
        .iter()
        .map(|c| (QualifiedField::bare(c.name.as_str()), c.ty.clone()))
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

#[allow(clippy::result_large_err)]
fn typecheck_cxl(
    node_name: &str,
    source: &str,
    schema: &Row,
    mode: AggregateMode,
    span: Span,
    scoped_vars: &cxl::resolve::ScopedVarsRegistry,
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
    let resolved = cxl::resolve::resolve_program_with_modules_and_vars(
        parse_result.ast,
        &field_refs,
        parse_result.node_count,
        &std::collections::HashMap::new(),
        scoped_vars,
    )
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
    cxl::typecheck::pass::type_check_with_mode_and_vars(resolved, schema, mode, scoped_vars)
        .map(|typed| typed.with_source(std::sync::Arc::from(source)))
        .map_err(|diags| {
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
        &format!("{name}:header.{section}"),
        &program,
        body_row,
        AggregateMode::Row,
        span,
        scoped_vars,
    ) {
        Ok(t) => t,
        Err(d) => {
            diags.push(d);
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
        &format!("{name}:footer.{section}"),
        &program,
        body_row,
        AggregateMode::GroupBy {
            group_by_fields: HashSet::new(),
        },
        span,
        scoped_vars,
    ) {
        Ok(t) => t,
        Err(d) => {
            diags.push(d);
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
/// Transform boundaries.
fn propagate_row(upstream: &Row, typed: &TypedProgram) -> Row {
    let mut out = upstream.declared_map().clone();
    cxl::ast::for_each_field_emit(&typed.program.statements, &mut |name, expr| {
        let emit_type = typed
            .types
            .get(expr.node_id().0 as usize)
            .and_then(|t| t.clone())
            .unwrap_or(Type::Any);
        out.insert(QualifiedField::bare(name), emit_type);
    });
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
    // `extract_aggregates` rejects `emit each` inside an aggregate body
    // because finalize emits one record per group and a fan-out's
    // cardinality cannot collapse onto a single group key. The walker
    // here still recurses so the typed `output_row` stays coherent with
    // every other emit-name collector — the rejection diagnostic
    // supersedes whatever schema this produces.
    cxl::ast::for_each_field_emit(&typed.program.statements, &mut |name, expr| {
        let emit_type = typed
            .types
            .get(expr.node_id().0 as usize)
            .and_then(|t| t.clone())
            .unwrap_or(Type::Any);
        out.insert(QualifiedField::bare(name), emit_type);
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
    Row::from_parts(out, upstream.declared_span, upstream.tail.clone())
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
        combine_inputs_entries.insert(
            qualifier.clone(),
            CombineInput {
                upstream_name: Arc::from(upstream_target),
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
        .insert(name.to_string(), Arc::clone(&typed_where));

    // Post-walk for E304 (unknown / 3-part qualified refs in where).
    let e304_before = diags.iter().filter(|d| d.code == "E304").count();
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
    let e304_fired = diags.iter().filter(|d| d.code == "E304").count() > e304_before;

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
        let mut resolved_map: HashMap<QualifiedField, (JoinSide, u32)> = HashMap::new();
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
        scoped_vars,
    ) {
        Ok(t) => t,
        Err(d) => {
            diags.push(d);
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
            "E200",
            format!("combine {combine_name:?} where-clause parse error: {msg}"),
            LabeledSpan::primary(span, String::new()),
        )]);
    }

    let field_refs: Vec<&str> = merged_row
        .field_names()
        .map(|qf| qf.name.as_ref())
        .collect();
    let resolved = match cxl::resolve::resolve_program_with_modules_and_vars(
        parse_result.ast,
        &field_refs,
        parse_result.node_count,
        &std::collections::HashMap::new(),
        scoped_vars,
    ) {
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

    match cxl::typecheck::pass::type_check_with_mode_and_vars(
        resolved,
        merged_row,
        cxl::typecheck::pass::AggregateMode::Row,
        scoped_vars,
    ) {
        Ok(typed) => Ok(typed.with_source(std::sync::Arc::from(wrapped.as_str()))),
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
