//! DIRECT column-level lineage: walk a compiled plan back to its sources.
//!
//! [`column_lineage`] walks `compiled.dag()` in topological order and, for every
//! output (sink) dataset column, resolves the **Source** dataset columns its
//! *value* is derived from — DIRECT lineage in OpenLineage terms — populating the
//! [`ColumnLineageDatasetFacet::fields`] map. Dataset identity comes from
//! [`crate::dataset::dataset_identity`] (#660); value derivation is read off each
//! operator's retained typed/compiled program.
//!
//! Because topo order processes every upstream node first, the working map always
//! stores terminals already resolved back to a Source dataset column — never an
//! intermediate reference — so a rename (`emit full = name`) or a multi-hop chain
//! collapses naturally: the stored `field` is the *source* column, the map key is
//! the *local* output column.
//!
//! ## Subtype model
//!
//! Each input→output link carries one DIRECT [`TransformationSubtype`]. Along a
//! path the dominant subtype wins, ranked `IDENTITY < TRANSFORMATION <
//! AGGREGATION` (identity is transparent; an aggregate anywhere on the path
//! dominates). The OpenLineage per-hop transformation *chain* is collapsed to this
//! single dominant subtype.
//!
//! ## Documented limitations
//!
//! - **INDIRECT** influence lineage (filter / join / group-by / sort) is not
//!   populated here — [`ColumnLineageDatasetFacet::dataset`] is left empty (#662).
//! - **Composition** nodes are treated opaquely: every output column derives from
//!   every composition input column. Precise traversal is a #653 follow-up.
//! - **Envelope** / `$doc` provenance is best-effort same-name passthrough only;
//!   precise envelope lineage is a #653 follow-up.
//! - A `match: collect` combine (no projection body) is resolved coarsely.
//! - Constant and `count(*)` columns (no source input) are omitted from `fields`.
//! - Engine-stamped columns (`$ck.*` / `$meta.*` / `$source.*` / `$widened`) are
//!   skipped, mirroring the default-writer strip.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use petgraph::Direction;
use petgraph::graph::NodeIndex;

use clinker_plan::plan::combine::encode_chain_column;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};
use clinker_plan::plan::{CompiledPlan, JoinSide, PlanNodeId, QualifiedField};
use clinker_record::Schema;
use cxl::ast::{Expr, Program, for_each_field_emit, program_support_into};
use cxl::plan::BindingArg;

use crate::dataset::{DatasetId, dataset_identity};
use crate::openlineage::{
    COLUMN_LINEAGE_FACET_SCHEMA_URL, ColumnLineageDatasetFacet, FieldLineage, InputField, PRODUCER,
    Transformation, TransformationSubtype, TransformationType,
};

/// DIRECT column-level lineage for a compiled plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanColumnLineage {
    /// Source (input) datasets, deduplicated, in first-seen order.
    pub inputs: Vec<DatasetId>,
    /// Each Output (sink) dataset paired with its DIRECT column-lineage facet.
    pub outputs: Vec<OutputColumnLineage>,
}

/// One sink dataset and the DIRECT column lineage of its columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputColumnLineage {
    pub dataset: DatasetId,
    /// `fields` populated with per-column DIRECT lineage; `dataset` (INDIRECT)
    /// left empty for #662.
    pub facet: ColumnLineageDatasetFacet,
}

/// Build the DIRECT column lineage of `compiled`.
///
/// `base_dir` is the workspace root (the directory containing the pipeline YAML),
/// threaded in by the caller exactly as [`dataset_identity`] requires — it is not
/// retained on [`CompiledPlan`].
pub fn column_lineage(compiled: &CompiledPlan, base_dir: &Path) -> PlanColumnLineage {
    let dag = compiled.dag();
    // Per node, per output column, the resolved Source terminals it derives from.
    let mut lineage: HashMap<PlanNodeId, ColumnTerminals> = HashMap::new();
    let mut inputs: Vec<DatasetId> = Vec::new();
    let mut seen_inputs: HashSet<DatasetId> = HashSet::new();
    let mut outputs: Vec<OutputColumnLineage> = Vec::new();

    for &idx in &dag.topo_order {
        let node = &dag.graph[idx];
        let node_id = node.id();

        let cols: ColumnTerminals = match node {
            PlanNode::Source { .. } => {
                let mut cols = ColumnTerminals::new();
                if let Some(ds) = dataset_identity(node, base_dir) {
                    for (col_idx, col) in node.output_schema_in(dag).columns().iter().enumerate() {
                        if node.output_schema_in(dag).field_metadata(col_idx).is_some() {
                            continue;
                        }
                        let mut terms = TermMap::new();
                        terms.insert(
                            Terminal::new(&ds.namespace, &ds.name, col),
                            Subtype::Identity,
                        );
                        cols.insert(col.to_string(), terms);
                    }
                    if seen_inputs.insert(ds.clone()) {
                        inputs.push(ds);
                    }
                }
                cols
            }

            PlanNode::Transform { resolved, .. } => {
                let up = single_upstream(dag, idx);
                // Explicit field emits (recursing through `emit each` fan-out).
                let mut emitted: HashMap<String, TermMap> = HashMap::new();
                if let Some(payload) = resolved {
                    for_each_field_emit(&payload.typed.program.statements, &mut |name, expr| {
                        if name.starts_with('$') {
                            return;
                        }
                        let local = field_ref_subtype(expr);
                        let mut reads = HashSet::new();
                        expr.support_into(&mut reads);
                        let slot = emitted.entry(name.to_string()).or_default();
                        for r in &reads {
                            merge_terminals(slot, upstream_col(&lineage, up, r), local);
                        }
                    });
                }
                // Output schema = explicit emits + open-row passthrough of the rest.
                let mut cols = ColumnTerminals::new();
                for_each_output_col(node, dag, |col| {
                    if let Some(terms) = emitted.get(col) {
                        cols.insert_nonempty(col, terms.clone());
                    } else {
                        cols.insert_nonempty(col, passthrough(&lineage, up, col));
                    }
                });
                cols
            }

            PlanNode::Aggregation { compiled, .. } => {
                let up = single_upstream(dag, idx);
                let input_schema = node.expected_input_schema_in(dag).map(|s| s.as_ref());
                let mut cols = ColumnTerminals::new();
                for emit in &compiled.emits {
                    if emit.output_name.starts_with('$') {
                        continue;
                    }
                    let mut terms = TermMap::new();
                    for (col, st) in aggregate_emit_sources(&emit.residual, compiled, input_schema)
                    {
                        merge_terminals(&mut terms, upstream_col(&lineage, up, &col), st);
                    }
                    cols.insert_nonempty(&emit.output_name, terms);
                }
                cols
            }

            PlanNode::Reshape { compiled_rules, .. } => {
                let up = single_upstream(dag, idx);
                // Base: every column is a conditional passthrough (non-matching
                // rows keep the original value).
                let mut cols = ColumnTerminals::new();
                for_each_output_col(node, dag, |col| {
                    cols.insert_nonempty(col, passthrough(&lineage, up, col));
                });
                // Then add each rule's mutate-set and synth-override derivations.
                for rule in compiled_rules.iter() {
                    let set_progs = rule
                        .set
                        .iter()
                        .chain(rule.synth.iter().flat_map(|synth| synth.overrides.iter()));
                    for (field, prog) in set_progs {
                        if field.starts_with('$') {
                            continue;
                        }
                        let local = program_field_ref_subtype(&prog.program);
                        let mut reads = HashSet::new();
                        program_support_into(&prog.program, &mut reads);
                        let slot = cols.0.entry(field.to_string()).or_default();
                        for r in &reads {
                            merge_terminals(slot, upstream_col(&lineage, up, r), local);
                        }
                    }
                }
                cols.prune_empty();
                cols
            }

            PlanNode::Combine {
                typed,
                resolved_column_map,
                driving_upstream,
                decomposed_from,
                ..
            } => {
                let map: &HashMap<QualifiedField, (JoinSide, u32)> = resolved_column_map;
                let bare = bare_side_index(map);
                let probe_idx = *driving_upstream;
                // The build side is the incoming neighbor that is not the probe.
                // For a self-join (`input: {a: src, b: src}`) both sides are the same
                // physical predecessor, so fall back to the probe node — the side
                // distinction is logical (via `resolved_column_map`'s `JoinSide`), not
                // a distinct node.
                let build_idx = dag
                    .graph
                    .neighbors_directed(idx, Direction::Incoming)
                    .find(|n| Some(*n) != probe_idx)
                    .or(probe_idx);
                let probe =
                    probe_idx.map(|n| (dag.graph[n].id(), dag.graph[n].output_schema_in(dag)));
                let build =
                    build_idx.map(|n| (dag.graph[n].id(), dag.graph[n].output_schema_in(dag)));

                let mut cols = ColumnTerminals::new();
                match typed {
                    Some(tp) => {
                        for_each_field_emit(&tp.program.statements, &mut |name, expr| {
                            if name.starts_with('$') {
                                return;
                            }
                            let local = field_ref_subtype(expr);
                            let mut refs = Vec::new();
                            collect_combine_refs(expr, &mut refs);
                            let mut terms = TermMap::new();
                            for qf in &refs {
                                let Some((side, cidx)) = resolve_side(qf, map, &bare) else {
                                    continue;
                                };
                                let resolved = match side {
                                    JoinSide::Probe => probe.as_ref(),
                                    JoinSide::Build => build.as_ref(),
                                };
                                if let Some((up_id, schema)) = resolved
                                    && let Some(col) = schema.column_name(cidx as usize)
                                {
                                    merge_terminals(
                                        &mut terms,
                                        lineage.get(up_id).and_then(|m| m.0.get(col)),
                                        local,
                                    );
                                }
                            }
                            cols.insert_nonempty(name, terms);
                        });
                    }
                    // Intermediate step of an N-ary decomposition: a body-less inner
                    // join that carries every input column forward by IDENTITY. Its
                    // output columns are the flat-encoded `__{qualifier}__{field}`
                    // names, and `resolved_column_map` is a 1:1 cover of them keyed by
                    // the original `(qualifier, field)`. Keying the lineage entry by
                    // those encoded names lets the *final* decomposed step resolve its
                    // probe references straight through this map.
                    None if decomposed_from.is_some() => {
                        for (qf, (side, cidx)) in map.iter() {
                            let resolved = match side {
                                JoinSide::Probe => probe.as_ref(),
                                JoinSide::Build => build.as_ref(),
                            };
                            if let Some((up_id, schema)) = resolved
                                && let Some(up_col) = schema.column_name(*cidx as usize)
                            {
                                let qualifier =
                                    qf.qualifier.as_deref().unwrap_or(qf.name.as_ref());
                                let out_name = encode_chain_column(qualifier, qf.name.as_ref());
                                let mut terms = TermMap::new();
                                merge_terminals(
                                    &mut terms,
                                    lineage.get(up_id).and_then(|m| m.0.get(up_col)),
                                    Subtype::Identity,
                                );
                                cols.insert_nonempty(&out_name, terms);
                            }
                        }
                    }
                    None => {
                        // `match: collect`: probe columns pass through by name; any
                        // remaining (collected) column derives coarsely from every
                        // build column.
                        let probe_names: HashSet<&str> = probe
                            .as_ref()
                            .map(|(_, s)| s.columns().iter().map(|c| c.as_ref()).collect())
                            .unwrap_or_default();
                        for_each_output_col(node, dag, |col| {
                            let mut terms = TermMap::new();
                            if probe_names.contains(col) {
                                if let Some((up_id, _)) = &probe {
                                    merge_terminals(
                                        &mut terms,
                                        lineage.get(up_id).and_then(|m| m.0.get(col)),
                                        Subtype::Identity,
                                    );
                                }
                            } else if let Some((up_id, _)) = &build
                                && let Some(m) = lineage.get(up_id)
                            {
                                for up_terms in m.0.values() {
                                    merge_terminals(
                                        &mut terms,
                                        Some(up_terms),
                                        Subtype::Transformation,
                                    );
                                }
                            }
                            cols.insert_nonempty(col, terms);
                        });
                    }
                }
                cols
            }

            PlanNode::Composition { .. } => {
                // Coarse: every output column derives from every input column.
                let mut all = TermMap::new();
                for up in upstream_ids(dag, idx) {
                    if let Some(m) = lineage.get(&up) {
                        for up_terms in m.0.values() {
                            merge_terminals(&mut all, Some(up_terms), Subtype::Transformation);
                        }
                    }
                }
                let mut cols = ColumnTerminals::new();
                for_each_output_col(node, dag, |col| {
                    cols.insert_nonempty(col, all.clone());
                });
                cols
            }

            PlanNode::Merge { .. } | PlanNode::Envelope { .. } => {
                // Row-preserving over N inputs of identical shape: union the
                // same-named column across every upstream.
                let ups = upstream_ids(dag, idx);
                let mut cols = ColumnTerminals::new();
                for_each_output_col(node, dag, |col| {
                    let mut terms = TermMap::new();
                    for up in &ups {
                        merge_terminals(
                            &mut terms,
                            lineage.get(up).and_then(|m| m.0.get(col)),
                            Subtype::Identity,
                        );
                    }
                    cols.insert_nonempty(col, terms);
                });
                cols
            }

            PlanNode::Output { .. } => {
                let up = single_upstream(dag, idx);
                let mut cols = ColumnTerminals::new();
                for_each_output_col(node, dag, |col| {
                    cols.insert_nonempty(col, passthrough(&lineage, up, col));
                });
                if let Some(ds) = dataset_identity(node, base_dir) {
                    record_output(&mut outputs, ds, &cols);
                }
                cols
            }

            // Row-preserving single-input passthrough. Enumerated rather than a
            // catch-all so a future PlanNode variant forces a deliberate lineage
            // rule instead of silently defaulting to passthrough.
            PlanNode::Route { .. }
            | PlanNode::Sort { .. }
            | PlanNode::Cull { .. }
            | PlanNode::CorrelationCommit { .. } => {
                let up = single_upstream(dag, idx);
                let mut cols = ColumnTerminals::new();
                for_each_output_col(node, dag, |col| {
                    cols.insert_nonempty(col, passthrough(&lineage, up, col));
                });
                cols
            }
        };

        lineage.insert(node_id, cols);
    }

    PlanColumnLineage { inputs, outputs }
}

// ---------------------------------------------------------------------------
// Working types
// ---------------------------------------------------------------------------

/// DIRECT subtype, ordered by dominance for path composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Subtype {
    Identity,
    Transformation,
    Aggregation,
}

impl Subtype {
    fn to_openlineage(self) -> TransformationSubtype {
        match self {
            Subtype::Identity => TransformationSubtype::Identity,
            Subtype::Transformation => TransformationSubtype::Transformation,
            Subtype::Aggregation => TransformationSubtype::Aggregation,
        }
    }
}

/// One Source dataset column: the terminal of a DIRECT lineage path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Terminal {
    namespace: String,
    name: String,
    field: String,
}

impl Terminal {
    fn new(namespace: &str, name: &str, field: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            name: name.to_string(),
            field: field.to_string(),
        }
    }
}

/// The Source terminals one column derives from, keyed for dedup; `BTreeMap`
/// keeps the emitted `inputFields` order deterministic.
type TermMap = BTreeMap<Terminal, Subtype>;

/// All of one node's output columns mapped to their resolved terminals.
#[derive(Default)]
struct ColumnTerminals(HashMap<String, TermMap>);

impl ColumnTerminals {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn insert(&mut self, col: String, terms: TermMap) {
        self.0.insert(col, terms);
    }

    /// Insert only when the column actually resolves to a source — constants and
    /// `count(*)` (empty term sets) are omitted.
    fn insert_nonempty(&mut self, col: &str, terms: TermMap) {
        if !terms.is_empty() {
            self.0.insert(col.to_string(), terms);
        }
    }

    fn prune_empty(&mut self) {
        self.0.retain(|_, terms| !terms.is_empty());
    }
}

// ---------------------------------------------------------------------------
// Graph traversal helpers
// ---------------------------------------------------------------------------

/// The single upstream node id of a single-input node, if any.
fn single_upstream(dag: &ExecutionPlanDag, idx: NodeIndex) -> Option<PlanNodeId> {
    dag.graph
        .neighbors_directed(idx, Direction::Incoming)
        .next()
        .map(|n| dag.graph[n].id())
}

/// Every upstream node id (incoming neighbors).
fn upstream_ids(dag: &ExecutionPlanDag, idx: NodeIndex) -> Vec<PlanNodeId> {
    dag.graph
        .neighbors_directed(idx, Direction::Incoming)
        .map(|n| dag.graph[n].id())
        .collect()
}

/// Visit each non-engine-stamped output column name of `node` in schema order.
fn for_each_output_col(node: &PlanNode, dag: &ExecutionPlanDag, mut visit: impl FnMut(&str)) {
    let schema = node.output_schema_in(dag);
    for (i, col) in schema.columns().iter().enumerate() {
        if schema.field_metadata(i).is_some() {
            continue;
        }
        visit(col);
    }
}

/// The terminals of `col` on `up`, if present.
fn upstream_col<'a>(
    lineage: &'a HashMap<PlanNodeId, ColumnTerminals>,
    up: Option<PlanNodeId>,
    col: &str,
) -> Option<&'a TermMap> {
    lineage.get(&up?).and_then(|m| m.0.get(col))
}

/// IDENTITY passthrough of `col` from `up` (subtype composes transparently).
fn passthrough(
    lineage: &HashMap<PlanNodeId, ColumnTerminals>,
    up: Option<PlanNodeId>,
    col: &str,
) -> TermMap {
    upstream_col(lineage, up, col).cloned().unwrap_or_default()
}

/// Merge `upstream` terminals into `target`, composing each with `local`
/// (dominant-subtype = rank-max) and keeping the strongest per terminal.
fn merge_terminals(target: &mut TermMap, upstream: Option<&TermMap>, local: Subtype) {
    let Some(up) = upstream else { return };
    for (terminal, &up_subtype) in up {
        let composed = local.max(up_subtype);
        target
            .entry(terminal.clone())
            .and_modify(|existing| *existing = (*existing).max(composed))
            .or_insert(composed);
    }
}

// ---------------------------------------------------------------------------
// Expression / program inspection
// ---------------------------------------------------------------------------

/// IDENTITY when `expr` is a single (bare or qualified) field reference — a
/// copy/rename — otherwise TRANSFORMATION.
fn field_ref_subtype(expr: &Expr) -> Subtype {
    if matches!(expr, Expr::FieldRef { .. } | Expr::QualifiedFieldRef { .. }) {
        Subtype::Identity
    } else {
        Subtype::Transformation
    }
}

/// IDENTITY when `program` is a single `emit <field> = <bare field>` copy.
fn program_field_ref_subtype(program: &Program) -> Subtype {
    let mut count = 0usize;
    let mut subtype = Subtype::Transformation;
    for_each_field_emit(&program.statements, &mut |_, expr| {
        count += 1;
        subtype = field_ref_subtype(expr);
    });
    if count == 1 {
        subtype
    } else {
        Subtype::Transformation
    }
}

/// The input column(s) and DIRECT subtype each contributing to one aggregate
/// output column, read off the post-extraction `residual`.
fn aggregate_emit_sources(
    residual: &Expr,
    compiled: &cxl::plan::CompiledAggregate,
    input_schema: Option<&Schema>,
) -> Vec<(String, Subtype)> {
    // A bare `GroupKey`/`AggSlot` is a pure passthrough/aggregate; once wrapped in
    // any expression the group-key contribution becomes a transformation.
    let bare_leaf = matches!(residual, Expr::GroupKey { .. } | Expr::AggSlot { .. });
    let mut leaves = Vec::new();
    collect_agg_leaves(residual, &mut leaves);

    let mut out = Vec::new();
    for leaf in leaves {
        match leaf {
            AggLeaf::Group(slot) => {
                if let Some(name) = compiled.group_by_fields.get(slot as usize) {
                    let st = if bare_leaf {
                        Subtype::Identity
                    } else {
                        Subtype::Transformation
                    };
                    out.push((name.clone(), st));
                }
            }
            AggLeaf::Agg(slot) => {
                if let Some(binding) = compiled.bindings.get(slot as usize) {
                    for name in binding_arg_input_names(&binding.arg, input_schema) {
                        out.push((name, Subtype::Aggregation));
                    }
                }
            }
            AggLeaf::Field(name) => {
                let st = if bare_leaf {
                    Subtype::Identity
                } else {
                    Subtype::Transformation
                };
                out.push((name.to_string(), st));
            }
        }
    }
    out
}

/// Leaf of an aggregate emit residual that carries column provenance.
enum AggLeaf<'a> {
    /// `GroupKey { slot }` — passthrough of a group-by column.
    Group(u32),
    /// `AggSlot { slot }` — an accumulator over input column(s).
    Agg(u32),
    /// A residual bare field reference (defensive; post-extraction these are
    /// normally rewritten to `Group`/`Agg`).
    Field(&'a str),
}

fn collect_agg_leaves<'a>(expr: &'a Expr, out: &mut Vec<AggLeaf<'a>>) {
    match expr {
        Expr::GroupKey { slot, .. } => out.push(AggLeaf::Group(*slot)),
        Expr::AggSlot { slot, .. } => out.push(AggLeaf::Agg(*slot)),
        Expr::FieldRef { name, .. } => {
            if !name.starts_with('$') {
                out.push(AggLeaf::Field(name));
            }
        }
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            collect_agg_leaves(lhs, out);
            collect_agg_leaves(rhs, out);
        }
        Expr::Unary { operand, .. } => collect_agg_leaves(operand, out),
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_agg_leaves(condition, out);
            collect_agg_leaves(then_branch, out);
            if let Some(eb) = else_branch {
                collect_agg_leaves(eb, out);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                collect_agg_leaves(s, out);
            }
            for arm in arms {
                collect_agg_leaves(&arm.pattern, out);
                collect_agg_leaves(&arm.body, out);
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            collect_agg_leaves(receiver, out);
            for a in args {
                collect_agg_leaves(a, out);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                collect_agg_leaves(a, out);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            collect_agg_leaves(receiver, out);
            collect_agg_leaves(index, out);
        }
        Expr::Closure { body, .. } => collect_agg_leaves(body, out),
        _ => {}
    }
}

/// The input column name(s) a single aggregate argument reads.
fn binding_arg_input_names(arg: &BindingArg, schema: Option<&Schema>) -> Vec<String> {
    match arg {
        BindingArg::Field(idx) => schema
            .and_then(|s| s.column_name(*idx as usize))
            .map(|n| vec![n.to_string()])
            .unwrap_or_default(),
        BindingArg::Wildcard => Vec::new(),
        BindingArg::Expr(e) => {
            let mut reads = HashSet::new();
            e.support_into(&mut reads);
            reads.into_iter().collect()
        }
        BindingArg::Pair(a, b) => {
            let mut v = binding_arg_input_names(a, schema);
            v.extend(binding_arg_input_names(b, schema));
            v
        }
    }
}

// ---------------------------------------------------------------------------
// Combine reference resolution
// ---------------------------------------------------------------------------

/// Collect every field reference (as a [`QualifiedField`]) read by a combine
/// body expression, skipping system (`$`) namespaces.
fn collect_combine_refs(expr: &Expr, out: &mut Vec<QualifiedField>) {
    match expr {
        Expr::FieldRef { name, .. } => {
            if !name.starts_with('$') {
                out.push(QualifiedField::bare(name.as_ref()));
            }
        }
        Expr::QualifiedFieldRef { parts, .. } => {
            if let Some(first) = parts.first() {
                if first.starts_with('$') {
                    return;
                }
                if parts.len() >= 2 {
                    out.push(QualifiedField::qualified(
                        parts[0].as_ref(),
                        parts[1].as_ref(),
                    ));
                } else {
                    out.push(QualifiedField::bare(parts[0].as_ref()));
                }
            }
        }
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            collect_combine_refs(lhs, out);
            collect_combine_refs(rhs, out);
        }
        Expr::Unary { operand, .. } => collect_combine_refs(operand, out),
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_combine_refs(condition, out);
            collect_combine_refs(then_branch, out);
            if let Some(eb) = else_branch {
                collect_combine_refs(eb, out);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                collect_combine_refs(s, out);
            }
            for arm in arms {
                collect_combine_refs(&arm.pattern, out);
                collect_combine_refs(&arm.body, out);
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            collect_combine_refs(receiver, out);
            for a in args {
                collect_combine_refs(a, out);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                collect_combine_refs(a, out);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            collect_combine_refs(receiver, out);
            collect_combine_refs(index, out);
        }
        Expr::Closure { body, .. } => collect_combine_refs(body, out),
        _ => {}
    }
}

/// Map a bare field name to its `(side, index)` when it is unambiguous across the
/// combine's inputs — mirroring the executor's `bare_to_side` derivation.
fn bare_side_index(
    map: &HashMap<QualifiedField, (JoinSide, u32)>,
) -> HashMap<String, (JoinSide, u32)> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for key in map.keys() {
        *counts.entry(key.name.as_ref()).or_default() += 1;
    }
    map.iter()
        .filter(|(key, _)| counts.get(key.name.as_ref()) == Some(&1))
        .map(|(key, v)| (key.name.to_string(), *v))
        .collect()
}

/// Resolve a combine body reference to `(side, column-index)`: qualified refs hit
/// the pre-resolved map; bare refs use the unambiguous-name index.
fn resolve_side(
    qf: &QualifiedField,
    map: &HashMap<QualifiedField, (JoinSide, u32)>,
    bare: &HashMap<String, (JoinSide, u32)>,
) -> Option<(JoinSide, u32)> {
    if qf.qualifier.is_some() {
        map.get(qf).copied()
    } else {
        bare.get(qf.name.as_ref()).copied()
    }
}

// ---------------------------------------------------------------------------
// Output facet assembly
// ---------------------------------------------------------------------------

/// Append (or merge into) the output dataset's column-lineage facet.
fn record_output(
    outputs: &mut Vec<OutputColumnLineage>,
    dataset: DatasetId,
    cols: &ColumnTerminals,
) {
    let fields: BTreeMap<String, FieldLineage> = cols
        .0
        .iter()
        .filter(|(_, terms)| !terms.is_empty())
        .map(|(col, terms)| {
            (
                col.clone(),
                FieldLineage {
                    input_fields: input_fields(terms),
                },
            )
        })
        .collect();

    if let Some(existing) = outputs.iter_mut().find(|o| o.dataset == dataset) {
        // Two Output nodes naming one dataset: merge their column maps.
        for (col, fl) in fields {
            existing.facet.fields.entry(col).or_insert(fl);
        }
        return;
    }
    outputs.push(OutputColumnLineage {
        dataset,
        facet: ColumnLineageDatasetFacet {
            producer: PRODUCER.to_string(),
            schema_url: COLUMN_LINEAGE_FACET_SCHEMA_URL.to_string(),
            fields,
            dataset: Vec::new(),
        },
    });
}

/// One terminal set → its OpenLineage `inputFields`, in `BTreeMap` (namespace,
/// name, field) order.
fn input_fields(terms: &TermMap) -> Vec<InputField> {
    terms
        .iter()
        .map(|(terminal, subtype)| InputField {
            namespace: terminal.namespace.clone(),
            name: terminal.name.clone(),
            field: terminal.field.clone(),
            transformations: vec![Transformation {
                transformation_type: TransformationType::Direct,
                subtype: Some(subtype.to_openlineage()),
                description: None,
                masking: None,
            }],
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use clinker_plan::CompileContext;
    use clinker_plan::config::parse_config;

    /// Compile inline YAML to a `CompiledPlan` (the public test path).
    fn compile(yaml: &str) -> CompiledPlan {
        parse_config(yaml)
            .expect("parse_config")
            .compile(&CompileContext::default())
            .expect("compile should succeed")
    }

    /// Build for a `/w` workspace root, so a source `path: data/x.csv` resolves
    /// to the deterministic terminal name `/w/data/x.csv`.
    fn lineage_of(yaml: &str) -> PlanColumnLineage {
        column_lineage(&compile(yaml), Path::new("/w"))
    }

    /// One DIRECT `file:`-namespaced input field.
    fn direct(name: &str, field: &str, subtype: TransformationSubtype) -> InputField {
        InputField {
            namespace: "file".to_string(),
            name: name.to_string(),
            field: field.to_string(),
            transformations: vec![Transformation {
                transformation_type: TransformationType::Direct,
                subtype: Some(subtype),
                description: None,
                masking: None,
            }],
        }
    }

    fn only_output(lineage: &PlanColumnLineage) -> &OutputColumnLineage {
        assert_eq!(
            lineage.outputs.len(),
            1,
            "expected exactly one output dataset"
        );
        &lineage.outputs[0]
    }

    fn assert_field(fields: &BTreeMap<String, FieldLineage>, col: &str, expected: &[InputField]) {
        let actual = fields
            .get(col)
            .unwrap_or_else(|| panic!("column {col:?} missing from lineage"));
        assert_eq!(actual.input_fields, expected, "lineage for column {col:?}");
    }

    // -- subtype composition --------------------------------------------------

    #[test]
    fn subtype_dominance_is_identity_lt_transformation_lt_aggregation() {
        assert!(Subtype::Identity < Subtype::Transformation);
        assert!(Subtype::Transformation < Subtype::Aggregation);
        assert_eq!(
            Subtype::Identity.max(Subtype::Aggregation),
            Subtype::Aggregation
        );
        assert_eq!(
            Subtype::Transformation.max(Subtype::Identity),
            Subtype::Transformation
        );
    }

    // -- Transform projection -------------------------------------------------

    #[test]
    fn transform_projection_maps_copy_rename_and_computed_columns() {
        let yaml = r#"
pipeline: { name: t }
nodes:
  - type: source
    name: emp
    config:
      name: emp
      type: csv
      path: data/emp.csv
      options: { has_header: true }
      schema:
        - { name: id, type: string }
        - { name: name, type: string }
        - { name: salary, type: string }
  - type: transform
    name: proj
    input: emp
    config:
      cxl: |
        emit id = id
        emit name = name
        emit salary = salary
        emit full = name
        emit tier = if salary.to_int() >= 90000 then "senior" else "junior"
  - type: output
    name: out
    input: proj
    config: { name: out, type: csv, path: out/t.csv }
"#;
        let lineage = lineage_of(yaml);
        let src = "/w/data/emp.csv";
        let fields = &only_output(&lineage).facet.fields;

        use TransformationSubtype::{Identity, Transformation};
        assert_field(fields, "id", &[direct(src, "id", Identity)]);
        assert_field(fields, "name", &[direct(src, "name", Identity)]);
        assert_field(fields, "salary", &[direct(src, "salary", Identity)]);
        // Rename carries the *source* column name, not the local output name.
        assert_field(fields, "full", &[direct(src, "name", Identity)]);
        // Computed column reads salary -> TRANSFORMATION.
        assert_field(fields, "tier", &[direct(src, "salary", Transformation)]);
        assert_eq!(fields.len(), 5, "no extra/omitted columns");

        // The lone source dataset is reported as an input.
        assert_eq!(
            lineage.inputs,
            vec![DatasetId {
                namespace: "file".to_string(),
                name: src.to_string(),
            }]
        );
    }

    #[test]
    fn transform_constant_column_is_omitted() {
        let yaml = r#"
pipeline: { name: k }
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: data/s.csv
      options: { has_header: true }
      schema:
        - { name: a, type: string }
  - type: transform
    name: t
    input: s
    config:
      cxl: |
        emit a = a
        emit k = "literal"
  - type: output
    name: out
    input: t
    config: { name: out, type: csv, path: out/k.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        assert!(fields.contains_key("a"));
        assert!(!fields.contains_key("k"), "constant column must be omitted");
    }

    #[test]
    fn multi_hop_rename_resolves_to_original_source_column() {
        let yaml = r#"
pipeline: { name: chain }
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: data/s.csv
      options: { has_header: true }
      schema:
        - { name: raw, type: string }
  - type: transform
    name: t1
    input: s
    config:
      cxl: |
        emit mid = raw
  - type: transform
    name: t2
    input: t1
    config:
      cxl: |
        emit final = mid
  - type: output
    name: out
    input: t2
    config: { name: out, type: csv, path: out/chain.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        assert_field(
            fields,
            "final",
            &[direct(
                "/w/data/s.csv",
                "raw",
                TransformationSubtype::Identity,
            )],
        );
    }

    // -- Aggregation ----------------------------------------------------------

    #[test]
    fn aggregation_marks_group_key_identity_and_sum_aggregation() {
        let yaml = r#"
pipeline: { name: a }
nodes:
  - type: source
    name: sales
    config:
      name: sales
      type: csv
      path: data/sales.csv
      options: { has_header: true }
      schema:
        - { name: region, type: string }
        - { name: amount, type: string }
  - type: aggregate
    name: agg
    input: sales
    config:
      group_by: [region]
      cxl: |
        emit region = region
        emit total = sum(amount.to_int())
        emit n = count(*)
  - type: output
    name: out
    input: agg
    config: { name: out, type: csv, path: out/a.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        let src = "/w/data/sales.csv";
        assert_field(
            fields,
            "region",
            &[direct(src, "region", TransformationSubtype::Identity)],
        );
        assert_field(
            fields,
            "total",
            &[direct(src, "amount", TransformationSubtype::Aggregation)],
        );
        assert!(
            !fields.contains_key("n"),
            "count(*) has no DIRECT source column"
        );
    }

    // -- Reshape --------------------------------------------------------------

    #[test]
    fn reshape_mutate_unions_passthrough_and_mutation_source() {
        let yaml = r#"
pipeline: { name: r }
nodes:
  - type: source
    name: plans
    config:
      name: plans
      type: csv
      path: data/plans.csv
      options: { has_header: true }
      schema:
        - { name: employee_id, type: string }
        - { name: plan_start, type: int }
        - { name: plan_end, type: int }
        - { name: status, type: string }
  - type: reshape
    name: backfill
    input: plans
    config:
      partition_by: [employee_id]
      order_by:
        - { field: plan_start, order: asc }
      rules:
        - name: split
          when: "plan_start - plan_end > 365"
          mutate:
            set:
              plan_end: "plan_start"
  - type: output
    name: out
    input: backfill
    config: { name: out, type: csv, path: out/r.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        let src = "/w/data/plans.csv";
        use TransformationSubtype::Identity;
        assert_field(
            fields,
            "employee_id",
            &[direct(src, "employee_id", Identity)],
        );
        assert_field(fields, "plan_start", &[direct(src, "plan_start", Identity)]);
        assert_field(fields, "status", &[direct(src, "status", Identity)]);
        // The mutated column unions its passthrough origin with the mutation
        // source (`plan_end := plan_start`), both IDENTITY copies. InputFields are
        // sorted by (namespace, name, field): plan_end before plan_start.
        assert_field(
            fields,
            "plan_end",
            &[
                direct(src, "plan_end", Identity),
                direct(src, "plan_start", Identity),
            ],
        );
        // No engine-stamped `$meta.*` audit column leaks into the lineage map.
        assert!(fields.keys().all(|k| !k.starts_with('$')));
    }

    // -- Combine --------------------------------------------------------------

    #[test]
    fn combine_resolves_each_column_to_its_own_source_across_the_join() {
        let yaml = r#"
pipeline: { name: c }
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: data/orders.csv
      options: { has_header: true }
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: data/events.csv
      options: { has_header: true }
      schema:
        - { name: order_id, type: string }
        - { name: actor, type: string }
  - type: combine
    name: joined
    input:
      orders: orders
      events: events
    config:
      where: "orders.order_id == events.order_id"
      match: first
      on_miss: skip
      cxl: |
        emit order_id = orders.order_id
        emit amount = orders.amount
        emit actor = events.actor
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config: { name: out, type: csv, path: out/c.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        use TransformationSubtype::Identity;
        assert_field(
            fields,
            "order_id",
            &[direct("/w/data/orders.csv", "order_id", Identity)],
        );
        assert_field(
            fields,
            "amount",
            &[direct("/w/data/orders.csv", "amount", Identity)],
        );
        assert_field(
            fields,
            "actor",
            &[direct("/w/data/events.csv", "actor", Identity)],
        );

        // Both sources are reported as inputs, deduplicated.
        let mut input_names: Vec<&str> = lineage.inputs.iter().map(|d| d.name.as_str()).collect();
        input_names.sort_unstable();
        assert_eq!(
            input_names,
            vec!["/w/data/events.csv", "/w/data/orders.csv"]
        );
    }

    // -- N-ary / self-join combine --------------------------------------------

    #[test]
    fn nary_combine_resolves_each_column_through_decomposed_steps() {
        // A 3-input combine decomposes into two binary steps; columns sourced from
        // the all-but-last inputs (`order_id`, `product_name`) flow through the
        // body-less intermediate step. They must resolve by IDENTITY to their true
        // source, not get smeared as TRANSFORMATION against the last build input.
        let yaml = r#"
pipeline: { name: nary }
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: data/orders.csv
      options: { has_header: true }
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: data/products.csv
      options: { has_header: true }
      schema:
        - { name: product_id, type: string }
        - { name: product_name, type: string }
        - { name: category_id, type: string }
  - type: source
    name: categories
    config:
      name: categories
      type: csv
      path: data/categories.csv
      options: { has_header: true }
      schema:
        - { name: category_id, type: string }
        - { name: category_name, type: string }
  - type: combine
    name: enriched
    input:
      orders: orders
      products: products
      categories: categories
    config:
      where: "orders.product_id == products.product_id and products.category_id == categories.category_id"
      match: first
      on_miss: skip
      cxl: |
        emit order_id = orders.order_id
        emit product_name = products.product_name
        emit category_name = categories.category_name
      propagate_ck: driver
  - type: output
    name: out
    input: enriched
    config: { name: out, type: csv, path: out/nary.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        use TransformationSubtype::Identity;
        assert_field(
            fields,
            "order_id",
            &[direct("/w/data/orders.csv", "order_id", Identity)],
        );
        assert_field(
            fields,
            "product_name",
            &[direct("/w/data/products.csv", "product_name", Identity)],
        );
        assert_field(
            fields,
            "category_name",
            &[direct("/w/data/categories.csv", "category_name", Identity)],
        );
    }

    #[test]
    fn self_join_resolves_both_sides_to_the_same_source() {
        // `input: {e: emp, m: emp}` — both sides are the same physical predecessor.
        // The build side must still resolve (it previously dropped to no lineage
        // because the build neighbor lookup excluded the only incoming node).
        let yaml = r#"
pipeline: { name: selfjoin }
nodes:
  - type: source
    name: emp
    config:
      name: emp
      type: csv
      path: data/emp.csv
      options: { has_header: true }
      schema:
        - { name: id, type: string }
        - { name: name, type: string }
        - { name: manager_id, type: string }
  - type: combine
    name: pairs
    input:
      e: emp
      m: emp
    config:
      where: "e.manager_id == m.id"
      match: first
      on_miss: skip
      cxl: |
        emit emp_name = e.name
        emit mgr_name = m.name
      propagate_ck: driver
  - type: output
    name: out
    input: pairs
    config: { name: out, type: csv, path: out/selfjoin.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        use TransformationSubtype::Identity;
        assert_field(fields, "emp_name", &[direct("/w/data/emp.csv", "name", Identity)]);
        // The build side resolves to the same source dataset/column.
        assert_field(fields, "mgr_name", &[direct("/w/data/emp.csv", "name", Identity)]);
    }
}
