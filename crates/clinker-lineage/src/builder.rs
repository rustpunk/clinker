//! Column-level lineage: walk a compiled plan back to its sources.
//!
//! [`column_lineage`] walks `compiled.dag()` in topological order and, for every
//! output (sink) dataset, computes two kinds of OpenLineage column lineage:
//!
//! - **DIRECT** (value-derivation): for each output column, the **Source**
//!   dataset columns its *value* is derived from, in
//!   [`ColumnLineageDatasetFacet::fields`].
//! - **INDIRECT** (influence): the Source columns that influence the dataset as a
//!   whole — filtering (Route / Cull), joining (Combine), grouping (Aggregation),
//!   sorting (Sort), conditional reshaping (Reshape `when:`) — collected once in
//!   [`ColumnLineageDatasetFacet::dataset`].
//!
//! Dataset identity comes from [`crate::dataset::dataset_identity`] (#660); value
//! derivation and influence are read off each operator's retained typed/compiled
//! program.
//!
//! Because topo order processes every upstream node first, the working maps store
//! terminals already resolved back to a Source dataset column — never an
//! intermediate reference — so a rename (`emit full = name`) or a multi-hop chain
//! collapses naturally, and an upstream filter/join influence flows through every
//! downstream node to the sink.
//!
//! A **Composition** node is traversed precisely: the walk recurses into the bound
//! body, seeds each input-port Source from the parent producer feeding that port,
//! and harvests the body's first output port — so each composition output column
//! resolves to its true source columns, and a filter / join / group-by inside the
//! body surfaces as INDIRECT influence on the sink. Nested compositions recurse
//! the same way.
//!
//! ## Envelope (`$doc`) lineage
//!
//! An output column whose value derives from an envelope read
//! (`$doc.<section>.<field>`, bare / indexed / inside a larger expression)
//! gets a DIRECT terminal on the **originating Source** dataset with the rendered
//! `$doc.…` path as its `field`. A `$doc` access carries no source qualifier, so
//! each node's feeding source datasets are tracked alongside the lineage and
//! influence maps (`node_doc_sources`), mirroring the planner's own source
//! attribution: a Source seeds its own dataset, a Combine takes only its driving
//! input, a Composition unions its bound input-port sources, and every other node
//! unions its direct upstreams. The read then attributes only to those feeding
//! sources whose envelope actually declares the section (`declared_doc_sections`),
//! so a multi-source fan-in never emits a false edge to a source whose document
//! cannot carry it. The same DIRECT rule covers a `$doc` read inside an Aggregate
//! emit (the read survives in the post-extraction residual). A `$doc` read inside
//! an influence predicate — a Route condition, a Cull `drop_group_when`, or a
//! Combine `where:` — instead surfaces as INDIRECT influence on those same feeding
//! sources (`FILTER` for Route / Cull, `JOIN` for Combine).
//!
//! ## Subtype model
//!
//! Each DIRECT input→output link carries one [`TransformationSubtype`]; along a
//! path the dominant subtype wins, ranked `IDENTITY < TRANSFORMATION <
//! AGGREGATION` (identity is transparent; an aggregate anywhere on the path
//! dominates). INDIRECT influences carry the per-node subtype (`FILTER` / `JOIN` /
//! `GROUP_BY` / `SORT` / `CONDITIONAL`) and accumulate downstream unchanged.
//!
//! ## Documented limitations
//!
//! - A column-grain `$doc` read is attributed (as DIRECT) in a Transform
//!   projection, a Combine body, a Composition body, and an Aggregate emit, and
//!   (as INDIRECT influence) in a Route / Cull / Combine `where:` predicate. Two
//!   `$doc` cases remain unattributed: a whole-section echo (`*_from_doc`) at
//!   document grain (no output column or CXL expression); and any `$doc` reference
//!   in a Reshape rule — the planner rejects those outright (`bind_reshape`'s E200
//!   guard, because Reshape re-runs its rules after a spill that drops envelope
//!   context), so no Reshape envelope read ever reaches this builder.
//! - A `match: collect` combine (no projection body) is resolved coarsely.
//! - INDIRECT influence covers the predicate / grouping / sort surfaces above (for
//!   record columns, plus `$doc` terms in the Route / Cull / Combine predicates);
//!   an aggregate's pre-aggregation row `filter`, a Transform-inline `filter`, and
//!   Reshape `order_by` / `partition_by` are not (yet) attributed as influence.
//! - Constant and `count(*)` columns (no source input) are omitted from `fields`.
//! - Engine-stamped columns (`$ck.*` / `$meta.*` / `$source.*` / `$widened`) are
//!   skipped, mirroring the default-writer strip.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use petgraph::Direction;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;

use clinker_plan::config::pipeline_node::MatchMode;
use clinker_plan::plan::combine::encode_chain_column;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};
use clinker_plan::plan::{
    CompiledPlan, JoinSide, PlanNodeId, PredicateSupport, QualifiedField, predicate_support,
};
use clinker_record::Schema;
use cxl::analyzer::doc_paths::{DocIndex, DocPath, classify_doc_index_chain};
use cxl::ast::{EmitTarget, Expr, Program, Statement, for_each_field_emit, program_support_into};
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

/// One sink dataset and the column lineage of its columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputColumnLineage {
    pub dataset: DatasetId,
    /// `facet.fields` carries per-column DIRECT (value-derivation) lineage;
    /// `facet.dataset` carries the whole-dataset INDIRECT influence lineage.
    pub facet: ColumnLineageDatasetFacet,
}

/// The result accumulators a scope walk fills: deduplicated input datasets and
/// per-sink output lineage. Shared by reference across the top-level walk and
/// every recursive composition-body walk, so a body's internal Sources and sinks
/// land in the same result as the parent scope.
#[derive(Default)]
struct ScopeSink {
    inputs: Vec<DatasetId>,
    seen_inputs: HashSet<DatasetId>,
    output_acc: Vec<OutputAcc>,
}

/// Per-node state a caller already resolved, pre-seeded into a scope walk. The
/// top-level walk seeds nothing; a Composition arm seeds each body input-port
/// Source from the parent producer feeding it, so those nodes are skipped rather
/// than resolving their placeholder identity into a phantom input.
#[derive(Default)]
struct ScopeSeed {
    lineage: HashMap<PlanNodeId, ColumnTerminals>,
    influence: HashMap<PlanNodeId, InfluenceMap>,
    doc_sources: HashMap<PlanNodeId, BTreeSet<DatasetId>>,
}

/// Build the DIRECT column lineage of `compiled`.
///
/// `base_dir` is the workspace root (the directory containing the pipeline YAML),
/// threaded in by the caller exactly as [`dataset_identity`] requires — it is not
/// retained on [`CompiledPlan`].
pub fn column_lineage(compiled: &CompiledPlan, base_dir: &Path) -> PlanColumnLineage {
    let mut sink = ScopeSink::default();
    // Per-source declared envelope sections — the filter that keeps a `$doc`
    // read from attributing to a source whose document cannot carry the section.
    let declared_sections = declared_doc_sections(compiled, base_dir);

    // Top-level scope: nothing pre-seeded.
    walk_scope(
        compiled,
        base_dir,
        compiled.dag(),
        ScopeSeed::default(),
        &declared_sections,
        &mut sink,
    );

    let outputs = sink
        .output_acc
        .into_iter()
        .map(OutputAcc::into_output)
        .collect();
    PlanColumnLineage {
        inputs: sink.inputs,
        outputs,
    }
}

/// Walk one DAG scope — the top-level pipeline or a composition body — in
/// topological order, accumulating each node's DIRECT terminals and INDIRECT
/// influence, and recording every Output sink into `output_acc`. Source datasets
/// are deduplicated into `inputs`/`seen_inputs`. Returns the per-node terminal and
/// influence maps so a caller (the Composition arm) can harvest a body's output
/// ports.
///
/// `seed` pre-populates nodes whose state the caller already resolved — a
/// composition body's input-port Source nodes, seeded from the parent producers
/// feeding the call site. Those nodes are skipped so their placeholder Source
/// identity is never resolved (which would inject a phantom input). At the top
/// level the seed is empty, so this is a behavior-preserving extraction of the
/// original single-scope walk. `declared_sections` is the per-run envelope
/// section index that gates `$doc` attribution.
fn walk_scope(
    compiled: &CompiledPlan,
    base_dir: &Path,
    dag: &ExecutionPlanDag,
    seed: ScopeSeed,
    declared_sections: &HashMap<DatasetId, BTreeSet<String>>,
    sink: &mut ScopeSink,
) -> (
    HashMap<PlanNodeId, ColumnTerminals>,
    HashMap<PlanNodeId, InfluenceMap>,
) {
    // Pre-seeded nodes keep their injected terminals/influence/doc-sources and are
    // not recomputed by the walk. Derived before the seeds move into the working
    // maps.
    let seeded: HashSet<PlanNodeId> = seed
        .lineage
        .keys()
        .chain(seed.influence.keys())
        .chain(seed.doc_sources.keys())
        .copied()
        .collect();
    // Per node, per output column, the resolved Source terminals it derives from.
    let mut lineage: HashMap<PlanNodeId, ColumnTerminals> = seed.lineage;
    // Per node, the whole-dataset INDIRECT influences accumulated from this node
    // and every upstream — flushed into each Output's facet `dataset[]`.
    let mut influence: HashMap<PlanNodeId, InfluenceMap> = seed.influence;
    // Per node, the Source datasets feeding it — the attribution target for a
    // `$doc` envelope read, which carries no source qualifier of its own.
    let mut doc_sources: HashMap<PlanNodeId, BTreeSet<DatasetId>> = seed.doc_sources;

    for &idx in &dag.topo_order {
        let node = &dag.graph[idx];
        let node_id = node.id();

        if seeded.contains(&node_id) {
            continue;
        }

        // The Source datasets this node draws from, resolved before the per-node
        // match so the emit-walkers can attribute a `$doc` read to them.
        let node_doc_srcs = node_doc_sources(node, dag, idx, base_dir, &doc_sources);

        // Set by the Composition arm to the body's output-port INDIRECT influence,
        // merged into this node's influence below.
        let mut comp_body_influence: Option<InfluenceMap> = None;
        let cols: ColumnTerminals = match node {
            PlanNode::Source { .. } => {
                let mut cols = ColumnTerminals::new();
                // The source's own dataset is exactly `node_doc_srcs`' single
                // element (computed above), so reuse it rather than recomputing
                // the string-allocating `dataset_identity` a second time.
                if let Some(ds) = node_doc_srcs.iter().next() {
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
                    if sink.seen_inputs.insert(ds.clone()) {
                        sink.inputs.push(ds.clone());
                    }
                }
                cols
            }

            PlanNode::Transform { resolved, .. } => {
                let up = single_upstream(dag, idx);
                // Explicit field emits, threading `let` and `emit each` scopes.
                let mut emitted: HashMap<String, TermMap> = HashMap::new();
                if let Some(payload) = resolved {
                    // A single-input operator reads upstream columns by name; a
                    // qualified leaf that is not an in-scope binding is a nested /
                    // struct access whose source is its base column (`address.city`
                    // → `address`), mirroring the aggregate and combine paths.
                    let resolve_unbound = |qf: &QualifiedField| -> Option<TermMap> {
                        let col = qf.qualifier.as_deref().unwrap_or(qf.name.as_ref());
                        upstream_col(&lineage, up, col).cloned()
                    };
                    let mut env = HashMap::new();
                    collect_field_emits(
                        &payload.typed.program.statements,
                        &mut env,
                        &mut emitted,
                        &resolve_unbound,
                        &node_doc_srcs,
                        declared_sections,
                    );
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
                    // Envelope reads in this emit, attributed against the feeding
                    // sources. A `$doc` left in the residual (outside any aggregate
                    // call) survives as a passthrough leaf and keeps the emit's own
                    // subtype (bare = IDENTITY, in-expression = TRANSFORMATION). A
                    // `$doc` that `extract_aggregates` hoisted into an accumulator
                    // argument (e.g. `sum(amount * $doc.x)`) is gone from the
                    // residual and is an AGGREGATION input — recover it from the
                    // binding. Skipped wholesale when no envelope-bearing source
                    // feeds this node.
                    if !node_doc_srcs.is_empty() {
                        let mut residual_docs = Vec::new();
                        collect_doc_paths_in_expr(&emit.residual, &mut residual_docs);
                        merge_doc_read_terms(
                            &mut terms,
                            &residual_docs,
                            field_ref_subtype(&emit.residual),
                            &node_doc_srcs,
                            declared_sections,
                        );
                        let mut agg_docs = Vec::new();
                        aggregate_binding_doc_paths(&emit.residual, compiled, &mut agg_docs);
                        merge_doc_read_terms(
                            &mut terms,
                            &agg_docs,
                            Subtype::Aggregation,
                            &node_doc_srcs,
                            declared_sections,
                        );
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
                        // A `$doc` read needs no handling here: the planner rejects
                        // any `$doc` reference in a Reshape rule (E200), because
                        // Reshape re-runs its rules after a per-group spill that
                        // drops envelope context. So a Reshape `set`/`overrides`
                        // program never carries one.
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
                match_mode,
                ..
            } => {
                let sides = combine_sides(dag, idx, resolved_column_map, *driving_upstream);
                let map = sides.map;
                let probe = sides.probe;
                let build = sides.build;
                let bare = sides.bare;

                let mut cols = ColumnTerminals::new();
                match typed {
                    Some(tp) => {
                        // A combine body reads `input.field` references resolved
                        // through the side map; `let` / `emit each` scopes thread the
                        // same way as a Transform.
                        let resolve_unbound = |qf: &QualifiedField| -> Option<TermMap> {
                            let (side, cidx) = resolve_side(qf, map, &bare)?;
                            let (up_id, schema) = match side {
                                JoinSide::Probe => probe.as_ref()?,
                                JoinSide::Build => build.as_ref()?,
                            };
                            let col = schema.column_name(cidx as usize)?;
                            lineage.get(up_id).and_then(|m| m.0.get(col)).cloned()
                        };
                        let mut env = HashMap::new();
                        let mut emitted: HashMap<String, TermMap> = HashMap::new();
                        collect_field_emits(
                            &tp.program.statements,
                            &mut env,
                            &mut emitted,
                            &resolve_unbound,
                            &node_doc_srcs,
                            declared_sections,
                        );
                        for (name, terms) in emitted {
                            cols.insert_nonempty(&name, terms);
                        }
                    }
                    // Intermediate step of an N-ary decomposition: a body-less inner
                    // join that carries every input column forward by IDENTITY. Its
                    // output columns are the flat-encoded `__{qualifier}__{field}`
                    // names, and `resolved_column_map` is a 1:1 cover of them keyed by
                    // the original `(qualifier, field)`. Keying the lineage entry by
                    // those encoded names lets the *final* decomposed step resolve its
                    // probe references straight through this map. A `match: collect`
                    // step (even when decomposed) has a real collected-array column,
                    // so it routes to the collect arm, not here.
                    None if decomposed_from.is_some()
                        && !matches!(match_mode, MatchMode::Collect) =>
                    {
                        for (qf, (side, cidx)) in map.iter() {
                            let resolved = match side {
                                JoinSide::Probe => probe.as_ref(),
                                JoinSide::Build => build.as_ref(),
                            };
                            if let Some((up_id, schema)) = resolved
                                && let Some(up_col) = schema.column_name(*cidx as usize)
                            {
                                let qualifier = qf.qualifier.as_deref().unwrap_or(qf.name.as_ref());
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

            PlanNode::Composition { body, .. } => {
                // Recurse into the bound body and stitch named ports. Each input
                // port is a real `Source` node in the body DAG (synthesized at bind
                // time), so seeding those Sources from the parent producers and
                // walking the body with the standard per-node rules resolves every
                // output column to its true upstream source columns — no coarse
                // all-to-all. `from_body` clones the body graph (O(body size),
                // negligible vs. record volume); nested compositions recurse
                // through this same arm.
                let mut cols = ColumnTerminals::new();
                if let Some(b) = compiled.body_of(*body) {
                    let body_dag = ExecutionPlanDag::from_body(b);
                    // Seed each bound input port's body Source from the parent
                    // producer feeding it (the composition's incoming port-tagged
                    // edge). Keyed by the port Source's globally-unique id, so the
                    // body walk skips it rather than resolving its placeholder
                    // identity into a phantom `clinker:<port>` input.
                    let mut seed = ScopeSeed::default();
                    for edge in dag.graph.edges_directed(idx, Direction::Incoming) {
                        let Some(port) = edge.weight().port.as_deref() else {
                            continue;
                        };
                        let Some(&body_src_idx) = b.port_name_to_node_idx.get(port) else {
                            continue;
                        };
                        let body_src_id = b.graph[body_src_idx].id();
                        let parent_id = dag.graph[edge.source()].id();
                        if let Some(terms) = lineage.get(&parent_id) {
                            seed.lineage.insert(body_src_id, terms.clone());
                        }
                        if let Some(inf) = influence.get(&parent_id) {
                            seed.influence.insert(body_src_id, inf.clone());
                        }
                        // Seed the port's body Source with the feeding parent's
                        // source datasets, so an in-body `$doc` read attributes to
                        // the real upstream source rather than the synthetic port.
                        if let Some(ds) = doc_sources.get(&parent_id) {
                            seed.doc_sources.insert(body_src_id, ds.clone());
                        }
                    }
                    let (body_lineage, mut body_influence) =
                        walk_scope(compiled, base_dir, &body_dag, seed, declared_sections, sink);
                    // Harvest the first declared output port — the records the
                    // composition surfaces, against the single `output_schema` the
                    // node carries — matching the runtime harvest.
                    if let Some((_, &out_idx)) = b.output_port_to_node_idx.iter().next() {
                        let out_id = b.graph[out_idx].id();
                        if let Some(body_out) = body_lineage.get(&out_id) {
                            for_each_output_col(node, dag, |col| {
                                cols.insert_nonempty(
                                    col,
                                    body_out.0.get(col).cloned().unwrap_or_default(),
                                );
                            });
                        }
                        comp_body_influence = body_influence.remove(&out_id);
                    }
                }
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

        let mut node_influence = node_indirect_influence(
            node,
            dag,
            idx,
            &lineage,
            &influence,
            &node_doc_srcs,
            declared_sections,
        );
        // A composition's in-body INDIRECT influence (filters / joins / group-bys
        // inside the body), harvested at its output port, joins the inherited
        // upstream influence. Set-union, so the parent influence carried through
        // both paths is not double-counted.
        if let Some(body_inf) = comp_body_influence {
            for (terminal, subs) in body_inf {
                node_influence.entry(terminal).or_default().extend(subs);
            }
        }

        if let PlanNode::Output { .. } = node
            && let Some(ds) = dataset_identity(node, base_dir)
        {
            record_output(&mut sink.output_acc, ds, &cols, &node_influence);
        }

        lineage.insert(node_id, cols);
        influence.insert(node_id, node_influence);
        doc_sources.insert(node_id, node_doc_srcs);
    }

    (lineage, influence)
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

/// INDIRECT influence subtype: how a column influences the dataset as a whole
/// (filtering, joining, grouping, sorting, conditionally reshaping) without its
/// value flowing into a specific output column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IndirectSub {
    Filter,
    Join,
    GroupBy,
    Sort,
    Conditional,
}

impl IndirectSub {
    fn to_openlineage(self) -> TransformationSubtype {
        match self {
            IndirectSub::Filter => TransformationSubtype::Filter,
            IndirectSub::Join => TransformationSubtype::Join,
            IndirectSub::GroupBy => TransformationSubtype::GroupBy,
            IndirectSub::Sort => TransformationSubtype::Sort,
            IndirectSub::Conditional => TransformationSubtype::Conditional,
        }
    }
}

/// The INDIRECT influences accumulated for a node: each Source terminal mapped
/// to the set of influence subtypes through which it reaches the dataset. The
/// `BTreeMap`/`BTreeSet` keep the emitted `dataset[]` order deterministic.
type InfluenceMap = BTreeMap<Terminal, BTreeSet<IndirectSub>>;

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
#[derive(Default, Clone)]
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

/// The Source datasets whose document context feeds `node` — the candidate
/// targets for a `$doc` envelope read (which carries no source qualifier).
/// Mirrors the three rules of the planner's `build_node_source_sets`
/// (`clinker_plan::config::pipeline`): a Source seeds its own dataset; a Combine
/// carries only its driving input's document context; every other node
/// (Composition included — its incoming neighbors are exactly its bound
/// input-port producers) unions its direct upstreams. Upstreams are already
/// resolved because the walk is topological. Keep the three rules in sync with
/// that planner function.
///
/// This returns the sources whose document *could* carry the read; the final
/// per-section narrowing — attribute a `$doc.<section>` read only to sources
/// whose envelope declares `<section>` — happens at attribution time
/// (`resolve_expr_terms`), so a multi-source fan-in never emits a false edge to
/// a source whose document lacks the section.
fn node_doc_sources(
    node: &PlanNode,
    dag: &ExecutionPlanDag,
    idx: NodeIndex,
    base_dir: &Path,
    doc_sources: &HashMap<PlanNodeId, BTreeSet<DatasetId>>,
) -> BTreeSet<DatasetId> {
    match node {
        PlanNode::Source { .. } => dataset_identity(node, base_dir).into_iter().collect(),
        PlanNode::Combine {
            driving_upstream, ..
        } => {
            // A joined record carries only the driving input's document context.
            // Each N-ary-decomposition step re-pins its driver to the prior
            // intermediate (a direct predecessor that carries the original
            // driver's context), so the driver stays pinnable through the chain.
            // If the planner ever cannot pin it (`driving_upstream == None`) the
            // document-context source is unidentifiable, so omit rather than union
            // every input — guessing the build side would assert false envelope
            // provenance, and omission is the honest degradation.
            match driving_upstream {
                Some(drv) => doc_sources
                    .get(&dag.graph[*drv].id())
                    .cloned()
                    .unwrap_or_default(),
                None => BTreeSet::new(),
            }
        }
        _ => union_doc_sources(dag, idx, doc_sources),
    }
}

/// Union of the Source datasets feeding every direct upstream of `idx`.
fn union_doc_sources(
    dag: &ExecutionPlanDag,
    idx: NodeIndex,
    doc_sources: &HashMap<PlanNodeId, BTreeSet<DatasetId>>,
) -> BTreeSet<DatasetId> {
    let mut out = BTreeSet::new();
    for up in upstream_ids(dag, idx) {
        if let Some(srcs) = doc_sources.get(&up) {
            out.extend(srcs.iter().cloned());
        }
    }
    out
}

/// Each Source dataset mapped to the envelope section names it declares — the
/// sections a `$doc.<section>.<field>` read may legitimately resolve against.
/// Built once from the top-level Source nodes (composition bodies are fed by
/// synthetic ports, never their own file sources, so they hold no real Source).
/// A source with no `envelope:` block maps to the empty set, so a `$doc` read is
/// never attributed to a source whose document cannot carry the section.
///
/// This is the lineage-side counterpart to the planner's per-source E341/E348
/// section-declaration check: that check rejects an undeclared read against a
/// *validated* source at compile time, but lets it through for an *unvalidated*
/// one (CSV / fixed-width / SWIFT), where this filter prevents the false edge.
fn declared_doc_sections(
    compiled: &CompiledPlan,
    base_dir: &Path,
) -> HashMap<DatasetId, BTreeSet<String>> {
    let mut out: HashMap<DatasetId, BTreeSet<String>> = HashMap::new();
    let dag = compiled.dag();
    for &idx in &dag.topo_order {
        let node = &dag.graph[idx];
        if let PlanNode::Source { resolved, .. } = node
            && let Some(ds) = dataset_identity(node, base_dir)
        {
            let sections: BTreeSet<String> = resolved
                .as_ref()
                .and_then(|payload| payload.source.envelope.as_ref())
                .map(|env| env.sections.keys().map(|k| k.to_string()).collect())
                .unwrap_or_default();
            out.entry(ds).or_default().extend(sections);
        }
    }
    out
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

/// IDENTITY when `expr` is a single copy of one leaf — a (bare or qualified)
/// field reference, a bare `$doc` envelope access, or a `$doc` access with only
/// literal index segments (`$doc.s.items[0]`) — otherwise TRANSFORMATION.
fn field_ref_subtype(expr: &Expr) -> Subtype {
    let is_bare_copy = matches!(
        expr,
        Expr::FieldRef { .. } | Expr::QualifiedFieldRef { .. } | Expr::DocAccess { .. }
    ) || matches!(classify_doc_index_chain(expr), Some(Ok(_)));
    if is_bare_copy {
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

// ---------------------------------------------------------------------------
// Program-aware emit walker
// ---------------------------------------------------------------------------

/// Walk a CXL program's statements collecting, per field-emit target, the
/// resolved source terminals its value derives from.
///
/// `env` is one lexical binding scope holding both kinds of in-program name
/// that a per-emit read-set walk misses:
///
/// - **`let` bindings:** `let x = a + b; emit y = x` — `y` derives from `a`/`b`.
///   On each `Let`, the RHS is resolved (against the upstream and earlier
///   bindings) and stored, so a later reference to the binding name expands to
///   its terminals rather than resolving to nothing.
/// - **`emit each` loop bindings:** `emit each it in items { emit sku = it.sku }`
///   — `sku` derives from `items`. The loop binding is bound to the source
///   array's terminals (as a TRANSFORMATION — element extraction), so a body
///   reference to `it` / `it.field` resolves to the array column.
///
/// Both live in one map so the innermost binding of a name wins (a `let` may
/// shadow a loop variable). `emit each` / `explode` bodies are a lexical block:
/// the whole `env` is saved on entry and restored on exit, so a body-local
/// binding never leaks into the enclosing or a sibling scope.
///
/// `resolve_unbound` resolves a leaf that names no in-scope binding — an
/// operator-input column — and differs per node kind (upstream column for
/// Transform; `input.field` via the combine resolver for Combine). Duplicate
/// field emits are last-wins, matching the runtime.
///
/// `doc_sources` are the Source datasets feeding this node: an envelope read
/// (`$doc.<section>.<field>`) carries no source qualifier, so it attributes to
/// those of them whose envelope declares the section (`declared_sections`). A
/// `$doc` read inside a `let` is captured the same way — the resolved terminals
/// land in `env` and expand at the binding's use site.
fn collect_field_emits(
    statements: &[Statement],
    env: &mut HashMap<String, TermMap>,
    emitted: &mut HashMap<String, TermMap>,
    resolve_unbound: &impl Fn(&QualifiedField) -> Option<TermMap>,
    doc_sources: &BTreeSet<DatasetId>,
    declared_sections: &HashMap<DatasetId, BTreeSet<String>>,
) {
    for stmt in statements {
        match stmt {
            Statement::Let { name, expr, .. } => {
                let terms =
                    resolve_expr_terms(expr, env, resolve_unbound, doc_sources, declared_sections);
                env.insert(name.to_string(), terms);
            }
            Statement::Emit {
                name,
                expr,
                target: EmitTarget::Field,
                ..
            } => {
                if name.starts_with('$') {
                    continue;
                }
                let terms =
                    resolve_expr_terms(expr, env, resolve_unbound, doc_sources, declared_sections);
                emitted.insert(name.to_string(), terms);
            }
            Statement::EmitEach {
                binding,
                source,
                body,
                ..
            }
            | Statement::ExplodeOuter {
                binding,
                source,
                body,
                ..
            } => {
                let src = resolve_expr_terms(
                    source,
                    env,
                    resolve_unbound,
                    doc_sources,
                    declared_sections,
                );
                let mut tagged = TermMap::new();
                merge_terminals(&mut tagged, Some(&src), Subtype::Transformation);
                // The body is a lexical block: save the scope, bind the loop
                // variable, recurse, then restore — body-local `let`s and the
                // loop binding both fall out of scope here.
                let saved = env.clone();
                env.insert(binding.to_string(), tagged);
                collect_field_emits(
                    body,
                    env,
                    emitted,
                    resolve_unbound,
                    doc_sources,
                    declared_sections,
                );
                *env = saved;
            }
            _ => {}
        }
    }
}

/// Resolve the source terminals an emit/let RHS expression derives from. The
/// whole expression's [`field_ref_subtype`] (IDENTITY for a bare copy/rename,
/// else TRANSFORMATION) is composed onto every leaf it reads — a record column
/// (via `resolve_unbound`) or an envelope `$doc` read (attributed to each
/// feeding `doc_sources` dataset whose envelope declares the read section, with
/// the rendered doc path as its `field`).
fn resolve_expr_terms(
    expr: &Expr,
    env: &HashMap<String, TermMap>,
    resolve_unbound: &impl Fn(&QualifiedField) -> Option<TermMap>,
    doc_sources: &BTreeSet<DatasetId>,
    declared_sections: &HashMap<DatasetId, BTreeSet<String>>,
) -> TermMap {
    let local = field_ref_subtype(expr);
    let mut out = TermMap::new();

    let mut refs = Vec::new();
    collect_field_refs(expr, &mut refs);
    for qf in &refs {
        let base = resolve_leaf_terms(qf, env, resolve_unbound);
        merge_terminals(&mut out, base.as_ref(), local);
    }

    // Envelope reads resolve to the feeding source datasets, not through the
    // upstream column map (a `$doc` access names a section/field, not a column).
    let mut doc_paths = Vec::new();
    collect_doc_paths_in_expr(expr, &mut doc_paths);
    merge_doc_read_terms(&mut out, &doc_paths, local, doc_sources, declared_sections);

    out
}

/// Merge the DIRECT terminals a set of `$doc` envelope reads derives from into
/// `out`, each composed with `local`. A `$doc` read carries no source qualifier,
/// so it attributes to every feeding `doc_sources` dataset whose envelope
/// declares the read section (`declared_sections`) — never emitting a false edge
/// to a source whose document cannot carry the section — with the rendered
/// `$doc.<section>.<field>` path as its `field`. The seed subtype is IDENTITY (a
/// read is itself a verbatim copy of the envelope value); `local` then dominates
/// it, so a bare `$doc` emit stays IDENTITY while a `$doc` inside an expression
/// composes to TRANSFORMATION. Shared by the Transform/Combine body walk
/// (`resolve_expr_terms`) and the Aggregate emit arm.
fn merge_doc_read_terms(
    out: &mut TermMap,
    doc_paths: &[DocPath],
    local: Subtype,
    doc_sources: &BTreeSet<DatasetId>,
    declared_sections: &HashMap<DatasetId, BTreeSet<String>>,
) {
    for path in doc_paths {
        let mut base = TermMap::new();
        for terminal in doc_read_terminals(path, doc_sources, declared_sections) {
            base.insert(terminal, Subtype::Identity);
        }
        merge_terminals(out, Some(&base), local);
    }
}

/// The Source terminals one `$doc` read resolves to: for each feeding source
/// whose envelope declares the read's section, a terminal naming that source
/// with the rendered `$doc.<section>.<field>` path as its `field`. The single
/// home of the section-declaration gate that keeps a multi-source fan-in from
/// emitting a false envelope edge — shared by the DIRECT (`merge_doc_read_terms`)
/// and INDIRECT (`add_doc_influence`) attribution paths so the gate cannot drift
/// between them.
fn doc_read_terminals<'a>(
    path: &'a DocPath,
    doc_sources: &'a BTreeSet<DatasetId>,
    declared_sections: &'a HashMap<DatasetId, BTreeSet<String>>,
) -> impl Iterator<Item = Terminal> + 'a {
    let rendered = render_doc_path(path);
    doc_sources
        .iter()
        .filter(move |ds| {
            declared_sections
                .get(*ds)
                .is_some_and(|sections| sections.contains(path.section.as_ref()))
        })
        .map(move |ds| Terminal::new(&ds.namespace, &ds.name, &rendered))
}

/// Resolve one leaf field reference to its source terminals: an in-scope binding
/// (`let` or `emit each` loop variable, matched by the reference's first segment)
/// wins, then `resolve_unbound` (the operator input).
fn resolve_leaf_terms(
    qf: &QualifiedField,
    env: &HashMap<String, TermMap>,
    resolve_unbound: &impl Fn(&QualifiedField) -> Option<TermMap>,
) -> Option<TermMap> {
    let first = qf.qualifier.as_deref().unwrap_or(qf.name.as_ref());
    if let Some(terms) = env.get(first) {
        return Some(terms.clone());
    }
    resolve_unbound(qf)
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
        Expr::QualifiedFieldRef { parts, .. } => {
            // Aggregates are single-input; a qualified ref in a residual carries
            // its base column in the first segment.
            if let Some(first) = parts.first()
                && !first.starts_with('$')
            {
                out.push(AggLeaf::Field(first.as_ref()));
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

/// `$doc` paths read inside the aggregate-function arguments referenced by
/// `residual`'s accumulator slots. `extract_aggregates` hoists an aggregated
/// expression (e.g. `sum(amount * $doc.Head.fx_rate)`) into a binding arg and
/// leaves only an `AggSlot` in the residual, so an envelope read there never
/// appears in the residual itself — recover it from the binding.
fn aggregate_binding_doc_paths(
    residual: &Expr,
    compiled: &cxl::plan::CompiledAggregate,
    out: &mut Vec<DocPath>,
) {
    let mut leaves = Vec::new();
    collect_agg_leaves(residual, &mut leaves);
    for leaf in leaves {
        if let AggLeaf::Agg(slot) = leaf
            && let Some(binding) = compiled.bindings.get(slot as usize)
        {
            binding_arg_doc_paths(&binding.arg, out);
        }
    }
}

/// `$doc` paths read by a single aggregate argument — the envelope analogue of
/// [`binding_arg_input_names`], which sees only record columns (`support_into`
/// excludes the `$doc` namespace).
fn binding_arg_doc_paths(arg: &BindingArg, out: &mut Vec<DocPath>) {
    match arg {
        BindingArg::Expr(e) => collect_doc_paths_in_expr(e, out),
        BindingArg::Pair(a, b) => {
            binding_arg_doc_paths(a, out);
            binding_arg_doc_paths(b, out);
        }
        BindingArg::Field(_) | BindingArg::Wildcard => {}
    }
}

// ---------------------------------------------------------------------------
// Field-reference collection and resolution
// ---------------------------------------------------------------------------

/// Collect every field reference (as a [`QualifiedField`]) read by an
/// expression, skipping system (`$`) namespaces. A bare `FieldRef` yields a
/// bare `QualifiedField`; a `QualifiedFieldRef` yields the (qualifier, field)
/// pair — used both for combine `input.field` resolution and for detecting an
/// `emit each` loop-binding reference by its first segment.
fn collect_field_refs(expr: &Expr, out: &mut Vec<QualifiedField>) {
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
            collect_field_refs(lhs, out);
            collect_field_refs(rhs, out);
        }
        Expr::Unary { operand, .. } => collect_field_refs(operand, out),
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_field_refs(condition, out);
            collect_field_refs(then_branch, out);
            if let Some(eb) = else_branch {
                collect_field_refs(eb, out);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                collect_field_refs(s, out);
            }
            for arm in arms {
                collect_field_refs(&arm.pattern, out);
                collect_field_refs(&arm.body, out);
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            collect_field_refs(receiver, out);
            for a in args {
                collect_field_refs(a, out);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                collect_field_refs(a, out);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            collect_field_refs(receiver, out);
            collect_field_refs(index, out);
        }
        Expr::Closure { body, .. } => collect_field_refs(body, out),
        _ => {}
    }
}

/// Collect every envelope (`$doc`) path an expression reads, as a resolved
/// [`DocPath`]. A bare `$doc.<section>.<field>` yields a two-level path; a `$doc`
/// access with trailing literal indices (`$doc.s.items[0]`) is classified as one
/// unit so its index expressions are not descended into as independent
/// references. The parallel of [`collect_field_refs`] for the off-schema `$doc`
/// namespace `collect_field_refs` deliberately skips.
///
/// Dynamic doc indices are a fail-fast compile error, so a compiled plan never
/// carries an unresolvable chain; the `Err` arm is descended defensively only so
/// a nested `$doc` is never silently lost.
fn collect_doc_paths_in_expr(expr: &Expr, out: &mut Vec<DocPath>) {
    // A `$doc` index chain is one path — classify before the generic descent so
    // its index expressions are not walked as separate references. Anything else
    // (a non-`$doc` index access, or an unreachable dynamic-index `Err`) falls
    // through to the generic descent, which still collects a `$doc` nested in the
    // index expression.
    if let Expr::IndexAccess { .. } = expr
        && let Some(Ok(path)) = classify_doc_index_chain(expr)
    {
        out.push(path);
        return;
    }
    if let Expr::DocAccess { section, field, .. } = expr {
        out.push(DocPath {
            section: section.clone(),
            field: field.clone(),
            indices: Vec::new(),
        });
        return;
    }
    match expr {
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            collect_doc_paths_in_expr(lhs, out);
            collect_doc_paths_in_expr(rhs, out);
        }
        Expr::Unary { operand, .. } => collect_doc_paths_in_expr(operand, out),
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_doc_paths_in_expr(condition, out);
            collect_doc_paths_in_expr(then_branch, out);
            if let Some(eb) = else_branch {
                collect_doc_paths_in_expr(eb, out);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                collect_doc_paths_in_expr(s, out);
            }
            for arm in arms {
                collect_doc_paths_in_expr(&arm.pattern, out);
                collect_doc_paths_in_expr(&arm.body, out);
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            collect_doc_paths_in_expr(receiver, out);
            for a in args {
                collect_doc_paths_in_expr(a, out);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                collect_doc_paths_in_expr(a, out);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            collect_doc_paths_in_expr(receiver, out);
            collect_doc_paths_in_expr(index, out);
        }
        Expr::Closure { body, .. } => collect_doc_paths_in_expr(body, out),
        _ => {}
    }
}

/// Render a resolved [`DocPath`] back to its CXL surface spelling
/// `$doc.<section>.<field>` with any literal index segments (`[0]` / `["key"]`).
/// This stable string is the `field` of the envelope terminal in the lineage
/// facet — the `$doc.` prefix marks an envelope-derived field and never collides
/// with a real column (record types may not declare `$`-prefixed columns).
///
/// A string-key index is emitted as a CXL string literal with `\` and `"`
/// escaped, so a key containing a quote (CXL string literals admit `\"`) renders
/// to an unambiguous, round-trippable field — and two distinct keys never
/// collapse to the same terminal.
fn render_doc_path(path: &DocPath) -> String {
    use std::fmt::Write as _;
    let mut s = format!("$doc.{}.{}", path.section, path.field);
    for idx in &path.indices {
        match idx {
            DocIndex::Int(n) => {
                let _ = write!(s, "[{n}]");
            }
            DocIndex::Key(k) => {
                let escaped = k.replace('\\', "\\\\").replace('"', "\\\"");
                let _ = write!(s, "[\"{escaped}\"]");
            }
        }
    }
    s
}

/// Collect every field reference read by a program's statements (`emit` / `let` /
/// `filter` predicates, and `emit each` source arrays + bodies). Used for the
/// combine `where:` residual program, whose conjunct columns still influence the
/// join even though they are neither an equality nor a range.
fn collect_stmt_field_refs(statements: &[Statement], out: &mut Vec<QualifiedField>) {
    for stmt in statements {
        match stmt {
            Statement::Emit { expr, .. }
            | Statement::Let { expr, .. }
            | Statement::ExprStmt { expr, .. } => collect_field_refs(expr, out),
            Statement::Filter { predicate, .. } => collect_field_refs(predicate, out),
            Statement::EmitEach { source, body, .. }
            | Statement::ExplodeOuter { source, body, .. } => {
                collect_field_refs(source, out);
                collect_stmt_field_refs(body, out);
            }
            _ => {}
        }
    }
}

/// Collect every envelope (`$doc`) path read by a program's statements — the
/// statement-level parallel of [`collect_doc_paths_in_expr`], mirroring
/// [`collect_stmt_field_refs`]. Used for the INDIRECT predicate surfaces whose
/// condition is a program rather than a bare expression: a Route branch
/// condition, a Cull group-drop decision, and a Combine `where:` residual.
/// (Reshape `when:` is deliberately absent — the planner rejects `$doc` in any
/// Reshape rule via `bind_reshape`'s E200 guard, so none reaches this builder.)
fn collect_doc_paths_in_stmts(statements: &[Statement], out: &mut Vec<DocPath>) {
    for stmt in statements {
        match stmt {
            Statement::Emit { expr, .. }
            | Statement::Let { expr, .. }
            | Statement::ExprStmt { expr, .. } => collect_doc_paths_in_expr(expr, out),
            Statement::Filter { predicate, .. } => collect_doc_paths_in_expr(predicate, out),
            Statement::EmitEach { source, body, .. }
            | Statement::ExplodeOuter { source, body, .. } => {
                collect_doc_paths_in_expr(source, out);
                collect_doc_paths_in_stmts(body, out);
            }
            _ => {}
        }
    }
}

/// The base (first dotted segment) of an input-column reference: a nested /
/// struct access (`address.city`) influences via its source column `address`.
/// Predicate read-sets keep qualified refs as the dotted path, but the upstream
/// lineage map is keyed by bare column name.
fn base_col(col: &str) -> &str {
    col.split('.').next().unwrap_or(col)
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

/// The probe / build side resolution context of a Combine node: the pre-resolved
/// `(side, index)` map, the unambiguous-bare-name index, and each side's upstream
/// `(id, schema)`. Shared by the DIRECT body resolution and the INDIRECT
/// JOIN-influence resolution.
struct CombineSides<'a> {
    map: &'a HashMap<QualifiedField, (JoinSide, u32)>,
    bare: HashMap<String, (JoinSide, u32)>,
    probe: Option<(PlanNodeId, &'a Arc<Schema>)>,
    build: Option<(PlanNodeId, &'a Arc<Schema>)>,
}

/// Resolve a Combine's probe / build sides. The build side is the incoming
/// neighbor that is not the probe; for a self-join (`input: {a: src, b: src}`)
/// both sides are the same physical predecessor, so it falls back to the probe
/// node — the side distinction is logical (`resolved_column_map`'s `JoinSide`),
/// not a distinct node.
fn combine_sides<'a>(
    dag: &'a ExecutionPlanDag,
    idx: NodeIndex,
    resolved_column_map: &'a HashMap<QualifiedField, (JoinSide, u32)>,
    driving_upstream: Option<NodeIndex>,
) -> CombineSides<'a> {
    let probe_idx = driving_upstream;
    let build_idx = dag
        .graph
        .neighbors_directed(idx, Direction::Incoming)
        .find(|n| Some(*n) != probe_idx)
        .or(probe_idx);
    CombineSides {
        map: resolved_column_map,
        bare: bare_side_index(resolved_column_map),
        probe: probe_idx.map(|n| (dag.graph[n].id(), dag.graph[n].output_schema_in(dag))),
        build: build_idx.map(|n| (dag.graph[n].id(), dag.graph[n].output_schema_in(dag))),
    }
}

// ---------------------------------------------------------------------------
// INDIRECT influence
// ---------------------------------------------------------------------------

/// The whole-dataset INDIRECT influences of `node`: every upstream node's
/// accumulated influence (flowed through unchanged) plus the columns this node
/// filters / joins / groups / sorts / conditionally reshapes on, each resolved
/// to its Source terminals via the upstream DIRECT `lineage`.
///
/// A predicate may also read the envelope (`$doc.<section>.<field>`); such a read
/// names a section/field, not a record column, so it resolves not through the
/// upstream lineage map but directly to each feeding `node_doc_srcs` dataset
/// whose envelope declares the section (`declared_sections`), mirroring the
/// DIRECT `$doc` attribution.
fn node_indirect_influence(
    node: &PlanNode,
    dag: &ExecutionPlanDag,
    idx: NodeIndex,
    lineage: &HashMap<PlanNodeId, ColumnTerminals>,
    influence: &HashMap<PlanNodeId, InfluenceMap>,
    node_doc_srcs: &BTreeSet<DatasetId>,
    declared_sections: &HashMap<DatasetId, BTreeSet<String>>,
) -> InfluenceMap {
    let mut inf = InfluenceMap::new();

    // Inherited: a filter / join / group anywhere upstream taints every
    // downstream dataset, so each upstream's influence flows through unchanged.
    for up in upstream_ids(dag, idx) {
        if let Some(up_inf) = influence.get(&up) {
            for (terminal, subs) in up_inf {
                inf.entry(terminal.clone())
                    .or_default()
                    .extend(subs.iter().copied());
            }
        }
    }

    // Local: this node's own influence predicate columns.
    match node {
        PlanNode::Route {
            branch_programs, ..
        } => {
            if let Some(PredicateSupport::RouteBranches(branches)) = predicate_support(node) {
                let up = single_upstream(dag, idx);
                for cols in &branches {
                    for col in cols {
                        add_upstream_influence(
                            &mut inf,
                            upstream_col(lineage, up, base_col(col)),
                            IndirectSub::Filter,
                        );
                    }
                }
            }
            // A branch condition may filter on the envelope; the branch programs
            // retain `$doc` (`predicate_support` strips it), so collect it here.
            let mut doc_paths = Vec::new();
            for prog in branch_programs.iter() {
                collect_doc_paths_in_stmts(&prog.program.statements, &mut doc_paths);
            }
            add_doc_influence(
                &mut inf,
                &doc_paths,
                node_doc_srcs,
                declared_sections,
                IndirectSub::Filter,
            );
        }
        PlanNode::Cull { config, typed, .. } => {
            let up = single_upstream(dag, idx);
            if let Some(PredicateSupport::CullDrop(cols)) = predicate_support(node) {
                for col in &cols {
                    add_upstream_influence(
                        &mut inf,
                        upstream_col(lineage, up, base_col(col)),
                        IndirectSub::Filter,
                    );
                }
            }
            // `partition_by` groups rows for the per-group drop decision, so the
            // partition columns influence which rows survive — GROUP_BY, mirroring
            // Aggregation's group keys.
            for col in &config.partition_by {
                add_upstream_influence(
                    &mut inf,
                    upstream_col(lineage, up, base_col(col)),
                    IndirectSub::GroupBy,
                );
            }
            // A `drop_group_when` predicate may read the envelope; the decision
            // program retains `$doc` (`predicate_support` strips it).
            let mut doc_paths = Vec::new();
            collect_doc_paths_in_stmts(&typed.program.statements, &mut doc_paths);
            add_doc_influence(
                &mut inf,
                &doc_paths,
                node_doc_srcs,
                declared_sections,
                IndirectSub::Filter,
            );
        }
        PlanNode::Aggregation { compiled, .. } => {
            let up = single_upstream(dag, idx);
            for col in &compiled.group_by_fields {
                add_upstream_influence(
                    &mut inf,
                    upstream_col(lineage, up, col),
                    IndirectSub::GroupBy,
                );
            }
        }
        PlanNode::Sort { sort_fields, .. } => {
            let up = single_upstream(dag, idx);
            for sf in sort_fields {
                add_upstream_influence(
                    &mut inf,
                    upstream_col(lineage, up, &sf.field),
                    IndirectSub::Sort,
                );
            }
        }
        PlanNode::Reshape { compiled_rules, .. } => {
            // A `when:` trigger cannot read `$doc`: the planner rejects any
            // envelope reference in a Reshape rule (E200, see `bind_reshape`), so
            // only record columns reach here.
            let up = single_upstream(dag, idx);
            for rule in compiled_rules.iter() {
                let mut reads = HashSet::new();
                program_support_into(&rule.when.program, &mut reads);
                for col in &reads {
                    add_upstream_influence(
                        &mut inf,
                        upstream_col(lineage, up, base_col(col)),
                        IndirectSub::Conditional,
                    );
                }
            }
        }
        PlanNode::Combine {
            resolved_column_map,
            driving_upstream,
            decomposed_predicate: Some(dp),
            ..
        } => {
            let sides = combine_sides(dag, idx, resolved_column_map, *driving_upstream);
            let mut refs = Vec::new();
            let mut doc_paths = Vec::new();
            for eq in &dp.equalities {
                collect_field_refs(&eq.left_expr, &mut refs);
                collect_field_refs(&eq.right_expr, &mut refs);
                collect_doc_paths_in_expr(&eq.left_expr, &mut doc_paths);
                collect_doc_paths_in_expr(&eq.right_expr, &mut doc_paths);
            }
            for range in &dp.ranges {
                collect_field_refs(&range.left_expr, &mut refs);
                collect_field_refs(&range.right_expr, &mut refs);
                collect_doc_paths_in_expr(&range.left_expr, &mut doc_paths);
                collect_doc_paths_in_expr(&range.right_expr, &mut doc_paths);
            }
            // Non-equi / non-range conjuncts (`!=`, `or`, function predicates)
            // live in the residual program; their columns still influence the join.
            if let Some(residual) = &dp.residual {
                collect_stmt_field_refs(&residual.program.statements, &mut refs);
                collect_doc_paths_in_stmts(&residual.program.statements, &mut doc_paths);
            }
            for qf in &refs {
                let Some((side, cidx)) = resolve_side(qf, sides.map, &sides.bare) else {
                    continue;
                };
                let resolved = match side {
                    JoinSide::Probe => sides.probe.as_ref(),
                    JoinSide::Build => sides.build.as_ref(),
                };
                if let Some((up_id, schema)) = resolved
                    && let Some(col) = schema.column_name(cidx as usize)
                {
                    add_upstream_influence(
                        &mut inf,
                        lineage.get(up_id).and_then(|m| m.0.get(col)),
                        IndirectSub::Join,
                    );
                }
            }
            // A `$doc` read in the join predicate carries no source qualifier, so
            // it attributes to the combine's driving document context
            // (`node_doc_srcs`), mirroring the DIRECT combine-body `$doc` rule.
            add_doc_influence(
                &mut inf,
                &doc_paths,
                node_doc_srcs,
                declared_sections,
                IndirectSub::Join,
            );
        }
        _ => {}
    }

    inf
}

/// Add every Source terminal in `up_terms` to `inf` as an INDIRECT influence of
/// subtype `sub` (the DIRECT subtype of the upstream path is irrelevant — only
/// the source column identity matters for whole-dataset influence).
fn add_upstream_influence(inf: &mut InfluenceMap, up_terms: Option<&TermMap>, sub: IndirectSub) {
    if let Some(terms) = up_terms {
        for terminal in terms.keys() {
            inf.entry(terminal.clone()).or_default().insert(sub);
        }
    }
}

/// Add each `$doc` envelope read in `doc_paths` to `inf` as an INDIRECT influence
/// of subtype `sub`. A `$doc` read names a section/field, not a record column, so
/// it resolves directly to each feeding `doc_srcs` dataset whose envelope
/// declares the read section (`declared_sections`) — never through the upstream
/// lineage map — with the rendered `$doc.<section>.<field>` path as its `field`.
/// The INDIRECT counterpart of [`merge_doc_read_terms`].
fn add_doc_influence(
    inf: &mut InfluenceMap,
    doc_paths: &[DocPath],
    doc_srcs: &BTreeSet<DatasetId>,
    declared_sections: &HashMap<DatasetId, BTreeSet<String>>,
    sub: IndirectSub,
) {
    for path in doc_paths {
        for terminal in doc_read_terminals(path, doc_srcs, declared_sections) {
            inf.entry(terminal).or_default().insert(sub);
        }
    }
}

// ---------------------------------------------------------------------------
// Output facet assembly
// ---------------------------------------------------------------------------

/// Accumulator for one sink dataset: its per-column DIRECT terminals and
/// whole-dataset INDIRECT influences, merged across every Output node that
/// resolves to the same dataset.
struct OutputAcc {
    dataset: DatasetId,
    fields: BTreeMap<String, TermMap>,
    influence: InfluenceMap,
}

impl OutputAcc {
    fn into_output(self) -> OutputColumnLineage {
        let fields = self
            .fields
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
        OutputColumnLineage {
            dataset: self.dataset,
            facet: ColumnLineageDatasetFacet {
                producer: PRODUCER.to_string(),
                schema_url: COLUMN_LINEAGE_FACET_SCHEMA_URL.to_string(),
                fields,
                dataset: indirect_input_fields(&self.influence),
            },
        }
    }
}

/// Accumulate one Output node's DIRECT columns and INDIRECT influences into the
/// sink dataset's entry, unioning with any prior Output node naming it.
fn record_output(
    acc: &mut Vec<OutputAcc>,
    dataset: DatasetId,
    cols: &ColumnTerminals,
    influence: &InfluenceMap,
) {
    if let Some(existing) = acc.iter_mut().find(|o| o.dataset == dataset) {
        for (col, terms) in &cols.0 {
            if terms.is_empty() {
                continue;
            }
            let slot = existing.fields.entry(col.clone()).or_default();
            merge_terminals(slot, Some(terms), Subtype::Identity);
        }
        for (terminal, subs) in influence {
            existing
                .influence
                .entry(terminal.clone())
                .or_default()
                .extend(subs.iter().copied());
        }
        return;
    }
    let fields = cols
        .0
        .iter()
        .filter(|(_, terms)| !terms.is_empty())
        .map(|(col, terms)| (col.clone(), terms.clone()))
        .collect();
    acc.push(OutputAcc {
        dataset,
        fields,
        influence: influence.clone(),
    });
}

/// An [`InfluenceMap`] → the facet's whole-dataset INDIRECT `dataset[]`: one
/// `InputField` per Source terminal, carrying one INDIRECT transformation per
/// influence subtype.
fn indirect_input_fields(influence: &InfluenceMap) -> Vec<InputField> {
    influence
        .iter()
        .map(|(terminal, subs)| InputField {
            namespace: terminal.namespace.clone(),
            name: terminal.name.clone(),
            field: terminal.field.clone(),
            transformations: subs
                .iter()
                .map(|sub| Transformation {
                    transformation_type: TransformationType::Indirect,
                    subtype: Some(sub.to_openlineage()),
                    description: None,
                    masking: None,
                })
                .collect(),
        })
        .collect()
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

    /// One INDIRECT `file:`-namespaced input field carrying the given subtypes.
    fn indirect(name: &str, field: &str, subtypes: &[TransformationSubtype]) -> InputField {
        InputField {
            namespace: "file".to_string(),
            name: name.to_string(),
            field: field.to_string(),
            transformations: subtypes
                .iter()
                .map(|s| Transformation {
                    transformation_type: TransformationType::Indirect,
                    subtype: Some(*s),
                    description: None,
                    masking: None,
                })
                .collect(),
        }
    }

    /// The output dataset whose name contains `substr` (for multi-output plans).
    fn output_named<'a>(lineage: &'a PlanColumnLineage, substr: &str) -> &'a OutputColumnLineage {
        lineage
            .outputs
            .iter()
            .find(|o| o.dataset.name.contains(substr))
            .unwrap_or_else(|| panic!("no output dataset containing {substr:?}"))
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

    // -- code-review regressions ----------------------------------------------

    #[test]
    fn transform_nested_ref_resolves_to_base_column() {
        // `emit city = addr.city` (struct/nested access) attributes to the base
        // source column `addr` instead of dropping the output column entirely.
        let yaml = r#"
pipeline: { name: tq }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: json
      options: { format: ndjson }
      path: data/rows.ndjson
      schema:
        - { name: addr, type: any }
  - type: transform
    name: t
    input: rows
    config: { cxl: "emit city = addr.city" }
  - type: output
    name: out
    input: t
    config:
      name: out
      type: json
      options: { format: ndjson }
      path: out/tq.ndjson
      include_unmapped: false
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        assert_field(
            fields,
            "city",
            &[direct(
                "/w/data/rows.ndjson",
                "addr",
                TransformationSubtype::Identity,
            )],
        );
    }

    #[test]
    fn combine_residual_conjunct_columns_are_join_influence() {
        // A non-equi (`!=`) conjunct lands in the decomposed predicate's residual;
        // its columns still influence the join and must appear in `dataset[]`.
        let yaml = r#"
pipeline: { name: cr }
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: data/a.csv
      options: { has_header: true }
      schema:
        - { name: k, type: string }
        - { name: region, type: string }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: data/b.csv
      options: { has_header: true }
      schema:
        - { name: k, type: string }
        - { name: region, type: string }
  - type: combine
    name: j
    input: { a: a, b: b }
    config:
      where: "a.k == b.k and a.region != b.region"
      match: first
      on_miss: skip
      cxl: "emit out = a.k"
      propagate_ck: driver
  - type: output
    name: out
    input: j
    config: { name: out, type: csv, path: out/cr.csv }
"#;
        let lineage = lineage_of(yaml);
        use TransformationSubtype::Join;
        assert_eq!(
            only_output(&lineage).facet.dataset,
            vec![
                indirect("/w/data/a.csv", "k", &[Join]),
                indirect("/w/data/a.csv", "region", &[Join]),
                indirect("/w/data/b.csv", "k", &[Join]),
                indirect("/w/data/b.csv", "region", &[Join]),
            ],
        );
    }

    #[test]
    fn let_in_emit_each_body_does_not_leak() {
        // A `let` inside an `emit each` body is block-scoped: it must not shadow
        // a same-named outer binding seen by a later statement. Here the body's
        // `let total = it` must not survive, so `emit out = total` resolves to the
        // outer `let total = base`, not the loop array.
        let yaml = r#"
pipeline: { name: ll }
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      options: { format: ndjson }
      path: data/s.ndjson
      schema:
        - { name: base, type: string }
        - { name: items, type: any }
  - type: transform
    name: t
    input: s
    config:
      cxl: |
        let total = base
        emit each it in items {
          let total = it
        }
        emit out = total
  - type: output
    name: out
    input: t
    config:
      name: out
      type: json
      options: { format: ndjson }
      path: out/ll.ndjson
      include_unmapped: false
      exclude: [items]
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        assert_field(
            fields,
            "out",
            &[direct(
                "/w/data/s.ndjson",
                "base",
                TransformationSubtype::Identity,
            )],
        );
    }

    // -- INDIRECT influence --------------------------------------------

    #[test]
    fn route_predicate_columns_are_filter_influence_not_per_column() {
        // A Route condition column influences every output port as FILTER, landing
        // in the top-level `dataset[]` — not duplicated into per-column lineage.
        let yaml = r#"
pipeline: { name: r }
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: data/s.csv
      options: { has_header: true }
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
  - type: route
    name: split
    input: s
    config:
      mode: exclusive
      conditions: { high: "amount > 100" }
      default: low
  - type: output
    name: hi
    input: split.high
    config: { name: hi, type: csv, path: out/hi.csv }
  - type: output
    name: lo
    input: split.low
    config: { name: lo, type: csv, path: out/lo.csv }
"#;
        let lineage = lineage_of(yaml);
        let src = "/w/data/s.csv";
        let hi = output_named(&lineage, "hi.csv");
        use TransformationSubtype::{Filter, Identity};
        assert_eq!(
            hi.facet.dataset,
            vec![indirect(src, "amount", &[Filter])],
            "route condition column is FILTER influence"
        );
        // The influence is NOT duplicated into the per-column DIRECT lineage:
        // `amount` still passes through as a plain DIRECT identity column.
        assert_field(
            &hi.facet.fields,
            "amount",
            &[direct(src, "amount", Identity)],
        );
        for fl in hi.facet.fields.values() {
            assert!(
                fl.input_fields
                    .iter()
                    .flat_map(|f| &f.transformations)
                    .all(|t| t.transformation_type == TransformationType::Direct),
                "per-column lineage must stay DIRECT-only"
            );
        }
    }

    #[test]
    fn cull_drop_predicate_columns_are_filter_influence() {
        let yaml = r#"
pipeline: { name: c }
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: data/s.csv
      options: { has_header: true }
      schema:
        - { name: employee_id, type: string }
        - { name: amount, type: int }
  - type: cull
    name: trim
    input: s
    config:
      partition_by: [employee_id]
      removed_to: removed
      rules:
        - name: drop_big
          drop_group_when: "sum(amount) > 100"
  - type: output
    name: out
    input: trim
    config: { name: out, type: csv, path: out/c.csv }
  - type: output
    name: audit
    input: trim.removed
    config: { name: audit, type: csv, path: out/audit.csv }
"#;
        let lineage = lineage_of(yaml);
        let src = "/w/data/s.csv";
        let out = output_named(&lineage, "c.csv");
        use TransformationSubtype::{Filter, GroupBy};
        // The drop predicate column is FILTER; the partition key is GROUP_BY
        // (it determines the per-group drop decision). Sorted by field name.
        assert_eq!(
            out.facet.dataset,
            vec![
                indirect(src, "amount", &[Filter]),
                indirect(src, "employee_id", &[GroupBy]),
            ],
        );
    }

    #[test]
    fn aggregation_group_by_columns_are_group_by_influence() {
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
        - { name: amount, type: int }
  - type: aggregate
    name: agg
    input: sales
    config:
      group_by: [region]
      cxl: |
        emit region = region
        emit total = sum(amount)
  - type: output
    name: out
    input: agg
    config: { name: out, type: csv, path: out/a.csv }
"#;
        let lineage = lineage_of(yaml);
        assert_eq!(
            only_output(&lineage).facet.dataset,
            vec![indirect(
                "/w/data/sales.csv",
                "region",
                &[TransformationSubtype::GroupBy]
            )],
        );
    }

    #[test]
    fn combine_join_keys_are_join_influence() {
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
        emit actor = events.actor
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config: { name: out, type: csv, path: out/c.csv }
"#;
        let lineage = lineage_of(yaml);
        use TransformationSubtype::Join;
        assert_eq!(
            only_output(&lineage).facet.dataset,
            vec![
                indirect("/w/data/events.csv", "order_id", &[Join]),
                indirect("/w/data/orders.csv", "order_id", &[Join]),
            ],
        );
    }

    #[test]
    fn reshape_when_guard_columns_are_conditional_influence() {
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
        let src = "/w/data/plans.csv";
        use TransformationSubtype::Conditional;
        // Sorted by (namespace, name, field): plan_end before plan_start.
        assert_eq!(
            only_output(&lineage).facet.dataset,
            vec![
                indirect(src, "plan_end", &[Conditional]),
                indirect(src, "plan_start", &[Conditional]),
            ],
        );
    }

    #[test]
    fn correlation_sort_key_is_sort_influence() {
        // `PlanNode::Sort` is engine-synthesized only: a source `correlation_key`
        // makes the enforcer inject a `__correlation_sort_<src>` Sort whose sort
        // keys are the (real) correlation columns.
        let yaml = r#"
pipeline: { name: cs }
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: data/orders.csv
      options: { has_header: true }
      correlation_key: [region]
      schema:
        - { name: region, type: string }
        - { name: order_id, type: string }
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: data/events.csv
      options: { has_header: true }
      correlation_key: [region]
      schema:
        - { name: region, type: string }
        - { name: order_id, type: string }
  - type: combine
    name: joined
    input: { orders: orders, events: events }
    config:
      where: "orders.order_id == events.order_id"
      match: first
      on_miss: skip
      cxl: |
        emit oid = orders.order_id
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config: { name: out, type: csv, path: out/cs.csv }
"#;
        let lineage = lineage_of(yaml);
        use TransformationSubtype::{Join, Sort};
        assert_eq!(
            only_output(&lineage).facet.dataset,
            vec![
                indirect("/w/data/events.csv", "order_id", &[Join]),
                indirect("/w/data/events.csv", "region", &[Sort]),
                indirect("/w/data/orders.csv", "order_id", &[Join]),
                indirect("/w/data/orders.csv", "region", &[Sort]),
            ],
        );
    }

    #[test]
    fn indirect_influence_accumulates_downstream() {
        // A filter (Cull) upstream of a group-by (Aggregation) — both influences
        // reach the final output's `dataset[]`.
        let yaml = r#"
pipeline: { name: acc }
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: data/s.csv
      options: { has_header: true }
      schema:
        - { name: region, type: string }
        - { name: amount, type: int }
  - type: cull
    name: trim
    input: s
    config:
      partition_by: [region]
      removed_to: removed
      rules:
        - name: drop_big
          drop_group_when: "sum(amount) > 1000"
  - type: aggregate
    name: agg
    input: trim
    config:
      group_by: [region]
      cxl: |
        emit region = region
        emit total = sum(amount)
  - type: output
    name: out
    input: agg
    config: { name: out, type: csv, path: out/acc.csv }
  - type: output
    name: audit
    input: trim.removed
    config: { name: audit, type: csv, path: out/audit.csv }
"#;
        let lineage = lineage_of(yaml);
        let src = "/w/data/s.csv";
        use TransformationSubtype::{Filter, GroupBy};
        // amount (cull filter) and region (group-by) both accumulate at the sink.
        assert_eq!(
            output_named(&lineage, "acc.csv").facet.dataset,
            vec![
                indirect(src, "amount", &[Filter]),
                indirect(src, "region", &[GroupBy]),
            ],
        );
    }

    // -- let / emit each / duplicate emit -------------------------------------

    #[test]
    fn let_bound_intermediate_resolves_to_its_source_reads() {
        // `let x = a + b; emit y = x` — `y` derives from `a` and `b` even though
        // the emit RHS names only the let binding.
        let yaml = r#"
pipeline: { name: letbind }
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: data/s.csv
      options: { has_header: true }
      schema:
        - { name: a, type: int }
        - { name: b, type: int }
  - type: transform
    name: t
    input: s
    config:
      cxl: |
        let x = a + b
        emit y = x
  - type: output
    name: out
    input: t
    config: { name: out, type: csv, path: out/letbind.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        let src = "/w/data/s.csv";
        use TransformationSubtype::Transformation;
        assert_field(
            fields,
            "y",
            &[
                direct(src, "a", Transformation),
                direct(src, "b", Transformation),
            ],
        );
    }

    #[test]
    fn emit_each_resolves_body_to_the_source_array_column() {
        // `emit each it in items { emit sku = it["sku"] }` — `sku` derives from the
        // `items` source array column, not from nothing.
        let yaml = r#"
pipeline: { name: explode }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: json
      options: { format: ndjson }
      path: data/rows.ndjson
      schema:
        - { name: items, type: any }
  - type: transform
    name: explode
    input: rows
    config:
      cxl: |
        emit each it in items {
          emit sku = it["sku"]
        }
  - type: output
    name: out
    input: explode
    config:
      name: out
      type: json
      options: { format: ndjson }
      path: out/explode.ndjson
      include_unmapped: false
      exclude: [items]
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        assert_field(
            fields,
            "sku",
            &[direct(
                "/w/data/rows.ndjson",
                "items",
                TransformationSubtype::Transformation,
            )],
        );
    }

    #[test]
    fn duplicate_emit_is_last_wins() {
        // Two top-level emits to one column: only the last reaches the sink, so the
        // lineage lists only the last source (matching the runtime).
        let yaml = r#"
pipeline: { name: dup }
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: data/s.csv
      options: { has_header: true }
      schema:
        - { name: list_price, type: float }
        - { name: sale_price, type: float }
  - type: transform
    name: t
    input: s
    config:
      cxl: |
        emit price = list_price
        emit price = sale_price
  - type: output
    name: out
    input: t
    config: { name: out, type: csv, path: out/dup.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        assert_field(
            fields,
            "price",
            &[direct(
                "/w/data/s.csv",
                "sale_price",
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

    #[test]
    fn agg_residual_qualified_ref_contributes_a_leaf() {
        // A qualified field ref in an aggregate residual (low-likelihood — an
        // aggregate is single-input) is no longer silently dropped: its base
        // column contributes a leaf instead of falling through the catch-all.
        use cxl::ast::NodeId;
        use cxl::lexer::Span;
        let expr = Expr::QualifiedFieldRef {
            node_id: NodeId(0),
            parts: vec!["amount".into(), "cents".into()].into(),
            span: Span::default(),
        };
        let mut leaves = Vec::new();
        collect_agg_leaves(&expr, &mut leaves);
        assert!(
            matches!(leaves.as_slice(), [AggLeaf::Field("amount")]),
            "qualified ref base column must contribute a leaf"
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

        // JOIN influence accumulates across both decomposed steps, each join key
        // resolved to its true source (not the intermediate encoded column).
        use TransformationSubtype::Join;
        assert_eq!(
            only_output(&lineage).facet.dataset,
            vec![
                indirect("/w/data/categories.csv", "category_id", &[Join]),
                indirect("/w/data/orders.csv", "product_id", &[Join]),
                indirect("/w/data/products.csv", "category_id", &[Join]),
                indirect("/w/data/products.csv", "product_id", &[Join]),
            ],
        );
    }

    #[test]
    fn four_input_combine_resolves_every_column_through_all_steps() {
        // A 4-input join decomposes into three binary steps; columns from the
        // earliest-joined inputs flow through two intermediate steps and must
        // still resolve to their true source by IDENTITY.
        let yaml = r#"
pipeline: { name: four }
nodes:
  - type: source
    name: a
    config: { name: a, type: csv, path: data/a.csv, options: { has_header: true }, schema: [ { name: id, type: string }, { name: a_val, type: string } ] }
  - type: source
    name: b
    config: { name: b, type: csv, path: data/b.csv, options: { has_header: true }, schema: [ { name: id, type: string }, { name: b_val, type: string } ] }
  - type: source
    name: c
    config: { name: c, type: csv, path: data/c.csv, options: { has_header: true }, schema: [ { name: id, type: string }, { name: c_val, type: string } ] }
  - type: source
    name: d
    config: { name: d, type: csv, path: data/d.csv, options: { has_header: true }, schema: [ { name: id, type: string }, { name: d_val, type: string } ] }
  - type: combine
    name: j
    input: { a: a, b: b, c: c, d: d }
    config:
      where: "a.id == b.id and b.id == c.id and c.id == d.id"
      match: first
      on_miss: skip
      cxl: |
        emit a_val = a.a_val
        emit b_val = b.b_val
        emit c_val = c.c_val
        emit d_val = d.d_val
      propagate_ck: driver
  - type: output
    name: out
    input: j
    config: { name: out, type: csv, path: out/four.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        use TransformationSubtype::Identity;
        assert_field(
            fields,
            "a_val",
            &[direct("/w/data/a.csv", "a_val", Identity)],
        );
        assert_field(
            fields,
            "b_val",
            &[direct("/w/data/b.csv", "b_val", Identity)],
        );
        assert_field(
            fields,
            "c_val",
            &[direct("/w/data/c.csv", "c_val", Identity)],
        );
        assert_field(
            fields,
            "d_val",
            &[direct("/w/data/d.csv", "d_val", Identity)],
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
        assert_field(
            fields,
            "emp_name",
            &[direct("/w/data/emp.csv", "name", Identity)],
        );
        // The build side resolves to the same source dataset/column.
        assert_field(
            fields,
            "mgr_name",
            &[direct("/w/data/emp.csv", "name", Identity)],
        );
    }

    // -- Envelope ($doc) lineage ----------------------------------------------

    /// One XML source declaring `BatchInfo` (`batch_id`, `nval`) and `Summary`
    /// (`total`), feeding a `transform` whose `cxl` body is spliced in. The glob
    /// `data/*.xml` resolves to the directory dataset `/w/data`.
    fn envelope_pipeline(body_cxl: &str) -> String {
        let indented: String = body_cxl.lines().map(|l| format!("        {l}\n")).collect();
        format!(
            r#"
pipeline: {{ name: env }}
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: data/*.xml
      options: {{ record_path: doc/records/record }}
      envelope:
        sections:
          BatchInfo:
            extract: {{ xml_path: "/doc/BatchInfo" }}
            fields:
              batch_id: string
              nval: int
          Summary:
            extract: {{ xml_path: "/doc/Summary" }}
            fields:
              total: int
      schema:
        - {{ name: amount, type: int }}
  - type: transform
    name: tag
    input: payments
    config:
      cxl: |
{indented}  - type: output
    name: out
    input: tag
    config: {{ name: out, type: csv, path: out/env.csv }}
"#
        )
    }

    #[test]
    fn bare_doc_access_is_direct_identity_on_the_source() {
        // A `$doc`-only column previously resolved to an empty term set and was
        // omitted; it now carries a DIRECT IDENTITY terminal naming the source
        // dataset and the rendered `$doc.<section>.<field>` path.
        let lineage = lineage_of(&envelope_pipeline(
            "emit amount = amount\nemit batch = $doc.BatchInfo.batch_id",
        ));
        let src = "/w/data";
        let fields = &only_output(&lineage).facet.fields;
        use TransformationSubtype::Identity;
        assert_field(fields, "amount", &[direct(src, "amount", Identity)]);
        assert_field(
            fields,
            "batch",
            &[direct(src, "$doc.BatchInfo.batch_id", Identity)],
        );
        assert_eq!(fields.len(), 2, "no extra/omitted columns");
    }

    #[test]
    fn doc_access_inside_an_expression_is_transformation() {
        // A doc access that is one leaf of a larger expression is TRANSFORMATION,
        // alongside the record column the expression also reads. InputFields sort
        // by field name: `$` precedes `a`.
        let lineage = lineage_of(&envelope_pipeline(
            "emit amount = amount\nemit x = $doc.BatchInfo.nval + amount",
        ));
        let src = "/w/data";
        let fields = &only_output(&lineage).facet.fields;
        use TransformationSubtype::Transformation;
        assert_field(
            fields,
            "x",
            &[
                direct(src, "$doc.BatchInfo.nval", Transformation),
                direct(src, "amount", Transformation),
            ],
        );
    }

    #[test]
    fn indexed_doc_access_renders_its_literal_index() {
        // A literal index segment is rendered into the `field` path verbatim, and
        // an indexed-but-whole-value doc read is still IDENTITY.
        let lineage = lineage_of(&envelope_pipeline(
            "emit amount = amount\nemit first = $doc.Summary.total[0]",
        ));
        let src = "/w/data";
        let fields = &only_output(&lineage).facet.fields;
        assert_field(
            fields,
            "first",
            &[direct(
                src,
                "$doc.Summary.total[0]",
                TransformationSubtype::Identity,
            )],
        );
    }

    #[test]
    fn doc_access_through_a_let_binding_resolves_to_the_source() {
        // `let b = $doc...; emit batch = b` — the binding's terminals expand at
        // the use site, so the doc read survives the indirection as IDENTITY.
        let lineage = lineage_of(&envelope_pipeline(
            "emit amount = amount\nlet b = $doc.BatchInfo.batch_id\nemit batch = b",
        ));
        let src = "/w/data";
        let fields = &only_output(&lineage).facet.fields;
        assert_field(
            fields,
            "batch",
            &[direct(
                src,
                "$doc.BatchInfo.batch_id",
                TransformationSubtype::Identity,
            )],
        );
    }

    #[test]
    fn combine_attributes_a_doc_read_to_the_driving_source_only() {
        // Both inputs declare `BatchInfo`, but a joined record carries only the
        // driving input's document context (the first-declared input absent row
        // stats). The body `$doc` read must attribute to the driver `drv` alone,
        // never the build side `bld`.
        let yaml = r#"
pipeline: { name: cdoc }
nodes:
  - type: source
    name: drv
    config:
      name: drv
      type: xml
      glob: data/drv/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
            fields:
              batch_id: string
      schema:
        - { name: k, type: string }
  - type: source
    name: bld
    config:
      name: bld
      type: xml
      glob: data/bld/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
            fields:
              batch_id: string
      schema:
        - { name: k, type: string }
  - type: combine
    name: j
    input: { drv: drv, bld: bld }
    config:
      where: "drv.k == bld.k"
      match: first
      on_miss: skip
      cxl: |
        emit k = drv.k
        emit batch = $doc.BatchInfo.batch_id
      propagate_ck: driver
  - type: output
    name: out
    input: j
    config: { name: out, type: csv, path: out/cdoc.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        assert_field(
            fields,
            "batch",
            &[direct(
                "/w/data/drv",
                "$doc.BatchInfo.batch_id",
                TransformationSubtype::Identity,
            )],
        );
    }

    #[test]
    fn doc_section_and_index_names_render_verbatim() {
        // The renderer reproduces the CXL surface spelling for arbitrary (e.g.
        // EDI/HL7 engine-fixed) section names and both index kinds.
        assert_eq!(
            render_doc_path(&DocPath {
                section: "transaction_set".into(),
                field: "control_number".into(),
                indices: vec![],
            }),
            "$doc.transaction_set.control_number"
        );
        assert_eq!(
            render_doc_path(&DocPath {
                section: "summary".into(),
                field: "rows".into(),
                indices: vec![DocIndex::Int(2), DocIndex::Key("k".into())],
            }),
            "$doc.summary.rows[2][\"k\"]"
        );
        assert_eq!(
            render_doc_path(&DocPath {
                section: "summary".into(),
                field: "items".into(),
                indices: vec![DocIndex::Int(-1)],
            }),
            "$doc.summary.items[-1]"
        );
        // A string key containing a quote / backslash is escaped, so the field
        // is unambiguous and two distinct keys never render to the same string.
        assert_eq!(
            render_doc_path(&DocPath {
                section: "meta".into(),
                field: "props".into(),
                indices: vec![DocIndex::Key("a\"b\\c".into())],
            }),
            "$doc.meta.props[\"a\\\"b\\\\c\"]"
        );
    }

    #[test]
    fn doc_read_against_an_undeclared_section_is_omitted() {
        // A SWIFT source's envelope schema is unvalidated, so the planner's
        // section-declaration check does not reject a `$doc` read against a
        // section the source never declares. Lineage must still not invent a
        // provenance edge for it: a read of a declared section is attributed,
        // a read of an undeclared section is omitted (no false edge).
        let yaml = r#"
pipeline: { name: swiftdoc }
nodes:
  - type: source
    name: message
    config:
      name: message
      type: swift
      glob: data/*.swift
      envelope:
        sections:
          basic:
            extract: { segment: "1" }
          app:
            extract: { segment: "2" }
      schema:
        - { name: tag, type: string }
  - type: transform
    name: tag
    input: message
    config:
      cxl: |
        emit tag = tag
        emit declared = $doc.basic.body
        emit ghost = $doc.nope.body
  - type: output
    name: out
    input: tag
    config: { name: out, type: csv, path: out/swiftdoc.csv }
"#;
        let lineage = lineage_of(yaml);
        let src = "/w/data";
        let fields = &only_output(&lineage).facet.fields;
        use TransformationSubtype::Identity;
        // The declared section attributes to the source.
        assert_field(
            fields,
            "declared",
            &[direct(src, "$doc.basic.body", Identity)],
        );
        // The undeclared section is dropped — the source's document cannot carry
        // it, so emitting an edge would assert false provenance.
        assert!(
            !fields.contains_key("ghost"),
            "a `$doc` read of an undeclared section must not be attributed"
        );
    }

    #[test]
    fn nary_combine_doc_read_attributes_to_the_driving_source_only() {
        // A joined record carries only the driving input's document context, so an
        // N-ary `$doc` body read attributes to the driver `a` alone — never fanned
        // out across the build-side inputs b/c (which would be false envelope
        // provenance). Each decomposition step re-pins its driver to the prior
        // intermediate, which carries `a`'s context, so the driver stays pinnable
        // through the chain. Every input declares `Head` so the read compiles.
        let yaml = r#"
pipeline: { name: narydoc }
nodes:
  - type: source
    name: a
    config:
      name: a
      type: xml
      glob: data/a/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields: { x: string }
      schema:
        - { name: k, type: string }
  - type: source
    name: b
    config:
      name: b
      type: xml
      glob: data/b/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields: { x: string }
      schema:
        - { name: k, type: string }
  - type: source
    name: c
    config:
      name: c
      type: xml
      glob: data/c/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields: { x: string }
      schema:
        - { name: k, type: string }
  - type: combine
    name: j
    input: { a: a, b: b, c: c }
    config:
      where: "a.k == b.k and b.k == c.k"
      match: first
      on_miss: skip
      cxl: |
        emit k = a.k
        emit head = $doc.Head.x
      propagate_ck: driver
  - type: output
    name: out
    input: j
    config: { name: out, type: csv, path: out/narydoc.csv }
"#;
        let lineage = lineage_of(yaml);
        let fields = &only_output(&lineage).facet.fields;
        // The envelope read resolves to the driver `a` only — no false edge to the
        // build-side sources `b` / `c`.
        assert_field(
            fields,
            "head",
            &[direct(
                "/w/data/a",
                "$doc.Head.x",
                TransformationSubtype::Identity,
            )],
        );
    }

    // -- Envelope ($doc) on aggregate / reshape / predicate surfaces ----------

    #[test]
    fn aggregate_emit_doc_read_is_direct_identity_on_the_source() {
        // A `$doc` read in an aggregate emit survives in the post-extraction
        // residual as a passthrough leaf (neither a group key nor an
        // accumulator); it carries a DIRECT IDENTITY terminal naming the source
        // and the rendered `$doc.…` path.
        let yaml = r#"
pipeline: { name: aggdoc }
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: data/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              batch_id: string
      schema:
        - { name: region, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: agg
    input: payments
    config:
      group_by: [region]
      cxl: |
        emit region = region
        emit total = sum(amount)
        emit batch = $doc.Head.batch_id
  - type: output
    name: out
    input: agg
    config: { name: out, type: csv, path: out/aggdoc.csv }
"#;
        let lineage = lineage_of(yaml);
        let src = "/w/data";
        let fields = &only_output(&lineage).facet.fields;
        use TransformationSubtype::{Aggregation, Identity};
        assert_field(fields, "region", &[direct(src, "region", Identity)]);
        assert_field(fields, "total", &[direct(src, "amount", Aggregation)]);
        assert_field(
            fields,
            "batch",
            &[direct(src, "$doc.Head.batch_id", Identity)],
        );
    }

    #[test]
    fn aggregate_emit_doc_read_inside_agg_arg_is_attributed() {
        // A `$doc` read nested inside an aggregate function argument is hoisted
        // by `extract_aggregates` into the accumulator binding — the residual
        // keeps only an `AggSlot` — so it must be recovered from the binding and
        // attributed as an AGGREGATION input on the source, not silently dropped.
        let yaml = r#"
pipeline: { name: aggdocarg }
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: data/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              fx_rate: int
      schema:
        - { name: region, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: agg
    input: payments
    config:
      group_by: [region]
      cxl: |
        emit region = region
        emit weighted = sum(amount * $doc.Head.fx_rate)
  - type: output
    name: out
    input: agg
    config: { name: out, type: csv, path: out/aggdocarg.csv }
"#;
        let lineage = lineage_of(yaml);
        let src = "/w/data";
        let fields = &only_output(&lineage).facet.fields;
        use TransformationSubtype::{Aggregation, Identity};
        assert_field(fields, "region", &[direct(src, "region", Identity)]);
        // The record column and the envelope read both feed the accumulator, so
        // both are AGGREGATION inputs on the source.
        assert_field(
            fields,
            "weighted",
            &[
                direct(src, "$doc.Head.fx_rate", Aggregation),
                direct(src, "amount", Aggregation),
            ],
        );
    }

    #[test]
    fn reshape_rule_rejects_doc_reference_at_compile_time() {
        // Reshape cannot read `$doc` in any rule fragment: it re-runs its rules
        // after a per-group spill that drops envelope context, so the planner
        // fails the compile (E200) rather than resolve `$doc` to null. There is
        // therefore no Reshape `$doc` surface for lineage to attribute — this
        // pins that contract so a future relaxation revisits the lineage arm.
        let yaml = r#"
pipeline: { name: rsdoc }
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: data/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              batch_id: string
      schema:
        - { name: region, type: string }
        - { name: amount, type: int }
        - { name: tag, type: string }
  - type: reshape
    name: tagit
    input: payments
    config:
      partition_by: [region]
      rules:
        - name: stamp
          when: "amount > 0"
          mutate:
            set:
              tag: "$doc.Head.batch_id"
  - type: output
    name: out
    input: tagit
    config: { name: out, type: csv, path: out/rsdoc.csv }
"#;
        let err = parse_config(yaml)
            .expect("parse_config")
            .compile(&CompileContext::default())
            .expect_err("Reshape `$doc` reference must fail to compile");
        assert!(
            err.iter()
                .any(|d| d.code == "E200" && d.message.contains("references `$doc`")),
            "expected an E200 `$doc` rejection, got: {err:?}"
        );
    }

    #[test]
    fn route_predicate_doc_read_is_filter_influence() {
        // A Route condition that tests the envelope (`amount > $doc.Head.threshold`)
        // attributes BOTH the record column `amount` and the envelope read as
        // FILTER influence.
        let yaml = r#"
pipeline: { name: rtdoc }
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: data/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              threshold: int
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
  - type: route
    name: split
    input: payments
    config:
      mode: exclusive
      conditions: { high: "amount > $doc.Head.threshold" }
      default: low
  - type: output
    name: hi
    input: split.high
    config: { name: hi, type: csv, path: out/hi.csv }
  - type: output
    name: lo
    input: split.low
    config: { name: lo, type: csv, path: out/lo.csv }
"#;
        let lineage = lineage_of(yaml);
        let src = "/w/data";
        let hi = output_named(&lineage, "hi.csv");
        use TransformationSubtype::Filter;
        // Sorted by field: `$doc.Head.threshold` (leading `$`) before `amount`.
        assert_eq!(
            hi.facet.dataset,
            vec![
                indirect(src, "$doc.Head.threshold", &[Filter]),
                indirect(src, "amount", &[Filter]),
            ],
        );
    }

    #[test]
    fn combine_where_doc_read_is_join_influence() {
        // A `$doc` read in a combine `where:` residual attributes as JOIN
        // influence to the combine's driving document context only — never the
        // build side — mirroring the DIRECT combine-body `$doc` rule.
        let yaml = r#"
pipeline: { name: cjdoc }
nodes:
  - type: source
    name: drv
    config:
      name: drv
      type: xml
      glob: data/drv/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              threshold: int
      schema:
        - { name: k, type: string }
        - { name: amount, type: int }
  - type: source
    name: bld
    config:
      name: bld
      type: xml
      glob: data/bld/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              threshold: int
      schema:
        - { name: k, type: string }
  - type: combine
    name: j
    input: { drv: drv, bld: bld }
    config:
      where: "drv.k == bld.k and drv.amount > $doc.Head.threshold"
      match: first
      on_miss: skip
      cxl: |
        emit k = drv.k
      propagate_ck: driver
  - type: output
    name: out
    input: j
    config: { name: out, type: csv, path: out/cjdoc.csv }
"#;
        let lineage = lineage_of(yaml);
        use TransformationSubtype::Join;
        // Sorted by (name, field): bld < drv; within drv `$doc.…` < `amount` < `k`.
        assert_eq!(
            only_output(&lineage).facet.dataset,
            vec![
                indirect("/w/data/bld", "k", &[Join]),
                indirect("/w/data/drv", "$doc.Head.threshold", &[Join]),
                indirect("/w/data/drv", "amount", &[Join]),
                indirect("/w/data/drv", "k", &[Join]),
            ],
        );
    }

    #[test]
    fn cull_drop_predicate_doc_read_is_filter_influence() {
        // A Cull `drop_group_when` predicate that tests the envelope attributes
        // the envelope read as FILTER influence alongside the record column.
        // Cull (unlike Reshape) folds its decision before any spill, so `$doc`
        // is supported there and reaches the lineage builder.
        let yaml = r#"
pipeline: { name: cudoc }
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: data/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          Head:
            extract: { xml_path: "/doc/Head" }
            fields:
              threshold: int
      schema:
        - { name: region, type: string }
        - { name: amount, type: int }
  - type: cull
    name: trim
    input: payments
    config:
      partition_by: [region]
      removed_to: removed
      rules:
        - name: drop_over_budget
          drop_group_when: "sum(amount) > $doc.Head.threshold"
  - type: output
    name: out
    input: trim
    config: { name: out, type: csv, path: out/cudoc.csv }
  - type: output
    name: audit
    input: trim.removed
    config: { name: audit, type: csv, path: out/audit.csv }
"#;
        let lineage = lineage_of(yaml);
        let src = "/w/data";
        let out = output_named(&lineage, "cudoc.csv");
        use TransformationSubtype::{Filter, GroupBy};
        // Sorted by (name, field): `$doc.Head.threshold` (leading `$`) < `amount`
        // < `region`. The partition key `region` is GROUP_BY; the predicate's
        // record column `amount` and envelope read are FILTER.
        assert_eq!(
            out.facet.dataset,
            vec![
                indirect(src, "$doc.Head.threshold", &[Filter]),
                indirect(src, "amount", &[Filter]),
                indirect(src, "region", &[GroupBy]),
            ],
        );
    }
}
