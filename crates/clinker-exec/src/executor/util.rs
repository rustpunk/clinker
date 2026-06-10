//! Cross-cutting executor utilities: dispatch-order scheduling,
//! record-schema reshaping, correlation-key copy, and small config/plan
//! lookups shared across the dispatch arms.

use std::collections::HashSet;
use std::sync::Arc;

use clinker_record::{Record, Schema, SchemaBuilder};
use indexmap::IndexMap;

use clinker_plan::config::PipelineConfig;

/// Resolve the dispatch order for a topological pass via the memory
/// arbitrator's [`next_runnable`](crate::pipeline::memory::MemoryArbitrator::next_runnable)
/// instead of walking `topo_order` blindly.
///
/// `candidates` is the subset of nodes this pass dispatches — the
/// init-phase ancestor closure on Pass 1, its complement on Pass 2, or
/// the whole DAG when there is no init phase. The returned vector lists
/// those same nodes in the order the executor should dispatch them.
///
/// At each step the runnable frontier is every not-yet-emitted candidate
/// whose graph predecessors *within `candidates`* are all emitted. The
/// arbitrator picks one frontier node by predicted memory impact (headroom
/// fit, then largest immediate freed-on-complete, then largest downstream
/// subtree reclaim, then stable topo index); finishing the chosen node may
/// open new frontier members for the next step.
///
/// Restricting the predecessor check to `candidates` is what lets Pass 1
/// run before Pass 2: an init-phase node is gated only by its init-phase
/// ancestors, never by runtime-DAG nodes that have not been dispatched
/// yet. The init-phase closure is exactly the set of init Transforms plus
/// their transitive upstream ancestors, so every init node's
/// non-init predecessors are themselves impossible — the restriction is
/// total within the closure, not a relaxation of any real dependency.
///
/// Determinism: with no volume estimates every candidate's
/// `predicted_peak_bytes` is `0`, so the arbitrator's headroom, freed-bytes,
/// and subtree-reclaim tiers are all-equal and selection collapses to the
/// lowest stable topo index. Greedily taking the lowest-topo-index frontier node
/// reproduces a plain front-to-back walk of `topo_order` byte-for-byte —
/// scheduling reorders the dispatch sequence only when real estimates
/// distinguish the candidates, and never changes record output, which is
/// independent of dispatch order.
pub(super) fn scheduled_pass_order(
    plan: &clinker_plan::plan::execution::ExecutionPlanDag,
    arbitrator: &crate::pipeline::memory::MemoryArbitrator,
    candidates: &HashSet<petgraph::graph::NodeIndex>,
    combine_probe_edges: &std::collections::HashMap<
        petgraph::graph::NodeIndex,
        petgraph::graph::NodeIndex,
    >,
) -> Vec<petgraph::graph::NodeIndex> {
    use clinker_plan::plan::scheduling_hint::SchedulingHint;

    let mut emitted: HashSet<petgraph::graph::NodeIndex> = HashSet::with_capacity(candidates.len());
    let mut order: Vec<petgraph::graph::NodeIndex> = Vec::with_capacity(candidates.len());
    let hint: &dyn SchedulingHint = plan;

    // Build-before-probe ordering (issue #300). A streaming-probe driver
    // producer must not be selected until the consuming Combine's build-side
    // predecessor cone has materialized — the Combine arm builds the hash
    // table on the main thread before redispatching the driver, and pinning
    // that order here keeps `--explain`'s dispatch sequence and the runtime
    // coherent. The build side is every incoming neighbor of the Combine
    // except the driver itself; a driver is gated until all such build
    // predecessors that are in this pass's candidate set are emitted. This
    // is acyclic because a certified driver is single-outgoing into the
    // Combine and never feeds the build cone, so the build predecessors
    // become runnable independently and eventually unblock the driver.
    let build_ready = |driver: petgraph::graph::NodeIndex,
                       emitted: &HashSet<petgraph::graph::NodeIndex>|
     -> bool {
        let Some(&combine_idx) = combine_probe_edges.get(&driver) else {
            return true;
        };
        plan.graph
            .neighbors_directed(combine_idx, petgraph::Direction::Incoming)
            .filter(|&pred| pred != driver)
            .all(|build_pred| !candidates.contains(&build_pred) || emitted.contains(&build_pred))
    };

    // A node is runnable once every in-`candidates` predecessor is
    // already emitted. Recomputed each step from `emitted` so that
    // finishing a node exposes its now-unblocked successors.
    while order.len() < candidates.len() {
        let runnable: Vec<petgraph::graph::NodeIndex> = candidates
            .iter()
            .copied()
            .filter(|idx| !emitted.contains(idx))
            .filter(|&idx| {
                plan.graph
                    .neighbors_directed(idx, petgraph::Direction::Incoming)
                    .all(|pred| !candidates.contains(&pred) || emitted.contains(&pred))
            })
            .filter(|&idx| build_ready(idx, &emitted))
            .collect();

        // The candidate set is the node set of an acyclic graph, so as
        // long as work remains at least one node has all in-set
        // predecessors satisfied. An empty frontier with work left would
        // mean a cycle, which the planner forbids.
        debug_assert!(
            !runnable.is_empty(),
            "scheduled_pass_order: empty runnable frontier with {} candidates left to emit",
            candidates.len() - order.len()
        );

        let chosen = arbitrator.next_runnable(&runnable, hint);
        emitted.insert(chosen);
        order.push(chosen);
    }

    order
}

/// Re-project `input` onto `target`: allocate a fresh `Record` whose
/// schema is `target`, copying over any upstream field that `target`
/// still declares. Used at operator boundaries to canonicalize the
/// `Arc<Schema>` on the Record so downstream `Arc::ptr_eq` checks hit
/// the fast path.
pub(crate) fn widen_record_to_schema(input: &Record, target: &Arc<Schema>) -> Record {
    if Arc::ptr_eq(input.schema(), target) {
        return input.clone();
    }
    let mut values: Vec<clinker_record::Value> = Vec::with_capacity(target.column_count());
    for (i, col) in target.columns().iter().enumerate() {
        let v = match input.get(col.as_ref()) {
            Some(v) => v.clone(),
            None => recover_engine_stamped_value(input, target, i, col.as_ref()),
        };
        values.push(v);
    }
    let mut out = Record::new(Arc::clone(target), values);
    // Widening reshapes the schema for the same document's row; carry
    // the envelope context forward so a downstream node reading
    // `$doc.<section>.<field>` resolves against the originating
    // document rather than the empty synthetic context.
    out.set_doc_ctx(Arc::clone(input.doc_ctx()));
    out
}

/// Recover an engine-stamped column's value when the input does not
/// expose it under the target column name. The N-ary combine
/// decomposition encodes intermediate-step columns as
/// `__<qualifier>__<name>`, so a `$ck.<field>` snapshot column
/// arrives at the final step's body construction under
/// `__<driver>__$ck.<field>` rather than `$ck.<field>` directly.
/// Walk the input schema for any column ending in `__<col>` and
/// recover the value; otherwise default to `Value::Null` (the column
/// remains an unstamped slot, matching the behavior for build sources
/// that never declared the correlation-key field).
fn recover_engine_stamped_value(
    input: &Record,
    target: &Arc<Schema>,
    target_idx: usize,
    target_col: &str,
) -> clinker_record::Value {
    let is_engine_stamped = target
        .field_metadata(target_idx)
        .is_some_and(|m| m.is_engine_stamped());
    if !is_engine_stamped {
        return clinker_record::Value::Null;
    }
    let suffix = format!("__{target_col}");
    if let Some(j) = input
        .schema()
        .columns()
        .iter()
        .position(|n| n.as_ref().ends_with(&suffix))
    {
        return input.values()[j].clone();
    }
    clinker_record::Value::Null
}

/// Copy build-side `$ck.<field>` columns from `build` into `out` per
/// the combine's `propagate_ck` spec. Driver wins on collision: if
/// `out` already carries a non-null value for the column (because
/// `widen_record_to_schema(driver, …)` filled it), the build value is
/// not written. This is the single runtime CK-copy path used by every
/// combine strategy (HashBuildProbe, IEJoin, GraceHash, SortMerge);
/// keeping the policy here means a strategy never has to encode the
/// `$ck` semantics itself.
///
/// `Driver` copies only the engine-managed synthetic CK
/// (`$ck.aggregate.<name>`, stamped
/// [`clinker_record::FieldMetadata::AggregateGroupIndex`]) — the user-
/// declared source CK is suppressed. `All` copies every `$ck.*` column
/// the build record carries. `Named(set)` copies only the listed
/// source-CK fields plus every synthetic-CK column unconditionally.
///
/// Synthetic CK is engine-managed lineage from a relaxed aggregate to
/// its source rows; the user did not declare it, so `propagate_ck`
/// (a knob over user-declared source CK) has no semantic meaning over
/// it. Without this exemption, the detect-phase fan-out from a
/// downstream failure to the aggregator's per-group source-row table
/// would lose its bridge whenever a Combine sat between the relaxed
/// aggregate and the failing node.
pub(crate) fn copy_build_ck_columns(
    out: &mut Record,
    build: &Record,
    spec: &clinker_plan::config::pipeline_node::PropagateCkSpec,
) {
    use clinker_plan::config::pipeline_node::PropagateCkSpec;
    let build_schema = Arc::clone(build.schema());
    for (idx, col) in build_schema.columns().iter().enumerate() {
        let Some(field_name) = col.strip_prefix("$ck.") else {
            continue;
        };
        let is_synthetic = matches!(
            build_schema.field_metadata(idx),
            Some(clinker_record::FieldMetadata::AggregateGroupIndex { .. }),
        );
        let allowed = is_synthetic
            || match spec {
                PropagateCkSpec::Driver => false,
                PropagateCkSpec::All => true,
                PropagateCkSpec::Named(names) => names.contains(field_name),
            };
        if !allowed {
            continue;
        }
        if out.schema().index(col.as_ref()).is_none() {
            // Output schema didn't widen for this column — the
            // plan-time `combine_output_row` filtered it out
            // already. Skip rather than silently lose data.
            continue;
        }
        // Driver wins on collision: a non-null value at this slot
        // came from the driver via `widen_record_to_schema`. Only
        // fill when the slot is still null. Synthetic-CK names are
        // unique per aggregate (`$ck.aggregate.<name>`), so a driver
        // and build pairing can collide only when both descend from
        // the same relaxed aggregate, in which case the slot is
        // already populated with the correct lineage.
        match out.get(col.as_ref()) {
            Some(clinker_record::Value::Null) | None => {
                let v = &build.values()[idx];
                out.set(col.as_ref(), v.clone());
            }
            Some(_) => {}
        }
    }
}

/// Widen `input`'s schema in place to include every key in `emitted`
/// that is not already declared. Allocates a fresh `Arc<Schema>` only
/// when new names appear; otherwise clones `input`. Used by the legacy
/// linear pipeline path where the emit set is determined at eval time
/// rather than via a plan-time `output_schema`, and by the
/// commit-phase window recompute which materializes new output rows
/// against the same emit shape the dispatcher's window arm produced.
pub(crate) fn record_with_emitted_fields(
    input: &Record,
    emitted: &IndexMap<String, clinker_record::Value>,
) -> Record {
    let mut missing: Vec<&str> = Vec::new();
    for key in emitted.keys() {
        if input.schema().index(key).is_none() {
            missing.push(key.as_str());
        }
    }
    if missing.is_empty() {
        return input.clone();
    }
    let n = input.schema().column_count();
    let mut builder = SchemaBuilder::with_capacity(n + missing.len());
    // Preserve every existing column AND its `FieldMetadata`. Without
    // explicit forwarding the engine-stamp annotation on `$ck.<field>`
    // shadow columns is dropped and downstream readers (the buffer-key
    // extractor at the Output arm, the projection fast path) treat the
    // column as a user-declared one.
    for i in 0..n {
        let name = input
            .schema()
            .column_name(i)
            .expect("column_name within column_count");
        match input.schema().field_metadata(i) {
            Some(meta) => builder = builder.with_field_meta(name, meta.clone()),
            None => builder = builder.with_field(name),
        }
    }
    for name in missing {
        builder = builder.with_field(name);
    }
    let widened = builder.build();
    widen_record_to_schema(input, &widened)
}

/// Resolve the pipeline `memory.limit` into a byte count for the
/// spill-threshold sizing the aggregate and sort arms perform.
///
/// Delegates to the authoritative [`clinker_plan::config::utils::parse_memory_limit_bytes`]
/// so the spill-threshold budget honors the same `K`/`M`/`G` binary
/// suffixes as the arbitrator ceiling. A second local parser previously
/// lived here and silently dropped the `K` suffix, so a `limit: "256K"`
/// sized a 256 KiB arbitrator but a 512 MiB aggregate spill budget; that
/// divergence is gone — there is exactly one limit parser. Narrowed from
/// the parser's `u64` to `usize` for the operator-facing budget fields;
/// on a 32-bit host a limit above `usize::MAX` saturates rather than
/// wrapping.
pub(crate) fn parse_memory_limit(config: &PipelineConfig) -> usize {
    usize::try_from(clinker_plan::config::utils::parse_memory_limit_bytes(
        config.pipeline.memory.limit.as_deref(),
    ))
    .unwrap_or(usize::MAX)
}

/// Build a `MemoryArbitrator` from the pipeline-level `memory:` block.
/// Resolves `memory.limit` through `parse_memory_limit_bytes` (defaults
/// to 512 MiB when omitted) and chooses the active policy by mapping the
/// `memory.backpressure` knob through `build_policy`. Used by both the
/// pipeline-scoped arbitrator and every per-arm budget the dispatch path
/// constructs.
pub(crate) fn build_arbitrator_from_config(
    config: &PipelineConfig,
) -> crate::pipeline::memory::MemoryArbitrator {
    let mem = &config.pipeline.memory;
    let limit = clinker_plan::config::utils::parse_memory_limit_bytes(mem.limit.as_deref());
    crate::pipeline::memory::MemoryArbitrator::with_policy(
        limit,
        0.80,
        crate::pipeline::memory::build_policy(mem.backpressure),
    )
}

#[cfg(test)]
mod tests {
    use super::parse_memory_limit;

    const MINIMAL_PIPELINE: &str = r#"
pipeline:
  name: budget_parse
  memory: { limit: "__LIMIT__" }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;

    fn budget_for(limit: &str) -> usize {
        let yaml = MINIMAL_PIPELINE.replace("__LIMIT__", limit);
        let config = clinker_plan::config::parse_config(&yaml).expect("minimal pipeline parses");
        parse_memory_limit(&config)
    }

    #[test]
    fn aggregate_spill_budget_honors_binary_suffixes() {
        // The aggregate / sort spill budget must resolve the same binary
        // suffixes as the arbitrator ceiling. A `K` suffix that the budget
        // parser silently dropped used to size a 256 KiB arbitrator but a
        // 512 MiB spill budget; this pins the single-parser equivalence.
        assert_eq!(budget_for("256K"), 256 * 1024);
        assert_eq!(budget_for("256k"), 256 * 1024);
        assert_eq!(budget_for("4M"), 4 * 1024 * 1024);
        assert_eq!(budget_for("2G"), 2 * 1024 * 1024 * 1024);
        assert_eq!(budget_for("1024"), 1024);
    }

    #[test]
    fn aggregate_spill_budget_matches_arbitrator_parser() {
        // The budget the aggregate arm sizes from and the arbitrator
        // ceiling come from one parser, so a suffixed limit yields the same
        // byte count on both paths — no divergence between the arbitrator
        // limit and the aggregate spill budget.
        for limit in ["256K", "4M", "2G", "1024"] {
            let yaml = MINIMAL_PIPELINE.replace("__LIMIT__", limit);
            let config = clinker_plan::config::parse_config(&yaml).expect("parses");
            let arbitrator_ceiling = clinker_plan::config::utils::parse_memory_limit_bytes(
                config.pipeline.memory.limit.as_deref(),
            );
            assert_eq!(
                parse_memory_limit(&config) as u64,
                arbitrator_ceiling,
                "aggregate spill budget for {limit:?} must equal the arbitrator ceiling"
            );
        }
    }
}
