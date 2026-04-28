//! Five-phase correlation-commit protocol.
//!
//! The orchestrator entry [`orchestrate`] is the single dispatch site
//! invoked from `dispatch.rs` when the planner-injected
//! [`crate::plan::execution::PlanNode::CorrelationCommit`] arm fires. It
//! runs the five phases in order:
//!
//! 1. [`detect`] — walks the runtime DLQ trigger set and the per-node
//!    `ck_set` lattice to compute a [`detect::RetractScope`] of affected
//!    `(aggregate_node, group_key)` and `(window_node, partition_key)`
//!    pairs plus the resolved [`crate::config::CorrelationFanoutPolicy`]
//!    per affected output.
//! 2. [`recompute_agg`] — for each affected aggregate group, calls
//!    `HashAggregator::retract_row` against the retained per-aggregate
//!    state and refinalizes; emits [`Delta`] entries describing the
//!    retracted-old/added-new aggregate output rows.
//! 3. [`recompute_window`] — for each window-bearing Transform whose
//!    IndexSpec was flagged for buffer-recompute, reruns the window
//!    evaluation over `partition − retracted_rows` and emits per-output
//!    Deltas. Empty `scope.windows` short-circuits to `Ok(vec![])`.
//! 4. [`replay`] — re-executes the deterministic downstream sub-DAG for
//!    the row deltas. Bounded by a defensive iteration cap; any overflow
//!    panics with the documented message because the protocol's
//!    termination proof is structural.
//! 5. [`flush`] — emits final DLQ entries for trigger + spared collateral
//!    per the resolved fan-out policy and flushes clean records to writers.
//!
//! Pipelines without any relaxed-CK aggregate short-circuit through the
//! [`is_relaxed_pipeline`] check at the top of [`orchestrate`] back to
//! the existing two-phase commit shape; non-relaxed workloads pay zero
//! retraction overhead.
//!
//! Per LD-011, the existing `PlanNode::CorrelationCommit` is redesigned
//! in place — there is no `CorrelationCommitV2` parallel variant.

use clinker_record::Record;
use petgraph::graph::NodeIndex;

use crate::config::CorrelationFanoutPolicy;
use crate::error::PipelineError;
use crate::executor::dispatch::{
    CommitStepPath, ExecutorContext, commit_correlation_buffers_strict,
};
use crate::plan::execution::ExecutionPlanDag;

pub(crate) mod detect;
pub(crate) mod flush;
pub(crate) mod recompute_agg;
pub(crate) mod recompute_window;
pub(crate) mod replay;

/// Commit-local delta produced by the recompute phases.
///
/// Each entry describes one retracted-old aggregate or window output row
/// paired with the corresponding added-new row produced after the
/// retract step. The replay phase walks downstream from each delta,
/// re-executing the deterministic portion of the DAG with the new row
/// substituted for the old one.
#[derive(Debug, Clone)]
pub(crate) struct Delta {
    /// Aggregate or window node that emitted the changed row. Replay
    /// resumes from this node's downstream successors.
    pub(crate) source_node: NodeIndex,
    /// The row that was emitted before the retract step ran. Replay
    /// removes it from any stateful downstream operator (e.g. Combine
    /// hash table build side).
    pub(crate) retract_old_row: Option<Record>,
    /// The row produced after retract; `None` when the group's surviving
    /// contributions empty out and no row is emitted post-retract.
    pub(crate) add_new_row: Option<Record>,
}

/// Five-phase orchestrator entry point invoked from the dispatcher's
/// `PlanNode::CorrelationCommit` arm.
///
/// Walks the retained `correlation_buffers` plus per-aggregate retained
/// state on `ExecutorContext`, dispatches to the per-phase submodules,
/// and resolves a [`CommitStepPath`] flag for the executor's
/// zero-overhead-on-strict-pipeline assertion (consumed by tests).
///
/// `commit_node_name` and `commit_group_by` mirror the
/// `PlanNode::CorrelationCommit` carrier shape so the orchestrator can
/// format DLQ trigger messages consistently with the strict path.
pub(crate) fn orchestrate(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    commit_node_name: &str,
    commit_group_by: &[String],
    max_group_buffer: u64,
) -> Result<(), PipelineError> {
    if !is_relaxed_pipeline(current_dag) {
        // Strict pipeline — short-circuit to today's two-phase flush
        // shape. Records the `FastPath` flag for the zero-overhead
        // invariant test assertion.
        ctx.commit_step_path = CommitStepPath::FastPath;
        return commit_correlation_buffers_strict(
            ctx,
            commit_node_name,
            commit_group_by,
            max_group_buffer,
        );
    }
    ctx.commit_step_path = CommitStepPath::FivePhase;

    // Phase 1: detect retract scope.
    let scope = detect::detect_retract_scope(ctx, current_dag);

    // Phase 2: recompute affected aggregate groups via retract_row +
    // finalize_in_place against the retained per-aggregate state.
    let mut deltas = recompute_agg::recompute_aggregates(ctx, &scope)?;

    // Phase 3: recompute affected window partitions. For each window
    // node flagged for buffer-recompute by the planner, rerun the
    // window evaluation over `partition − retracted_rows` and emit a
    // per-record Delta. An empty `scope.windows` (no windows downstream
    // of the relaxed aggregate, or no retracted row landed in any
    // captured partition) short-circuits with zero deltas.
    let window_deltas = recompute_window::recompute_window_partitions(ctx, &scope)?;
    deltas.extend(window_deltas);

    // Phase 4: replay the deterministic downstream sub-DAG. Walks
    // downstream from each delta; the iteration cap is the defensive
    // structural-termination guard.
    replay::replay_subdag(ctx, current_dag, &deltas)?;

    // Phase 5: flush. Operates on the post-replay correlation_buffers,
    // honoring the resolved CorrelationFanoutPolicy per affected output.
    flush::flush_buffered(ctx, &scope, commit_group_by)
}

/// True when the current DAG carries at least one aggregate whose
/// `group_by` omits a CK field visible upstream — i.e. one that
/// activates the retraction protocol.
///
/// Pipelines whose every aggregate covers its parent's CK set (and
/// pipelines whose sources declared no CK at all) short-circuit to
/// today's strict commit body so the strict workload runs with zero
/// retraction overhead. The classification is lattice-driven via
/// `node_properties.ck_set` populated at plan time, so the runtime
/// check stays consistent with the plan.
fn is_relaxed_pipeline(current_dag: &ExecutionPlanDag) -> bool {
    current_dag.graph.node_indices().any(|idx| {
        let crate::plan::execution::PlanNode::Aggregation { config, .. } = &current_dag.graph[idx]
        else {
            return false;
        };
        let parent_ck = current_dag
            .graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .next()
            .and_then(|p| current_dag.node_properties.get(&p))
            .map(|p| p.ck_set.clone())
            .unwrap_or_default();
        crate::plan::execution::group_by_omits_any_ck_field(&config.group_by, &parent_ck)
    })
}

/// Resolve the per-output [`CorrelationFanoutPolicy`] per the precedence
/// chain: per-Output override > per-pipeline default > documented default
/// (`Any`). The Combine-level override surfaces through the relaxed
/// orchestrator's recompute pass when delta records derive from a
/// Combine; the strict path consults only Output and pipeline because
/// no Combine ever participates in collateral fan-out without a relaxed
/// aggregate downstream.
pub(crate) fn output_fanout_policy(
    ctx: &ExecutorContext<'_>,
    output_name: &str,
) -> CorrelationFanoutPolicy {
    let output_override = ctx
        .output_configs
        .iter()
        .find(|o| o.name == output_name)
        .and_then(|o| o.correlation_fanout_policy);
    output_override
        .or(ctx.config.error_handling.correlation_fanout_policy)
        .unwrap_or_default()
}

/// Decide whether a collateral record should be spared from rollback
/// under the resolved [`CorrelationFanoutPolicy`].
///
/// * `Any` — never spare; every record in the trigger group is DLQ'd
///   (today's behavior across all pipelines).
/// * `All` — spare collateral whose CK tuple does NOT fully match the
///   trigger's tuple. Records that share only some CK fields with the
///   trigger pass through to the writer.
/// * `Primary` — spare collateral whose primary CK field does NOT match
///   the trigger's primary CK. Used by audit-style sinks that retain
///   provenance and accept partial-rollback semantics.
pub(crate) fn should_spare_collateral(
    policy: CorrelationFanoutPolicy,
    is_full_tuple_match: bool,
    is_primary_match: bool,
) -> bool {
    match policy {
        CorrelationFanoutPolicy::Any => false,
        CorrelationFanoutPolicy::All => !is_full_tuple_match,
        CorrelationFanoutPolicy::Primary => !is_primary_match,
    }
}
