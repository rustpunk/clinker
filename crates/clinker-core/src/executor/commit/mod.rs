//! Correlation-commit orchestration for relaxed-CK pipelines.
//!
//! On strict pipelines [`orchestrate`] short-circuits to
//! [`commit_correlation_buffers`] and the relaxed code paths never
//! run. On relaxed pipelines (any aggregate whose `group_by` omits a
//! CK field visible upstream) the orchestrator runs a
//! cascading-retraction loop:
//!
//! 1. [`detect`] computes the initial retract scope. It eagerly
//!    populates `scope.aggregates` with every relaxed-CK aggregate in
//!    the DAG (independent of whether the forward-pass surfaced any
//!    triggers), then folds in per-source-row IDs from the buffered
//!    error cells via the per-cell synthetic-CK lineage.
//! 2. [`recompute_agg`] applies the per-iteration retract delta to
//!    each relaxed-CK aggregator via `retract_row` + `finalize_in_place`,
//!    then projects the post-retract finalize output into the
//!    producer's `node_buffers` slot under the deferred region's
//!    narrow `buffer_schema`. Re-builds any node-rooted window arenas
//!    rooted at the producer so a windowed-Transform member finds its
//!    runtime slot populated.
//! 3. [`dispatch::dispatch_deferred_subdag`] re-feeds every
//!    [`crate::plan::deferred_region::DeferredRegion`] through the same
//!    operator arms `dispatch.rs` runs on the forward pass — flipping
//!    `ExecutorContext::in_deferred_dispatch` is the only
//!    forward/commit-pass distinction.
//! 4. The orchestrator captures the iteration's new error cells into
//!    a cross-iteration `error_archive`, then re-runs
//!    `detect_retract_scope` against the live buffer to recover any
//!    fresh source-row ids the dispatcher's per-record routes parked
//!    there. Both signals fold into the cumulative scope via
//!    [`detect::RetractScope::expand_with_dlq_events`]. Empty delta →
//!    convergence; non-empty → loop iterates.
//! 5. After convergence, the archive merges back into the live buffer
//!    so [`flush::flush_buffered`] sees both the converged record set
//!    and the cumulative error history. Writers and the DLQ then drain
//!    once, honoring the resolved [`CorrelationFanoutPolicy`].
//!
//! Writer commit boundary: deferred-Output records admitted on
//! iteration N are speculative — iteration N+1's recompute may
//! invalidate them. Each iteration ends with `restore_baseline` so
//! the next iteration's dispatch sees a clean record slate; the
//! `error_archive` carries the accumulated error signal forward
//! across restores so the final flush can emit DLQ entries for
//! every retracted source row regardless of when in the loop the
//! failure first surfaced. Writers open + write + close exactly once
//! per Output per pipeline run.
//!
//! The cap on the loop is the sum of `parent_node_count`,
//! `body_node_counts`, `|source_rows|`, and `1`: each iteration must
//! add at least one source row to the trigger set because the deferred
//! dispatcher only widens the scope when a deferred operator fails on
//! a record that previously did not trigger; source rows are bounded;
//! therefore the loop terminates. The body sum is defensive structural
//! headroom for a body's continuation walk surfacing fresh triggers
//! across iterations. An overflow panics — the bound is structural,
//! not pragmatic.
//!
//! `PlanNode::CorrelationCommit` is redesigned in place — there is no
//! parallel V2 variant; the rip-and-replace posture forbids parallel
//! shapes.

use std::collections::HashMap;

use clinker_record::GroupByKey;

use crate::config::CorrelationFanoutPolicy;
use crate::error::PipelineError;
use crate::executor::dispatch::{
    CommitStepPath, CorrelationErrorRecord, CorrelationGroupBuffer, ExecutorContext,
    commit_correlation_buffers,
};
use crate::plan::execution::ExecutionPlanDag;

pub(crate) mod detect;
pub(crate) mod dispatch;
pub(crate) mod flush;
pub(crate) mod recompute_agg;

/// New DLQ entries produced by the deferred-region dispatcher on a
/// single commit-pass iteration.
///
/// The orchestrator's outer loop feeds these back to
/// [`detect::RetractScope::expand_with_dlq_events`] so a member-arm
/// failure on a record that previously did not trigger widens the
/// next iteration's retract scope. Carries the minimum the expand
/// pass needs: the source row id for `retract_row` reuse.
#[derive(Debug, Clone)]
pub(crate) struct DlqEvent {
    pub(crate) source_row: u64,
}

/// Orchestrator entry point invoked from the dispatcher's
/// `PlanNode::CorrelationCommit` arm.
///
/// Strict pipelines (no relaxed-CK aggregate) short-circuit to
/// [`commit_correlation_buffers`] with zero retraction overhead.
/// Relaxed pipelines run the cascading-retraction loop described in
/// the module-level docs.
///
/// The `PlanNode::CorrelationCommit` carrier's `name`,
/// `commit_group_by`, and `max_group_buffer` fields are not threaded
/// through here: [`commit_correlation_buffers`] reads
/// `ctx.correlation_max_group_buffer` directly, and the per-group
/// emission shape carries enough context in the buffer cell itself
/// to format DLQ trigger messages without the carrier's name.
pub(crate) fn orchestrate(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
) -> Result<(), PipelineError> {
    if !is_relaxed_pipeline(ctx, current_dag) {
        ctx.commit_step_path = CommitStepPath::FastPath;
        return commit_correlation_buffers(ctx);
    }
    ctx.commit_step_path = CommitStepPath::ThreePhase;

    // Initial detect — every source row already in the trigger set
    // before any deferred-dispatch iteration.
    let mut scope = detect::detect_retract_scope(ctx, current_dag);

    // Iteration 0 retract delta: the initial cumulative set lives in
    // `scope.seen_source_rows`. Subsequent iterations retract only
    // the per-iteration delta returned by `expand_with_dlq_events`,
    // because `retract_row` is not idempotent on a row already taken
    // out of the aggregator's contributions.
    let initial_rows: Vec<u32> = scope.seen_source_rows.iter().copied().collect();

    // Snapshot the forward-pass baseline buffer state. The strict-Output
    // records and forward-pass error events captured here are stable
    // across the cascading-retraction loop; deferred-Output admissions
    // are speculative per iteration and get wiped via `restore_baseline`
    // at the top of each loop body. None when no terminal CorrelationCommit
    // node configured a buffer (degenerate case — the loop still runs for
    // scope-expansion accounting but nothing flushes).
    let baseline = ctx.correlation_buffers.clone();

    // Loop cap: each iteration must add ≥ 1 source row to the trigger
    // set (otherwise the loop exits via empty `expand_with_dlq_events`
    // return), and source rows are bounded by `ctx.all_records.len()`.
    // The `parent + body node_count + 1` slack accounts for the initial
    // iteration (which retracts the initial trigger set, not new rows)
    // plus defensive headroom against any non-source-row reason the
    // scope could grow. Body node counts fold in because a body's
    // continuation walk can surface fresh source-row triggers across
    // iterations; the body sum is structural headroom, not a new
    // termination contributor (termination is still |source_rows|-
    // bounded). Test-only override (`TEST_LOOP_CAP_OVERRIDE`) lets the
    // defensive panic-message contract be exercised with a synthetic
    // small cap; production callers always see the structural value.
    let body_node_count: usize = ctx
        .artifacts
        .composition_bodies
        .values()
        .map(|b| b.graph.node_count())
        .sum();
    let cap = test_cap_override()
        .unwrap_or(current_dag.graph.node_count() + body_node_count + ctx.all_records.len() + 1);
    let mut iter = 0usize;

    // Cumulative error archive across iterations. Each iteration's
    // dispatch may park new per-record errors in correlation buffer
    // cells; those signals must survive `restore_baseline` so the final
    // flush DLQs the contributing source rows even after the cascading
    // retract drains the failing aggregate emit's lineage to empty
    // (which would otherwise hide the original error from flush).
    // Speculative deferred-Output records, however, must NOT survive —
    // a row admitted in iteration N may be retracted in iteration N+1.
    // The archive splits the two: keep the error signals, drop the
    // records.
    let mut error_archive: HashMap<Vec<GroupByKey>, CorrelationGroupBuffer> = HashMap::new();

    let mut iteration_rows: Vec<u32> = initial_rows;
    loop {
        if iter >= cap {
            panic!(
                "cascading-retraction loop exceeded {cap} iterations; this is \
                 a planner bug — each iteration must add at least one source \
                 row to the trigger set, and source rows are bounded.",
            );
        }
        iter += 1;
        ctx.counters.retraction.iterations += 1;

        recompute_agg::recompute_aggregates(ctx, current_dag, &scope, &iteration_rows)?;
        // The deferred-region members route per-record errors into the
        // correlation buffer (carrying the synthetic-CK column the
        // aggregator stamped) AND emit non-buffer-routed errors (e.g.
        // aggregate-finalize) directly to `dlq_entries`, surfaced via
        // the dispatcher's returned `DlqEvent` vector. Folding both
        // through a single `expand_with_dlq_events` call widens the
        // next iteration's retract scope. Buffer-routed errors are
        // recovered by re-running detect against the live buffer:
        // `detect_retract_scope` expands each freshly-failed aggregate
        // emit through its `input_rows_by_group_index` back to
        // contributing source rows — the same lineage the strict-path
        // detect uses.
        let direct_events = dispatch::dispatch_deferred_subdag(ctx, current_dag, &scope)?;
        let post_dispatch_scope = detect::detect_retract_scope(ctx, current_dag);
        archive_iteration_errors(&mut error_archive, ctx.correlation_buffers.as_ref());
        let mut new_events: Vec<DlqEvent> = direct_events;
        for row in &post_dispatch_scope.seen_source_rows {
            new_events.push(DlqEvent {
                source_row: *row as u64,
            });
        }
        let new_triggers = scope.expand_with_dlq_events(&new_events);
        if new_triggers.is_empty() {
            break;
        }
        iteration_rows = new_triggers;

        // Reset the live buffer to the forward-pass baseline before
        // the next iteration. Deferred-Output records admitted on this
        // iteration are speculative — iteration N+1's recompute may
        // invalidate them. The error_archive above carries forward the
        // per-iteration error signal so the final flush sees both the
        // converged record set AND the cumulative error history.
        restore_baseline(&mut ctx.correlation_buffers, &baseline);
    }

    // Merge the archived errors back into the live buffer so the flush
    // sees the cumulative error history from every iteration.
    merge_archive_into_live(&mut ctx.correlation_buffers, error_archive);

    flush::flush_buffered(ctx, &scope)
}

/// Capture every error_messages / error_rows entry from the live
/// correlation buffer into the cross-iteration archive, deduplicating
/// against entries already there (by `(key, row_num)` so the same
/// failure observed across iterations counts once). Records are NOT
/// archived — they are speculative per-iteration writes that
/// `restore_baseline` discards.
fn archive_iteration_errors(
    archive: &mut HashMap<Vec<GroupByKey>, CorrelationGroupBuffer>,
    live: Option<&HashMap<Vec<GroupByKey>, CorrelationGroupBuffer>>,
) {
    let Some(live) = live else { return };
    for (key, group) in live {
        if group.error_messages.is_empty() && group.error_rows.is_empty() {
            continue;
        }
        let entry = archive.entry(key.clone()).or_default();
        for err in &group.error_messages {
            let already = entry
                .error_messages
                .iter()
                .any(|e| e.row_num == err.row_num && e.error_message == err.error_message);
            if !already {
                entry.error_messages.push(CorrelationErrorRecord {
                    row_num: err.row_num,
                    original_record: err.original_record.clone(),
                    category: err.category,
                    error_message: err.error_message.clone(),
                    stage: err.stage.clone(),
                    route: err.route.clone(),
                });
            }
        }
        for &row in &group.error_rows {
            entry.error_rows.insert(row);
        }
        if group.overflowed {
            entry.overflowed = true;
        }
    }
}

/// Fold the archived per-iteration error history into the live
/// buffer's group cells so the final flush observes both the
/// converged record set and the cumulative errors.
fn merge_archive_into_live(
    live: &mut Option<HashMap<Vec<GroupByKey>, CorrelationGroupBuffer>>,
    archive: HashMap<Vec<GroupByKey>, CorrelationGroupBuffer>,
) {
    let Some(live_map) = live.as_mut() else {
        return;
    };
    for (key, archived) in archive {
        let entry = live_map.entry(key).or_default();
        for err in archived.error_messages {
            let already = entry
                .error_messages
                .iter()
                .any(|e| e.row_num == err.row_num && e.error_message == err.error_message);
            if !already {
                entry.error_messages.push(err);
            }
        }
        for row in archived.error_rows {
            entry.error_rows.insert(row);
        }
        if archived.overflowed {
            entry.overflowed = true;
        }
    }
}

/// Replace the live correlation buffer with a fresh clone of the
/// forward-pass baseline. Called at the top of every cascading-
/// retraction iteration so deferred-Output records and deferred-pass
/// error events from prior iterations cannot leak into the converged
/// flush state. Strict-Output records and forward-pass error events
/// are part of the baseline and survive the restore.
fn restore_baseline(
    live: &mut Option<HashMap<Vec<GroupByKey>, CorrelationGroupBuffer>>,
    baseline: &Option<HashMap<Vec<GroupByKey>, CorrelationGroupBuffer>>,
) {
    *live = baseline.clone();
}

/// True when the current DAG (parent-scope) or any of its composition
/// bodies carries at least one aggregate whose `group_by` omits a CK
/// field visible upstream — i.e. one that activates the retraction
/// protocol.
///
/// Pipelines whose every aggregate covers its parent's CK set (and
/// pipelines whose sources declared no CK at all) short-circuit to
/// today's strict commit body so the strict workload runs with zero
/// retraction overhead. Body bodies must be inspected here because
/// the orchestrator's commit-time deferred dispatcher recurses into a
/// Composition's body via `recurse_into_body` only when the relaxed
/// path is selected; without checking bodies, a body-internal relaxed
/// Aggregate would silently route through the strict FastPath and its
/// deferred region would never be dispatched. The body-side check
/// reads each `BoundBody.deferred_regions` populated at compile time
/// by the deferred-region detector.
fn is_relaxed_pipeline(ctx: &ExecutorContext<'_>, current_dag: &ExecutionPlanDag) -> bool {
    let parent_relaxed = current_dag.graph.node_indices().any(|idx| {
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
    });
    if parent_relaxed {
        return true;
    }
    // Recurse into every Composition body. A body that itself carries
    // a deferred region triggers the relaxed path. Bodies whose
    // deferred regions are empty are inspected one level deeper —
    // their nested compositions may still reach a body with a
    // deferred region. The recursion mirrors the dispatcher's nested-
    // body walk in `recurse_into_body`.
    current_dag.graph.node_indices().any(|idx| {
        let crate::plan::execution::PlanNode::Composition { body, .. } = &current_dag.graph[idx]
        else {
            return false;
        };
        ctx.artifacts.body_of(*body).is_some_and(|b| {
            crate::plan::composition_body::body_or_descendants_have_deferred_region(
                ctx.artifacts,
                b,
            )
        })
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

#[cfg(test)]
thread_local! {
    /// Per-thread override for the cascading-retraction loop cap. Set by
    /// tests via [`with_test_loop_cap`] to drive the defensive panic-
    /// message contract under a synthetic small cap; unset in production.
    static TEST_LOOP_CAP: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn test_cap_override() -> Option<usize> {
    TEST_LOOP_CAP.with(|c| c.get())
}

#[cfg(not(test))]
fn test_cap_override() -> Option<usize> {
    None
}

/// Run `body` with the cascading-retraction loop cap forced to `cap`.
/// Test-only; the override unsets on exit so a panicking body cannot
/// leak the override into a sibling test on the same thread.
#[cfg(test)]
pub(crate) fn with_test_loop_cap<R>(cap: usize, body: impl FnOnce() -> R) -> R {
    struct Guard {
        prev: Option<usize>,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            TEST_LOOP_CAP.with(|c| c.set(self.prev));
        }
    }
    let prev = TEST_LOOP_CAP.with(|c| c.replace(Some(cap)));
    let _guard = Guard { prev };
    body()
}
