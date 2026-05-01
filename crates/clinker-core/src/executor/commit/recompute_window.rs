//! Recompute affected analytic-window partitions during commit.
//!
//! For each [`super::detect::WindowRetractEntry`] in the scope, walks
//! the captured `RetainedWindowState` and reruns the window evaluation
//! over `partition − retracted_rows` for every affected partition. Per
//! surviving record position, builds a [`super::Delta`] pairing the
//! pre-retract output the dispatcher emitted with the post-retract row
//! the recomputed window context produces.
//!
//! Frame interaction is uniform across `rows` and `range` because the
//! recompute pass walks the surviving partition slice in `order_by`
//! value order — frame boundaries reshape naturally on top of the
//! filtered slice. All `$window.*` builtins flow through the same
//! code path because the wholesale-recompute strategy does not depend
//! on per-builtin reverse semantics. `lag(n)` / `lead(n)` near a
//! partition boundary observe the new positional shift induced by the
//! retract; `collect` and `distinct` rebuild their array / set state
//! from scratch, so the post-retract output never carries stale
//! set-shaped contents.
//!
//! The pass takes ownership of the per-window state on completion via
//! `take` so the orchestrator's flush phase observes a drained map.
//! Memory accounting (`charged_bytes`) is dropped with the state.

use std::sync::Arc;

use clinker_record::{Record, Value};
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, SkipReason};
use petgraph::graph::NodeIndex;

use super::{Delta, detect::RetractScope};
use crate::error::PipelineError;
use crate::executor::dispatch::{ExecutorContext, RetainedWindowState};
use crate::pipeline::arena::Arena;
use crate::pipeline::window_context::PartitionWindowContext;
use crate::plan::execution::ExecutionPlanDag;

/// Run the wholesale recompute for every window in `scope.windows`.
///
/// Returns the accumulated [`Delta`] list the orchestrator's replay
/// phase walks downstream. An empty `scope.windows` short-circuits to
/// `Ok(vec![])` so non-window relaxed pipelines pay zero overhead.
pub(crate) fn recompute_window_partitions(
    ctx: &mut ExecutorContext<'_>,
    plan: &ExecutionPlanDag,
    scope: &RetractScope,
) -> Result<Vec<Delta>, PipelineError> {
    if scope.windows.is_empty() {
        return Ok(Vec::new());
    }

    // Drain the retained state up front so we can borrow `ctx` mutably
    // for evaluator construction and SecondaryIndex lookup without an
    // outstanding aliasing borrow. The drained map is rebuilt as
    // `_drained` only to release memory at function exit.
    let mut drained: std::collections::HashMap<NodeIndex, RetainedWindowState> =
        std::mem::take(&mut ctx.relaxed_window_states);
    let mut deltas: Vec<Delta> = Vec::new();

    for entry in &scope.windows {
        let Some(retained) = drained.remove(&entry.window_node) else {
            continue;
        };
        let window_index = retained.window_index;
        let transform_idx = retained.transform_idx;

        // Resolve the WindowRuntime through the spec's root. Source-
        // rooted windows root in Phase-0; node-rooted windows root at
        // the upstream operator's finalize. Either way, the runtime
        // owns the arena+index pair the dispatcher's window arm
        // admitted retained state against — `Arc::clone` keeps both
        // alive across the recompute pass without re-materializing.
        let spec = match plan.indices_to_build.get(window_index) {
            Some(s) => s,
            None => continue,
        };
        let runtime = match ctx.window_runtime.resolve(&spec.root, window_index) {
            Some(r) => r,
            None => {
                // No runtime materialized for this slot — buffer-mode
                // admission could not have populated retained state.
                // Defensive return.
                continue;
            }
        };
        let arena = Arc::clone(&runtime.arena);
        let secondary_index = Arc::clone(&runtime.index);

        // Build a fresh evaluator mirroring the dispatcher's setup.
        // ProgramEvaluator carries per-evaluator state (regex caches,
        // distinct sets); a fresh instance per recompute pass keeps
        // the original walk's evaluator untouched.
        let typed = Arc::clone(&ctx.compiled_transforms[transform_idx].typed);
        let has_distinct = ctx.compiled_transforms[transform_idx].has_distinct();
        let mut evaluator = ProgramEvaluator::new(typed, has_distinct);
        let transform_name = ctx.compiled_transforms[transform_idx].name.clone();

        // Per partition: build the surviving arena-position slice and
        // rerun every retained `(arena_pos, source_row, pre_retract)`
        // triple through the windowed evaluator. The original index
        // slice gives the canonical `order_by` ordering for
        // `range`/`rows` frames; filtering preserves order.

        for (partition_key, retracted_positions) in &entry.partition_retracts {
            let Some(original_partition) = secondary_index.get(partition_key) else {
                continue;
            };
            let surviving: Vec<u32> = original_partition
                .iter()
                .copied()
                .filter(|p| !retracted_positions.contains(p))
                .collect();

            let Some(rows) = retained.partition_outputs.get(partition_key) else {
                continue;
            };

            // Count this partition once, before walking its rows. The
            // aggregator counter increments per delta; the partition
            // counter increments per (window_node, partition_key) pair
            // touched, mirroring the wholesale-recompute granularity
            // that `--explain` advertises.
            ctx.counters.retraction.partitions_recomputed += 1;
            for (arena_pos, rn, pre_retract) in rows {
                if retracted_positions.contains(arena_pos) {
                    // The retracted row itself: emit a retract-only
                    // Delta (no replacement) so the replay phase drops
                    // the stale output from any buffered output sink.
                    deltas.push(Delta {
                        source_node: entry.window_node,
                        retract_old_row: Some(pre_retract.clone()),
                        add_new_row: None,
                    });
                    continue;
                }
                let pos_in_partition = match surviving.iter().position(|p| p == arena_pos) {
                    Some(i) => i,
                    None => {
                        // A surviving record whose arena position fell
                        // out of the partition slice means the
                        // SecondaryIndex disagreed with the dispatcher's
                        // partition assignment — defensive: emit the
                        // pre-retract row as a retract-only delta and
                        // continue rather than evaluate against an
                        // empty partition.
                        deltas.push(Delta {
                            source_node: entry.window_node,
                            retract_old_row: Some(pre_retract.clone()),
                            add_new_row: None,
                        });
                        continue;
                    }
                };

                // Pull the input record off the arena. The arena is the
                // canonical buffer for source records; the window arm
                // never had to clone the source row body to admit it.
                let input = arena_record_at(&arena, *arena_pos);
                let eval_ctx = EvalContext {
                    stable: ctx.stable,
                    source_file: ctx.source_file_arc,
                    source_row: *rn,
                };
                let wctx = PartitionWindowContext::new(&arena, &surviving, pos_in_partition);
                let result = evaluator
                    .eval_record(&eval_ctx, &input, Some(&wctx))
                    .map_err(|e| PipelineError::Compilation {
                        transform_name: transform_name.clone(),
                        messages: vec![format!(
                            "window recompute eval failed at partition {:?}: {e}",
                            partition_key
                        )],
                    })?;

                let new_row = match result {
                    EvalResult::Emit { fields, metadata } => {
                        let mut out = crate::executor::record_with_emitted_fields(&input, &fields);
                        for (name, value) in &fields {
                            out.set(name, value.clone());
                        }
                        for (k, v) in &metadata {
                            let _ = out.set_meta(k, v.clone());
                        }
                        Some(out)
                    }
                    EvalResult::Skip(SkipReason::Filtered)
                    | EvalResult::Skip(SkipReason::Duplicate) => None,
                };

                deltas.push(Delta {
                    source_node: entry.window_node,
                    retract_old_row: Some(pre_retract.clone()),
                    add_new_row: new_row,
                });
            }
        }
    }

    // Anything left in the drained map is a window node that was
    // captured at admission time but did not surface in the retract
    // scope (no retracted rows landed in any of its partitions). The
    // memory release is the Drop on `_drained` going out of scope.
    let _drained = drained;
    Ok(deltas)
}

/// Reconstruct a `Record` view of arena position `pos`. Mirrors
/// `evaluate_single_transform_windowed`'s implicit reliance on the
/// arena holding source columns: every field declared in the arena's
/// projected schema is present at the position.
///
/// The Record adopts `arena.schema()` directly — the arena's
/// `Arc<Schema>` is the canonical source-side projection used by
/// every downstream evaluator. The evaluator reads inputs by name;
/// schema canonicalization for the emit-side output Record happens in
/// `record_with_emitted_fields` against the typed program's output
/// row.
fn arena_record_at(arena: &Arena, pos: u32) -> Record {
    use clinker_record::RecordStorage;
    let schema = Arc::clone(arena.schema());
    let mut values: Vec<Value> = Vec::with_capacity(schema.column_count());
    for col in schema.columns() {
        let v = arena
            .resolve_field(pos, col.as_ref())
            .cloned()
            .unwrap_or(Value::Null);
        values.push(v);
    }
    Record::new(schema, values)
}
