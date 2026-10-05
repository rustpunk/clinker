# The Retraction Protocol

When an aggregate's `group_by` omits a correlation-key field that is visible upstream, a single correlation group no longer maps cleanly onto a single aggregate group — one CK group can span many aggregate groups. The strict-collateral DLQ shape (roll back the whole group, including the aggregate output row) would then over-reject: a single bad source row would void an entire department's total. The retraction protocol is the engine's answer. It retracts only the failing records' contributions and refinalizes the affected aggregate groups, so surviving contributions still produce a correct output row.

This page is the protocol itself, woven from three operator surfaces: the aggregate's strict-vs-retraction path selection, the synthetic `$ck.aggregate.<name>` lineage column that lifts post-aggregate failures, and the buffer-mode window behavior. It builds directly on the lineage substrate in [Correlation Key Lifecycle & Rollback Narrowing](correlation-lifecycle.md). For the per-operator memory/CPU footprint and the explain/metrics surfaces, see [Operator Retraction Cost Reference](retraction-cost-reference.md).

*User-facing view: the User Guide's "Correlation Keys" / "Aggregate Nodes" pages.*

*Interactive companion: the [retraction-loop explainer](retraction-explainer.html) replays the commit loop phase by phase on three of the engine's test pipelines — the aggregator state, the correlation buffer, the retract set and every retraction counter at each step.*

## Path selection: strict vs. retraction

The engine inspects each aggregate's `group_by` against the upstream CK lattice (the union of `$ck.*` shadow columns visible at the aggregate's input). Authors do not configure this — the engine inspects the configuration and picks the correct path:

- **`group_by` covers every upstream CK field — strict-collateral path.** Each emitted row inherits the correlation identity of its inputs, the aggregate emits one row per group, and a DLQ trigger anywhere in the group rolls back the whole group including the aggregate output row. This is the zero-overhead default; strict aggregates short-circuit to the two-phase commit body and pay no retraction overhead.

  ```yaml
  - type: aggregate
    name: order_totals
    input: orders
    config:
      group_by: [order_id]               # strict — covers the upstream CK
      cxl: |
        emit total = sum(amount)
  ```

- **`group_by` omits any upstream CK field — retraction protocol path.** A single correlation group may span multiple aggregate groups; CK fields omitted from `group_by` stop being visible to downstream consumers of this aggregate's output as user-named columns. The engine retracts only the failing records and refinalizes affected groups, so the aggregate output row reflects the surviving contributions.

  ```yaml
  - type: aggregate
    name: dept_totals
    input: orders
    config:
      group_by: [department]             # retraction protocol is active
      cxl: |
        emit total = sum(amount)
  ```

On the strict path, aggregate output rows inherit the correlation meta of the records that fed them. If any input record in a correlation group fails, the surviving records in that group still flow through the aggregator and produce one aggregate row — but that aggregate row is itself DLQ'd as a collateral and never reaches the writer.

On the retraction path, the engine retracts only the failing records and refinalizes affected groups, so the aggregate output row reflects the surviving contributions. Operators downstream of a retraction-mode aggregate run only at commit time, on the post-recompute aggregate emits, and they re-run on every iteration of the commit loop below; only the last iteration's results are written. A non-deterministic CXL builtin such as `now` is therefore evaluated once per iteration, and the written value is the last iteration's.

## E15Y: streaming incompatibility

The retraction protocol's runtime constraint is enforced automatically once the engine has classified the aggregate. A retraction-mode aggregate is incompatible with `strategy: streaming` and is rejected with **E15Y**:

```bash
clinker explain --code E15Y   # retraction-mode aggregate incompatible with strategy: streaming
```

The reason is structural: streaming aggregates emit at group-boundary close, before the terminal correlation commit, and that early emit defeats the rollback window the retraction protocol depends on. There is nothing left to retract from once a streaming group has already emitted and been handed downstream. The engine selects the path from `group_by` content, so an author who writes `strategy: streaming` on what turns out to be a relaxed-CK aggregate gets the compile-time E15Y rather than silent incorrect behavior.

## Reversible vs. BufferRequired accumulators

The cost of refinalizing a group depends on whether the accumulator can be run in reverse:

- **Reversible accumulators** (`sum`, `count`, `avg`, `weighted_avg`, `collect`, `any`) carry a per-row lineage map `(input_row_id → group_index)` alongside accumulator state. A retract is O(retracted_rows) reverse-op calls plus one `finalize_in_place`. Per input row the aggregator keeps the row's `SourceRowId` with its group index, plus the row's source name, which retract does not read; `--explain` estimates the lineage at ~8 bytes per row. `sum`, `avg` and `weighted_avg` hold exact sums, which subtract exactly, so a retracted group finalizes to the bytes of a fresh fold over the surviving rows, at any memory limit. A `sum` whose every contribution is retracted has no value left and finalizes to null, as a group with no rows does.

- **BufferRequired accumulators** (`min`, `max`) cannot be unwound by a reverse op — removing the current max, for instance, requires knowing the second-largest value, which the running accumulator never retained. They hold per-group raw contributions until commit and recompute affected groups from `contributions − retracted_rows`.

The full per-accumulator memory formulas live in [Operator Retraction Cost Reference](retraction-cost-reference.md).

## Synthetic correlation column

A retraction-mode aggregate emits one engine-managed `$ck.aggregate.<name>` column on its output schema, alongside the user-emitted bindings (`[group_by_columns] ++ [emitted_binding_columns]`). The column carries the aggregator's per-group index at finalize and costs ~16 bytes per emitted row (the `Value::Integer` payload plus its slot overhead). It is hidden from default writer output, mirroring the source-CK shadow column posture, and lives outside any user-visible CXL surface — authors never write or read it.

The synthetic column is the lineage hook that lifts the **post-aggregate** retract path. Without it, a failure on an aggregate output row would have no way back to the source rows that produced it: the aggregate has already collapsed many source rows into one. The column lets the orchestrator's detect phase decode the per-group index back to the contributing source row ids via the retained aggregator's `input_rows` table, and the recompute phase then retracts those source rows just as it would retract a directly-failing source record — matching the upstream-failure DLQ fan-out semantic.

## Where retraction triggers are sourced

Retraction handles failures on both sides of the aggregate, via two different lineage hooks:

- **Upstream of a retraction-mode aggregate** (Source ingest, Transform evaluation, Combine probe, Validation): retraction is fine-grained. The failing record carries `$ck.<field>` shadow columns, the engine identifies its correlation group from those columns, and `retract_row` removes that record's specific contribution from every affected aggregate group while leaving every other contributing record intact.

- **Downstream of a retraction-mode aggregate** (a Transform that fails on an aggregate output row, an Output writer that rejects an aggregate row): the failing record carries the synthetic `$ck.aggregate.<name>` lineage column described above. The detect phase resolves that column to the contributing source row ids and feeds them into the same recompute pipeline as upstream failures.

Both surfaces converge on one recompute pipeline. The end-to-end demo at `examples/pipelines/retract-demo/` runs both surfaces in one pipeline (a Transform failing on an aggregate output row alongside an upstream Transform error).

## The commit loop

The deferred region downstream of each relaxed aggregate (its producer) does not run on the forward pass: the producer runs, keeps its aggregator state, and parks its output; Sinks outside a region buffer their rows in correlation cells keyed by the row's `$ck.*` values, holding per-record failures there too. The forward-pass buffer is saved as a baseline. At commit (`executor/commit/mod.rs`):

1. **Detect.** Cells holding failures are triggers. A source-key cell contributes its failing rows; a cell keyed by `$ck.aggregate.<name>` is decoded to the group index and expanded to every contributing `SourceRowId`. The result seeds the retract set.
2. **Recompute.** This iteration's new rows are retracted from every relaxed aggregate (a row the aggregate never saw is a tolerated "not found"), and every non-empty group is re-emitted in full; a group with no rows left is not emitted. With an empty delta this step is skipped.
3. **Dispatch.** The region members re-run in topological order on the re-emitted rows; their per-record failures are held in buffer cells, and a failure that goes straight to the dead-letter queue (such as an aggregate finalize failure) is captured with its source row.
4. **Re-detect and expand.** Detect runs again on the live buffer, its source rows are combined with the captured direct failures, and failures are copied to an error archive (messages only, no records). Rows not already in the retract set are the next iteration's delta. If there are none the loop stops; otherwise the live buffer is replaced by the baseline and the loop repeats.
5. **Flush.** The archive is merged back into the buffer and the cells are committed: a dirty cell dead-letters each failure as a trigger and its rows as collateral; a clean cell is written. Contributors retracted from a failed aggregate row are not dead-lettered themselves: they are simply no longer in any group.

The loop is bounded by plan nodes (composition-body nodes included) + source rows + 1 iterations, since every iteration must add at least one source row; reaching the bound is currently a `panic!` rather than a typed error ([#1378](https://github.com/rustpunk/clinker/issues/1378)).

## Window interaction

When the pipeline has any relaxed aggregate, the planner marks every windowed Transform whose `partition_by` does not cover the window's correlation-key set as needing a buffered recompute (`requires_buffer_recompute`), and `--explain` counts those windows as buffer-mode windows. What happens at run time: windows rooted at a relaxed aggregate are rebuilt from the re-emitted rows on every iteration of the commit loop, so a window after the aggregate sees the post-retract groups. Windows upstream of the aggregate are never re-evaluated, yet the mark still lifts the E150 check for Source-anchored windows that are never rerun ([#1376](https://github.com/rustpunk/clinker/issues/1376)).

## Degrade fallback

The design: when retraction's preconditions break at run time, the orchestrator degrades to dead-lettering the whole affected group, the strict-collateral shape. Today:

- An aggregate whose recompute cannot proceed (no retained state, a failed retract, or a failed re-emit) has its output slot drained and is added to a degrade list, and `degrade_fallback_count` is incremented. Nothing reads the list, so the strict-collateral dead-lettering never happens: the aggregate's groups are lost rather than dead-lettered, and a region member that needed the drained output can stop the run with an internal error ([#1288](https://github.com/rustpunk/clinker/issues/1288)).
- A relaxed aggregate whose state spills cannot finalize in place; the run stops with an internal "spill failed" error instead of degrading ([#1288](https://github.com/rustpunk/clinker/issues/1288)).
- There is no window degrade path.

## See also

- [Correlation Key Lifecycle & Rollback Narrowing](correlation-lifecycle.md) — the `$ck.<field>` shadow columns, `SourceRowId` lineage, and `per_source_rollback_cursors` map this protocol consumes.
- [Operator Retraction Cost Reference](retraction-cost-reference.md) — the per-operator cost table, the `=== Retraction ===` explain block, and the `retraction` counters.
- [Memory Arbitration & Scheduling](memory-arbitration.md) — the RSS budget and spill thresholds; a relaxed aggregate that crosses them currently fails the run rather than degrading.
