# The Retraction Protocol

When an aggregate's `group_by` omits a correlation-key field that is visible upstream, a single correlation group no longer maps cleanly onto a single aggregate group — one CK group can span many aggregate groups. The strict-collateral DLQ shape (roll back the whole group, including the aggregate output row) would then over-reject: a single bad source row would void an entire department's total. The retraction protocol is the engine's answer. It retracts only the failing records' contributions and refinalizes the affected aggregate groups, so surviving contributions still produce a correct output row.

This page is the protocol itself, woven from three operator surfaces: the aggregate's strict-vs-retraction path selection, the synthetic `$ck.aggregate.<name>` lineage column that lifts post-aggregate failures, and the buffer-mode window behavior. It builds directly on the lineage substrate in [Correlation Key Lifecycle & Rollback Narrowing](correlation-lifecycle.md). For the per-operator memory/CPU footprint and the explain/metrics surfaces, see [Operator Retraction Cost Reference](retraction-cost-reference.md).

*User-facing view: the User Guide's "Correlation Keys" / "Aggregate Nodes" pages.*

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

On the retraction path, the engine retracts only the failing records and refinalizes affected groups, so the aggregate output row reflects the surviving contributions. Operators downstream of a retraction-mode aggregate run only at commit time on the post-recompute aggregate emits, so non-deterministic CXL builtins (e.g. `now`) evaluate exactly once per output row and need no special-casing.

## E15Y: streaming incompatibility

The retraction protocol's runtime constraint is enforced automatically once the engine has classified the aggregate. A retraction-mode aggregate is incompatible with `strategy: streaming` and is rejected with **E15Y**:

```bash
clinker explain --code E15Y   # retraction-mode aggregate incompatible with strategy: streaming
```

The reason is structural: streaming aggregates emit at group-boundary close, before the terminal correlation commit, and that early emit defeats the rollback window the retraction protocol depends on. There is nothing left to retract from once a streaming group has already emitted and been handed downstream. The engine selects the path from `group_by` content, so an author who writes `strategy: streaming` on what turns out to be a relaxed-CK aggregate gets the compile-time E15Y rather than silent incorrect behavior.

## Reversible vs. BufferRequired accumulators

The cost of refinalizing a group depends on whether the accumulator can be run in reverse:

- **Reversible accumulators** (`sum`, `count`, `collect`, `any`) carry a per-row lineage map `(input_row_id → group_index)` alongside accumulator state. A retract is O(retracted_rows) reverse-op calls plus one `finalize_in_place`. The lineage map costs ~8 bytes/row plus the per-group `input_rows` Vec inline cost.

- **BufferRequired accumulators** (`min`, `max`, `avg`, `weighted_avg`) cannot be unwound by a reverse op — removing the current max, for instance, requires knowing the second-largest value, which the running accumulator never retained. They hold per-group raw contributions until commit and recompute affected groups from `contributions − retracted_rows`.

The full per-accumulator memory formulas live in [Operator Retraction Cost Reference](retraction-cost-reference.md).

## Synthetic correlation column

A retraction-mode aggregate emits one engine-managed `$ck.aggregate.<name>` column on its output schema, alongside the user-emitted bindings (`[group_by_columns] ++ [emitted_binding_columns]`). The column carries the aggregator's per-group index at finalize and costs ~16 bytes per emitted row (the `Value::Integer` payload plus its slot overhead). It is hidden from default writer output, mirroring the source-CK shadow column posture, and lives outside any user-visible CXL surface — authors never write or read it.

The synthetic column is the lineage hook that lifts the **post-aggregate** retract path. Without it, a failure on an aggregate output row would have no way back to the source rows that produced it: the aggregate has already collapsed many source rows into one. The column lets the orchestrator's detect phase decode the per-group index back to the contributing source row ids via the retained aggregator's `input_rows` table, and the recompute phase then retracts those source rows just as it would retract a directly-failing source record — matching the upstream-failure DLQ fan-out semantic.

## Where retraction triggers are sourced

Retraction handles failures on both sides of the aggregate, via two different lineage hooks:

- **Upstream of a retraction-mode aggregate** (Source ingest, Transform evaluation, Combine probe, Validation): retraction is fine-grained. The failing record carries `$ck.<field>` shadow columns, the engine identifies its correlation group from those columns, and `retract_row` removes that record's specific contribution from every affected aggregate group while leaving every other contributing record intact.

- **Downstream of a retraction-mode aggregate** (a Transform that fails on an aggregate output row, an Output writer that rejects an aggregate row): the failing record carries the synthetic `$ck.aggregate.<name>` lineage column described above. The detect phase resolves that column to the contributing source row ids and feeds them into the same recompute pipeline as upstream failures.

Both surfaces converge on one recompute pipeline. The end-to-end demo at `examples/pipelines/retract-demo/` runs both surfaces in one pipeline (a Transform failing on an aggregate output row alongside an upstream Transform error).

## Window interaction: buffer-mode and wholesale recompute

When a window sits downstream of a relaxed-CK aggregate whose dropped correlation-key fields overlap the window's `group_by`, the planner switches the window from streaming-emit to **buffer-mode**. Streaming windows are structurally incompatible with retraction for the same reason streaming aggregates are: a streaming window emits per-partition as the partition closes, leaving nothing to retract. The plan-time derivation detects the overlap between the aggregate's dropped CK fields and the window's `partition_by` axis and forces buffer mode.

A buffer-mode window stores per-partition raw row buffers until commit. On retraction, it reruns the configured `$window.*` evaluation over `partition − retracted_rows` and emits per-output deltas through the replay phase. All 13 `$window.*` builtins are covered uniformly by this **wholesale recompute** — there is no per-function reverse op the way Reversible aggregate accumulators have; the window simply re-evaluates the surviving partition end to end. This keeps ranking functions (`row_number`, `rank`, `dense_rank`), positional functions (`lag`, `lead`, `first_value`, `last_value`), and iterable predicates (`any`, `every`, `exists`, `not_exists`) all correct after a retract without bespoke per-builtin unwind logic.

## Degrade fallback

When retraction's preconditions break at runtime — an aggregate spilled before retract reached it, or a window partition exceeded the memory budget — the orchestrator degrades to "DLQ entire affected group/partition", the same strict-collateral DLQ shape every aggregate uses on the strict path. Each degrade increments `correlation.retract.degrade_fallback_count`; persistent non-zero values point at a tighter memory budget or a smaller correlation-key cardinality. The degrade path and its metrics are detailed in [Operator Retraction Cost Reference](retraction-cost-reference.md).

## See also

- [Correlation Key Lifecycle & Rollback Narrowing](correlation-lifecycle.md) — the `$ck.<field>` shadow columns, `(row_id, source_name)` lineage pairs, and `per_source_rollback_cursors` map this protocol consumes.
- [Operator Retraction Cost Reference](retraction-cost-reference.md) — the per-operator cost table, the `=== Retraction ===` explain block, and the `correlation.retract.*` counters.
- [Memory Arbitration & Scheduling](memory-arbitration.md) — the spill thresholds whose breach triggers the degrade fallback.
