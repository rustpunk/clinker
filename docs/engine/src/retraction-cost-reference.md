# Operator Retraction Cost Reference

This is the capacity-planning reference for pipelines running the retraction protocol. An aggregate whose `group_by` omits any upstream CK field activates retraction automatically (see [The Retraction Protocol](retraction-protocol.md) for the path-selection rules and [Correlation Key Lifecycle & Rollback Narrowing](correlation-lifecycle.md) for the underlying lineage substrate). Each operator on the post-source DAG carries a different cost profile under retraction; the table below is the centerpiece — it summarizes the per-operator footprint so you can size memory and pick `propagate_ck` settings before pipelines hit production.

*User-facing view: the User Guide's "Correlation Keys" / "Aggregate Nodes" pages.*

## Per-operator cost table

| Operator | Retraction cost |
|---|---|
| Source | None at retraction time. The CK shadow columns are stamped at ingest; replay never re-reads the source file. |
| Transform | Runs only at commit time on post-recompute aggregate emits when sitting inside a deferred region, once per iteration of the commit loop. Cost = O(rows_emitted_post_recompute) per region member per iteration, no extra state held. Non-deterministic CXL builtins (e.g. `now`) are evaluated once per iteration; the last iteration's value is written. |
| Aggregate (strict, `group_by` covers upstream CK lattice) | None. Strict aggregates short-circuit to today's two-phase commit body and pay zero retraction overhead. |
| Aggregate (retraction-mode, Reversible bindings) | Per-row lineage map `(input_row_id → group_index)` carried alongside accumulator state (each row's `SourceRowId` with its group index; `--explain` estimates ~8 bytes/row) plus one synthetic `$ck.aggregate.<name>` shadow column on every output row at ~16 bytes/row. Retract is O(retracted_rows) reverse-op calls plus one `finalize_in_place`. Reversible accumulators: `sum`, `count`, `avg`, `weighted_avg`, `collect`, `any`. |
| Aggregate (retraction-mode, BufferRequired bindings) | Per-group raw contributions held until commit, plus one synthetic `$ck.aggregate.<name>` shadow column on every output row at ~16 bytes/row. Memory cost = O(input_rows × Σ binding_value_size) plus the synthetic-column tail. Retract recomputes affected groups from `contributions − retracted_rows`. BufferRequired accumulators: `min`, `max`. A binding list with one of them puts the whole Aggregate on this path. |
| Combine (driver propagation) | One propagated `$ck.<field>` slot from the driver record. No retraction state held by the combine itself; replay carries upstream deltas through. |
| Combine (`propagate_ck: all` / `named: [...]`) | Same per-row cost as driver propagation, plus the widened output schema's `$ck.<field>` columns must be re-populated on replay. Cost scales with the output schema width, not retraction frequency. |
| Window | A window rooted at a relaxed aggregate is rebuilt from the re-emitted rows on every iteration: O(re-emitted rows) per iteration. Windows upstream of the aggregate are not re-evaluated, although the planner's buffer-mode mark lifts the E150 check for them ([#1376](https://github.com/rustpunk/clinker/issues/1376)). |
| Output | Holds rows in correlation buffer cells until commit. Every iteration that continues restores the forward-pass buffer and re-runs the region, so rows are re-produced, not substituted in place; after the last iteration clean cells flush to the writer and dirty cells dead-letter. `correlation_fanout_policy: all` and `primary` currently behave like `any` ([#1375](https://github.com/rustpunk/clinker/issues/1375)). |

## Degrade fallback and metrics counters

The degrade fallback is designed to dead-letter the whole affected group when retraction's preconditions break at run time. Today a degraded aggregate's output is drained and the degrade list is never read, so its groups are lost rather than dead-lettered ([#1288](https://github.com/rustpunk/clinker/issues/1288)), and a relaxed aggregate whose state spills stops the run with an internal error instead of degrading ([#1288](https://github.com/rustpunk/clinker/issues/1288)). See [The Retraction Protocol](retraction-protocol.md#degrade-fallback).

The metrics spool reports the run-time counters under its `retraction` object (see the User Guide's Metrics & Monitoring page). Each is `0` on strict pipelines:

- `iterations` — commit-loop iterations run (each recompute → dispatch → re-detect cycle).
- `groups_recomputed` — rows re-emitted by relaxed aggregates during recompute, counting every non-empty group re-emitted, not only the changed ones.
- `partitions_dispatched` — windowed-Transform member dispatches during the commit pass (one per member per iteration), not partitions.
- `degrade_fallback_count` — aggregates that took the degrade path, per iteration.
- `synthetic_ck_columns_emitted_total` — `$ck.aggregate.<name>` values written, on the forward pass and on every recompute.
- `synthetic_ck_fanout_lookups_total` — aggregate-keyed trigger cells decoded back to their group, on every detect.
- `synthetic_ck_fanout_rows_expanded_total` — contributing source rows those lookups produced.

Use the explain block (below) for plan-time capacity sizing, the metrics spool for post-run confirmation.

## The `=== Retraction ===` explain block

Pipelines whose at least one Aggregate has a `group_by` that omits a correlation-key field get a `=== Retraction ===` block in the `clinker run --explain` text output. The engine selects the retraction-mode path automatically based on `group_by` content; the block is silent on every other pipeline, so strict-correlation and non-correlated `--explain` output stays identical to today's text. (For the rest of the explain surface — buffer classes, arbitration parameters, the `=== Statistics ===` section — see the broader explain documentation.)

The block opens with a one-line summary —

```
retraction enabled — N relaxed aggregates, M buffer-mode windows, fanout policy: <policy>.
```

— followed by one block per retraction-mode Aggregate and one per buffer-mode window index.

**Per retraction-mode Aggregate** the block reports:

- the resolved accumulator path (`Reversible` or `BufferRequired`),
- the per-row lineage memory cost (`~8 bytes/row` for Reversible, `n/a` for BufferRequired which holds raw contributions instead),
- the per-aggregate synthetic-CK column and its ~16-byte/output-row cost,
- the worst-case degrade fallback when retraction's preconditions break at runtime.

**Per buffer-mode window index** the block reports:

- the source name and `partition_by` fields,
- the per-row buffer cost in `Value` slots over the index's arena fields,
- the worst-case partition memory ceiling under degrade.

Group cardinality is honestly surfaced as "unknown at plan time" — the planner has no group-cardinality side-table to consult before the run. Use this per-operator cost table and the per-row figures the explain block prints for capacity planning, then confirm the live shape via `clinker metrics collect` after the first production run.

## See also

- [The Retraction Protocol](retraction-protocol.md) — the path-selection rules, E15Y, synthetic column, and buffer-mode window mechanics the costs above quantify.
- [Correlation Key Lifecycle & Rollback Narrowing](correlation-lifecycle.md) — the `$ck.*` shadow columns and `per_source_rollback_cursors` map the cost model accounts for.
- [Combine Join Strategies](combine-internals.md) — `propagate_ck:` modes and their replay-time output-schema-width cost.
- [Memory Arbitration & Scheduling](memory-arbitration.md) — the RSS budget and spill thresholds; a relaxed aggregate that crosses them currently fails the run rather than degrading.
