# Operator Retraction Cost Reference

This is the capacity-planning reference for pipelines running the retraction protocol. An aggregate whose `group_by` omits any upstream CK field activates retraction automatically (see [The Retraction Protocol](retraction-protocol.md) for the path-selection rules and [Correlation Key Lifecycle & Rollback Narrowing](correlation-lifecycle.md) for the underlying lineage substrate). Each operator on the post-source DAG carries a different cost profile under retraction; the table below is the centerpiece — it summarizes the per-operator footprint so you can size memory and pick `propagate_ck` settings before pipelines hit production.

*User-facing view: the User Guide's "Correlation Keys" / "Aggregate Nodes" pages.*

## Per-operator cost table

| Operator | Retraction cost |
|---|---|
| Source | None at retraction time. The CK shadow columns are stamped at ingest; replay never re-reads the source file. |
| Transform | Runs only at commit time on post-recompute aggregate emits when sitting inside a deferred region. Cost = O(rows_emitted_post_recompute) per region member, no extra state held. Non-deterministic CXL builtins (e.g. `now`) evaluate exactly once per output row, same as on a non-retraction pipeline. |
| Aggregate (strict, `group_by` covers upstream CK lattice) | None. Strict aggregates short-circuit to today's two-phase commit body and pay zero retraction overhead. |
| Aggregate (retraction-mode, Reversible bindings) | Per-row lineage map `(input_row_id → group_index)` carried alongside accumulator state — ~8 bytes/row plus the per-group `input_rows` Vec inline cost — plus one synthetic `$ck.aggregate.<name>` shadow column on every output row at ~16 bytes/row. Retract is O(retracted_rows) reverse-op calls plus one `finalize_in_place`. Reversible accumulators: `sum`, `count`, `collect`, `any`. |
| Aggregate (retraction-mode, BufferRequired bindings) | Per-group raw contributions held until commit, plus one synthetic `$ck.aggregate.<name>` shadow column on every output row at ~16 bytes/row. Memory cost = O(input_rows × Σ binding_value_size) plus the synthetic-column tail. Retract recomputes affected groups from `contributions − retracted_rows`. BufferRequired accumulators: `min`, `max`, `avg`, `weighted_avg`. |
| Combine (driver propagation) | One propagated `$ck.<field>` slot from the driver record. No retraction state held by the combine itself; replay carries upstream deltas through. |
| Combine (`propagate_ck: all` / `named: [...]`) | Same per-row cost as driver propagation, plus the widened output schema's `$ck.<field>` columns must be re-populated on replay. Cost scales with the output schema width, not retraction frequency. |
| Window (streaming) | None — streaming windows are incompatible with a retraction-mode aggregate whose dropped CK fields overlap `partition_by`. The plan-time derivation switches such windows into buffer mode. |
| Window (buffer-mode) | Per-partition raw row buffers held until commit. Memory cost = O(largest partition × per-row-size). Retract reruns the configured `$window.*` evaluation over `partition − retracted_rows`. Covers all 13 `$window.*` builtins uniformly via wholesale recompute. |
| Output | Holds retracted rows in `correlation_buffers` until commit. Replay substitutes the post-retract row in place; clean records flush to the writer, dirty records DLQ per the resolved `correlation_fanout_policy`. |

## Degrade fallback and metrics counters

When retraction's preconditions break at runtime (an aggregate spilled before retract reached it, or a window partition exceeded the memory budget), the orchestrator degrades to "DLQ entire affected group/partition" — the same strict-collateral DLQ shape every aggregate uses on the strict path. Each degrade increments `correlation.retract.degrade_fallback_count`; persistent non-zero values point at a tighter memory budget or a smaller correlation-key cardinality. The spill thresholds whose breach triggers this fallback are described in [Memory Arbitration & Scheduling](memory-arbitration.md).

The `clinker metrics collect` spool reports the runtime counterpart to the plan-time table above:

- `correlation.retract.groups_recomputed`
- `correlation.retract.partitions_recomputed`
- `correlation.retract.subdag_replay_rows`
- `correlation.retract.output_rows_retracted_total`
- `correlation.retract.degrade_fallback_count`
- `correlation.retract.synthetic_ck_columns_emitted_total`
- `correlation.retract.synthetic_ck_fanout_lookups_total`
- `correlation.retract.synthetic_ck_fanout_rows_expanded_total`

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
- [Memory Arbitration & Scheduling](memory-arbitration.md) — the RSS budget and spill thresholds that govern when retraction degrades to whole-group DLQ.
