# retract-demo — runnable end-to-end retraction example

This pipeline ingests `orders` and `audit_events`, joins them on
`order_id`, computes per-record running totals over a department-
partitioned analytic window, aggregates per `(department, day)`, and
writes a CSV report. When a row fails downstream, the engine retracts
that row's contribution from the aggregate and replays the affected
records — without dropping the entire department's data.

The demo's input contains one sentinel row whose `quality_score`
column is the literal string `BAD`. The validation transform calls
`to_int(quality_score)` on every record; the cast fails on the
sentinel, surfacing a DLQ trigger to the orchestrator. Because the
downstream aggregate's `group_by` is `[department, day]` (omitting
the pipeline-level correlation key `order_id`), the engine routes
the pipeline through the retraction-aware five-phase commit instead
of the strict-collateral two-phase fast path.

## Run it

The pipeline's `path:` fields are relative to the pipeline directory,
so invoke `clinker` from inside `examples/pipelines/retract-demo/`:

```sh
cd examples/pipelines/retract-demo
cargo run -p clinker -- run pipeline.yaml
```

You should see something like:

```
INFO clinker: Pipeline complete: 12 total, 6 ok, 6 written, 1 dlq
```

12 source rows fan out to 6 aggregate output rows (one per
`(department, day)` group). 1 DLQ entry — the trigger — lands in
`output/dlq.csv`. The retracted row's department-day group still
appears in `output/report.csv`, with the totals recomputed over the
surviving rows. A representative excerpt:

```
department,day,total,min_amount,n,order_ids
HR,2024-01-03,350,90,2,"[{""String"":""O04""},{""String"":""O09""}]"
ENG,2024-01-02,1190,440,2,"[{""String"":""O06""},{""String"":""O07""}]"
```

If you remove the BAD row from `data/orders.csv` and rerun, the HR
2024-01-03 row stays at `total=350, n=2` — the retract path produces
the same output the user would have gotten from a clean rerun. The
smoke test at `crates/clinker-core/tests/retract_demo_smoke.rs`
asserts this bit-for-bit equivalence.

## Inspect the plan

```sh
cargo run -p clinker -- run pipeline.yaml --explain
```

The explain output's retraction section makes the engine's
strategy choice visible:

```
=== Retraction ===

retraction enabled — 1 relaxed aggregates, 1 buffer-mode windows, fanout policy: any.

Aggregate 'dept_totals':
  retraction: relaxed-CK enabled, BufferRequired accumulator path, lineage memory n/a (buffer-mode holds raw contributions) (cardinality unknown at plan time), worst-case degrade-fallback: DLQ entire affected group when retract precondition breaks at runtime.

Window index [0] (source 'orders', partition_by ["department"]):
  retraction: window buffer recompute, partition cardinality unknown at plan time, per-row buffer ~6× sizeof(Value).
  worst-case partition memory ceiling under degrade: O(largest partition × per-row-size); degrade-fallback drops the affected partition's recompute and DLQ's its rows.
```

The `BufferRequired accumulator path` line is significant — the
aggregate's bindings include `collect(order_id)`, which the
engine cannot retract via reverse-op. Buffer mode retains every
contribution so the finalize step recomputes deterministically
after a retract. The companion window line spells the partition's
worst-case memory ceiling so a capacity planner has the data they
need before the pipeline goes to production.

## What's happening under the hood

The orchestrator routes the failing row through four phases:

- **Detect.** The validation transform's `to_int` call fails on the
  sentinel `quality_score`. The error reaches the correlation buffer
  with the row's `$ck.order_id` snapshot, populating the buffer's
  `error_rows` set for that CK group.

- **Recompute aggregates.** The orchestrator iterates the relaxed-CK
  aggregator's lineage for the failing row's CK, calling `retract_row`
  on the buffer-mode accumulator. The aggregator drops the failing
  row's contribution from `sum(amount)`, `min(amount)`, `count(*)`,
  and `collect(order_id)` and re-emits the corrected aggregate output
  for the affected `(department, day)` group.

- **Replay.** The corrected aggregate output flows through the
  downstream sub-DAG. The `running_totals` window's buffer-recompute
  flag triggers a wholesale partition recompute over the surviving
  rows, so any record whose `$window.lag` / `$window.first` /
  `$window.sum` reference the retracted row gets a corrected value.

- **Flush.** The post-replay flush phase emits the retract-corrected
  output rows to the CSV writer. Under `correlation_fanout_policy:
  any` the trigger's CK group is collateral-DLQ'd, but the demo's
  one-row-per-CK-group geometry makes the trigger its own (and only)
  contributor — exactly one DLQ entry, no collateral.

## Reference

- [`docs/src/pipeline/correlation-keys.md`](../../../docs/src/pipeline/correlation-keys.md) — the
  `error_handling.correlation_key` field, `$ck.<field>` shadow
  columns, and the strict-vs-relaxed routing rule.
- [`docs/src/pipeline/aggregate.md`](../../../docs/src/pipeline/aggregate.md) — Aggregate node
  `group_by` and binding semantics.
- [`docs/src/pipeline/combine.md`](../../../docs/src/pipeline/combine.md) — Combine node
  `propagate_ck` field.
- [`docs/src/cxl/windows.md`](../../../docs/src/cxl/windows.md) — `$window.*`
  builtins and the analytic-window configuration block.
