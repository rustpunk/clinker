# retract-demo — runnable end-to-end retraction example

This pipeline ingests `orders` and `audit_events`, joins them on
`order_id`, computes per-record running totals over a department-
partitioned analytic window, aggregates per `(department, day)`, and
runs one final post-aggregate validation Transform before writing a
CSV report. The demo exercises both retraction surfaces:

- **Upstream-of-aggregate failure.** A sentinel row in
  `data/orders.csv` carries the literal string `BAD` in its
  `quality_score` column. The `validate` Transform calls
  `to_int(quality_score)` on every record; the cast fails on the
  sentinel, surfacing a DLQ trigger before the relaxed-CK aggregate
  has finalized. The orchestrator retracts that row's contribution
  from `dept_totals` and replays the affected records.

- **Downstream-of-aggregate failure.** The `dept_validate` Transform
  evaluates `if total < 500 then "x".to_int() else 0` against each
  aggregate output row. Every HR `(department, day)` whose
  surviving total falls below 500 fails the projection. The
  aggregate output row carries an engine-stamped synthetic
  `$ck.aggregate.dept_totals` column; the orchestrator's detect
  phase decodes that column back to the contributing source rows
  via the retained aggregator's `input_rows` table and retracts
  them. The failing aggregate group then re-finalizes over zero
  surviving contributions and disappears from the report.

Both paths route through the same five-phase retraction-aware
commit body because the aggregate's `group_by` is `[department,
day]` (omitting both sources' `order_id` correlation key) — the
relaxed-CK lattice routes every aggregate-touching pipeline through
the same orchestrator.

## Run it

The pipeline's `path:` fields are relative to the pipeline directory,
so invoke `clinker` from inside `examples/pipelines/retract-demo/`:

```sh
cd examples/pipelines/retract-demo
cargo run -p clinker -- run pipeline.yaml
```

You should see something like:

```
INFO clinker: Pipeline complete: 12 total, 3 ok, 3 written, 4 dlq
```

12 source rows produce 6 initial aggregate output rows (one per
`(department, day)` group). The post-aggregate `dept_validate`
predicate fails on every HR group; the synthetic-CK fan-out
retracts the contributing source rows so the final report
`output/report.csv` contains only the three ENG rows. The DLQ
contains one upstream `transform:validate` trigger (the BAD
sentinel) plus one `transform:dept_validate` trigger per failing
HR group:

```
department,day,total,min_amount,n,order_ids,anomaly_check
ENG,2024-01-01,500,500,1,"[{""String"":""O05""}]",0
ENG,2024-01-02,1190,440,2,"[{""String"":""O06""},{""String"":""O07""}]",0
ENG,2024-01-03,940,330,2,"[{""String"":""O10""},{""String"":""O11""}]",0
```

If you remove the BAD row from `data/orders.csv` and rerun, the
post-aggregate failures fire on the same three HR groups (the
predicate is a property of the surviving totals, not of the
sentinel) and the writer payload is bit-for-bit identical to the
retract-corrected run. The smoke test at
`crates/clinker-core/tests/retract_demo_smoke.rs` asserts this
equivalence.

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
  synthetic CK: $ck.aggregate.dept_totals (engine-stamped, +16 B/output-row, hidden from default writers)

Window index [0] (source 'orders', partition_by ["department"]):
  retraction: window buffer recompute, partition cardinality unknown at plan time, per-row buffer ~6× sizeof(Value).
  worst-case partition memory ceiling under degrade: O(largest partition × per-row-size); degrade-fallback drops the affected partition's recompute and DLQ's its rows.
```

The `BufferRequired accumulator path` line is significant — the
aggregate's bindings include `collect(order_id)`, which the
engine cannot retract via reverse-op. Buffer mode retains every
contribution so the finalize step recomputes deterministically
after a retract. The synthetic-CK line records the lineage column
the relaxed aggregate stamps on every output row at finalize; the
column is hidden from default writers, lifts the post-aggregate
retract path the `dept_validate` Transform exercises, and costs
~16 bytes per emitted aggregate row (the `Value::Integer`
payload plus its slot overhead). The companion window line spells
the partition's worst-case memory ceiling so a capacity planner
has the data they need before the pipeline goes to production.

## What's happening under the hood

The orchestrator runs the same five phases regardless of where the
failure entered the DAG. Two failure surfaces fire in this demo —
upstream-of-aggregate (the `validate` cast on `quality_score`) and
downstream-of-aggregate (the `dept_validate` predicate on the
aggregate's totals).

- **Detect.** Each failing record lands in the correlation buffer
  keyed by its engine-stamped tail. Upstream `validate` failures
  key on `$ck.order_id` (the source-CK shadow column). Downstream
  `dept_validate` failures key on `$ck.aggregate.dept_totals` (the
  synthetic shadow column the aggregate stamps at finalize); the
  detect phase decodes that key back to the contributing source
  row ids via the retained aggregator's `input_rows` table.

- **Recompute aggregates.** The orchestrator calls `retract_row` on
  every affected source row, then re-emits the corrected aggregate
  output. For HR groups whose every contributing source row was
  retracted, the post-retract finalize emits zero rows for that
  group; the group disappears from the downstream sub-DAG.

- **Replay.** The corrected aggregate output flows through the
  downstream sub-DAG. The `running_totals` window's buffer-recompute
  flag triggers a wholesale partition recompute over the surviving
  rows, so any record whose `$window.lag` / `$window.first` /
  `$window.sum` reference the retracted row gets a corrected value.
  The `dept_validate` Transform re-runs its predicate against the
  post-retract aggregate output.

- **Flush.** The post-replay flush phase emits the retract-corrected
  output rows to the CSV writer. Under `correlation_fanout_policy:
  any` the trigger's CK group is collateral-DLQ'd; the demo's
  one-row-per-CK-group geometry on the upstream surface and the
  per-aggregate-row geometry on the downstream surface produces
  one DLQ entry per failing trigger.

## Reference

- [`docs/src/pipeline/correlation-keys.md`](../../../docs/src/pipeline/correlation-keys.md) — the
  per-source `correlation_key:` field, `$ck.<field>` shadow columns,
  and the strict-vs-relaxed routing rule.
- [`docs/src/pipeline/aggregate.md`](../../../docs/src/pipeline/aggregate.md) — Aggregate node
  `group_by` and binding semantics.
- [`docs/src/pipeline/combine.md`](../../../docs/src/pipeline/combine.md) — Combine node
  `propagate_ck` field.
- [`docs/src/cxl/windows.md`](../../../docs/src/cxl/windows.md) — `$window.*`
  builtins and the analytic-window configuration block.
