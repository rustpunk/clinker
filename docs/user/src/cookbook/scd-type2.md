# Slowly-Changing Dimensions (SCD Type 2)

This recipe splits over-long dimension records into closed historical rows plus a continuation row, the shape an SCD Type 2 backfill produces. It uses a [Reshape](../nodes/reshape.md) node to both **mutate** the record that needs splitting and **synthesize** the continuation row, per correlation group.

It ships as a runnable pipeline at `examples/pipelines/scd_type2.yaml`.

## The problem

Each subject (here, an employee) has a history of dimension records — benefit plans, addresses, price tiers — each with a validity window. A record whose window runs longer than it should (it was never split when the underlying fact changed) needs to be **closed** at the boundary, with a fresh **continuation** record carrying the rest of the window forward. That is a per-group transformation: the whole employee's history is one correlation group, and the split must observe the original group, not a half-mutated one.

Reshape fits exactly: it groups by `partition_by`, and within each group a rule can both `mutate` the trigger record and `synthesize` a derived one — all against the **original** group snapshot (the [no-cascade contract](../nodes/reshape.md#no-cascade)).

## Input data

`data/scd_plans.csv` — each employee's plan history, with start/end day-numbers:

```csv
employee_id,plan_start,plan_end,status
E001,100,90,baseline
E001,1000,100,baseline
E002,200,150,baseline
E002,2000,300,baseline
E003,300,250,baseline
E004,400,380,baseline
E004,1500,400,baseline
E005,500,450,baseline
```

`E001`, `E002`, and `E004` each have one row whose window exceeds the one-year boundary (`plan_start - plan_end > 365`); the others are already short enough.

## Pipeline

`scd_type2.yaml`:

```yaml
pipeline:
  name: scd_type2_backfill
  # A small budget forces the spill path even on this tiny fixture, so the
  # example also demonstrates bounded-memory Reshape. Raise or remove it for
  # production volumes.
  memory: { limit: "16K", backpressure: spill }

nodes:
  - type: source
    name: plans
    config:
      name: plans
      type: csv
      path: ./data/scd_plans.csv
      options:
        has_header: true
      schema:
        - { name: employee_id, type: string }
        - { name: plan_start, type: int }
        - { name: plan_end, type: int }
        - { name: status, type: string }

  - type: reshape
    name: backfill
    input: plans
    config:
      partition_by: [employee_id]
      order_by:
        - { field: plan_start, order: asc }
      rules:
        - name: split_long_plan
          when: "plan_start - plan_end > 365"
          mutate:
            set:
              plan_end: "plan_start"        # close the over-long window
          synthesize:
            copy_from: none
            overrides:
              employee_id: "employee_id"
              plan_start: "plan_start"
              plan_end: "plan_end"          # the rest of the window
              status: "'synthesized'"

  - type: output
    name: out
    input: backfill
    config:
      name: out
      type: csv
      path: ./output/scd_type2.csv

error_handling:
  strategy: continue
```

## Run it

```bash
cargo run -p clinker -- run examples/pipelines/scd_type2.yaml
```

## Expected output

`output/scd_type2.csv`:

```csv
employee_id,plan_start,plan_end,status
E001,100,90,baseline
E001,1000,1000,baseline
E001,1000,100,synthesized
E002,200,150,baseline
E002,2000,2000,baseline
E002,2000,300,synthesized
E003,300,250,baseline
E004,400,380,baseline
E004,1500,1500,baseline
E004,1500,400,synthesized
E005,500,450,baseline
```

Eight input rows produce eleven output rows: the three trigger rows have their `plan_end` closed at `plan_start`, and each emits one `status=synthesized` continuation row. The untriggered rows (`E003`, `E005`, the short rows of every employee) pass through unchanged.

## How it works

### One rule, two actions

The single rule's `when` predicate selects the trigger rows. For each trigger row:

- **`mutate.set`** rewrites `plan_end` to `plan_start`, closing the over-long window at its start boundary. The mutated row keeps its identity and is emitted in place.
- **`synthesize`** derives a brand-new continuation row. `copy_from: none` starts from an all-null base and every column is supplied by an `overrides` expression, so the continuation row is fully constructed from the trigger's values rather than copied. It is marked `status=synthesized` so downstream stages can tell originals from engine-derived rows.

Because Reshape applies every rule against the **original** group snapshot, the `mutate` and the `synthesize` both read the trigger row as it arrived — the mutation never feeds back into the synthesis.

### Audit provenance

Reshape stamps `$meta.synthetic`, `$meta.synthesized_by`, and `$meta.mutated_by` on every output row (see [Audit stamps](../nodes/reshape.md#audit-stamps)). These stay out of the default CSV output but are available for downstream CXL — a follow-on Route or Transform can filter on `$meta.synthetic` to handle generated rows separately.

### Bounded memory and spill

The example's `memory.limit: "16K"` with `backpressure: spill` is deliberately tiny so the run exercises Reshape's disk-spill path on a small fixture. Reshape buffers each employee's group, and when the budget trips it spills the raw input records to disk and re-runs synthesis on reload — the output is identical whether a group stayed in memory or round-tripped through disk. The per-stage spill volume appears in `clinker run --explain` and in the post-run spill summary. For real workloads, drop the artificial limit (the default budget is 512 MB) and Reshape stays in memory until it genuinely needs to spill. See [Reshape's memory model](../nodes/reshape.md#memory-model) and [Memory & Spill](../ops/memory.md) for the full picture.

### Idempotence

Re-running this pipeline over its own output does not re-trigger: a closed row has `plan_start - plan_end == 0`, and a `synthesized` continuation row likewise sits inside the boundary, so the `when` predicate fires only on genuinely over-long windows. That makes the backfill safe to apply repeatedly.
