# Backfill, Then Cull for Review

This recipe chains two grouping operators: a [Reshape](../nodes/reshape.md) node backfills each subject's history, then a [Cull](../nodes/cull.md) node sets aside whole subjects that need manual review — routing them to a second output stream instead of dropping them.

It ships as a runnable pipeline at `examples/pipelines/employee_plan_backfill.yaml`.

## The problem

You have run an SCD-style backfill over each employee's benefit-plan history (closing over-long windows and synthesizing continuation rows). After backfilling, some employees end up with a large or otherwise unusual plan history that an analyst should eyeball before it lands in the clean dataset. You want two outputs:

- a **clean** stream for the employees whose history looks fine, and
- a **review** stream for the flagged employees — their records intact, not discarded, not errored.

That is a per-group decision based on an aggregate property of the whole group ("this employee has more than three plan rows"), and the flagged records belong on a *second valid data stream*, not in the dead-letter queue. Cull fits exactly: it groups by `partition_by`, evaluates a group-level `drop_group_when` predicate, and emits removed groups on a first-class `removed_to` side-output port.

## Input data

`data/employee_plans.csv` — each employee's plan history, with start/end day-numbers:

```csv
employee_id,plan_start,plan_end,status
E001,100,90,baseline
E001,1000,100,baseline
E002,200,150,baseline
E003,50,40,baseline
E003,300,250,baseline
E003,600,550,baseline
E003,900,850,baseline
```

`E001` has one over-long window (`1000 - 100 > 365`); `E003` has four plan rows.

## The pipeline

```yaml
nodes:
  - type: source
    name: plans
    config:
      name: plans
      type: csv
      path: ./data/employee_plans.csv
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
              plan_end: "plan_start"
          synthesize:
            copy_from: trigger
            overrides:
              status: "'synthesized'"

  - type: cull
    name: flag_large_histories
    input: backfill
    config:
      partition_by: [employee_id]
      removed_to: review
      rules:
        - name: too_many_plans
          drop_group_when: "count(*) > 3"

  - type: output
    name: out
    input: flag_large_histories         # main port — kept employees
    config: { name: out, type: csv, path: ./output/employee_plans_clean.csv }

  - type: output
    name: review
    input: flag_large_histories.review  # side-output port — flagged employees
    config: { name: review, type: csv, path: ./output/employee_plans_review.csv }
```

## How it works

1. **Reshape (`backfill`)** groups by `employee_id` and, for the over-long window, closes it (`plan_end = plan_start`) and synthesizes a continuation row marked `status=synthesized`. `E001` gains a synthesized row, ending up with three rows.
2. **Cull (`flag_large_histories`)** groups the backfilled rows by `employee_id` again and evaluates `count(*) > 3` over each whole group. `E003` has four rows, so the **whole `E003` group** is routed to the `review` side-output port; `E001` (three rows) and `E002` (one row) flow to the main output.
3. **Two outputs** draw the two ports: `out` references the Cull node by name (the main port, kept employees); `review` references `flag_large_histories.review` (the side-output port, flagged employees).

The main output (`employee_plans_clean.csv`) carries `E001` (with its synthesized continuation row) and `E002`; the review output (`employee_plans_review.csv`) carries all four of `E003`'s rows. Both streams carry the unchanged input schema — Cull does not widen, and the flagged records are valid rows on a normal data edge, never DLQ entries.

## Expressing group-level conditions

`drop_group_when` is an **aggregate** predicate over the whole group. CXL's bare aggregates are `sum` / `count` / `min` / `max` / `avg` / `collect` / `weighted_avg` — there is no bare `any()`. To flag a group when **any** row matches a condition, sum an indicator and compare to zero:

```yaml
rules:
  # Flag the whole employee for review if any plan row is still flagged
  # `status == 'error'` after backfill.
  - name: any_error
    drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
```

See the [Cull node reference](../nodes/cull.md) for the full predicate vocabulary, the producer-side port model, and the bounded-memory spill behavior.
