# Reshape Nodes

Reshape nodes observe a whole correlation group and, per group, **mutate** the rows whose state caused a rule to fire while **synthesizing** new rows derived from those trigger rows. They are the node for "look at everything an entity did, then fix one record and insert the record that should have been there" — work no other node can do:

- **Aggregate** reduces a group to one summary row.
- **Transform** emits 0 or 1 record per input record.
- **Combine** joins records across sources.

None of those produce new records derived from a group's observed state while preserving the originals. Reshape does.

Reshape is a **blocking grouping operator**: it buffers every record of a group before any output row leaves, because a rule cannot decide what to synthesize until it has seen the whole group. It has a single output.

## Basic structure

```yaml
- type: reshape
  name: backfill_plans
  input: plans
  config:
    partition_by: [employee_id]
    order_by:
      - { field: plan_start, order: asc }
    rules:
      - name: fix_long_plan_years
        when: "plan_start - plan_end > 365"
        mutate:
          set:
            plan_end: "plan_start"
        synthesize:
          copy_from: trigger
          overrides:
            status: "'synthesized'"
```

For each `employee_id` group, every row where `plan_start - plan_end > 365` (the trigger) has its `plan_end` rewritten and a new row synthesized from it with `status` overridden to `synthesized`.

## `partition_by`

A list of field names. Records sharing the same values for all `partition_by` fields form one group, and every rule observes and acts within a single group. This is the correlation key the operator reasons over.

## `order_by`

Optional. A list of sort fields (`{ field, order }`, where `order` is `asc` or `desc`) applied within each group before rules run, so order-dependent synthesis is deterministic. Nulls sort last. Arrival order breaks ties.

## Rules

Each entry in `rules:` is a declarative rule with a name, a trigger predicate, and optional mutation and synthesis actions. Rules are evaluated in declaration order — but every rule observes the **same original group snapshot** (see [No cascade](#no-cascade) below).

### `when` — the trigger predicate

`when` is a CXL boolean expression evaluated against each row in the group. A row for which `when` is true is a **trigger row** for that rule: its `mutate` rewrites it, and its `synthesize` derives new rows from it. CXL boolean operators are `and` / `or` / `not` (Clinker's expression language is not SQL).

### `mutate` — in-place trigger-row mutation

```yaml
mutate:
  set:
    plan_end: "plan_start"
    note: "concat(note, ' (corrected)')"
```

Each `set:` entry is `field: <CXL expression>`. The expression evaluates against the original trigger row and overwrites that field's value on the row.

Two restrictions are enforced at compile time:

- A `set:` target must already exist in the upstream schema. Reshape mutates existing columns; it does not add new ones. Emit the column from an upstream Transform first if you need it.
- A `set:` may not write a `partition_by` field — group identity must survive Reshape.

### `synthesize` — deriving new rows

```yaml
synthesize:
  copy_from: trigger
  overrides:
    plan_date: "'2024-01-01'"
    status: "'synthesized'"
```

For each trigger row, `synthesize` emits one new row:

- `copy_from: trigger` — the new row starts as a copy of the trigger row's values, then `overrides` are applied on top.
- `copy_from: none` — the new row starts all-null; `overrides` must supply every column (enforced at compile time, so a synthesized row is never silently empty).

Each `overrides:` entry is `field: <CXL expression>`, evaluated against the trigger row.

## No cascade

**Every rule observes the original group state.** A row mutated by rule A is not re-observed by rule B, and rule B's `when` predicate sees the row's original values, not rule A's edits. This is a deliberate guarantee:

- **Determinism** — cascade would make rule order silently change output.
- **Single observation** — the group is observed once.

To sequence dependent transformations, chain two Reshape nodes in the DAG so the second observes the first's output.

## Mutation conflicts

If two rules write the **same field** on the **same row**, that is a mutation conflict. Some conflicts are caught at compile time when the rules' selectors statically overlap; content-dependent collisions that cannot be proven at compile time are caught at runtime.

A runtime conflict routes a dead-letter-queue entry under the `mutation_conflict` category, and the **whole correlation group rolls back** — none of that group's mutated or synthesized rows reach the output. The DLQ entry's stage label is `reshape:<node>:<rule_a>+<rule_b>`, naming the colliding rule pair. See [Error Handling & DLQ](../pipelines/error-handling.md).

## Audit stamps

Reshape stamps three engine-written columns on its output records so the provenance of a synthesized or mutated row is queryable downstream:

| Column | Meaning |
|--------|---------|
| `$meta.synthetic` | `true` on a synthesized row, `false` on an original (including a mutated trigger row) |
| `$meta.synthesized_by` | the `<node>:<rule>` that synthesized the row (empty on originals) |
| `$meta.mutated_by` | comma-separated `<node>:<rule>` labels of every rule that mutated the row (empty if none) |

Like the `$ck.*` correlation columns, these `$meta.*` columns stay out of the default writer output — they are available for downstream CXL and audit, not silently dumped into your output files.

## Memory model

Reshape is a **blocking, grouped** operator: it groups every input record by `partition_by` before any rule fires, because each rule must observe its whole correlation group (the [no-cascade contract](#no-cascade) forbids folding a group incrementally). It therefore cannot stream — the full group set materializes before the first output row leaves.

That per-group buffer is governed by the same central memory arbitrator every other blocking operator polls (see [Memory & Spill](../ops/memory.md)). As records are grouped, Reshape tracks the live in-memory footprint and, whenever the run crosses the **soft** spill threshold (80% of `memory.limit` by default), it spills buffered groups to disk:

- **What spills:** the **raw input records**, never the post-processed output rows. On reload at finalize, mutation and synthesis re-run in memory exactly as they would have without spilling, so the output is byte-identical whether a group stayed resident or round-tripped through disk. Spilling input records (rather than output rows) is also what keeps a `copy_from: none` synthesized row — built against the wider output schema — from being reconstructed against the wrong schema; input records all share one uniform schema.
- **Spill priority:** `15`, between grace-hash Combine (`10`) and external sort (`20`). A grouped record buffer costs more to evict than grace partitions (reload pays the re-synthesis CPU) but less than an external-sort merge. Reshape **cannot back-pressure** — once its predecessor has drained there is no upstream channel to pause — so under memory pressure it always spills rather than pausing a producer.
- **Hysteresis:** spill is driven by the resident-byte budget with the run's RSS as a backstop. A transient RSS spike that the OS reclaims lets grouping resume in memory rather than thrashing one spill per record; the buffer spills the largest resident groups first and stops as soon as the footprint drops back under the threshold.
- **Skew (one giant group):** a single correlation group too large to fit on its own is spilled **incrementally** — sliced by the upper bits of each record's arrival hash so successive spill waves evict fractions of the one group — while smaller groups stay resident under the budget. A skewed partition never forces buffering the whole oversized group before any of it reaches disk.

Reshape's buffer is **byte-budget-only**: it does not honor the `error_handling.max_group_buffer` record-count cap. That cap bounds the [correlation-commit](../pipelines/correlation-keys.md) group buffer used by the retraction machinery, a different buffer; Reshape's footprint is bounded by `memory.limit` and the spill path, not by a per-group record count.

The on-disk spill volume Reshape produces is surfaced per stage in `clinker run --explain` (the **Estimated spill volume** and **Spill compression** sections) and, after a run, in the actual per-stage spill totals.
