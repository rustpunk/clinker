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

A runtime conflict routes a dead-letter-queue entry under the `mutation_conflict` category, and the **whole correlation group rolls back** — none of that group's mutated or synthesized rows reach the output. The DLQ entry's stage label is `reshape:<node>:<rule_a>+<rule_b>`, naming the colliding rule pair. See [Error Handling & DLQ](error-handling.md).

## Audit stamps

Reshape stamps three engine-written columns on its output records so the provenance of a synthesized or mutated row is queryable downstream:

| Column | Meaning |
|--------|---------|
| `$meta.synthetic` | `true` on a synthesized row, `false` on an original (including a mutated trigger row) |
| `$meta.synthesized_by` | the `<node>:<rule>` that synthesized the row (empty on originals) |
| `$meta.mutated_by` | comma-separated `<node>:<rule>` labels of every rule that mutated the row (empty if none) |

Like the `$ck.*` correlation columns, these `$meta.*` columns stay out of the default writer output — they are available for downstream CXL and audit, not silently dumped into your output files.

## Memory model

Reshape accumulates per-group state in memory: a group must be fully buffered before its rules run. Group buffering is bounded by the correlation group cap (`error_handling.max_group_buffer`). Disk-spill governance for these buffers is part of Reshape's ongoing memory-management work.
