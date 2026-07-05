# Cull Nodes

Cull nodes observe a whole correlation group and remove the entire group when a group-level predicate holds, routing the removed records to a **side-output port** instead of discarding them. They are the node for "look at everything an entity did, then set the whole entity aside for review or reprocessing" — work that operates on an aggregate property of a group, not on individual rows:

- **Route** fans out *per record* on a row predicate.
- **Reshape** mutates rows and synthesizes new ones *within* a group.
- **Aggregate** reduces a group to one summary row.

None of those remove a whole correlation group based on an aggregate property of the group and emit the removed rows on a second stream. Cull does.

Cull is a **blocking grouping operator**: it buffers every record of a group before any output leaves, because a group-level predicate (e.g. "this group has more than 100 rows") cannot be decided until the whole group is seen. It has **two output ports**: the main port (kept groups) and the `removed_to` side-output port (removed groups).

## Basic structure

```yaml
- type: cull
  name: flag_large_histories
  input: backfill
  config:
    partition_by: [employee_id]
    removed_to: review
    rules:
      - name: too_many_plans
        drop_group_when: "count(*) > 3"
```

For each `employee_id` group, if the group holds more than three rows the **whole group** is routed to the `review` side output; every other group flows to the main output.

## `partition_by`

A list of field names. Records sharing the same values for all `partition_by` fields form one group, and the removal predicate observes one whole group at a time. This is the correlation key the operator reasons over. `partition_by` must cover every visible correlation-key field so group identity is preserved on both output ports.

## `order_by`

Optional. A list of sort fields (`{ field, order }`, where `order` is `asc` or `desc`) applied within each group before its predicate runs, so an order-sensitive predicate is deterministic. Nulls sort last. Arrival order breaks ties.

## Rules

Each entry in `rules:` is a declarative removal rule with a name and a `drop_group_when` predicate. **A group is removed when any rule's predicate holds** (the rules are OR-combined).

### `drop_group_when` — the group-level removal predicate

`drop_group_when` is a CXL boolean expression evaluated in **aggregate context** over the whole group (group-by = `partition_by`). Because it is an aggregate expression, it uses CXL's aggregate functions:

| Aggregate | Meaning |
|-----------|---------|
| `count(*)` | number of rows in the group |
| `sum(<expr>)` | sum of an expression over the group |
| `min(<expr>)` / `max(<expr>)` | minimum / maximum over the group |
| `avg(<expr>)` | mean over the group |

```yaml
rules:
  - name: too_many_plans
    drop_group_when: "count(*) > 3"
  - name: high_total
    drop_group_when: "sum(amount) > 10000"
```

CXL's bare aggregate vocabulary is `sum` / `count` / `min` / `max` / `avg` / `collect` / `weighted_avg` — there is **no bare `any()` aggregate**. To express "remove the group if any row matches a condition", sum an indicator and compare to zero:

```yaml
rules:
  - name: drop_error_groups
    # Remove any account group containing at least one `error` row.
    drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
```

Ordered comparisons (`>`, `<`, `>=`, `<=`) work over every comparable aggregate type, not just numbers — the predicate uses the same comparison rules as a Transform. Numbers, strings, and dates all order:

```yaml
rules:
  - name: late_alphabet
    drop_group_when: "max(name) > 'M'"          # string ordering
  - name: recent_hire
    drop_group_when: "max(hired) >= #2020-01-01#" # date ordering (`#YYYY-MM-DD#` literal)
```

A group whose aggregate operand is null (for example `max(...)` over an all-null column) compares as false — a null operand never removes the group and never errors.

### Comments in a predicate

A `drop_group_when` predicate may carry a `#` line-comment to explain the rule inline. Each rule's predicate is parsed on its own, so a trailing comment applies only to that rule:

```yaml
rules:
  - name: drop_error_groups
    drop_group_when: "sum(if status == 'error' then 1 else 0) > 0  # any error row removes the group"
  - name: high_total
    drop_group_when: "sum(amount) > 10000                          # large accounts"
```

The comment is source text only — it never changes the compiled decision. (A `#YYYY-MM-DD#` date literal is unaffected: it lexes as a date, not a comment.)

## Output ports: main and `removed_to`

Cull has two producer-side output ports, the same mechanism a [Route](route.md) uses for its branches — not the dead-letter queue. Removed records are **valid rows the operator deliberately partitions onto a second stream**, not errors.

Downstream nodes draw from the two ports by reference:

- The **main output** (kept groups) is referenced by the Cull node's bare name: `input: flag_large_histories`.
- The **side output** (removed groups) is referenced as `<cull>.<removed_to>`: `input: flag_large_histories.review`.

```yaml
  - type: output
    name: kept
    input: flag_large_histories            # main port — kept groups
    config: { name: kept, type: csv, path: kept.csv }

  - type: output
    name: review
    input: flag_large_histories.review     # side-output port — removed groups
    config: { name: review, type: csv, path: review.csv }
```

`removed_to` must be a non-empty name distinct from the Cull node's own name (enforced at compile time, so the two ports are always distinguishable).

A single downstream node may draw from **both** ports — for example a [Merge](merge.md) recombining the kept and removed streams (`inputs: [flag_large_histories, flag_large_histories.review]`) — and receives the union of both ports' records.

### `removed_to` is not the DLQ

The `removed_to` port carries the **unchanged upstream schema** — Cull does not widen, and both ports emit exactly the input columns. Removed records are not `DlqEntry`s and never appear in the dead-letter queue or its counters; they flow down a normal data edge to whatever node draws the `removed_to` port (an audit sink, a reprocessing branch, another transform). Use the DLQ for *errors*; use a Cull side output for *valid records you want to handle separately*. See [Error Handling & DLQ](../pipelines/error-handling.md) for the error path.

## Memory model

Cull is a **blocking, grouped** operator: it groups every input record by `partition_by` before any record leaves, because the group-level `drop_group_when` predicate is an aggregate property of the whole group and cannot be folded into a per-record keep/remove decision. It therefore cannot stream — the full group set materializes before the first output row leaves.

That per-group buffer is governed by the same central memory arbitrator every other blocking operator polls (see [Memory & Spill](../ops/memory.md)). As records are grouped, Cull tracks the live in-memory footprint and, whenever the run crosses the **soft** spill threshold (80% of `memory.limit` by default), it spills buffered groups to disk:

- **What spills:** the **raw input records**. On reload at finalize, each group is re-split onto its output port exactly as it would have been without spilling, so the output is identical whether a group stayed resident or round-tripped through disk — including within-group row order, which is restored to arrival order after a reload. (The per-group removal decision is computed from an in-memory aggregate over the same records; that aggregate state is `O(distinct groups)` and is never spilled — only the raw records spill. It cannot spill because Cull has no upstream channel to pause, so instead it is **bounded by a hard check**: if the group cardinality grows so large that the decision state alone would exceed `memory.limit`, the run fails loud with a memory-budget error rather than growing it unbounded. See the limit below.)
- **Spill priority:** `15`, between grace-hash Combine (`10`) and external sort (`20`), matching Reshape. Cull **cannot back-pressure** — once its predecessor has drained there is no upstream channel to pause — so under memory pressure it always spills its own buffer in-thread rather than pausing a producer.
- **Largest-first, stop at the threshold:** when the budget trips, Cull evicts the largest resident groups first and stops as soon as the resident footprint drops back under the soft threshold — it does not drain every group.
- **Skew (one giant group):** a single correlation group whose resident tail alone exceeds the budget is spilled **incrementally** — sliced by the upper bits of each record's admission sequence — while smaller groups stay resident, so the ingest-time resident peak stays bounded even under one giant skewed group.

The on-disk spill volume Cull produces is surfaced per stage in `clinker run --explain` (the **Estimated spill volume** and **Spill compression** sections) and, after a run, in the actual per-stage spill totals.

### Limit: a single group must fit the finalize budget

Cull evaluates its group-level predicate against the *whole* group at once, so even though cross-group and ingest-time peaks spill to disk, the finalize reload of one group needs that group to fit the memory budget. Skew slicing bounds the ingest peak, but a single correlation group larger than `memory.limit` has no in-budget representation. Rather than risk an out-of-memory crash, the run **fails loud** with a diagnostic naming the offending group's size. Raise `memory.limit`, or partition the input so no one correlation group is that large.

There is a second, symmetric bound on the **number** of groups. The per-group removal decision is held in an in-memory aggregate that is `O(distinct groups)` and — unlike the raw records — cannot spill. If a partition key is so high-cardinality that the decision state alone would exceed `memory.limit` (many small groups rather than one giant group), the run likewise **fails loud** with a memory-budget error naming the drop-decision state, rather than growing that state unbounded. Raise `memory.limit`, or coarsen the `partition_by` key so the group count fits the budget.
