# Aggregate Nodes

Aggregate nodes group records by one or more fields and compute summary values using CXL aggregate functions. They consume all input records in a group before emitting a single summary record per group.

## Basic structure

```yaml
- type: aggregate
  name: dept_totals
  input: employees
  config:
    group_by: [department]
    cxl: |
      emit total_salary = sum(salary)
      emit headcount = count(*)
      emit avg_salary = avg(salary)
```

Group-by fields pass through automatically -- you do not need to emit them. In this example, the output records contain `department`, `total_salary`, `headcount`, and `avg_salary`.

## Group-by fields

The `group_by:` field is a list of field names from the input schema. Records sharing the same values for all group-by fields are placed in the same group.

```yaml
    group_by: [region, department]
    cxl: |
      emit total_salary = sum(salary)
      emit max_salary = max(salary)
```

This produces one output record per unique `(region, department)` combination.

## Global aggregation

An empty `group_by` list treats the entire input as a single group, producing exactly one output record:

```yaml
- type: aggregate
  name: grand_totals
  input: orders
  config:
    group_by: []
    cxl: |
      emit grand_total = sum(amount)
      emit record_count = count(*)
      emit avg_order = avg(amount)
```

## Aggregate functions

The following aggregate functions are available in CXL:

| Function | Description |
|----------|-------------|
| `sum(field)` | Sum of all values in the group |
| `count(*)` | Number of records in the group |
| `avg(field)` | Arithmetic mean |
| `min(field)` | Minimum value |
| `max(field)` | Maximum value |
| `collect(field)` | Collect all values into an array |
| `weighted_avg(value, weight)` | Weighted average |

## Strategy hint

The `strategy:` field controls how aggregation is executed:

```yaml
- type: aggregate
  name: totals
  input: sorted_data
  config:
    group_by: [account_id]
    strategy: streaming
    cxl: |
      emit total = sum(amount)
```

| Strategy | Behavior |
|----------|----------|
| `auto` | Default. The optimizer chooses based on whether the input is provably sorted for the group-by keys. |
| `hash` | Force hash aggregation. Works on any input ordering. Holds all groups in memory (with disk spill if memory budget is exceeded). |
| `streaming` | Require streaming aggregation. Processes one group at a time with O(1) memory per group. **Compile-time error** if the input is not provably sorted for the group-by keys. |

### When to use streaming

If your source declares a `sort_order:` that covers the group-by fields, the optimizer will automatically choose streaming aggregation. Use `strategy: streaming` as an explicit assertion -- it turns a silent fallback to hash aggregation into a compile error, which is useful for catching sort-order regressions.

### When to use hash

Hash aggregation works on unsorted input and is the safe default. It uses more memory but handles any data ordering. Memory-aware disk spill kicks in when RSS approaches the pipeline's `memory_limit`.

## Correlation-key interaction

In a pipeline whose sources declare `correlation_key:` fields, the engine inspects each aggregate's `group_by` against the upstream CK lattice (the union of `$ck.*` shadow columns visible at the aggregate's input):

- `group_by` covers every upstream CK field — strict-collateral path. The aggregate emits one row per group, the row inherits the correlation identity of its inputs, and a DLQ trigger anywhere in the group rolls back the whole group including the aggregate output. Zero retraction overhead.
- `group_by` omits any upstream CK field — retraction protocol path. A single correlation group may span multiple aggregate groups; CK fields omitted from `group_by` stop being visible to downstream consumers of this aggregate's output as user-named columns. The engine retracts only the failing records and refinalizes affected groups.

Authors do not configure this — the engine selects the path automatically based on `group_by` content. A retraction-mode aggregate is incompatible with `strategy: streaming` (rejected with `E15Y`, because streaming aggregates emit at group-boundary close before the terminal correlation commit and that defeats the rollback window). Non-deterministic CXL builtins (e.g. `now`) downstream of a retraction-mode aggregate are rejected with `E15W`. See [Correlation Keys](correlation-keys.md#aggregate-interaction) for the full lattice rules.

A retraction-mode aggregate emits one engine-managed `$ck.aggregate.<name>` shadow column on its output schema, alongside `[group_by_columns] ++ [emitted_binding_columns]`. The column carries the aggregator's per-group index at finalize and costs ~16 bytes per emitted row (the `Value::Integer` payload plus its slot overhead); it is hidden from default writer output. The synthetic column is the lineage hook that lifts the post-aggregate retract path: a Transform or Output that fails on an aggregate output row carries the column on the failing record, the orchestrator's detect phase decodes the index back to the contributing source row ids, and the recompute phase retracts those source rows so the failing aggregate row's contributors are removed from the writer payload — matching the upstream-failure DLQ-fan-out semantic. See [Correlation Keys → Where retraction triggers are sourced](correlation-keys.md#where-retraction-triggers-are-sourced) and the runnable demo at [`examples/pipelines/retract-demo/`](https://github.com/rustpunk/clinker/tree/main/examples/pipelines/retract-demo).

The retraction protocol carries a per-aggregate cost — Reversible accumulators use a per-row lineage map, BufferRequired accumulators hold raw contributions until commit. Both paths additionally pay ~16 bytes per output row for the synthetic-CK shadow column. The [operator-by-operator retraction cost reference](correlation-keys.md#operator-by-operator-retraction-cost-reference) has the per-operator breakdown; `clinker run --explain` reports the live per-aggregate detail including the synthetic-CK line.

## Complete example

```yaml
- type: source
  name: transactions
  config:
    name: transactions
    type: csv
    path: "./data/transactions.csv"
    schema:
      - { name: account_id, type: string }
      - { name: txn_date, type: date }
      - { name: amount, type: float }
      - { name: category, type: string }
    sort_order:
      - { field: "account_id", order: asc }

- type: aggregate
  name: account_summary
  input: transactions
  config:
    group_by: [account_id]
    strategy: streaming
    cxl: |
      emit total_amount = sum(amount)
      emit txn_count = count(*)
      emit avg_amount = avg(amount)
      emit max_amount = max(amount)
      emit categories = collect(category)

- type: output
  name: summary_output
  input: account_summary
  config:
    name: summary_output
    type: csv
    path: "./output/account_summary.csv"
```
