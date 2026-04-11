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
