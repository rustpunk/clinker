# Aggregation & Rollups

This recipe demonstrates grouping records and computing summary statistics. The pipeline filters active sales records, then rolls them up by department.

## Input data

`sales.csv`:

```csv
id,department,amount,status,rep
1,Engineering,5000,active,Alice
2,Marketing,3000,active,Bob
3,Engineering,7000,active,Carol
4,Sales,4000,inactive,Dave
5,Marketing,2000,active,Eva
6,Engineering,9500,active,Frank
7,Sales,6000,active,Grace
8,Marketing,1500,inactive,Hank
```

## Pipeline

`dept_rollup.yaml`:

```yaml
pipeline:
  name: dept_rollup

nodes:
  - type: source
    name: sales
    config:
      name: sales
      type: csv
      path: "./sales.csv"
      schema:
        - { name: id, type: int }
        - { name: department, type: string }
        - { name: amount, type: float }
        - { name: status, type: string }
        - { name: rep, type: string }

  - type: transform
    name: active_only
    input: sales
    config:
      cxl: |
        filter status == "active"

  - type: aggregate
    name: rollup
    input: active_only
    config:
      group_by: [department]
      cxl: |
        emit total = sum(amount)
        emit count = count(*)
        emit average = avg(amount)
        emit maximum = max(amount)
        emit minimum = min(amount)

  - type: output
    name: report
    input: rollup
    config:
      name: dept_totals
      type: csv
      path: "./output/dept_totals.csv"
```

## Run it

```bash
clinker run dept_rollup.yaml --dry-run
clinker run dept_rollup.yaml
```

## Expected output

`output/dept_totals.csv`:

```csv
department,total,count,average,maximum,minimum
Engineering,21500,3,7166.67,9500,5000
Marketing,5000,2,2500,3000,2000
Sales,6000,1,6000,6000,6000
```

One row per department. The inactive records (Dave's $4000, Hank's $1500) are excluded by the filter.

## How aggregation works

### Group-by keys

The `group_by` field lists the columns that define each group. Records with the same values for all group-by columns are aggregated together. The group-by columns appear automatically in the output -- you do not need to emit them.

### Aggregate functions

Available aggregate functions in CXL:

| Function | Description |
|----------|-------------|
| `sum(expr)` | Sum of values |
| `count(*)` | Number of records |
| `avg(expr)` | Arithmetic mean |
| `min(expr)` | Minimum value |
| `max(expr)` | Maximum value |
| `first(expr)` | First value encountered |
| `last(expr)` | Last value encountered |

### Strategy selection

Clinker offers two aggregation strategies:

- **Hash aggregation** (default): Builds an in-memory hash map keyed by the group-by columns. Works with any input order. Memory usage is proportional to the number of distinct groups.

- **Streaming aggregation**: Processes records in order, emitting each group's result as soon as the next group starts. Requires input sorted by the group-by keys. Uses minimal memory regardless of the number of groups.

The default strategy (`auto`) selects streaming when the optimizer can prove the input is sorted by the group-by keys, and hash otherwise. You can force a strategy:

```yaml
    config:
      group_by: [department]
      strategy: streaming   # requires sorted input
```

See [Memory Tuning](../ops/memory.md) for details on memory implications.

## Variations

### Multiple group-by keys

```yaml
    config:
      group_by: [department, region]
      cxl: |
        emit total = sum(amount)
        emit count = count(*)
```

Produces one row per unique (department, region) combination.

### Pre-aggregation transform

Compute derived fields before aggregating:

```yaml
  - type: transform
    name: prepare
    input: sales
    config:
      cxl: |
        filter status == "active"
        emit department = department
        emit amount = amount
        emit is_large = amount >= 5000

  - type: aggregate
    name: rollup
    input: prepare
    config:
      group_by: [department]
      cxl: |
        emit total = sum(amount)
        emit large_count = sum(if is_large then 1 else 0)
        emit small_count = sum(if not is_large then 1 else 0)
```

### Aggregation followed by routing

Aggregate first, then route the summary rows:

```yaml
  - type: aggregate
    name: rollup
    input: active_only
    config:
      group_by: [department]
      cxl: |
        emit total = sum(amount)

  - type: route
    name: split_by_total
    input: rollup
    config:
      mode: exclusive
      conditions:
        large: "total >= 10000"
      default: small
```

This routes departments with over $10,000 in total sales to one output and the rest to another.

### No group-by (grand total)

Omit `group_by` to aggregate all records into a single output row:

```yaml
    config:
      cxl: |
        emit grand_total = sum(amount)
        emit record_count = count(*)
        emit average_amount = avg(amount)
```
