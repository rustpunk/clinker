# Window Functions

Window functions allow CXL expressions to access aggregated values across a set of records within an analytic window. Unlike aggregate functions (which collapse groups into single rows), window functions attach computed values to each individual record.

Window functions are accessed via the `$window.*` namespace and require an `analytic_window:` configuration on the transform node.

## Configuring an analytic window

Window functions are only available in transform nodes that declare an `analytic_window:` section in YAML:

```yaml
nodes:
  - name: ranked_sales
    type: transform
    input: raw_sales
    analytic_window:
      group_by: [region]
      sort_by:
        - field: amount
          order: desc
    cxl: |
      emit region = region
      emit amount = amount
      emit running_total = $window.sum(amount)
      emit rank_position = $window.count()
```

### Window configuration fields

| Field | Description |
|-------|-------------|
| `group_by` | List of fields to partition the window by (the SQL `PARTITION BY` axis). |
| `sort_by` | List of `{ field, order }` ordering specifications (`order` is `asc` or `desc`). |
| `source` | Optional explicit source-name reference for cross-source windows. |
| `on` | Optional cross-source partition-lookup field. |

Frame specification (`frame: { rows: ... }` / `frame: { range: ... }`) is not yet plumbed through the YAML parser; today every window evaluates with a `rows: unbounded_preceding..current_row` semantic, which matches the SQL default for the listed window functions. See [the deferred-work tracker](https://github.com/rustpunk/clinker/issues) for status of explicit frame syntax.

## Aggregate window functions

These compute aggregate values over the window frame.

### $window.sum(field)

Sum of the field values in the window frame.

```
emit running_total = $window.sum(amount)
```

### $window.avg(field)

Average of the field values in the window frame. Returns Float.

```
emit moving_avg = $window.avg(amount)
```

### $window.min(field)

Minimum value in the window frame.

```
emit window_min = $window.min(amount)
```

### $window.max(field)

Maximum value in the window frame.

```
emit window_max = $window.max(amount)
```

### $window.count()

Count of records in the window frame. Takes no arguments.

```
emit window_size = $window.count()
```

### $window.first_value(field)

Returns the value of `field` at the first record of the window frame
(ordered by `sort_by`). Equivalent to SQL `FIRST_VALUE(field)`.

```
emit opening_amount = $window.first_value(amount)
```

### $window.last_value(field)

Returns the value of `field` at the last record of the window frame
(ordered by `sort_by`). Equivalent to SQL `LAST_VALUE(field)`.

```
emit closing_amount = $window.last_value(amount)
```

## Ranking window functions

Zero-argument integer functions that return the current row's rank
within its partition.

### $window.row_number()

1-indexed position of the current record within its partition.

```
emit row_idx = $window.row_number()
```

### $window.rank()

SQL `RANK()`: rows that share the same `sort_by` tuple receive the same
rank, and the next distinct row jumps by the size of the tie group.

```
emit sales_rank = $window.rank()
```

### $window.dense_rank()

SQL `DENSE_RANK()`: ties share a rank with no gaps between distinct
ranks.

```
emit sales_dense_rank = $window.dense_rank()
```

## Positional window functions

These access specific records by position within the window frame.

### $window.first()

Returns the value of the current field from the first record in the window frame.

```
emit first_amount = $window.first()
```

### $window.last()

Returns the value of the current field from the last record in the window frame.

```
emit last_amount = $window.last()
```

### $window.lag(n)

Returns the value from `n` records before the current record. Returns `null` if there is no record at that offset.

```
emit prev_amount = $window.lag(1)
emit two_back = $window.lag(2)
```

### $window.lead(n)

Returns the value from `n` records after the current record. Returns `null` if there is no record at that offset.

```
emit next_amount = $window.lead(1)
```

## Iterable window functions

These evaluate predicates or collect values across the window.

### $window.any(predicate)

Returns `true` if the predicate is true for any record in the window.

```
emit has_high = $window.any(amount > 1000)
```

### $window.every(predicate)

Returns `true` if the predicate is true for every record in the window.

```
emit all_positive = $window.every(amount > 0)
```

### $window.exists(predicate)

Returns `true` if the predicate is true for at least one record in the
window — a SQL-fluency alias of `$window.any`.

```
emit any_high = $window.exists(amount > 1000)
```

### $window.not_exists(predicate)

Returns `true` if no record in the window satisfies the predicate.
Equivalent to `not $window.exists(predicate)` and to
`$window.every(not predicate)`.

```
emit none_negative = $window.not_exists(amount < 0)
```

### $window.collect(field)

Collects all values of the field in the window into an array.

```
emit all_amounts = $window.collect(amount)
```

### $window.distinct(field)

Collects distinct values of the field in the window into an array.

```
emit unique_regions = $window.distinct(region)
```

`$window.collect` and `$window.distinct` emit arrays. The CSV, XML,
and fixed-width writers reject an array-valued field, so route such a
result to a JSON output, or coerce the emitted array to a scalar in a
downstream `Transform` (for example `emit regions = unique_regions.join(";")`)
before a tabular sink.

## Complete example

```yaml
nodes:
  - name: sales_analysis
    type: transform
    input: daily_sales
    analytic_window:
      group_by: [store_id]
      sort_by:
        - field: sale_date
          order: asc
    cxl: |
      emit store_id = store_id
      emit sale_date = sale_date
      emit daily_revenue = revenue
      emit week_avg = $window.avg(revenue)
      emit week_total = $window.sum(revenue)
      emit prev_day_revenue = $window.lag(1)
      emit day_over_day = revenue - ($window.lag(1) ?? revenue)
```

This computes per-store running averages and totals over the partition's history-up-to-and-including the current row.

## Correlation-key error handling

Window functions work correctly when a pipeline uses
[correlation keys](../pipelines/correlation-keys.md) for group-atomic
error handling: if records are retracted from an upstream group, the
window recomputes the affected partitions so its output stays
consistent. There is nothing to configure.
