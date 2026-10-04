# Window Functions

Window functions allow CXL expressions to access aggregated values across a set of records within an analytic window. Unlike aggregate functions (which collapse groups into single rows), window functions attach computed values to each individual record.

Window functions are accessed via the `$window.*` namespace and require an `analytic_window:` configuration on the transform node.

*Interactive companion: the [window functions explainer](windows-explainer.html) shows, for any row, which rows of its partition each function reads.*

## Configuring an analytic window

Window functions are only available in transform nodes that declare an `analytic_window:` section in YAML:

```yaml
nodes:
  - name: ranked_sales
    type: transform
    input: raw_sales
    config:
      analytic_window:
        group_by: [region]
        sort_by:
          - field: amount
            order: desc
      cxl: |
        emit region = region
        emit amount = amount
        emit region_total = $window.sum(amount)
        emit running_total = $window.cumulative_sum(amount)
        emit rank_position = $window.row_number()
```

### Window configuration fields

| Field | Description |
|-------|-------------|
| `group_by` | List of fields to partition the window by (the SQL `PARTITION BY` axis). |
| `sort_by` | List of `{ field, order, null_order }` ordering specifications: `order` is `asc` (default) or `desc`, and `null_order` is `first` or `last` (default `last`), placing null keys before or after every value. Values compare by the rule every sort uses; see [How values are ordered](../nodes/sink.md#how-values-are-ordered). `null_order: drop` is rejected; see [Nulls in `sort_by`](#nulls-in-sort_by). |
| `source` | Optional explicit source-name reference for cross-source windows. |
| `on` | Optional cross-source partition-lookup field. |

### Which rows a function reads

There is no frame option. A window function reads one of three things:

- **The whole partition.** `sum`, `avg`, `min`, `max`, `count`, `first_value`, `last_value`, `first()`, `last()`, `any`, `every`, `exists`, `not_exists`, `collect` and `distinct` read every row of the record's partition, whatever the record's position. Every record in a partition gets the same `$window.sum(amount)`.
- **The partition up to the current record.** `cumulative_sum` is the running total, from the partition's first record (in `sort_by` order) through the current one.
- **A position.** `row_number`, `rank` and `dense_rank` give the current record's place in `sort_by` order; `lag(n)` and `lead(n)` read the record `n` places before or after it.

### Nulls in `sort_by`

`sort_by` only orders the rows of a partition; every row of the partition is
still seen by the window functions and written by the Transform. A row whose
key is null is placed by `null_order`: before every value with `first`, after
every value with `last`, in either direction.

`null_order: drop` is rejected when the pipeline is planned:

```text
transform "running": `null_order: drop` is not allowed on `analytic_window.sort_by` for field "amount": `sort_by` only orders the rows of a window partition, placing nulls `first` or `last`, and cannot remove a row. To remove the rows whose "amount" is null, delete `null_order: drop` and add a Transform before this node with `config: { cxl: "filter not amount.is_null()" }`.
```

The one fix is the filter the error prints: delete `null_order: drop` and
add a Transform before the windowed one whose whole `config` is the printed
line. That leaves rows with a null key out of every partition, and also
removes them from the windowed Transform's output:

```yaml
- type: transform
  name: with_amount
  input: orders
  config: { cxl: "filter not amount.is_null()" }
```

For a field CXL cannot write as a bare name, the error prints a
`source_name:` line instead; see
[`source_name`](../nodes/source.md#source_name--read-a-differently-named-physical-column).

## Aggregate window functions

These compute values over the record's whole partition, except `cumulative_sum`, which stops at the current record.

### $window.sum(field)

Sum of the field values across the whole partition. Null and non-numeric values are skipped; a sum of integers returns a Float.

```
emit running_total = $window.sum(amount)
```

### $window.cumulative_sum(field)

Running total of the field values from the partition's first record (in `sort_by` order) through the current record. Like `$window.sum`, a sum of integers returns a Float.

```
emit running_total = $window.cumulative_sum(amount)
```

### $window.avg(field)

Average of the field values across the whole partition. Returns Float.

```
emit moving_avg = $window.avg(amount)
```

### $window.min(field)

Minimum value in the partition.

```
emit window_min = $window.min(amount)
```

### $window.max(field)

Maximum value in the partition.

```
emit window_max = $window.max(amount)
```

### $window.count()

Number of records in the partition, the same for every record in it. Takes no arguments. For a record's position, use `$window.row_number()`.

```
emit window_size = $window.count()
```

### $window.first_value(field)

Returns the value of `field` at the first record of the partition
(ordered by `sort_by`). Equivalent to SQL `FIRST_VALUE(field)`.

```
emit opening_amount = $window.first_value(amount)
```

### $window.last_value(field)

Returns the value of `field` at the last record of the partition
(ordered by `sort_by`), the same for every record in it.

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

These return a whole record by position within the partition. Name the field to read after the call, as in `$window.lag(1).amount`. Without a field name the call returns null on every record, and no error is raised.

### $window.first().field

The first record of the partition, in `sort_by` order.

```
emit first_amount = $window.first().amount
```

### $window.last().field

The last record of the partition, in `sort_by` order.

```
emit last_amount = $window.last().amount
```

### $window.lag(n).field

The record `n` places before the current record. Returns `null` if there is no record at that offset.

```
emit prev_amount = $window.lag(1).amount
emit two_back = $window.lag(2).amount
```

### $window.lead(n).field

The record `n` places after the current record. Returns `null` if there is no record at that offset.

```
emit next_amount = $window.lead(1).amount
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

`$window.collect` and `$window.distinct` emit arrays. JSON writes them as native
arrays, XML as repeated child elements, and CSV as a delimited cell. Before a
scalar-only sink such as fixed-width, coerce the value in a downstream
`Transform` (for example `emit regions = unique_regions.join(";")`).

## Complete example

```yaml
nodes:
  - name: sales_analysis
    type: transform
    input: daily_sales
    config:
      analytic_window:
        group_by: [store_id]
        sort_by:
          - field: sale_date
            order: asc
      cxl: |
        emit store_id = store_id
        emit sale_date = sale_date
        emit daily_revenue = revenue
        emit store_avg = $window.avg(revenue)
        emit store_total = $window.sum(revenue)
        emit revenue_to_date = $window.cumulative_sum(revenue)
        emit prev_day_revenue = $window.lag(1).revenue
        emit day_over_day = revenue - ($window.lag(1).revenue ?? revenue)
```

For each sale this adds the store's average and total over all of its days, its revenue to date, and the change from the previous day.

## Correlation-key error handling

Window functions work correctly when a pipeline uses
[correlation keys](../pipelines/correlation-keys.md) for group-atomic
error handling: if records are retracted from an upstream group, the
window recomputes the affected partitions so its output stays
consistent. There is nothing to configure.
