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
      partition_by: [region]
      order_by:
        - field: amount
          direction: desc
      frame:
        type: rows
        start: unbounded_preceding
        end: current_row
    cxl: |
      emit region = region
      emit amount = amount
      emit running_total = $window.sum(amount)
      emit rank_position = $window.count()
```

### Window configuration fields

| Field | Description |
|-------|-------------|
| `partition_by` | List of fields to partition the window by (like SQL `PARTITION BY`) |
| `order_by` | List of ordering specifications (`field` + `direction`) |
| `frame.type` | Frame type: `rows` or `range` |
| `frame.start` | Frame start: `unbounded_preceding`, `current_row`, or `preceding(n)` |
| `frame.end` | Frame end: `unbounded_following`, `current_row`, or `following(n)` |

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

### $window.all(predicate)

Returns `true` if the predicate is true for all records in the window.

```
emit all_positive = $window.all(amount > 0)
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

## Complete example

```yaml
nodes:
  - name: sales_analysis
    type: transform
    input: daily_sales
    analytic_window:
      partition_by: [store_id]
      order_by:
        - field: sale_date
          direction: asc
      frame:
        type: rows
        start: preceding(6)
        end: current_row
    cxl: |
      emit store_id = store_id
      emit sale_date = sale_date
      emit daily_revenue = revenue
      emit week_avg = $window.avg(revenue)
      emit week_total = $window.sum(revenue)
      emit prev_day_revenue = $window.lag(1)
      emit day_over_day = revenue - ($window.lag(1) ?? revenue)
```

This computes a 7-day rolling average and total per store, along with day-over-day revenue change.

## Retraction interaction

When a window sits downstream of a relaxed-CK aggregate whose dropped correlation-key fields overlap the window's `partition_by`, the planner switches the window from streaming-emit to buffer-mode. The window operator stores per-partition raw row buffers until commit; on retraction, it reruns the configured `$window.*` evaluation over `partition − retracted_rows` and emits per-output deltas through the replay phase.

All 13 window functions are covered uniformly by wholesale recompute. The [operator-by-operator retraction cost reference](../pipeline/correlation-keys.md#operator-by-operator-retraction-cost-reference) has the per-operator memory ceilings; `clinker run --explain` reports the live per-window detail.
