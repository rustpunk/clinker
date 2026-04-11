# Aggregate Functions

Aggregate functions operate across grouped record sets in aggregate nodes, collapsing multiple input records into summary rows. They are distinct from [window functions](windows.md), which attach computed values to each individual record.

## Aggregate functions

CXL provides 7 aggregate functions. These are called as free-standing function calls (not method calls) within the CXL block of an aggregate node.

| Function | Signature | Returns | Description |
|----------|-----------|---------|-------------|
| `sum(expr)` | Numeric | Numeric | Sum of values |
| `count(*)` | -- | Int | Count of records in the group |
| `avg(expr)` | Numeric | Float | Arithmetic mean |
| `min(expr)` | Any | Any | Minimum value |
| `max(expr)` | Any | Any | Maximum value |
| `collect(expr)` | Any | Array | All values collected into an array |
| `weighted_avg(value, weight)` | Numeric, Numeric | Float | Weighted arithmetic mean |

## YAML aggregate node

Aggregate functions are used inside the `cxl:` block of a node with `type: aggregate`. The node must declare `group_by:` fields.

```yaml
nodes:
  - name: dept_summary
    type: aggregate
    input: employees
    group_by: [department]
    cxl: |
      emit total_salary = sum(salary)
      emit headcount = count(*)
      emit avg_salary = avg(salary)
      emit max_salary = max(salary)
      emit min_salary = min(salary)
```

### Group-by fields pass through automatically

Fields listed in `group_by:` are automatically included in the output. You do NOT need to emit them -- they are carried through as group keys.

In the example above, `department` is automatically present in every output record without an explicit `emit department = department` statement.

## Function details

### sum(expr) -> Numeric

Computes the sum of the expression across all records in the group. Null values are skipped.

```yaml
cxl: |
  emit total_revenue = sum(price * quantity)
```

### count(*) -> Int

Counts the number of records in the group. The argument is the wildcard `*`.

```yaml
cxl: |
  emit num_orders = count(*)
```

### avg(expr) -> Float

Computes the arithmetic mean. Null values are skipped. Returns Float.

```yaml
cxl: |
  emit avg_order_value = avg(order_total)
```

### min(expr) -> Any

Returns the minimum value in the group. Works on numeric, string, and date types.

```yaml
cxl: |
  emit earliest_order = min(order_date)
  emit lowest_price = min(unit_price)
```

### max(expr) -> Any

Returns the maximum value in the group. Works on numeric, string, and date types.

```yaml
cxl: |
  emit latest_order = max(order_date)
  emit highest_price = max(unit_price)
```

### collect(expr) -> Array

Collects all values of the expression into an array. Useful for building lists of values per group.

```yaml
cxl: |
  emit all_order_ids = collect(order_id)
```

### weighted_avg(value, weight) -> Float

Computes a weighted average: `sum(value * weight) / sum(weight)`. Takes two arguments.

```yaml
cxl: |
  emit weighted_price = weighted_avg(unit_price, quantity)
```

## Aggregates vs. windows

| Feature | Aggregate node | Window function |
|---------|---------------|-----------------|
| Record output | One row per group | One row per input record |
| Syntax | `sum(field)` (free-standing) | `$window.sum(field)` (namespace) |
| Configuration | `type: aggregate` + `group_by:` | `type: transform` + `analytic_window:` |
| Use case | Summarize groups | Enrich records with group context |

## Combining aggregates with expressions

Aggregate function calls can be mixed with regular CXL expressions in emit statements:

```yaml
nodes:
  - name: category_stats
    type: aggregate
    input: products
    group_by: [category]
    cxl: |
      emit total_revenue = sum(price * quantity)
      emit avg_price = avg(price)
      emit margin_pct = (sum(revenue) - sum(cost)) / sum(revenue) * 100
      emit product_count = count(*)
      emit has_premium = max(price) > 100
```

## Restrictions

- `let` bindings in aggregate transforms are restricted to row-pure expressions (no aggregate function calls in `let`).
- `filter` in aggregate transforms runs pre-aggregation -- it filters input records before grouping.
- `distinct` is not permitted inside aggregate transforms. Place a separate distinct transform upstream.

## Complete example

```yaml
pipeline:
  name: sales_summary
  nodes:
    - name: raw_sales
      type: source
      format: csv
      path: sales.csv

    - name: monthly_summary
      type: aggregate
      input: raw_sales
      group_by: [region, month]
      cxl: |
        emit total_sales = sum(amount)
        emit order_count = count(*)
        emit avg_order = avg(amount)
        emit top_sale = max(amount)
        emit all_reps = collect(sales_rep)

    - name: output
      type: output
      input: monthly_summary
      format: csv
      path: summary.csv
```
