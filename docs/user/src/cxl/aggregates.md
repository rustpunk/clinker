# Aggregate Functions

Aggregate functions operate across grouped record sets in aggregate nodes, collapsing multiple input records into summary rows. They are distinct from [window functions](windows.md), which attach computed values to each individual record.

## Aggregate functions

CXL provides 7 aggregate functions. These are called as free-standing function calls (not method calls) within the CXL block of an aggregate node.

| Function | Signature | Returns | Description |
|----------|-----------|---------|-------------|
| `sum(expr)` | Numeric | Int, Float or Decimal (the input's type) | Sum of values |
| `count(*)` | -- | Int | Count of records in the group |
| `avg(expr)` | Numeric | Float, or Decimal for a decimal input | Arithmetic mean |
| `min(expr)` | Any | Any | Minimum value |
| `max(expr)` | Any | Any | Maximum value |
| `collect(expr)` | Any | Array | All values collected into an array |
| `weighted_avg(value, weight)` | Numeric, Numeric | Float / Decimal | Weighted arithmetic mean |

## YAML aggregate node

Aggregate functions are used inside the `cxl:` block of a node with `type: aggregate`. The node must declare `group_by:` fields.

```yaml
nodes:
  - name: dept_summary
    type: aggregate
    input: employees
    config:
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

### sum(expr) -> Int, Float or Decimal

Computes the sum of the expression across all records in the group. Null values are skipped.

```yaml
cxl: |
  emit total_revenue = sum(price * quantity)
```

The result has the type of the values summed: integers give an integer, floats
a float, decimals a decimal. Integers summed with floats give a float, and
integers summed with decimals a decimal. An integer sum outside the 64-bit
integer range is an error.

A float sum is the exact total of the group's values, rounded once to the
nearest float. It does not depend on the order rows arrive in or on
`memory.limit`: the same group gives the same bytes whether the Aggregate holds
every group in memory or spills and merges partial sums. A group that holds
`1e16`, `1.0` and `-1e16` sums to `1`, in any order, where adding the floats
left to right gives `0` or `1` depending on the order. Integers mixed with floats
are added exactly too, so an integer larger than 2^53 is not rounded before it
is added. A NaN in the group makes the sum NaN, and `+inf` with `-inf` makes it
NaN. Results can differ in the last bit from a version that rounded after each
addition.

A decimal sum is the exact total of the group's values, rounded once (half to
even) only when it does not fit a decimal at its scale. Its scale is the
largest scale among the group's values, zeros and integers included, so the
sum of `1.00`, `-1.00` and `2` is `2.00` whatever order the rows arrive in. It
is an error only when the whole group's exact total is outside the decimal
range, ±79,228,162,514,264,337,593,543,950,335; a group whose running total
passes outside the range and comes back is fine. The error's fix aggregates the
column as floats, `sum(amount.to_float())` with your column in place of
`amount`, when a binary float's range and precision will do.

A group whose values are all null gives null. Null is never a substitute for a
failure: a group that fails is an `aggregate_finalize` error (see [Error
categories](../pipelines/error-handling.md#error-categories)), which under
`strategy: continue` goes to the dead-letter output.

#### Decimal and float in one group

A decimal is never added to a float without an explicit conversion, in an
aggregate as in `amount + price`. A `sum`, `avg` or `weighted_avg` whose values
in one group include both a decimal and a float fails that group with:

```text
decimal and float in one group: a decimal is never added to a float without an explicit conversion; declare the column that holds the floats `type: decimal` in its Source schema, so every value in the group is a decimal
```

When the typechecker can see the mix, for example
`sum(if flag then amount else price)`, the pipeline does not compile (E200; see
[Conditionals](conditionals.md)). The run-time error covers what it cannot see:
a value whose type is only known at run time, such as an untyped column or a
`numeric` result like `amount.clamp(0, 100)`. Declaring the float column
`type: decimal` in its Source schema keeps the total exact: the reader parses
the column's text as a decimal, so every value in the group is a decimal. (A
JSON number read into a `decimal` column is still parsed through a float first;
see [#1299](https://github.com/rustpunk/clinker/issues/1299).) When the floats are computed upstream rather than read
from a Source column, there is no column to retype: convert the decimal values
with `.to_float()` instead, accepting binary float precision.

### count(*) -> Int

Counts the number of records in the group. The argument is the wildcard `*`.

```yaml
cxl: |
  emit num_orders = count(*)
```

### avg(expr) -> Float or Decimal

Computes the arithmetic mean. Null values are skipped.

```yaml
cxl: |
  emit avg_order_value = avg(order_total)
```

`avg(x)` is `sum(x) / count(x)`: the group's exact sum, rounded once as `sum`
rounds it, divided by the number of non-null values, so it is exact over floats
and decimals alike and does not depend on row order or `memory.limit`. Over
decimals the result
is a decimal, the quotient at full precision, so `avg(amount)` and
`sum(amount) / count(amount)` give the same digits and the same scale. Over
floats, and over integers mixed with floats, the result is a float. Over
integers alone it is a float: the exact integer total, converted once to a
float, divided by the count.

A decimal total outside the decimal range is an error, as for `sum`, and so is
a group mixing decimals and floats. A group whose values are all null gives
null.

### min(expr) -> Any

Returns the minimum value in the group. Works on numeric, string, and date types. Null values are skipped; a group whose values are all null gives null.

```yaml
cxl: |
  emit earliest_order = min(order_date)
  emit lowest_price = min(unit_price)
```

Values compare by the rule sorting uses (see [How values are ordered](../nodes/sink.md#how-values-are-ordered)):

- Integers, floats and decimals compare by their exact value, so a column that holds both integers and floats (for example one built by an `if` whose branches return an integer and a float) is compared value by value.
- NaN is the largest value, above `inf`.
- Strings, dates and datetimes order as they do in a Sink sort.

The result does not depend on the order rows arrive in, or on the memory limit.

When several values in the group are equal under that rule, `min` returns the same one every time: an integer before a decimal before a float, the decimal with fewer fractional digits, and the float with a negative sign before one with a positive sign. So `min` of `1` and `1.0` is `1`, `min` of the decimals `1.0` and `1.00` is `1.0`, and `min` of `-0.0` and `0.0` is `-0.0`.

### max(expr) -> Any

Returns the maximum value in the group. Works on numeric, string, and date types. Null values are skipped; a group whose values are all null gives null.

```yaml
cxl: |
  emit latest_order = max(order_date)
  emit highest_price = max(unit_price)
```

Values compare as they do for [`min`](#minexpr---any): numbers by their exact value across integer, float and decimal, and NaN is the largest value, so a group that holds a NaN has NaN as its maximum. The result does not depend on the order rows arrive in, or on the memory limit.

When several values in the group are equal, `max` picks in the reverse order to `min`: a float before a decimal before an integer, the decimal with more fractional digits, and the float with a positive sign. So `max` of `1` and `1.0` is `1.0`, `max` of the decimals `1.0` and `1.00` is `1.00`, and `max` of `-0.0` and `0.0` is `0.0`.

### collect(expr) -> Array

Collects all values of the expression into an array. Useful for building lists of values per group.

```yaml
cxl: |
  emit all_order_ids = collect(order_id)
```

Because `collect` emits an array, JSON writes it as a native array, XML as
repeated child elements, and CSV as a delimited cell. Coerce it to a scalar for
a format such as fixed-width (for example
`emit ids = all_order_ids.join(";")`).

### weighted_avg(value, weight) -> Float or Decimal

Computes a weighted average: `sum(value * weight) / sum(weight)`. Takes two arguments.

```yaml
cxl: |
  emit weighted_price = weighted_avg(unit_price, quantity)
```

`weighted_avg(v, w)` is `sum(v * w) / sum(w)`, with each row's `v * w`
computed as it is in any expression and both sums exact, rounded once as `sum`
rounds them. When either the value or the weight is a `decimal`, the result is
a decimal at full division precision, and it has the same digits and scale as
`sum(v * w) / sum(w)`. Otherwise it is a float. Over floats each row's
`v * w` is a float product, and the products and the weights are summed
exactly, so the average does not depend on row order or `memory.limit`. Over
integers alone the two exact totals are each converted once to a float and
divided.

These groups fail with an `aggregate_finalize` error rather than writing a
value:

- **Zero total weight.** The group's weights add up to exactly zero, so the
  average divides by zero, as `x / 0` does in any expression. Rows whose weight
  is zero add nothing to the average, so the error's fix drops them with a
  Transform before the Aggregate, `config: { cxl: "filter qty != 0" }` with
  your weight column in place of `qty`. A group whose non-zero weights cancel,
  such as a sale and its return, still totals zero after that filter and still
  fails.
- **A row's product out of range.** A row's decimal `value * weight` is outside
  the decimal range. Retracting that row clears the error. The error's fix
  computes the average in floats,
  `weighted_avg(price.to_float(), qty.to_float())` with your columns in place
  of `price` and `qty`.
- **A total or the quotient out of range.** A decimal total is outside the
  decimal range, or the quotient is because the weights nearly cancel.
- **Decimal and float in one group**, in one row or across rows (see
  [above](#decimal-and-float-in-one-group)).

Mixing a `decimal` with a binary `float` across the two arguments is a type
error when the typechecker can see it. Declare the float column
`type: decimal` in its Source schema so both arguments are decimals, or, when
the float is computed rather than read from a Source column, convert the
decimal argument with `.to_float()`. A group with no row whose value and
weight are both non-null gives null.

## Aggregates vs. windows

| Feature | Aggregate node | Window function |
|---------|---------------|-----------------|
| Record output | One row per group | One row per input record |
| Syntax | `sum(field)` (free-standing) | `$window.sum(field)` (namespace) |
| Configuration | `type: aggregate` + `group_by:` | `type: transform` + `analytic_window:` |
| Use case | Summarize groups | Enrich records with group context |

An Aggregate's `sum`, `avg` and `weighted_avg` are exact (see
[`sum`](#sumexpr---int-float-or-decimal)). A window function's `$window.sum` and
`$window.avg` are not: they still add in the order the rows of the partition
are held, so a window sum over floats can differ in its last bits from the
Aggregate's sum of the same values.

## Combining aggregates with expressions

Aggregate function calls can be mixed with regular CXL expressions in emit statements:

```yaml
nodes:
  - name: category_stats
    type: aggregate
    input: products
    config:
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
      type: sink
      input: monthly_summary
      format: json
      path: summary.json
```

This pipeline outputs JSON because `all_reps = collect(sales_rep)`
emits an array, which the tabular writers (CSV/XML/fixed-width)
reject; drop the `collect` binding or coerce it with a downstream
`Transform` to keep a CSV sink.
