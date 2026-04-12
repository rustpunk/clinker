# Statements

CXL programs are sequences of statements that execute top-to-bottom against each input record. Statement order matters -- later statements can reference values produced by earlier ones.

## emit

The `emit` statement produces an output field. Each `emit` becomes a column in the output record.

```
emit name = expression
```

```bash
$ cxl eval -e 'emit greeting = "hello"' -e 'emit doubled = 21 * 2'
```

```json
{
  "greeting": "hello",
  "doubled": 42
}
```

Multiple `emit` statements build up the output record field by field:

```bash
$ cxl eval -e 'emit first = "Alice"' -e 'emit last = "Smith"' \
    -e 'emit full = first + " " + last'
```

```json
{
  "first": "Alice",
  "last": "Smith",
  "full": "Alice Smith"
}
```

## let

The `let` statement creates a local variable binding. The variable is available to subsequent statements but is NOT included in the output record.

```
let name = expression
```

```bash
$ cxl eval -e 'let tax_rate = 0.21' -e 'emit tax = 100 * tax_rate'
```

```json
{
  "tax": 21.0
}
```

Note that `tax_rate` does not appear in the output -- only `emit` statements produce output fields.

## filter

The `filter` statement excludes records where the condition evaluates to false. When a filter excludes a record, remaining statements do not execute (short-circuit).

```
filter condition
```

```bash
$ cxl eval -e 'filter amount > 0' -e 'emit result = amount * 2' \
    --field amount=5
```

```json
{
  "result": 10
}
```

When the filter condition is false, the entire record is dropped and no output is produced.

Filters can appear anywhere in the statement sequence. Place them early to skip unnecessary computation:

```
filter status == "active"
let discount = if tier == "gold" then 0.2 else 0.1
emit final_price = price * (1 - discount)
```

## distinct

The `distinct` statement deduplicates records. The bare form deduplicates on all emitted fields. The `by` form deduplicates on a specific field.

```
distinct
distinct by field_name
```

In a pipeline, `distinct` tracks values seen so far and drops records that have already been emitted with the same key.

## emit meta

The `emit meta` statement writes a value to the `$meta.*` namespace -- per-record metadata that is not part of the output columns. Metadata can be read by downstream nodes via `$meta.field`.

```
emit meta quality_flag = if amount < 0 then "suspect" else "ok"
```

Access metadata downstream:

```
filter $meta.quality_flag == "ok"
```

## trace

The `trace` statement emits debug logging. It has no effect on the output record. Trace messages are only visible when tracing is enabled at the appropriate level.

```
trace "processing record"
trace warn "unusual value detected"
trace info if amount > 10000 then "high value transaction"
```

Trace levels: `trace` (default), `debug`, `info`, `warn`, `error`. An optional guard condition (via `if`) limits when the trace fires.

## Statement ordering

Statements execute sequentially. A statement can reference any field or variable defined by a preceding `emit` or `let`:

```bash
$ cxl eval -e 'let base = 100' -e 'let rate = 0.15' \
    -e 'emit subtotal = base * rate' \
    -e 'emit total = base + subtotal'
```

```json
{
  "subtotal": 15.0,
  "total": 115.0
}
```

Referencing a name before it is defined is a resolve-time error:

```
emit total = base + tax    # error: 'base' is not defined yet
let base = 100
let tax = base * 0.21
```

## use

The `use` statement imports a CXL module for reuse. See [Modules & use](modules.md) for details.

```
use shared.dates as d
emit fy = d::fiscal_year(invoice_date)
```
