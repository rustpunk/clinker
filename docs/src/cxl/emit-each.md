# Emit Each

The `emit each` statement fans one input record into multiple output records -- one per element of an array on the input. The body emits the fields each output record carries.

## Syntax

```
emit each <binding> in <source> {
  <statements>
}
```

- `<binding>` is the identifier the body uses to refer to the current array element. The conventional name is `it` (same as the [closure](closures.md) parameter), but any identifier is accepted.
- `<source>` is any expression producing an array. Typically a field reference on the input record.
- The body is a block of `let` and `emit` statements that produce one output record per iteration.

## Worked example

Suppose each input record carries an `items` array of objects, each with `sku` and `price`:

```ndjson
{"order_id":"O-1","items":[{"sku":"a","price":10},{"sku":"b","price":20},{"sku":"c","price":5}]}
```

A transform that fans each input into one record per item:

```yaml
- type: transform
  name: explode
  input: orders
  config:
    cxl: |
      emit each it in items {
        emit order_id = order_id
        emit sku = it["sku"]
        emit price = it["price"]
      }
```

For the input above, the transform produces three output records:

```ndjson
{"order_id":"O-1","sku":"a","price":10}
{"order_id":"O-1","sku":"b","price":20}
{"order_id":"O-1","sku":"c","price":5}
```

The body reads both `it` (the current element) and `order_id` (an outer record field). Outer-record fields remain visible inside the body for every iteration.

## Cardinality

If the source array has N elements, `emit each` produces exactly N output records. Empty array sources produce zero records. A `null` source also produces zero records -- no DLQ entry, no error -- mirroring the explode-on-null convention used elsewhere in CXL.

A non-array, non-null source raises a runtime type-mismatch error and routes the originating record to the DLQ.

## Output schema

The body's `emit` statements define the output record's field set, the same way `emit` does in a regular transform body. Fields the body does not emit fall under the Output node's `include_unmapped` policy (see [Output Nodes](../pipeline/output.md#unmapped-input-field-passthrough)).

Fields written by the body shadow same-named fields on the originating input record.

## Nested emit_each is rejected

Each transform body may contain at most one level of `emit each`. The parser rejects an `emit each` inside another `emit each` body:

```
emit each it in items {
  emit each sub in it["children"] { ... }   -- parse error
}
```

If you need a cartesian product, precompute the flattened array (for example with [`.flat_map`](builtins-array.md#flat_mapit--array---array)) and use a single `emit each` over the result.

## Body-statement restrictions

Within the body, only `let`, `emit`, and `trace` are accepted. `filter`, `distinct`, and nested `emit each` are rejected at evaluation time -- a body filter would split work between branches the engine can't represent. Move filter/distinct logic into a downstream transform, or pre-filter the source array with `.filter` before the `emit each` block.

## Safety cap: `max_expansion`

To bound fan-out, every transform body carries a `max_expansion` cap on the cumulative records `emit each` may produce from a single original input record. If the cap is exceeded, the originating record routes to the DLQ with category `expansion_limit_exceeded` instead of producing a truncated or unbounded result. The default cap is 10000.

See [Transform Nodes -> Expansion Cap](../pipeline/transform.md#expansion-cap-max_expansion) for the YAML field and tuning guidance.

## See also

- [Closures](closures.md) -- closures bind a similar `it` parameter inside method calls.
- [Array Methods](builtins-array.md) -- `flat_map` is the in-expression cousin of `emit each`.
- [Nested Paths](nested-paths.md) -- bracket-index access on the body binding.
- [Transform Nodes](../pipeline/transform.md) -- the `max_expansion` cap and DLQ routing.
- [Error Handling & DLQ](../pipeline/error-handling.md) -- DLQ category semantics.
