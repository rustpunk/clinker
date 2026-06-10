# Emit Each

The `emit each` statement fans one input record into multiple output records -- one per element of an array on the input. The body emits the fields each output record carries. A trailing [`outer`](#preserving-the-trigger-row-outer) modifier preserves the trigger row when the array is empty or null.

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

When fan-out [nests](#nested-fan-out-fan-out-within-fan-out), the cardinalities multiply: an outer array of M elements whose inner arrays have N elements each produces up to M×N records. The cumulative [`max_expansion`](#safety-cap-max_expansion) cap bounds that product.

A non-array, non-null source raises a runtime type-mismatch error and routes the originating record to the DLQ.

## Preserving the trigger row: `outer`

A trailing `outer` modifier switches `emit each` to its outer-join variant. The grammar is identical except for the keyword after the source:

```
emit each <binding> in <source> outer {
  <statements>
}
```

The only behavioral difference is what happens when the source is `null` or an empty array. Plain `emit each` drops the trigger row entirely (zero output records). The `outer` variant instead emits the trigger row **once**, with `<binding>` bound to `null`:

| Source       | `emit each ...`         | `emit each ... outer`                       |
| ------------ | ----------------------- | ------------------------------------------- |
| 3-element    | 3 records               | 3 records (identical)                       |
| empty array  | 0 records               | 1 record, binding = `null`                  |
| `null`       | 0 records               | 1 record, binding = `null`                  |

This is the shape SQL engines spell `LATERAL VIEW OUTER EXPLODE` (Spark, Hive) or an outer `UNNEST` (DuckDB): "for each tag on this article emit a tagged row, **but keep articles that have no tags**."

Using the worked example above with an order that carries no items:

```ndjson
{"order_id":"O-2","items":[]}
```

```yaml
- type: transform
  name: explode_outer
  input: orders
  config:
    cxl: |
      emit each it in items outer {
        emit order_id = order_id
        emit sku = it["sku"]
        emit price = it["price"]
      }
```

produces a single record that keeps `order_id` while the per-item fields read through the null binding:

```ndjson
{"order_id":"O-2","sku":null,"price":null}
```

Outer-record fields (like `order_id`) and any `emit` statements preceding the block still apply to the preserved trigger row, so an `outer` row is never bare.

The source type rule is slightly wider than plain `emit each`: a statically-`null` source is accepted (it is the case the variant exists to handle), alongside arrays and `Any`. Everything else in this page — the cumulative `max_expansion` cap, the [nesting](#nested-fan-out-fan-out-within-fan-out) rules, the body-statement restrictions — applies unchanged to the `outer` variant. The two variants compose freely: an `outer` block may nest inside a plain `emit each` block and vice versa.

## Output schema

The body's `emit` statements define the output record's field set, the same way `emit` does in a regular transform body. Fields the body does not emit fall under the Output node's `include_unmapped` policy (see [Output Nodes](../pipeline/output.md#unmapped-input-field-passthrough)).

Fields written by the body shadow same-named fields on the originating input record.

## Nested fan-out: fan-out within fan-out

An `emit each` body may itself contain `emit each` blocks — fan-out within fan-out for one trigger row. This is the canonical "for each article, for each section, for each tag, emit a row" shape:

```
emit each section in article["sections"] {
  emit each tag in section["tags"] {
    emit article_id = article_id
    emit section = section["name"]
    emit tag = tag
  }
}
```

For one input article, this produces one output record per (section, tag) pair. The inner binding (`tag`) reads the current inner element; the outer binding (`section`) and any outer-record field (`article_id`) stay visible inside the inner body. A field name reused as both an outer and inner binding shadows lexically — the inner binding wins inside the inner body, and the outer value is restored when the inner block finishes.

Emits are positional: an `emit` placed in the outer body *before* a nested block applies to every leaf record that block produces, but an `emit` placed *after* a nested block does not retroactively reach the records that block already emitted. Put the fields shared across leaves above the nested block.

Plain and `outer` blocks compose in any order. An inner plain `emit each` over an empty or null array contributes no records for that branch, while an inner `emit each ... outer` preserves one trigger row (inner binding bound to `null`) — exactly the per-level semantics from the [single-level table](#preserving-the-trigger-row-outer), applied at each level.

Nesting is bounded to 32 levels so that adversarially deep input cannot exhaust the parser stack; legitimate document fan-out is only a few levels deep. Beyond that bound, parsing fails with a "nesting too deep" diagnostic.

The flat-array workaround (precompute a flattened array with [`.flat_map`](builtins-array.md#flat_mapit--array---array) and use a single `emit each`) is still available and may be clearer for a simple two-level cartesian product, but is no longer required.

## Body-statement restrictions

Within the body, `let`, `emit`, `trace`, and nested `emit each` / `emit each ... outer` are accepted. `filter` and `distinct` are rejected at evaluation time -- a body filter would split work between branches the engine can't represent. Move filter/distinct logic into a downstream transform, or pre-filter the source array with `.filter` before the `emit each` block.

## Safety cap: `max_expansion`

To bound fan-out, every transform body carries a `max_expansion` cap on the cumulative records `emit each` may produce from a single original input record. The cap is **cumulative across all nesting levels**: every leaf record a nested fan-out produces charges against one shared budget, so nesting cannot multiply past the cap undetected. If the cap is exceeded, the originating record routes to the DLQ with category `expansion_limit_exceeded` instead of producing a truncated or unbounded result. The default cap is 10000.

See [Transform Nodes -> Expansion Cap](../pipeline/transform.md#expansion-cap-max_expansion) for the YAML field and tuning guidance.

## See also

- [Closures](closures.md) -- closures bind a similar `it` parameter inside method calls.
- [Array Methods](builtins-array.md) -- `flat_map` is the in-expression cousin of `emit each`.
- [Nested Paths](nested-paths.md) -- bracket-index access on the body binding.
- [Transform Nodes](../pipeline/transform.md) -- the `max_expansion` cap and DLQ routing.
- [Error Handling & DLQ](../pipeline/error-handling.md) -- DLQ category semantics.
