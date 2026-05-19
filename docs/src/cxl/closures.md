# Closures

CXL supports arrow-syntax closures as arguments to closure-bearing array builtins like [`filter`, `map`, `find`, `any`, and `flat_map`](builtins-array.md). They give CXL a way to express element-by-element predicates and projections over nested arrays carried inside a single record -- without writing a separate transform node per element.

## Syntax

```
it => expression
```

A closure has one parameter, named `it`, and a single expression body. The arrow `=>` separates them.

```yaml
- type: transform
  name: filter_items
  input: orders
  config:
    cxl: |
      emit kept = items.filter(it => it["price"] > 5)
```

The body is an expression, not a block of statements. Use `if/then/else` or `match` if you need branching inside a closure.

```yaml
    cxl: |
      emit price_buckets = items.map(it =>
        if it["price"] >= 100 then "premium"
        else if it["price"] >= 10 then "standard"
        else "value")
```

## Parameter name

The parameter is always `it`. Other identifiers are not accepted as the closure binding:

```
items.filter(item => item["price"] > 5)   -- parse error
items.filter(it => it["price"] > 5)       -- ok
```

`it` is recognized in expression position only inside a closure body. Outside of one, it has no special meaning.

## Lexical capture

Inside the closure body, the outer record's fields and `let` bindings remain visible. For each iteration the closure parameter `it` is bound to the current element, the body evaluates, then `it` is removed before the next iteration.

```yaml
    cxl: |
      let threshold = 10
      emit kept = items.filter(it => it["price"] > threshold)
```

Here the closure body reads both `it` (the current array element) and `threshold` (an outer `let` binding). The record's fields are also reachable by name -- a closure over `items` can still read `customer_id`, `region`, or any other field on the same record.

## Where closures appear

Closures are valid only as method-call arguments to closure-bearing builtins. They cannot be assigned to variables, stored in fields, or passed to non-closure builtins:

```
let f = it => it * 2          -- rejected at resolve time
emit doubler = it => it * 2   -- rejected at resolve time
```

If you need to share a closure across multiple call sites, repeat the literal closure expression. CXL has no first-class function values.

## Null propagation

Closure-bearing builtins applied to a null receiver return null without evaluating the body. The body is also never called on records where the array is null:

```yaml
    cxl: |
      emit kept = items.filter(it => it["price"] > 5)
      -- when `items` is null, `kept` is null; the body never runs
```

This matches the null-propagation policy on every other builtin -- see [Null Handling](nulls.md) for the wider rules.

## Worked example: filter and map over a nested array

Suppose each input record carries an `items` array of objects, each with `sku` and `price`:

```ndjson
{"order_id":"O-1","items":[{"sku":"a","price":10},{"sku":"b","price":20},{"sku":"c","price":5}]}
```

A transform that drops cheap items and projects the remaining SKUs:

```yaml
- type: transform
  name: filter_items
  input: orders
  config:
    cxl: |
      emit order_id = order_id
      emit kept = items.filter(it => it["price"] > 5)
      emit kept_skus = items.filter(it => it["price"] > 5).map(it => it["sku"])
```

For the input above, the transform produces:

```json
{
  "order_id": "O-1",
  "kept": [{"sku": "a", "price": 10}, {"sku": "b", "price": 20}],
  "kept_skus": ["a", "b"]
}
```

Bracket-index access (`it["price"]`) reaches into each map element. See [Nested Paths](nested-paths.md) for the full traversal surface.

## See also

- [Array Methods](builtins-array.md) -- the closure-bearing builtins (`filter`, `map`, `find`, `any`, `flat_map`).
- [Map Methods](builtins-map.md) -- callable on map elements inside a closure body.
- [Nested Paths](nested-paths.md) -- bracket-index and dotted-path navigation through nested arrays and maps.
- [Emit Each](emit-each.md) -- statement that fans one input record into many output records, using a binding similar to the closure parameter.
