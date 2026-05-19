# Array Methods

CXL provides closure-bearing and non-closure array builtins for traversing and transforming nested arrays carried on a single record. The closure-bearing methods take an arrow-syntax [closure](closures.md) and evaluate it once per element.

## Null propagation

Every array method returns `null` when the receiver is `null`. The closure body is not invoked on a null receiver.

## Closure-bearing methods

### filter(it => Bool) -> Array

Returns a new array containing the elements for which the closure body evaluates to `true`.

```yaml
- type: transform
  name: filter_items
  input: orders
  config:
    cxl: |
      emit kept = items.filter(it => it["price"] > 5)
```

For an input record where `items` is `[{"sku":"a","price":10},{"sku":"b","price":20},{"sku":"c","price":5}]`, `kept` is `[{"sku":"a","price":10},{"sku":"b","price":20}]`.

### map(it => T) -> Array

Returns a new array whose elements are the closure body's value for each input element. The element type need not match the input element type.

```yaml
    cxl: |
      emit skus = items.map(it => it["sku"])
      emit doubled_prices = items.map(it => it["price"] * 2)
```

`skus` is `["a", "b", "c"]`; `doubled_prices` is `[20, 40, 10]`.

### find(it => Bool) -> Element | Null

Returns the first element for which the closure body evaluates to `true`. Returns `null` if no element matches.

```yaml
    cxl: |
      emit first_premium = items.find(it => it["price"] > 15)
```

`first_premium` is `{"sku":"b","price":20}` for the running example.

### any(it => Bool) -> Bool

Returns `true` if the closure body evaluates to `true` for at least one element. Returns `false` if no element matches (including on an empty array).

```yaml
    cxl: |
      emit has_cheap = items.any(it => it["price"] < 10)
```

`has_cheap` is `true`.

### flat_map(it => Array) -> Array

Like `map`, but the closure body returns an array per input element; the results are concatenated into a single flat array. A null body result contributes no elements; a non-array body result contributes a single element.

```yaml
    cxl: |
      emit all_tags = items.flat_map(it => it["tags"])
```

For input items carrying `tags` arrays (e.g. `[{"sku":"a","tags":["new"]},{"sku":"b","tags":["sale","new"]}]`), `all_tags` is `["new","sale","new"]`.

## Non-closure methods

### remove(index: Int) -> Array

Returns a new array with the element at the given 0-based index removed. The original array is unchanged.

```yaml
    cxl: |
      emit shifted = items.remove(1)
```

`shifted` is `[{"sku":"a","price":10},{"sku":"c","price":5}]` -- index 0 is preserved, index 2 shifts down to index 1.

If the index is negative or out of range, `remove` returns the receiver array unchanged.

### length() -> Int

Returns the number of elements in the array. `length` is also defined on strings (see [String Methods](builtins-string.md)).

```yaml
    cxl: |
      emit item_count = items.length()
```

`item_count` is `3`.

### join(separator: String) -> String

Joins an array of values into a single string with the given separator between elements. Defined as a string method (see [String Methods](builtins-string.md)) but accepts array receivers.

```yaml
    cxl: |
      emit sku_list = items.map(it => it["sku"]).join(", ")
```

`sku_list` is `"a, b, c"`.

## Bracket indexing vs `.remove`

Bracket indexing (`items[0]`) reads an element by position and returns `null` when out of range. `.remove(idx)` returns a new array with the element dropped; out-of-range indices leave the array unchanged. See [Nested Paths](nested-paths.md) for the index-access surface.

## See also

- [Closures](closures.md) -- the `it => body` form used by closure-bearing array methods.
- [Map Methods](builtins-map.md) -- builtins that operate on the map elements typically iterated by these array methods.
- [Nested Paths](nested-paths.md) -- bracket-index and dotted-path access through nested arrays and maps.
- [Emit Each](emit-each.md) -- fan one input record into many output records, one per array element.
