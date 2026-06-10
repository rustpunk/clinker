# Map Methods

CXL provides five built-in methods for working with map values (key-value pairs). Maps arise naturally from JSON object inputs, from the [`set`](#setkey-string-value-any---map) builder below, and from upstream emits that produce nested structures.

All map methods return new values -- they never mutate the receiver. This is copy-on-write semantics: chaining `.set` then `.remove_field` produces a fresh map at each step, leaving the upstream binding untouched.

## Null propagation

Every map method returns `null` when the receiver is `null` or is not a `Value::Map`.

## Method reference

### keys() -> Array

Returns the map's keys as an array of strings, preserving insertion order.

```yaml
- type: transform
  name: list_keys
  input: rows
  config:
    cxl: |
      emit field_names = profile.keys()
```

For an input record where `profile` is `{"name":"Alice","tier":"gold","since":"2021-04"}`, `field_names` is `["name","tier","since"]`.

### values() -> Array

Returns the map's values as an array, preserving insertion order. Value types are heterogeneous -- the array carries each value as-is.

```yaml
    cxl: |
      emit field_values = profile.values()
```

`field_values` is `["Alice","gold","2021-04"]`.

### merge(other: Map) -> Map

Returns a new map containing every key from the receiver and from `other`. On conflicting keys, `other`'s value wins.

```yaml
    cxl: |
      emit enriched = profile.merge(overrides)
```

For `profile = {"name":"Alice","tier":"gold"}` and `overrides = {"tier":"platinum","since":"2021-04"}`, `enriched` is `{"name":"Alice","tier":"platinum","since":"2021-04"}`.

### set(key: String, value: Any) -> Map

Returns a new map with `key` set to `value`. If the key was already present, its value is replaced; insertion order is preserved.

```yaml
    cxl: |
      emit stamped = profile.set("region", "us-east")
```

`stamped` is `{"name":"Alice","tier":"gold","since":"2021-04","region":"us-east"}`.

#### Nested paths

`key` may be a dotted/indexed path that descends into nested maps and arrays, so a single `set` writes into a deep document. Dots separate map keys; a `[n]` suffix indexes an array.

```yaml
    cxl: |
      emit moved = profile.set("address.city", "NYC")
      emit relabel = order.set("items[0].sku", "A-100")
```

- **Auto-create.** Missing intermediate map segments are created as empty maps, so a path can build structure that does not yet exist. `{}.set("a.b.c", 7)` returns `{"a":{"b":{"c":7}}}`. This is what lets `set` assemble a nested document from scratch (matching jq `setpath` and Bloblang assignment).
- **Type conflict -> null.** If an intermediate segment already exists but is the wrong kind for the next step -- descending into a key whose value is a scalar, indexing a map with `[n]`, or naming a field on an array -- the whole operation returns `null`. Nothing is partially written.
- **Array index past the end -> null.** Indexing past the last element returns `null` for the whole operation; arrays are never silently grown. The path can only overwrite an array slot that already exists.
- **A bare key is a single key, not a path.** `"region"` writes the top-level `region`. Only `.` and `[n]` introduce nesting; a key with neither behaves exactly as before.

For `profile = {"name":"Alice","address":{"city":"LA"}}`, `profile.set("address.city", "NYC")` is `{"name":"Alice","address":{"city":"NYC"}}` -- the sibling `name` and any other `address` keys are preserved.

> **Known limitation.** Because `.` and `[` are path syntax, `set` cannot target a key whose name *literally* contains a `.` or `[` (for example a JSON field literally named `"a.b"`). To write such a key, build it with [`merge`](#mergeother-map---map) and a map literal; to remove it, use [`remove_field`](#remove_fieldkey-string---map), which matches the exact key string.

### remove_field(key: String) -> Map

Returns a new map without `key`. If the key was absent, the receiver is returned unchanged.

```yaml
    cxl: |
      emit slim = profile.remove_field("since")
```

`slim` is `{"name":"Alice","tier":"gold"}`.

## Worked example: chained set + remove_field

Map methods compose naturally because each returns a new map.

```yaml
- type: transform
  name: rewrite_profile
  input: rows
  config:
    cxl: |
      emit profile =
        profile.set("region", "us-east").remove_field("internal_id")
```

For `profile = {"name":"Alice","internal_id":"ix-77","tier":"gold"}`, the emitted `profile` is `{"name":"Alice","tier":"gold","region":"us-east"}`. The internal_id slot is removed and the region slot is appended; both happen on a fresh map so the upstream record's `profile` is unaffected for any other downstream branch.

## Parentheses are required

All map methods are method calls and must be written with parentheses, even the zero-argument ones:

```
profile.keys()         -- ok
profile.keys           -- parses as a field lookup, not a method call
```

`profile.keys` parses as a [dotted path](nested-paths.md) -- a lookup for a field literally named `keys` inside `profile`. That path almost certainly returns `null`. Always include the parentheses when invoking a map method.

## Using map methods inside array closures

Map methods compose with [closure-bearing array builtins](builtins-array.md) when the array elements are themselves maps.

```yaml
    cxl: |
      emit enriched_items = items.map(it => it.set("region", "us-east"))
      emit item_keys = items.map(it => it.keys())
```

Each `it` is a map; the closure body invokes a map method on it. `enriched_items` is an array where every element gained a `region` field. `item_keys` is an array of key-name arrays, one per element.

## See also

- [Closures](closures.md) -- arrow-syntax closures often invoke map methods on their `it` binding.
- [Array Methods](builtins-array.md) -- closure-bearing array methods commonly carry maps as their elements.
- [Nested Paths](nested-paths.md) -- bracket-index access (`profile["name"]`) reads a single key without producing a new map.
