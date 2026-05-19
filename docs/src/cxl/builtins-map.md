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
