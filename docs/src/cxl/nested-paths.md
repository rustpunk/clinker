# Nested Paths

CXL records can carry nested arrays and maps as field values (for example, a JSON input where each record has an `items` array of objects). Reaching into that structure uses two complementary forms: dotted paths and bracket indices.

## Dotted paths

A dotted identifier path reads a static field name from a map.

```
doc.metadata.tenant
```

Each segment must be a valid identifier. Dotted paths are resolved at compile time -- the typechecker walks the structure declared in the source schema and reports a missing-field error if any segment doesn't exist.

```yaml
- type: transform
  name: project_tenant
  input: events
  config:
    cxl: |
      emit tenant = doc.metadata.tenant
      emit user_id = doc.user.id
```

Use dotted paths for structures whose shape is fixed and known at authoring time.

## Bracket indices

A bracket index reads a runtime-computed key. The receiver may be an array (integer index) or a map (string index).

```
items[0]
profile["name"]
items.map(it => it["sku"])
```

Bracket indices are dynamic -- the index expression evaluates per record. The typechecker treats the result as `Any` and does not assert that the key is present.

### Integer index on an array

```yaml
- type: transform
  name: first_item
  input: orders
  config:
    cxl: |
      emit head = items[0]
      emit second = items[1]
```

For `items = [{"sku":"a"},{"sku":"b"},{"sku":"c"}]`, `head` is `{"sku":"a"}` and `second` is `{"sku":"b"}`.

Out-of-range indices return `null`. Negative indices also return `null` (CXL does not support negative indexing).

### String index on a map

```yaml
    cxl: |
      emit name = profile["name"]
      emit tier = profile["tier"]
```

Missing keys return `null` -- the lookup never raises an error. This is the same null-propagation policy [closure builtins](closures.md) use on their receivers.

## Mixing forms

The two forms compose in either order:

```yaml
    cxl: |
      emit first_sku = items[0]["sku"]
      emit profile_email = users.profile["email"]
```

`items[0]["sku"]` is two bracket indices chained -- an integer index against the array, then a string index against the resulting map. `users.profile["email"]` walks a dotted path to reach `profile` (a map field on `users`), then bracket-indexes into it for a runtime key.

## Null propagation

Every nested-access form propagates null end-to-end. If the receiver is null, the result is null without evaluating the index expression:

```yaml
    cxl: |
      emit sku = items[0]["sku"]
      -- when `items` is null, `sku` is null
      -- when `items[0]` is null, `sku` is also null
```

This matches the null behavior on dotted paths and on method-call receivers. Records with missing intermediate structure produce nulls in their derived fields rather than aborting the transform.

## Method calls on indexed values

A bracket-indexed expression is a regular value, so it composes with any method or further index:

```yaml
    cxl: |
      emit head_sku_upper = items[0]["sku"].upper()
      emit cheap_skus = items.filter(it => it["price"] < 10).map(it => it["sku"])
```

The first chain reads a string out of nested structure and uppercases it. The second filters an array of maps by a numeric field and projects the SKU strings out.

## See also

- [Closures](closures.md) -- closures over arrays of maps typically use bracket-index on the `it` binding.
- [Array Methods](builtins-array.md) -- traversal builtins that consume nested arrays.
- [Map Methods](builtins-map.md) -- builders and accessors for map values.
- [Null Handling](nulls.md) -- the wider null-propagation rules.
