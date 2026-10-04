# Null Handling

Null values in CXL represent missing or absent data. CXL uses null propagation -- most operations on null produce null -- with specific tools for detecting and handling nulls.

*Interactive companion: the [null explainer](nulls-explainer.html) shows, step by step, how an expression is worked out when a field is null, and whether a `filter` keeps the record.*

## Null propagation

When a method receives a null receiver, it returns null without executing. This is called null propagation and applies to all methods except the [introspection methods](builtins-introspection.md).

```bash
$ cxl eval -e 'emit result = null.upper()'
```

```json
{
  "result": null
}
```

Propagation flows through method chains:

```bash
$ cxl eval -e 'emit result = null.trim().upper().length()'
```

```json
{
  "result": null
}
```

## Null propagation exceptions

Four methods are exempt from null propagation and actively handle null receivers:

| Method | Null behavior |
|--------|--------------|
| `is_null()` | Returns `true` |
| `type_of()` | Returns `"null"` |
| `is_empty()` | Returns `true` |
| `catch(x)` | Returns `x` |

`debug(label)` is not one of them: on a null receiver it returns null without logging, like any other method.

```bash
$ cxl eval -e 'emit a = null.is_null()
emit b = null.type_of()
emit c = null.catch("fallback")'
```

```json
{
  "a": true,
  "b": "null",
  "c": "fallback"
}
```

## Null coalesce operator (??)

The `??` operator returns its left operand if non-null, otherwise its right operand. It is the primary tool for providing default values.

```bash
$ cxl eval -e 'emit a = null ?? "default"
emit b = "present" ?? "default"'
```

```json
{
  "a": "default",
  "b": "present"
}
```

Chain multiple `??` operators for fallback chains:

```bash
$ cxl eval -e 'emit result = null ?? null ?? "last resort"'
```

```json
{
  "result": "last resort"
}
```

## Three-valued logic

Boolean operations with null follow three-valued logic: read null as "unknown", and a result is decided only when the known side settles it.

### and

| Left | Right | Result |
|------|-------|--------|
| `true` | `null` | `null` |
| `false` | `null` | `false` |
| `null` | `true` | `null` |
| `null` | `false` | `false` |
| `null` | `null` | `null` |

The key insight: `false and null` is `false` because the result is false regardless of the unknown value.

### or

| Left | Right | Result |
|------|-------|--------|
| `true` | `null` | `true` |
| `false` | `null` | `null` |
| `null` | `true` | `true` |
| `null` | `false` | `null` |
| `null` | `null` | `null` |

The key insight: `true or null` is `true` because the result is true regardless of the unknown value.

### not

| Operand | Result |
|---------|--------|
| `true` | `false` |
| `false` | `true` |
| `null` | `null` |

## Arithmetic with null

Any arithmetic operation involving null produces null:

```bash
$ cxl eval -e 'emit result = 5 + null'
```

```json
{
  "result": null
}
```

## Comparison with null

`==` and `!=` never produce null. Two nulls are equal, and a null is not equal to any other value:

```bash
$ cxl eval -e 'emit a = null == null
emit b = null != 100'
```

```json
{
  "a": true,
  "b": true
}
```

Every other comparison (`<`, `>`, `<=`, `>=`) involving null produces null, as arithmetic does:

```bash
$ cxl eval -e 'emit result = null > 100'
```

```json
{
  "result": null
}
```

## Null in conditions

A condition that comes out null is treated as "not true":

- `filter` keeps a record only when its condition is exactly `true`. A null condition drops the record, just as `false` does.
- `if` takes the `else` branch when its condition is null, or produces null when there is no `else`.
- A `match` without a subject skips an arm whose condition is null. A `match` with a subject compares with `==`, so a `null => …` arm matches a null subject.

Because `not null` is also null, a record whose `amount` is null fails both `filter amount > 100` and `filter not (amount > 100)`. And because `!=` never produces null, `filter amount != 100` keeps it. Test for null explicitly with `is_null()`, or give a default first with `??`, when it matters which way an empty value goes.

To test for null, use `is_null()`:

```bash
$ cxl eval -e 'emit result = null.is_null()'
```

```json
{
  "result": true
}
```

## Practical patterns

### Fallback values with ??

```
emit name = raw_name ?? "Unknown"
emit amount = raw_amount ?? 0
emit active = is_active ?? false
```

### Safe conversion with try_* and ??

```
emit price = raw_price.try_float() ?? 0.0
emit qty = raw_qty.try_int() ?? 1
```

### Explicit null testing

```
filter not amount.is_null()
emit has_email = not email.is_null()
```

### Catch method (equivalent to ??)

```
emit name = raw_name.catch("Unknown")
```

### Conditional null handling

```
emit status = if amount.is_null() then "missing"
    else if amount < 0 then "invalid"
    else "ok"
```

### Filter blank or null

```
# Filter out records where name is null or empty string
filter not name.is_empty()
```

### Null-safe chaining

When working with fields that may be null, place the null check early or use `??`:

```
# Safe: coalesce first, then transform
emit normalized = (raw_name ?? "").trim().upper()

# Safe: test before use
emit name = if raw_name.is_null() then "N/A" else raw_name.trim()
```
