# Introspection & Debug

CXL provides 4 introspection methods and 1 debug method. The four introspection methods are the **only** methods that accept `null` receivers without propagating null -- they are designed specifically for inspecting and handling null values. `debug` follows ordinary null propagation.

## type_of() -> String

Returns the type name of the receiver as a string. Works on any value, including `null`.

Type name strings: `"string"`, `"int"`, `"float"`, `"decimal"`, `"bool"`, `"date"`, `"datetime"`, `"null"`, `"array"`, `"map"`.

```bash
$ cxl eval -e 'emit a = 42.type_of()' -e 'emit b = "hello".type_of()' \
    -e 'emit c = null.type_of()'
```

```json
{
  "a": "int",
  "b": "string",
  "c": "null"
}
```

Useful for branching on dynamic types:

```
emit formatted = match value.type_of() {
  "int"   => value.to_string().concat(" (integer)"),
  "float" => value.round_to(2).to_string().concat(" (decimal)"),
  _       => value.to_string()
}
```

## is_null() -> Bool

Returns `true` if the receiver is null, `false` otherwise. This is the primary way to test for null values -- it is NOT subject to null propagation.

```bash
$ cxl eval -e 'emit a = null.is_null()' -e 'emit b = 42.is_null()'
```

```json
{
  "a": true,
  "b": false
}
```

Use in filter statements:

```
filter not field.is_null()
```

## is_empty() -> Bool

Returns `true` for empty strings, empty arrays, or null values. Returns `false` for all other values.

```bash
$ cxl eval -e 'emit a = "".is_empty()' -e 'emit b = "hello".is_empty()' \
    -e 'emit c = null.is_empty()'
```

```json
{
  "a": true,
  "b": false,
  "c": true
}
```

Useful for filtering out blank or missing records:

```
filter not name.is_empty()
```

## catch(fallback: Any) -> Any

Returns the receiver if it is non-null, otherwise returns the fallback value. This is the method equivalent of the `??` operator.

```bash
$ cxl eval -e 'emit a = null.catch("default")' \
    -e 'emit b = "present".catch("default")'
```

```json
{
  "a": "default",
  "b": "present"
}
```

`catch` and `??` are interchangeable:

```
# These two are equivalent:
emit name = raw_name.catch("Unknown")
emit name = raw_name ?? "Unknown"
```

## debug(label: String) -> Any

Passes the receiver through unchanged while emitting a trace log with the given label. Zero overhead when tracing is disabled. The return value is always the receiver, making it safe to insert into any expression chain. A null receiver returns null without logging.

```bash
$ cxl eval -e 'emit result = 42.debug("check value")'
```

```json
{
  "result": 42
}
```

Insert `debug` anywhere in a method chain for inspection without affecting the output:

```
emit total = price.debug("price")
    * qty.debug("qty")
```

When tracing is enabled, this produces log lines like:

```
TRACE source_row=1 source_file=input.csv: price: Integer(100)
TRACE source_row=1 source_file=input.csv: qty: Integer(5)
```

## Null-safe summary

| Method | Null receiver behavior |
|--------|----------------------|
| `type_of()` | Returns `"null"` |
| `is_null()` | Returns `true` |
| `is_empty()` | Returns `true` |
| `catch(x)` | Returns `x` |
| `debug(l)` | Returns `null`, logs nothing (propagation) |
| All other methods | Return `null` (propagation) |
