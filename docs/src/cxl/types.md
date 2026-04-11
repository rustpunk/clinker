# Types & Literals

CXL has 9 value types. Every field value, literal, and expression result is one of these types.

## Value types

| Type | Rust backing | Description |
|------|-------------|-------------|
| Null | `Value::Null` | Missing or absent value |
| Bool | `bool` | `true` or `false` |
| Integer | `i64` | 64-bit signed integer |
| Float | `f64` | 64-bit double-precision float |
| String | `Box<str>` | UTF-8 text |
| Date | `NaiveDate` | Calendar date without timezone |
| DateTime | `NaiveDateTime` | Date and time without timezone |
| Array | `Vec<Value>` | Ordered collection of values |
| Map | `IndexMap<Box<str>, Value>` | Key-value pairs |

## Literal syntax

### Integers

Standard decimal notation. Negative values use the unary minus operator.

```bash
$ cxl eval -e 'emit a = 42' -e 'emit b = -5' -e 'emit c = 0'
```

```json
{
  "a": 42,
  "b": -5,
  "c": 0
}
```

### Floats

Decimal notation with a dot. Must have digits on both sides of the decimal point.

```bash
$ cxl eval -e 'emit a = 3.14' -e 'emit b = -0.5'
```

```json
{
  "a": 3.14,
  "b": -0.5
}
```

### Strings

Double-quoted or single-quoted. Supports escape sequences: `\\`, `\"`, `\'`, `\n`, `\t`, `\r`.

```bash
$ cxl eval -e 'emit greeting = "hello world"'
```

```json
{
  "greeting": "hello world"
}
```

### Booleans

The keywords `true` and `false`.

```bash
$ cxl eval -e 'emit flag = true' -e 'emit neg = not flag'
```

```json
{
  "flag": true,
  "neg": false
}
```

### Dates

Hash-delimited ISO 8601 format: `#YYYY-MM-DD#`.

```bash
$ cxl eval -e 'emit d = #2024-01-15#'
```

```json
{
  "d": "2024-01-15"
}
```

### Null

The keyword `null`.

```bash
$ cxl eval -e 'emit nothing = null'
```

```json
{
  "nothing": null
}
```

## Schema types

When declaring column types in YAML pipeline schemas, use these type names:

| Schema type | CXL type | Description |
|-------------|----------|-------------|
| `string` | String | Text values |
| `int` | Integer | 64-bit integers |
| `float` | Float | 64-bit floats |
| `bool` | Bool | Boolean values |
| `date` | Date | Calendar dates |
| `date_time` | DateTime | Date and time |
| `array` | Array | Ordered collections |
| `numeric` | Int or Float | Union type -- accepts either |
| `any` | Any | Unknown type -- no type constraints |
| `nullable(T)` | Nullable(T) | Wrapper -- value may be null |

Example YAML schema declaration:

```yaml
schema:
  employee_id: int
  name: string
  salary: nullable(float)
  start_date: date
```

## Type promotion

CXL automatically promotes types in mixed expressions:

**Int + Float promotes to Float:**

```bash
$ cxl eval -e 'emit result = 2 + 3.5'
```

```json
{
  "result": 5.5
}
```

**Null + T produces Nullable(T):** Any operation involving `null` produces a nullable result.

```bash
$ cxl eval -e 'emit result = null + 5'
```

```json
{
  "result": null
}
```

**Nullable(A) + B unifies to Nullable(unified):** When a nullable value meets a non-nullable value, the result type wraps the unified inner type in Nullable.

## Type unification rules

The type checker follows these rules when two types meet in an expression:

1. Same types unify to themselves: `Int + Int` produces `Int`
2. `Any` unifies with anything: `Any + T` produces `T`
3. `Numeric` resolves to the concrete type: `Numeric + Int` produces `Int`, `Numeric + Float` produces `Float`
4. `Int` promotes to `Float`: `Int + Float` produces `Float`
5. `Null` wraps: `Null + T` produces `Nullable(T)`
6. `Nullable` propagates: `Nullable(A) + B` produces `Nullable(unified(A, B))`
7. Incompatible types fail: `String + Int` is a type error
