# Types & Literals

CXL has 9 value types. Every field value, literal, and expression result is one of these types.

## Value types

| Type | Rust backing | Description |
|------|-------------|-------------|
| Null | `Value::Null` | Missing or absent value |
| Bool | `bool` | `true` or `false` |
| Integer | `i64` | 64-bit signed integer |
| Float | `f64` | 64-bit double-precision float |
| Decimal | `Decimal` | Exact base-10 fixed-point number for money/financials |
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
| `decimal` | Decimal | Exact base-10 fixed-point (money) — see below |
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

## The `decimal` type

`float` is an IEEE-754 binary float: it cannot represent most base-10 fractions
exactly, so `0.1 + 0.2` is `0.30000000000000004`, not `0.3`. That rounding is
unacceptable for money. The `decimal` type is an **exact base-10 fixed-point**
number — `0.10 + 0.20` is exactly `0.30` — and is the correct type for
monetary amounts, prices, tax, and any figure that must round like decimal
arithmetic on paper.

Declare a decimal column with `type: decimal` and a `scale` (the number of
fractional digits). `precision` (total significant digits) is optional
validation metadata:

```yaml
schema:
  - { name: amount, type: decimal, scale: 2 }
  - { name: tax_rate, type: decimal, scale: 4 }
```

A `decimal` column parses its raw text into an exact value and rounds off any
excess precision to the column `scale` (round-half-to-even, the unbiased
"banker's rounding" used in accounting), so a `scale: 2` column stores `2.567`
as `2.57`. This is one edge of a **boundary contract**: a declared `scale` pins
a value to that many places at the boundary it is declared on — a source
column's scale on read, an output column's scale on write (see [Aggregating
decimals](#aggregating-decimals)) — while decimals keep full precision *inside*
the pipeline.

### Arithmetic rules

- **`decimal ⊗ decimal → decimal`** — exact.
- **`decimal ⊗ int → decimal`** — the integer widens exactly, so `amount + 1`
  and `price * quantity` stay exact decimals.
- **`decimal ⊗ float` is a type error.** Mixing an exact decimal with a binary
  float would silently lose precision, so CXL rejects it and asks for an
  explicit cast. Choose the trade-off deliberately:
  - `amount.to_float() * rate` — opt into binary float precision.
  - `rate.to_decimal() * amount` — bring the float into exact decimal math
    (the float→decimal step is the one acknowledged lossy conversion).
- **Division and `avg`** compute at full precision — the exact quotient, not a
  binary-float approximation. Inside the pipeline a computed decimal keeps every
  digit; it is pinned to a fixed number of places only at a boundary that
  declares a `scale`. Declaring the *output* column `type: decimal` with a
  `scale` rounds the value to that many places on write (banker's rounding),
  exactly as a `decimal` *source* column rounds on read — so `avg(amount)`
  emitted into a `scale: 2` output column writes `1.33`, not the full quotient.
  With no declared output scale the full precision is preserved; use an explicit
  round in CXL when you need fixed places mid-pipeline.

Comparisons follow the same rule: `decimal < int` is fine, `decimal < float`
requires a cast.

### Casting

`x.to_decimal()` converts an int, string, or float into a decimal (`try_decimal`
is the lenient form that yields `null` on failure). `d.to_int()`,
`d.to_float()`, and `d.to_string()` convert a decimal back out.

### Worked example — an exact invoice total

```bash
$ cxl eval -e 'emit total = ("19.99".to_decimal() * 3) + "4.80".to_decimal()'
```

```json
{
  "total": "64.77"
}
```

`19.99 * 3 = 59.97`, `+ 4.80 = 64.77` — exact, with no binary-float drift.
(In a pipeline, declare the source columns `type: decimal` instead of casting;
JSON output renders a decimal as a scale-preserving string.)

### Aggregating decimals

`sum`, `avg`, `min`, `max`, `count`, and `distinct` all work over a `decimal`
column and stay exact — no binary float ever touches a running total:

- `sum(amount)` and `avg(amount)` return a `decimal`. The sum is the exact
  total to the cent; the average is the exact full-precision quotient.
- `min` / `max` return the exact extremum, and `count` returns an integer.
- Group-by and `distinct` keys are scale-normalized: two decimals that are
  numerically equal group together regardless of scale, so `2.50` and `2.5`
  fall in one group. This holds even when the aggregation spills to disk.

To pin an aggregate result to fixed places on the way out, declare the **output**
column `type: decimal` with a `scale`: the value is rounded to that scale on
write (banker's rounding), so `avg(amount)` over `1.00, 1.00, 2.00` writes `1.33`
into a `scale: 2` output column while `sum(amount)` stays `4.00`. An output
column with no declared scale keeps the full-precision quotient. This applies to
every format — CSV, JSON, and fixed-width — because the rounding happens as the
record is projected onto the output. For fixed-width output it is often
required: a full-precision quotient overflows a narrow numeric field, which is a
hard error, whereas the rounded value fits.

`weighted_avg` also stays exact over decimals: a decimal value or weight (or
both) gives an exact `sum(value * weight) / sum(weight)` at full division
precision, and a zero total weight returns null. A decimal in one position
mixed with a binary `float` in the other is a type error, matching the
`decimal ⊗ float` arithmetic rule — cast with `.to_decimal()` or `.to_float()`
so the value and weight share one numeric domain.

## Type unification rules

When two types meet in an expression, CXL coerces them automatically:

- Numbers combine: mixing an integer and a float gives a float (`2 + 3.5` is `5.5`).
- Anything combined with `null` is `null`.
- Mismatched types are an error: `String + Int` fails. Convert first with `.to_int()` or `.to_string()` so both sides are the same type.
