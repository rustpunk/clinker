# Conversion Methods

CXL provides two families of conversion methods: **strict** (6 methods) and **lenient** (5 methods). Strict conversions raise an error on failure, halting pipeline execution. Lenient conversions return `null` on failure, allowing graceful handling of dirty data.

All conversion methods accept any receiver type (`Any`).

## Strict conversions

Use strict conversions for required fields where invalid data should halt processing.

### to_int() -> Int

Converts the receiver to an integer. Errors on failure.

- Float: truncates toward zero
- String: parses as integer
- Bool: `true` becomes `1`, `false` becomes `0`

```bash
$ cxl eval -e 'emit result = "42".to_int()'
```

```json
{
  "result": 42
}
```

```bash
$ cxl eval -e 'emit result = 3.9.to_int()'
```

```json
{
  "result": 3
}
```

### to_float() -> Float

Converts the receiver to a float. Errors on failure.

- Integer: promotes to float
- String: parses as float

```bash
$ cxl eval -e 'emit result = "3.14".to_float()'
```

```json
{
  "result": 3.14
}
```

```bash
$ cxl eval -e 'emit result = 42.to_float()'
```

```json
{
  "result": 42.0
}
```

### to_string() -> String

Converts any value to its string representation. Never fails.

```bash
$ cxl eval -e 'emit result = 42.to_string()'
```

```json
{
  "result": "42"
}
```

```bash
$ cxl eval -e 'emit result = true.to_string()'
```

```json
{
  "result": "true"
}
```

### to_bool() -> Bool

Converts the receiver to a boolean. Errors on failure.

- String: `"true"`, `"1"`, `"yes"` become `true`; `"false"`, `"0"`, `"no"` become `false` (case-insensitive)
- Integer: `0` is `false`, everything else is `true`

```bash
$ cxl eval -e 'emit result = "yes".to_bool()'
```

```json
{
  "result": true
}
```

```bash
$ cxl eval -e 'emit result = 0.to_bool()'
```

```json
{
  "result": false
}
```

### to_date([format: String]) -> Date

Parses a string to a Date. Without a format argument, expects ISO 8601 (`YYYY-MM-DD`). With a format, uses chrono strftime syntax.

```bash
$ cxl eval -e 'emit result = "2024-03-15".to_date()'
```

```json
{
  "result": "2024-03-15"
}
```

```bash
$ cxl eval -e 'emit result = "15/03/2024".to_date("%d/%m/%Y")'
```

```json
{
  "result": "2024-03-15"
}
```

### to_datetime([format: String]) -> DateTime

Parses a string to a DateTime. Without a format argument, expects ISO 8601 (`YYYY-MM-DDTHH:MM:SS`). With a format, uses chrono strftime syntax.

```bash
$ cxl eval -e 'emit result = "2024-03-15T14:30:00".to_datetime()'
```

```json
{
  "result": "2024-03-15T14:30:00"
}
```

## Lenient conversions

Use lenient conversions for optional or dirty data fields. They return `null` instead of raising errors, making them safe to combine with `??` for fallback values.

### try_int() -> Int

Attempts to convert to integer. Returns `null` on failure.

```bash
$ cxl eval -e 'emit a = "42".try_int()' -e 'emit b = "abc".try_int()'
```

```json
{
  "a": 42,
  "b": null
}
```

### try_float() -> Float

Attempts to convert to float. Returns `null` on failure.

```bash
$ cxl eval -e 'emit a = "3.14".try_float()' -e 'emit b = "N/A".try_float()'
```

```json
{
  "a": 3.14,
  "b": null
}
```

### try_bool() -> Bool

Attempts to convert to boolean. Returns `null` on failure.

```bash
$ cxl eval -e 'emit a = "yes".try_bool()' -e 'emit b = "maybe".try_bool()'
```

```json
{
  "a": true,
  "b": null
}
```

### try_date([format: String]) -> Date

Attempts to parse a string as a Date. Returns `null` on failure.

```bash
$ cxl eval -e 'emit a = "2024-03-15".try_date()' \
    -e 'emit b = "not a date".try_date()'
```

```json
{
  "a": "2024-03-15",
  "b": null
}
```

### try_datetime([format: String]) -> DateTime

Attempts to parse a string as a DateTime. Returns `null` on failure.

```bash
$ cxl eval -e 'emit a = "2024-03-15T14:30:00".try_datetime()' \
    -e 'emit b = "invalid".try_datetime()'
```

```json
{
  "a": "2024-03-15T14:30:00",
  "b": null
}
```

## When to use each

**Strict conversions** (`to_*`) for:
- Required fields that must be valid
- Schema-enforced data where bad input should halt the pipeline
- Fields already validated upstream

**Lenient conversions** (`try_*`) for:
- Optional fields that may be missing or malformed
- Dirty data with mixed formats
- Fields where a fallback value is acceptable

## Practical patterns

**Safe numeric parsing with fallback:**

```
emit amount = raw_amount.try_float() ?? 0.0
```

**Parse dates from multiple formats:**

```
emit parsed = raw_date.try_date("%Y-%m-%d")
    ?? raw_date.try_date("%m/%d/%Y")
    ?? raw_date.try_date("%d-%b-%Y")
```

**Strict conversion for required fields:**

```
emit employee_id = raw_id.to_int()    # halts on bad data -- correct behavior
emit salary = raw_salary.to_float()   # must be numeric
```

**Lenient conversion for optional fields:**

```
emit bonus = raw_bonus.try_float()    # null if missing or non-numeric
emit total = salary + (bonus ?? 0.0)  # safe arithmetic
```
