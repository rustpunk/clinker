# The cxl CLI Tool

The `cxl` command-line tool validates, evaluates, and formats CXL source files. It is the standalone companion to the Clinker pipeline engine, useful for testing expressions, validating transforms, and debugging CXL logic.

## Commands

### cxl check

Parse, resolve, and type-check a `.cxl` file. Reports errors with source locations and fix suggestions.

```bash
$ cxl check transform.cxl
ok: transform.cxl is valid
```

On errors:

```
error[parse]: expected expression, found '}' (at transform.cxl:12)
  help: check for missing operand or extra closing brace
```

```
error[resolve]: unknown field 'amoutn' (at transform.cxl:5)
  help: did you mean 'amount'?
```

```
error[typecheck]: cannot apply '+' to String and Int (at transform.cxl:8)
  help: convert one operand — use .to_int() or .to_string()
```

### cxl eval

Evaluate CXL expressions against provided data and print the result as JSON.

**Inline expression:**

```bash
$ cxl eval -e 'emit result = 1 + 2'
```

```json
{
  "result": 3
}
```

**From a file with field values:**

```bash
$ cxl eval transform.cxl \
    --field Price=10.5 \
    --field Qty=3
```

**From a file with JSON input:**

```bash
$ cxl eval transform.cxl --record '{"price": 10.5, "qty": 3}'
```

**Multiple inline expressions:**

```bash
$ cxl eval -e 'let tax = 0.21' -e 'emit net = price * (1 - tax)' \
    --field price=100
```

```json
{
  "net": 79.0
}
```

### cxl fmt

Parse and pretty-print a `.cxl` file in canonical format with normalized whitespace and consistent styling.

```bash
$ cxl fmt transform.cxl
```

Output is printed to stdout. Redirect to overwrite:

```bash
$ cxl fmt transform.cxl > transform.cxl.tmp && mv transform.cxl.tmp transform.cxl
```

## Input data

### --field name=value

Provide individual field values as key-value pairs. Values are automatically type-inferred:

| Input | Inferred type | Example |
|-------|--------------|---------|
| Integer pattern | Int | `--field count=42` |
| Decimal pattern | Float | `--field price=10.5` |
| `true` / `false` | Bool | `--field active=true` |
| `null` | Null | `--field value=null` |
| Anything else | String | `--field name=Alice` |

```bash
$ cxl eval -e 'emit t = amount.type_of()' --field amount=42
```

```json
{
  "t": "Int"
}
```

```bash
$ cxl eval -e 'emit t = name.type_of()' --field name=Alice
```

```json
{
  "t": "String"
}
```

### --record JSON

Provide a full JSON object as input. Mutually exclusive with `--field`.

```bash
$ cxl eval -e 'emit total = price * qty' \
    --record '{"price": 10.5, "qty": 3}'
```

```json
{
  "total": 31.5
}
```

JSON types map directly:

| JSON type | CXL type |
|-----------|----------|
| `null` | Null |
| `true` / `false` | Bool |
| integer number | Int |
| decimal number | Float |
| `"string"` | String |
| `[array]` | Array |

## Output format

Output is always JSON. Each `emit` statement produces a key-value pair:

```bash
$ cxl eval -e 'emit a = 1' -e 'emit b = "two"' -e 'emit c = true'
```

```json
{
  "a": 1,
  "b": "two",
  "c": true
}
```

Date and DateTime values are serialized as ISO 8601 strings:

```bash
$ cxl eval -e 'emit d = #2024-03-15#'
```

```json
{
  "d": "2024-03-15"
}
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success (or warnings only) |
| 1 | Parse, resolve, type-check, or evaluation errors |
| 2 | I/O error (file not found, invalid JSON, etc.) |

## Pipeline context in eval mode

When running `cxl eval`, a minimal pipeline context is provided:

| Variable | Value |
|----------|-------|
| `$pipeline.name` | `"cxl-eval"` |
| `$pipeline.execution_id` | Zeroed UUID |
| `$pipeline.batch_id` | Zeroed UUID |
| `$pipeline.start_time` | Current wall-clock time |
| `$pipeline.source_file` | Filename or `"<inline>"` |
| `$pipeline.source_row` | `1` |
| `now` | Current wall-clock time (live) |

## Practical usage

**Quick expression testing:**

```bash
$ cxl eval -e 'emit result = "hello world".upper().split(" ").length()'
```

```json
{
  "result": 2
}
```

**Validate a transform file:**

```bash
$ cxl check transforms/enrich_orders.cxl && echo "Valid"
```

**Test conditional logic:**

```bash
$ cxl eval -e 'emit tier = match {
    amount > 1000 => "high",
    amount > 100 => "med",
    _ => "low"
  }' \
    --field amount=500
```

```json
{
  "tier": "med"
}
```

**Test date operations:**

```bash
$ cxl eval -e 'emit year = d.year()' -e 'emit month = d.month()' \
    -e 'emit next_week = d.add_days(7)' \
    --record '{"d": "2024-03-15"}'
```

**Test null handling:**

```bash
$ cxl eval -e 'emit safe = raw.try_int() ?? 0' --field raw=abc
```

```json
{
  "safe": 0
}
```
