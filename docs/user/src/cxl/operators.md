# Operators & Expressions

CXL provides arithmetic, comparison, boolean, null coalescing, and string operators. Boolean logic uses keywords (`and`, `or`, `not`), not symbols.

## Arithmetic operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition (or string concatenation) | `2 + 3` |
| `-` | Subtraction | `10 - 4` |
| `*` | Multiplication | `3 * 5` |
| `/` | Division | `10 / 3` |
| `%` | Modulo (remainder) | `10 % 3` |

```bash
$ cxl eval -e 'emit result = 2 + 3 * 4'
```

```json
{
  "result": 14
}
```

Multiplication binds tighter than addition, so `2 + 3 * 4` is `2 + (3 * 4) = 14`, not `(2 + 3) * 4 = 20`.

```bash
$ cxl eval -e 'emit result = 10 % 3'
```

```json
{
  "result": 1
}
```

## Comparison operators

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equal | `x == 0` |
| `!=` | Not equal | `x != 0` |
| `>` | Greater than | `x > 10` |
| `<` | Less than | `x < 10` |
| `>=` | Greater than or equal | `x >= 10` |
| `<=` | Less than or equal | `x <= 10` |

```bash
$ cxl eval -e 'emit result = 5 > 3' --field dummy=1
```

```json
{
  "result": true
}
```

## Boolean operators

CXL uses **keywords** for boolean logic. The symbols `&&`, `||`, and `!` are not valid CXL syntax.

| Operator | Description | Example |
|----------|-------------|---------|
| `and` | Logical AND | `a and b` |
| `or` | Logical OR | `a or b` |
| `not` | Logical NOT (unary) | `not a` |

```bash
$ cxl eval -e 'emit result = true and not false'
```

```json
{
  "result": true
}
```

```bash
$ cxl eval -e 'emit result = 5 > 3 or 10 < 2'
```

```json
{
  "result": true
}
```

## Null coalesce operator

The `??` operator returns its left operand if non-null, otherwise its right operand.

```bash
$ cxl eval -e 'emit result = null ?? "default"'
```

```json
{
  "result": "default"
}
```

```bash
$ cxl eval -e 'emit result = "present" ?? "default"'
```

```json
{
  "result": "present"
}
```

## String concatenation

The `+` operator concatenates strings when both operands are strings.

```bash
$ cxl eval -e 'emit result = "hello" + " " + "world"'
```

```json
{
  "result": "hello world"
}
```

## Unary operators

| Operator | Description | Example |
|----------|-------------|---------|
| `-` | Numeric negation | `-x` |
| `not` | Boolean negation | `not done` |

```bash
$ cxl eval -e 'emit result = -42'
```

```json
{
  "result": -42
}
```

## Method calls

Methods are called on a receiver using dot notation:

```bash
$ cxl eval -e 'emit result = "hello".upper()'
```

```json
{
  "result": "HELLO"
}
```

Methods can be chained:

```bash
$ cxl eval -e 'emit result = "  hello  ".trim().upper()'
```

```json
{
  "result": "HELLO"
}
```

## Field references

Bare identifiers reference fields from the input record:

```bash
$ cxl eval -e 'emit result = price * qty' \
    --field price=10 \
    --field qty=3
```

```json
{
  "result": 30
}
```

Qualified field references use dot notation for multi-source pipelines: `source.field`.

## Operator precedence

From highest (binds tightest) to lowest:

| Precedence | Operators | Associativity |
|-----------|-----------|---------------|
| 1 (highest) | `.` (method calls, field access) | Left |
| 2 | `-` (unary), `not` | Prefix |
| 3 | `*` `/` `%` | Left |
| 4 | `+` `-` | Left |
| 5 | `==` `!=` `>` `<` `>=` `<=` | Left |
| 6 | `and` | Left |
| 7 | `or` | Left |
| 8 (lowest) | `??` | Right |

Use parentheses to override precedence:

```bash
$ cxl eval -e 'emit result = (2 + 3) * 4'
```

```json
{
  "result": 20
}
```

## Comments

Line comments start with `#` (when not followed by a digit -- digit-prefixed `#` starts a date literal):

```
# This is a comment
emit total = price * qty  # inline comment
emit deadline = #2024-12-31#  # this is a date literal, not a comment
```
