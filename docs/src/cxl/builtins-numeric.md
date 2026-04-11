# Numeric Methods

CXL provides 8 built-in methods for numeric operations. These methods work on both Integer and Float values (the `Numeric` receiver type). All return `null` when the receiver is `null`.

## abs() -> Numeric

Returns the absolute value. Preserves the original type (Int stays Int, Float stays Float).

```bash
$ cxl eval -e 'emit result = (-42).abs()'
```

```json
{
  "result": 42
}
```

```bash
$ cxl eval -e 'emit result = (-3.14).abs()'
```

```json
{
  "result": 3.14
}
```

## ceil() -> Int

Rounds up to the nearest integer. Returns the value unchanged for integers.

```bash
$ cxl eval -e 'emit result = 3.2.ceil()'
```

```json
{
  "result": 4
}
```

```bash
$ cxl eval -e 'emit result = (-3.2).ceil()'
```

```json
{
  "result": -3
}
```

## floor() -> Int

Rounds down to the nearest integer. Returns the value unchanged for integers.

```bash
$ cxl eval -e 'emit result = 3.8.floor()'
```

```json
{
  "result": 3
}
```

```bash
$ cxl eval -e 'emit result = (-3.2).floor()'
```

```json
{
  "result": -4
}
```

## round([decimals: Int]) -> Float

Rounds to the specified number of decimal places. Default is 0 decimal places.

```bash
$ cxl eval -e 'emit result = 3.456.round()'
```

```json
{
  "result": 3.0
}
```

```bash
$ cxl eval -e 'emit result = 3.456.round(2)'
```

```json
{
  "result": 3.46
}
```

## round_to(decimals: Int) -> Float

Rounds to the specified number of decimal places. Unlike `round()`, the `decimals` argument is required.

```bash
$ cxl eval -e 'emit result = 3.14159.round_to(3)'
```

```json
{
  "result": 3.142
}
```

Use `round_to` when you want to be explicit about precision in financial or scientific calculations:

```bash
$ cxl eval -e 'emit price = 19.995.round_to(2)'
```

```json
{
  "price": 20.0
}
```

## clamp(min: Numeric, max: Numeric) -> Numeric

Constrains the value to the given range. Returns `min` if the value is below it, `max` if above, or the value itself if within range.

```bash
$ cxl eval -e 'emit result = 150.clamp(0, 100)'
```

```json
{
  "result": 100
}
```

```bash
$ cxl eval -e 'emit result = (-5).clamp(0, 100)'
```

```json
{
  "result": 0
}
```

```bash
$ cxl eval -e 'emit result = 50.clamp(0, 100)'
```

```json
{
  "result": 50
}
```

## min(other: Numeric) -> Numeric

Returns the smaller of the receiver and the argument.

```bash
$ cxl eval -e 'emit result = 10.min(20)'
```

```json
{
  "result": 10
}
```

```bash
$ cxl eval -e 'emit result = 10.min(5)'
```

```json
{
  "result": 5
}
```

## max(other: Numeric) -> Numeric

Returns the larger of the receiver and the argument.

```bash
$ cxl eval -e 'emit result = 10.max(20)'
```

```json
{
  "result": 20
}
```

```bash
$ cxl eval -e 'emit result = 10.max(5)'
```

```json
{
  "result": 10
}
```

## Practical examples

**Clamp a percentage:**

```
emit pct = (completed / total * 100).clamp(0, 100).round_to(1)
```

**Absolute difference:**

```
emit diff = (actual - expected).abs()
```

**Floor division for batch numbering:**

```
emit batch = (row_number / 1000).floor()
```
