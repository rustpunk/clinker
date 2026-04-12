# Conditionals

CXL provides two conditional expression forms: `if/then/else` and `match`. Both are expressions -- they return values and can be used anywhere an expression is expected.

## If / then / else

The basic conditional expression:

```
if condition then value else alternative
```

```bash
$ cxl eval -e 'emit label = if amount > 100 then "high" else "low"' \
    --field amount=250
```

```json
{
  "label": "high"
}
```

The `else` branch is optional. When omitted, records where the condition is false produce `null`:

```bash
$ cxl eval -e 'emit bonus = if score > 90 then score * 0.1' \
    --field score=80
```

```json
{
  "bonus": null
}
```

### Chained conditionals

Chain multiple conditions with `else if`:

```bash
$ cxl eval -e 'emit tier = if amount > 1000 then "platinum"
    else if amount > 500 then "gold"
    else if amount > 100 then "silver"
    else "bronze"' \
    --field amount=750
```

```json
{
  "tier": "gold"
}
```

### Nested usage

Since `if/then/else` is an expression, it can be used inside other expressions:

```bash
$ cxl eval -e 'emit price = base * (if member then 0.8 else 1.0)' \
    --field base=100 \
    --field member=true
```

```json
{
  "price": 80.0
}
```

## Match

The `match` expression provides pattern matching. It comes in two forms: value matching (with a subject) and condition matching (without a subject).

### Value form (with subject)

Match a subject expression against literal patterns:

```
match subject {
  pattern1 => result1,
  pattern2 => result2,
  _ => default
}
```

```bash
$ cxl eval -e 'emit label = match status {
    "A" => "Active",
    "I" => "Inactive",
    "P" => "Pending",
    _ => "Unknown"
  }' \
    --field status=A
```

```json
{
  "label": "Active"
}
```

The wildcard `_` is the catch-all arm. It matches any value not covered by preceding arms.

### Condition form (without subject)

When no subject is provided, each arm's pattern is evaluated as a boolean condition. This is CXL's equivalent of SQL's `CASE WHEN`:

```
match {
  condition1 => result1,
  condition2 => result2,
  _ => default
}
```

```bash
$ cxl eval -e 'emit tier = match {
    amount > 1000 => "high",
    amount > 100 => "medium",
    _ => "low"
  }' \
    --field amount=500
```

```json
{
  "tier": "medium"
}
```

### Practical examples

**Tiered pricing:**

```
emit discount = match {
  qty >= 1000 => 0.25,
  qty >= 100  => 0.15,
  qty >= 10   => 0.05,
  _           => 0.0
}
```

**Status code mapping:**

```
emit status_text = match http_code {
  200 => "OK",
  201 => "Created",
  400 => "Bad Request",
  404 => "Not Found",
  500 => "Internal Server Error",
  _   => "HTTP " + http_code.to_string()
}
```

**Region classification:**

```
emit region = match country {
  "US" => "North America",
  "CA" => "North America",
  "MX" => "North America",
  "GB" => "Europe",
  "DE" => "Europe",
  "FR" => "Europe",
  _    => "Other"
}
```

### Match arms are evaluated in order

The first matching arm wins. Place more specific conditions before general ones:

```
# Correct: specific before general
emit category = match {
  amount > 10000 => "enterprise",
  amount > 1000  => "business",
  _              => "personal"
}

# Wrong: first arm always matches
emit category = match {
  amount > 0     => "personal",    # catches everything positive
  amount > 1000  => "business",    # never reached
  amount > 10000 => "enterprise",  # never reached
  _              => "unknown"
}
```
