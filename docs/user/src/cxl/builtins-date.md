# Date & Time Methods

CXL provides 13 built-in methods for date and time manipulation. These methods work on Date and DateTime values. All return `null` when the receiver is `null`.

## Component extraction

### year() -> Int

Returns the year component.

```bash
$ cxl eval -e 'emit result = #2024-03-15#.year()'
```

```json
{
  "result": 2024
}
```

### month() -> Int

Returns the month component (1-12).

```bash
$ cxl eval -e 'emit result = #2024-03-15#.month()'
```

```json
{
  "result": 3
}
```

### day() -> Int

Returns the day-of-month component (1-31).

```bash
$ cxl eval -e 'emit result = #2024-03-15#.day()'
```

```json
{
  "result": 15
}
```

### hour() -> Int

Returns the hour component (0-23). **DateTime only** -- returns `null` for Date values.

```bash
$ cxl eval -e 'emit result = "2024-03-15T14:30:00".to_datetime().hour()'
```

```json
{
  "result": 14
}
```

### minute() -> Int

Returns the minute component (0-59). **DateTime only** -- returns `null` for Date values.

```bash
$ cxl eval -e 'emit result = "2024-03-15T14:30:00".to_datetime().minute()'
```

```json
{
  "result": 30
}
```

### second() -> Int

Returns the second component (0-59). **DateTime only** -- returns `null` for Date values.

```bash
$ cxl eval -e 'emit result = "2024-03-15T14:30:45".to_datetime().second()'
```

```json
{
  "result": 45
}
```

## Date arithmetic

### add_days(n: Int) -> Date

Adds `n` days to the date. Use negative values to subtract. Works on both Date and DateTime.

```bash
$ cxl eval -e 'emit result = #2024-01-15#.add_days(10)'
```

```json
{
  "result": "2024-01-25"
}
```

```bash
$ cxl eval -e 'emit result = #2024-01-15#.add_days(-5)'
```

```json
{
  "result": "2024-01-10"
}
```

### add_months(n: Int) -> Date

Adds `n` months to the date. Day is clamped to the last day of the target month if necessary.

```bash
$ cxl eval -e 'emit result = #2024-01-31#.add_months(1)'
```

```json
{
  "result": "2024-02-29"
}
```

```bash
$ cxl eval -e 'emit result = #2024-03-15#.add_months(-2)'
```

```json
{
  "result": "2024-01-15"
}
```

### add_years(n: Int) -> Date

Adds `n` years to the date. Leap day (Feb 29) is clamped to Feb 28 in non-leap years.

```bash
$ cxl eval -e 'emit result = #2024-02-29#.add_years(1)'
```

```json
{
  "result": "2025-02-28"
}
```

## Date difference

### diff_days(other: Date) -> Int

Returns the number of days between the receiver and the argument (`receiver - other`). Positive when the receiver is later.

```bash
$ cxl eval -e 'emit result = #2024-03-15#.diff_days(#2024-03-01#)'
```

```json
{
  "result": 14
}
```

```bash
$ cxl eval -e 'emit result = #2024-01-01#.diff_days(#2024-03-15#)'
```

```json
{
  "result": -74
}
```

### diff_months(other: Date) -> Int

Returns the difference in months between two dates.

> **Note:** This method currently returns `null` (unimplemented). Use `diff_days` and divide by 30 as an approximation.

### diff_years(other: Date) -> Int

Returns the difference in years between two dates.

> **Note:** This method currently returns `null` (unimplemented). Use `diff_days` and divide by 365 as an approximation.

## Formatting

### format_date(format: String) -> String

Formats the date/datetime using a `chrono` format string. See [chrono format syntax](https://docs.rs/chrono/latest/chrono/format/strftime/index.html).

Common format specifiers:

| Specifier | Description | Example |
|-----------|-------------|---------|
| `%Y` | 4-digit year | `2024` |
| `%m` | 2-digit month | `03` |
| `%d` | 2-digit day | `15` |
| `%H` | Hour (24h) | `14` |
| `%M` | Minute | `30` |
| `%S` | Second | `00` |
| `%B` | Full month name | `March` |
| `%b` | Abbreviated month | `Mar` |
| `%A` | Full weekday | `Friday` |

```bash
$ cxl eval -e 'emit result = #2024-03-15#.format_date("%B %d, %Y")'
```

```json
{
  "result": "March 15, 2024"
}
```

```bash
$ cxl eval -e 'emit result = #2024-03-15#.format_date("%Y/%m/%d")'
```

```json
{
  "result": "2024/03/15"
}
```

## Practical examples

**Fiscal year calculation (April start):**

```
let d = invoice_date
emit fiscal_year = if d.month() < 4 then d.year() - 1 else d.year()
```

**Age in days:**

```
emit days_since = now.diff_days(created_date)
```

**Quarter:**

```
emit quarter = match {
  invoice_date.month() <= 3  => "Q1",
  invoice_date.month() <= 6  => "Q2",
  invoice_date.month() <= 9  => "Q3",
  _                          => "Q4"
}
```

**ISO week format:**

```
emit formatted = order_date.format_date("%Y-W%V")
```
