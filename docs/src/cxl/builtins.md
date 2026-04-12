# Built-in Methods

CXL provides 68 built-in scalar methods organized into 8 categories. Methods are called on a receiver value using dot notation: `receiver.method(args)`.

## Null propagation

Most methods return `null` when the receiver is `null`. This means null values flow through method chains without causing errors. The exceptions are documented in [Introspection & Debug](builtins-introspection.md).

## Method categories

### [String Methods](builtins-string.md) (24 methods)

Text manipulation: case conversion, trimming, padding, searching, splitting, regex matching.

| Method | Description |
|--------|-------------|
| `upper`, `lower` | Case conversion |
| `trim`, `trim_start`, `trim_end` | Whitespace removal |
| `starts_with`, `ends_with`, `contains` | Substring testing |
| `replace` | Find and replace |
| `substring`, `left`, `right` | Extraction |
| `pad_left`, `pad_right` | Padding |
| `repeat`, `reverse` | Repetition and reversal |
| `length` | Character count |
| `split`, `join` | Splitting and joining |
| `matches`, `find`, `capture` | Regex operations |
| `format`, `concat` | Formatting and concatenation |

### [Numeric Methods](builtins-numeric.md) (8 methods)

Rounding, clamping, and comparison for integers and floats.

| Method | Description |
|--------|-------------|
| `abs` | Absolute value |
| `ceil`, `floor` | Ceiling and floor |
| `round`, `round_to` | Rounding to decimal places |
| `clamp` | Constrain to range |
| `min`, `max` | Pairwise minimum/maximum |

### [Date & Time Methods](builtins-date.md) (13 methods)

Date component extraction, arithmetic, and formatting.

| Method | Description |
|--------|-------------|
| `year`, `month`, `day` | Date component extraction |
| `hour`, `minute`, `second` | Time component extraction (DateTime only) |
| `add_days`, `add_months`, `add_years` | Date arithmetic |
| `diff_days`, `diff_months`, `diff_years` | Date difference |
| `format_date` | Custom date formatting |

### [Conversion Methods](builtins-conversion.md) (11 methods)

Type conversion in strict (error on failure) and lenient (null on failure) variants.

| Method | Description |
|--------|-------------|
| `to_int`, `to_float`, `to_string`, `to_bool` | Strict conversion |
| `to_date`, `to_datetime` | Strict date parsing |
| `try_int`, `try_float`, `try_bool` | Lenient conversion |
| `try_date`, `try_datetime` | Lenient date parsing |

### [Introspection & Debug](builtins-introspection.md) (5 methods)

Type inspection, null checking, and debugging. These are the only methods that accept null receivers without propagating null.

| Method | Description |
|--------|-------------|
| `type_of` | Returns the type name as a string |
| `is_null` | Tests for null |
| `is_empty` | Tests for empty string, empty array, or null |
| `catch` | Null fallback (equivalent to `??`) |
| `debug` | Passthrough with tracing side effect |

### [Path Methods](builtins-path.md) (5 methods)

File path component extraction.

| Method | Description |
|--------|-------------|
| `file_name` | Full filename with extension |
| `file_stem` | Filename without extension |
| `extension` | File extension |
| `parent` | Parent directory path |
| `parent_name` | Parent directory name |
