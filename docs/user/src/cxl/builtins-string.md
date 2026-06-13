# String Methods

CXL provides 24 built-in methods for string manipulation. All string methods return `null` when the receiver is `null` (null propagation).

## Case conversion

### upper()

Converts all characters to uppercase.

```bash
$ cxl eval -e 'emit result = "hello world".upper()'
```

```json
{
  "result": "HELLO WORLD"
}
```

### lower()

Converts all characters to lowercase.

```bash
$ cxl eval -e 'emit result = "Hello World".lower()'
```

```json
{
  "result": "hello world"
}
```

## Whitespace trimming

### trim()

Removes leading and trailing whitespace.

```bash
$ cxl eval -e 'emit result = "  hello  ".trim()'
```

```json
{
  "result": "hello"
}
```

### trim_start()

Removes leading whitespace only.

```bash
$ cxl eval -e 'emit result = "  hello  ".trim_start()'
```

```json
{
  "result": "hello  "
}
```

### trim_end()

Removes trailing whitespace only.

```bash
$ cxl eval -e 'emit result = "  hello  ".trim_end()'
```

```json
{
  "result": "  hello"
}
```

## Substring testing

### starts_with(prefix: String) -> Bool

Tests whether the string starts with the given prefix.

```bash
$ cxl eval -e 'emit result = "hello world".starts_with("hello")'
```

```json
{
  "result": true
}
```

### ends_with(suffix: String) -> Bool

Tests whether the string ends with the given suffix.

```bash
$ cxl eval -e 'emit result = "report.csv".ends_with(".csv")'
```

```json
{
  "result": true
}
```

### contains(substring: String) -> Bool

Tests whether the string contains the given substring.

```bash
$ cxl eval -e 'emit result = "hello world".contains("lo wo")'
```

```json
{
  "result": true
}
```

## Find and replace

### replace(find: String, replacement: String) -> String

Replaces all occurrences of `find` with `replacement`.

```bash
$ cxl eval -e 'emit result = "foo-bar-baz".replace("-", "_")'
```

```json
{
  "result": "foo_bar_baz"
}
```

## Extraction

### substring(start: Int [, length: Int]) -> String

Extracts a substring starting at `start` (0-based character index). If `length` is provided, takes at most that many characters. If omitted, takes all remaining characters.

```bash
$ cxl eval -e 'emit result = "hello world".substring(6)'
```

```json
{
  "result": "world"
}
```

```bash
$ cxl eval -e 'emit result = "hello world".substring(0, 5)'
```

```json
{
  "result": "hello"
}
```

### left(n: Int) -> String

Returns the first `n` characters.

```bash
$ cxl eval -e 'emit result = "hello world".left(5)'
```

```json
{
  "result": "hello"
}
```

### right(n: Int) -> String

Returns the last `n` characters.

```bash
$ cxl eval -e 'emit result = "hello world".right(5)'
```

```json
{
  "result": "world"
}
```

## Padding

### pad_left(width: Int [, char: String]) -> String

Left-pads the string to the given width. Default pad character is a space.

```bash
$ cxl eval -e 'emit result = "42".pad_left(5, "0")'
```

```json
{
  "result": "00042"
}
```

```bash
$ cxl eval -e 'emit result = "hi".pad_left(6)'
```

```json
{
  "result": "    hi"
}
```

### pad_right(width: Int [, char: String]) -> String

Right-pads the string to the given width. Default pad character is a space.

```bash
$ cxl eval -e 'emit result = "hi".pad_right(6, ".")'
```

```json
{
  "result": "hi...."
}
```

## Repetition and reversal

### repeat(n: Int) -> String

Repeats the string `n` times.

```bash
$ cxl eval -e 'emit result = "ab".repeat(3)'
```

```json
{
  "result": "ababab"
}
```

### reverse() -> String

Reverses the characters in the string.

```bash
$ cxl eval -e 'emit result = "hello".reverse()'
```

```json
{
  "result": "olleh"
}
```

## Length

### length() -> Int

Returns the number of characters in the string. Also works on arrays, returning the number of elements.

```bash
$ cxl eval -e 'emit result = "hello".length()'
```

```json
{
  "result": 5
}
```

## Splitting and joining

### split(delimiter: String) -> Array

Splits the string by the delimiter, returning an array of strings.

```bash
$ cxl eval -e 'emit result = "a,b,c".split(",")'
```

```json
{
  "result": ["a", "b", "c"]
}
```

### join(delimiter: String) -> String

Joins an array of values into a string with the given delimiter. The receiver must be an array.

```bash
$ cxl eval -e 'emit result = "a,b,c".split(",").join(" - ")'
```

```json
{
  "result": "a - b - c"
}
```

## Regex operations

### matches(pattern: String) -> Bool

Tests whether the string fully matches the given regex pattern.

```bash
$ cxl eval -e 'emit result = "abc123".matches("^[a-z]+[0-9]+$")'
```

```json
{
  "result": true
}
```

### find(pattern: String) -> Bool

Tests whether the string contains a substring matching the given regex pattern (partial match).

```bash
$ cxl eval -e 'emit result = "hello world 42".find("[0-9]+")'
```

```json
{
  "result": true
}
```

### capture(pattern: String [, group: Int]) -> String

Extracts a capture group from the first regex match. Default group is 0 (the full match).

```bash
$ cxl eval -e 'emit result = "order-12345".capture("order-([0-9]+)", 1)'
```

```json
{
  "result": "12345"
}
```

## Formatting and concatenation

### format(fmt: String) -> String

Formats the receiver value as a string.

```bash
$ cxl eval -e 'emit result = 42.format("")'
```

```json
{
  "result": "42"
}
```

### concat(args: String...) -> String

Concatenates the receiver with one or more string arguments. Null arguments are treated as empty strings.

```bash
$ cxl eval -e 'emit result = "hello".concat(" ", "world")'
```

```json
{
  "result": "hello world"
}
```

This is variadic -- it accepts any number of string arguments:

```bash
$ cxl eval -e 'emit result = "a".concat("b", "c", "d")'
```

```json
{
  "result": "abcd"
}
```
