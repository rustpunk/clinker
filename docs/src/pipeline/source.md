# Source Nodes

Source nodes read data from files and are the entry points of every pipeline. They have no `input:` field -- they produce records, they do not consume them.

## Basic structure

```yaml
- type: source
  name: customers
  config:
    name: customers
    type: csv
    path: "./data/customers.csv"
    schema:
      - { name: customer_id, type: int }
      - { name: name, type: string }
      - { name: email, type: string }
      - { name: status, type: string }
      - { name: amount, type: float }
```

## Schema declaration

The `schema:` field is **required** on every source node. Clinker does not infer types from data -- you must declare each column's name and CXL type explicitly. This schema drives compile-time type checking across the entire pipeline.

Each entry is a `{ name, type }` pair:

```yaml
schema:
  - { name: employee_id, type: string }
  - { name: salary, type: int }
  - { name: hired_at, type: date_time }
  - { name: is_active, type: bool }
  - { name: notes, type: nullable(string) }
```

### Available types

| Type | Description |
|------|-------------|
| `string` | UTF-8 text |
| `int` | 64-bit signed integer |
| `float` | 64-bit IEEE 754 floating point |
| `bool` | Boolean (`true` / `false`) |
| `date` | Calendar date |
| `date_time` | Date with time component |
| `array` | Ordered sequence of values |
| `numeric` | Union of `int` and `float` -- resolved during type unification |
| `any` | Unknown type -- field used in type-agnostic contexts |
| `nullable(T)` | Nullable wrapper around any inner type (e.g. `nullable(int)`) |

## Format types

The `type:` field inside `config:` selects the file format. Supported values: `csv`, `json`, `xml`, `fixed_width`.

### CSV

```yaml
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: "./data/orders.csv"
    schema:
      - { name: order_id, type: int }
      - { name: customer_id, type: int }
      - { name: amount, type: float }
      - { name: order_date, type: date }
    options:
      delimiter: ","         # Default: ","
      quote_char: "\""       # Default: "\""
      has_header: true        # Default: true
      encoding: "utf-8"      # Default: "utf-8"
```

All CSV options are optional. With no `options:` block, Clinker uses standard RFC 4180 defaults.

### JSON

```yaml
- type: source
  name: events
  config:
    name: events
    type: json
    path: "./data/events.json"
    schema:
      - { name: event_id, type: string }
      - { name: timestamp, type: date_time }
      - { name: payload, type: string }
    options:
      format: ndjson          # array | ndjson | object (auto-detect if omitted)
      record_path: "$.data"   # JSONPath to records array
```

- `array` -- the file is a single JSON array of objects.
- `ndjson` -- one JSON object per line (newline-delimited JSON).
- `object` -- single top-level object; use `record_path` to locate the records array within it.

If `format` is omitted, Clinker auto-detects based on file content.

### XML

```yaml
- type: source
  name: catalog
  config:
    name: catalog
    type: xml
    path: "./data/catalog.xml"
    schema:
      - { name: product_id, type: int }
      - { name: name, type: string }
      - { name: price, type: float }
    options:
      record_path: "//product"          # XPath to record elements
      attribute_prefix: "@"             # Prefix for XML attribute fields
      namespace_handling: strip         # strip | qualify
```

- `strip` (default) -- removes namespace prefixes from element and attribute names.
- `qualify` -- preserves namespace-qualified names.

### Fixed-width

```yaml
- type: source
  name: legacy_data
  config:
    name: legacy_data
    type: fixed_width
    path: "./data/mainframe.dat"
    schema:
      - { name: account_id, type: string }
      - { name: balance, type: float }
      - { name: status_code, type: string }
    options:
      line_separator: crlf    # Line ending style
```

Fixed-width sources require a separate format schema (`.schema.yaml` file) that defines field positions, widths, and padding. The `schema:` on the source body declares CXL types for compile-time checking; the format schema defines the physical layout.

## Sort order

If your source data is pre-sorted, declare the sort order so the optimizer can use streaming aggregation instead of hash aggregation:

```yaml
- type: source
  name: sorted_transactions
  config:
    name: sorted_transactions
    type: csv
    path: "./data/transactions_sorted.csv"
    schema:
      - { name: account_id, type: string }
      - { name: txn_date, type: date }
      - { name: amount, type: float }
    sort_order:
      - { field: "account_id", order: asc }
      - { field: "txn_date", order: asc }
```

Sort order declarations are trusted -- Clinker does not verify that the data is actually sorted. If the data violates the declared order, downstream streaming aggregation may produce incorrect results.

The shorthand form is also accepted -- a bare string defaults to ascending:

```yaml
    sort_order:
      - "account_id"
      - { field: "txn_date", order: desc }
```

## Array paths

For nested data (JSON/XML sources with embedded arrays), `array_paths` controls how nested arrays are handled:

```yaml
- type: source
  name: invoices
  config:
    name: invoices
    type: json
    path: "./data/invoices.json"
    schema:
      - { name: invoice_id, type: int }
      - { name: customer, type: string }
      - { name: line_item, type: string }
      - { name: line_amount, type: float }
    array_paths:
      - path: "$.line_items"
        mode: explode         # One output record per array element
      - path: "$.tags"
        mode: join            # Concatenate array elements into a string
        separator: ","
```

- `explode` (default) -- produces one output record per array element, with parent fields repeated.
- `join` -- concatenates array elements into a single string using the specified `separator`.
