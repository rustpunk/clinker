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

### `long_unique` — storage hint for high-cardinality text

A string column may carry an optional `long_unique: true` flag. It is an
**advisory, opt-in hint**, not a type change: it tells Clinker the column's
values are *long and effectively unique* — never repeated across records — so
the run uses less memory for that column. Typical candidates are UUIDs
rendered as text, street addresses, and free-text comment or note fields.

```yaml
schema:
  - { name: ticket_id,  type: string, long_unique: true }   # 36-char UUID
  - { name: notes,      type: string, long_unique: true }   # free text
  - { name: department, type: string }                      # low-cardinality, default
```

The flag lowers memory use only. A value's content and its
comparison, grouping, join, sort, and output behavior are all unchanged — a
`long_unique` value behaves identically to the same text in any other column.
Omitting the flag (the common case) leaves the default behavior untouched. Set
it only when you know a column is genuinely high-cardinality free text; on a
column whose values repeat, leave it off.

## Transport vs format

A source declaration has two independent layers:

- **Transport** (`transport:`) selects *where* the records come from. The only transport today is `file` — read bytes from the filesystem, resolved through one of the file matchers (`path` / `glob` / `regex` / `paths`). `transport:` is optional and defaults to `file`, so a source that omits it reads from disk exactly as before.
- **Format** (`type:`) selects *how* the bytes decode into records: `csv`, `json`, `xml`, `fixed_width`, `edifact`, `x12`, `hl7`, `swift`.

```yaml
- type: source
  name: orders
  config:
    name: orders
    transport: file        # optional; this is the default
    type: csv              # the on-disk format
    path: "./data/orders.csv"
    schema:
      - { name: order_id, type: int }
```

A `file` transport requires **exactly one** file matcher (`path`, `glob`, `regex`, or `paths`). Declaring none fails validation with `E211`; declaring more than one fails with `E210`. Both are reported at config-load time, before any file is opened.

## Format types

The `type:` field inside `config:` selects the on-disk format. Each format
has its own reference page covering its options and decoding model:

| `type:` | Format | Reference |
|---------|--------|-----------|
| `csv` | Delimited text (RFC 4180) | [CSV Format](../formats/csv.md) |
| `json` | Array / NDJSON / wrapper object | [JSON Format](../formats/json.md) |
| `xml` | XPath-selected record elements | [XML Format](../formats/xml.md) |
| `fixed_width` | Column-positioned legacy extracts | [Fixed-Width Format](../formats/fixed-width.md) |
| `edifact` | UN/EDIFACT interchanges | [EDIFACT Format](../formats/edifact.md) |
| `x12` | ANSI ASC X12 interchanges | [X12 Format](../formats/x12.md) |
| `hl7` | HL7 v2.x pipe-and-hat messages | [HL7 v2 Format](../formats/hl7.md) |
| `swift` | SWIFT MT (FIN) messages | [SWIFT MT Format](../formats/swift.md) |

The same `schema:` rules apply regardless of format: the reader maps each
decoded record onto the declared schema, and undeclared input fields fall
under the [`on_unmapped`](#on_unmapped--undeclared-input-fields) policy
below.

## `on_unmapped` — undeclared input fields

The per-source `on_unmapped` policy decides what to do with input fields the source's `schema:` block does not name. Three modes — `auto_widen` (default), `drop`, `reject`:

```yaml
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: "./data/orders.csv"
    on_unmapped:
      mode: auto_widen     # default; other values: drop, reject
    schema:
      - { name: order_id, type: string }
      - { name: amount, type: numeric }
```

See [Auto-Widen & Schema Drift](../formats/auto-widen.md) for the full
specification: how undeclared columns flow through each downstream node
type, the `include_unmapped` Output flag, **E315** merge-policy
mismatch, and fixed-width behavior.

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

## Watermarks

An event-time watermark declares which column on the source carries
each record's *event time* — the wall-clock instant the event
happened, distinct from when Clinker read the row. When set, Clinker
takes the column on every record, subtracts the source's `delay`, and
uses the result to track event-time progress so downstream time
windows know when to close. The delay-corrected value is also stamped
on every record as
[`$source.event_time`](../cxl/system-variables.md), the column a
downstream [time-windowed aggregate](aggregate.md#time-windowed-aggregates)
uses to assign records to windows.

```yaml
- type: source
  name: clicks
  config:
    name: clicks
    type: csv
    path: "./data/clicks.csv"
    options:
      has_header: true
    watermark:
      column: event_ts       # must be date_time or date
      delay: 5s              # bounded out-of-order tolerance
      idle_timeout: 30s      # flip partitions to idle if quiet
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
      - { name: amount, type: int }
```

Fields:

- `column` (required) — the schema column whose value is each
  record's event time. The column's declared type must be `date_time`
  or `date`. A `column:` that names a field absent from `schema:`
  raises [**E154**](../ops/exit-codes.md#plan-time-diagnostic-codes);
  a `column:` whose declared type is neither raises **E155**.

- `delay` (optional duration, default unset) — bounded out-of-order
  tolerance. Each record's event time is shifted earlier by `delay`
  before being folded into the watermark, so the source's effective
  watermark trails its observed max event time by this amount.
  Mirrors Flink's
  [`BoundedOutOfOrdernessWatermarks`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/built_in/#fixed-amount-of-lateness).
  Without `delay`, the watermark advances strictly to the observed
  max — a single late record routes to the DLQ.

- `idle_timeout` (optional duration, default unset) — if a source
  stays quiet longer than this, it stops holding back downstream
  window-close progress, so windows keep closing when one source
  pauses. Unset means the source never goes idle.

Durations use the suffixes `ms`, `s`, `m`, `h`, `d`. `ms` is
matched before the single-character `s`, so `500ms` reads as 500
milliseconds, not 500 seconds with a stray `m`.

A pipeline whose aggregate declares `time_window:` **must** have a
`watermark.column` on every upstream-reachable source. Without it,
event-time progress can never advance and the window can never close —
the planner rejects this with
[**E156**](../ops/exit-codes.md#plan-time-diagnostic-codes).

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
