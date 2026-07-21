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
| `any` | Unknown type -- field used in type-agnostic contexts |
| `nullable(T)` | Nullable wrapper around any inner type (e.g. `nullable(int)`) |

A source column's declared type must be **concrete**: `numeric` — the
inference-only `int | float` union CXL resolves during type unification — is
**not** a valid source column type. Declaring one is rejected at compile with
[**E158**](../ops/exit-codes.md#plan-time-diagnostic-codes); declare `int` or
`float` explicitly.

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

### `source_name` — read a differently-named physical column

A column may carry an optional `source_name` naming the **physical** input
column it reads from, when that differs from the exposed `name`. The reader
matches input fields by physical name and re-labels the value under `name`, so
downstream CXL and the output see `name` carrying the physical column's data.

```yaml
schema:
  # read the physical `cust_id` column, expose it downstream as `customer_id`
  - { name: customer_id, type: string, source_name: cust_id }
```

Omitting `source_name` (the common case) reads the input field whose key equals
`name`, unchanged from before. A channel `schema` patch's `rename` op sets this
alias automatically (see [Channels](../pipelines/channels.md)).

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
      - { name: amount, type: float }
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

## Multi-value fields

A field that holds more than one value is declared on the **schema column**,
not on the pipeline:

```yaml
schema:
  - { name: order_id, type: string }
  - { name: tags, type: string, multiple: true }
```

`multiple: true` says the column holds zero or more values of its declared
type. Reading collects every occurrence of the field into one array — a single
occurrence is still an array, so downstream code never has to branch on how
many values happened to arrive. A field absent from a record has no column at
all and resolves to null, exactly as any other absent column does. CXL sees the
column as an `array`; the declared `type:` describes each element and drives
coercion. The declaration describes the shape of the
data, so it serves both directions: a writer that can encode repetition reads
the same declaration.

Both ends of the declaration are checked at compile, so a shape the formats
cannot carry fails before a run starts rather than mid-stream:

| Format | As a source | As an output |
|---|---|---|
| `json` | native — an array | native — an array |
| `xml` | native — repeated child elements | not yet ([issue 916](https://github.com/rustpunk/clinker/issues/916)) |
| `csv` | needs a `split_values` entry | not yet ([issue 917](https://github.com/rustpunk/clinker/issues/917)) |
| `fixed_width` | needs a `split_values` entry | not yet ([issue 918](https://github.com/rustpunk/clinker/issues/918)) |
| `edifact`, `x12`, `hl7`, `swift` | no — repetition is positional | no — repetition is positional |

A `multiple: true` column reaching an output that cannot encode it is `E359`; one
on a source that cannot produce it is `E361`. Run `clinker explain --code E361`
for the full remediation of either.

**`csv` and `fixed_width` opt in through `split_values`.** Neither wire format
repeats a field, but a cell's text may hold several values separated by a
delimiter, which is what a `split_values` entry declares. Until the entry is
there the column has nothing to fill it and `E361` says so; once it is, the
column is accepted. (Acting on the entry in those two readers is
[issue 917](https://github.com/rustpunk/clinker/issues/917) — the plan-time
contract lands first.)

**The segment formats are a permanent no**, not a pending one. Repetition there
is a positional coordinate rather than a list: a repeated composite is written
as two axes interleaved in one element (`11:B:1^12:B:2`), which a flat array
cannot represent without losing the component axis. The faithful shape is one
column per coordinate — for HL7, that is what `options.split_fields` produces,
with a writer that reassembles the wire field byte-for-byte.

### One record per value: `split_to_rows`

`split_to_rows` fans a record out to one record per occurrence of a repeated
field. Each entry is either a bare field name or a full mapping, and the two
forms mix freely in one list:

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
      - { name: line_no, type: int }
    split_to_rows:
      - tags                      # shorthand: field name, all defaults
      - field: line_items         # full form
        keep_empty: true
        mode: extract
        position_column: line_no
```

| Key | Default | Meaning |
|-----|---------|---------|
| `field` | — | The repeated field, as a flattened dotted name |
| `keep_empty` | `true` | Whether a record whose field is empty or absent survives |
| `mode` | `extract` | `extract` — the occurrence becomes the record; `split` — the record shape is kept |
| `position_column` | none | Column receiving each occurrence's 1-based position |

The field is named as it appears in the **input document**, not as the schema
exposes it: a column declared `source_name:` is addressed by that
`source_name`. The same rule applies to `split_values` below.

**`keep_empty` defaults to `true`.** A record whose field holds an empty array,
or carries no such field at all, is emitted with that field unset rather than
disappearing. Several widely used engines drop the record instead; a vanished
row is the costliest failure mode there is, so dropping is opt-in here.

**`mode: extract`** (the default) makes the occurrence the record: its own
fields are lifted out from under the field name and every field outside the
group is merged onto each output. `{"orders": [{"id": 1}]}` yields a top-level
`id`, and repeated `<Item><name>` children yield `name`. When lifting lands an
occurrence's field on a name an outside field already occupies, the occurrence
wins — it is the record, so its own value is not shadowed by the parent it was
merged with. A `position_column` wins over both: you named it, so a field of
the same name inside or outside the occurrence gives way to the index.

**`mode: split`** preserves the record shape: the occurrence's fields keep
their dotted path (`orders.id`, `Item.name`) and each output carries exactly
one occurrence.

Entries apply in declaration order, so two entries multiply. Declaring the same
field twice is rejected at compile (`E358`), as is fanning out a field the
schema also declares `multiple: true` — the attribute collects the occurrences
into one array, the fan-out spends them one per record, and a field cannot be
both. On an **XML** source, two entries may not name nested element groups
either (`Item` and `Item.part`): that reader assigns each element to one
occurrence group by document position, and a nested pair leaves the inner
group's membership ambiguous. A JSON source accepts the nested pair, applying
the entries in order to produce a two-level expansion.

### Several values in one cell: `split_values`

`split_values` parses a delimited cell into the several values a `multiple:`
column holds. It takes the same bare-name-or-mapping shorthand:

```yaml
    split_values:
      - tags                      # shorthand: default delimiter `;`
      - field: codes              # full form
        delimiter: "|"
    schema:
      - { name: tags,  type: string, multiple: true }
      - { name: codes, type: string, multiple: true }
```

The delimiter defaults to `;`. A `split_values` field the schema does not
declare `multiple: true` is rejected at compile (`E358`): splitting produces
several values, and only a multi-value column can hold them. So is an entry
naming a column's exposed name when that column reads a differently-named input
field — the split runs against the document's own field names, so name the
`source_name`.

### Migrating from `array_paths`

`array_paths:` was the earlier form of these declarations. It is no longer read,
and a source still carrying it is rejected at compile (`E360`) rather than
running with the fan-out silently dropped. An `explode` path becomes a
`split_to_rows:` entry (`mode: extract` reproduces the old projection), a
delimited cell becomes a `split_values:` entry, and a path kept as an array
becomes `multiple: true` on the schema column.

### Format notes

Both knobs are honored by the **JSON** and **XML** file readers, and
`split_to_rows` / `split_values` by a `rest` source decoding JSON. A
`multiple: true` column on any other input format is rejected at compile
(`E361`) rather than accepted and ignored; `split_to_rows` on one is still
accepted and inert, tracked at
[issue 912](https://github.com/rustpunk/clinker/issues/912), and a `rest` source
decoding XML at
[issue 911](https://github.com/rustpunk/clinker/issues/911).

**JSON** — the field names the key holding the array (`line_items`, or
`order.line_items` for an array nested under an object). A field present but
holding a single object or scalar rather than an array counts as one
occurrence, and is projected exactly as a one-element array would be — many
producers unwrap a lone element, and XML cannot express the difference at all,
so the two readers agree on this. Only a field that is absent, or holds an
empty array, has no occurrence and is governed by `keep_empty`.

**XML** — the field is the repeated child element's dotted path relative to the
record element (`Item`, or `Items.Item` when nested). Repetition and absence
are indistinguishable in XML, so a record with no occurrence of the element is
governed by `keep_empty` exactly as an empty array is. See
[XML Format](../formats/xml.md#repeated-elements) for the full rules.
