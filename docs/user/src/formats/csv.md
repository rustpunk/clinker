# CSV Format

CSV is the default file format. The reader decodes each CSV row (including
quoted multiline cells) into a record whose fields are matched
positionally (or by header name) against the source's declared
`schema:`; the writer reverses the process. CSV pairs with the `file`
transport — see [Source Nodes](../nodes/source.md) for the transport /
format split and the schema rules every source shares.

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
      delimiter: ","         # default ","
      quote_char: "\""       # default "\""
      has_header: true        # default true
      encoding: "utf-8"      # default "utf-8"
```

## Options

All CSV options are optional. With no `options:` block, Clinker uses
standard [RFC 4180](https://www.rfc-editor.org/rfc/rfc4180) defaults.

| Option | Default | Description |
|--------|---------|-------------|
| `delimiter` | `,` | Field separator, **exactly one ASCII byte**. Set to `\t` for TSV, `;` for semicolon-delimited exports. |
| `quote_char` | `"` | Quote character that escapes delimiters and newlines inside a field, **exactly one ASCII byte**. |
| `has_header` | `true` | When `true`, the first line names the columns and is consumed, not emitted. When `false`, fields bind to `schema:` positionally. |
| `encoding` | `utf-8` | Character set each field — including the header row — is decoded through. Supported values are `utf-8` (the default) and `iso-8859-1` (aliases `latin-1`, `latin1`). See [Encoding](#encoding). |

`delimiter` and `quote_char` are each a single byte on the wire, so each must
be **exactly one ASCII character**. An empty, multi-character, or non-ASCII
value (for example `"||"` or `"→"`) is rejected at plan validation — it is
never silently truncated to its first byte.

## Encoding

The reader decodes every field through the source's declared `encoding`:

- **`utf-8`** (the default) is strict — a byte sequence that is not valid
  UTF-8 fails the run loudly rather than substituting replacement
  characters, so a mis-declared encoding is caught instead of silently
  corrupting data.
- **`iso-8859-1`** (Latin-1; also spelled `latin-1` or `latin1`) maps each
  byte `0xNN` to codepoint `U+00NN`, so high bytes such as `0xE9` (`é`)
  from legacy exports decode correctly.

The same `options.encoding` setting is available on a CSV Sink. It applies to
headers, body fields, joined values, embedded JSON cells and reconstructed
envelope rows. UTF-8 is the default on both sides. Names are case-insensitive;
hyphens, underscores and spaces are ignored. `UTF8`, `ISO8859_1`, `latin1`,
`Latin-1` and `l1` resolve to the corresponding canonical spelling.

Latin-1 is **true ISO-8859-1**: bytes `0x80..0x9F` are the corresponding control
codepoints, not punctuation from another code page. Characters above `U+00FF`
(for example `€`) cannot be written in Latin-1 and cause an error; there is no
replacement character or fallback. UTF-8 input strips its leading UTF-8 BOM;
Latin-1 preserves an initial `EF BB BF` sequence as the three ordinary characters
`ï»¿`. CSV grammar is parsed from bytes before either header or body text is
decoded.

An **unsupported** encoding is rejected during configuration admission with a
precise error naming the value and a supported correction. Charset is part of
the semantic plan identity; changing input or output charset changes that
identity. Aliases and an omitted or explicit UTF-8 default resolve identically.
Malformed UTF-8 in a header or body cell is an input-data error
(`source.data.invalid`), including when the header is read to discover columns.

The closed encoding policy across formats is:

| Format | Encoding policy |
|--------|-----------------|
| CSV, X12 | Authored `options.encoding`: UTF-8 or true ISO-8859-1. |
| JSON, XML, fixed-width, SWIFT MT | UTF-8 only; no authored encoding override. |
| EDIFACT | In-band repertoire: UNOA/UNOB ASCII, UNOC ISO-8859-1, UNOY UTF-8; no authored override. |
| HL7 | In-band repertoire: blank/`ASCII` means ASCII, `UNICODE UTF-8` means UTF-8; no authored override. |

Single-schema and multi-record CSV sources support the same two encodings,
including textual column headers, discriminator fields, body cells and declared
envelope sections. Each file applies its own leading-BOM rule. A UTF-8 BOM is
recognized only at the start of that file; the same bytes inside a field are data.

## Header handling

With `has_header: true`, the header row's names bind input columns to the
`schema:` entries — column order in the file may differ from the schema.
With `has_header: false`, binding is strictly positional, so the schema
order **must** match the file's column order.

Input columns the schema does not name are governed by the source's
[`on_unmapped`](../formats/auto-widen.md) policy, the same as every other
format.

## Multi-value cells (`split_values`)

A CSV cell holds one string, but that string may pack several values behind a
delimiter (`1,a;b;c`). Declare the column `multiple: true` and add a
`split_values` entry naming the field and its delimiter, and the reader parses
the cell into an array:

```yaml
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: ./orders.csv
    split_values:
      - { field: tags, delimiter: ";" }
    schema:
      - { name: order_id, type: string }
      - { name: tags, type: string, multiple: true }
```

`tags` reads as `["a", "b", "c"]`. An empty cell is an empty array; a cell with
no delimiter is a one-element array; each element is coerced to the column's
declared `type:`. A quoted cell is unquoted first, so a delimiter inside the
quotes is not a boundary. A `multiple: true` column with no covering
`split_values` entry is rejected at compile
([E361](https://github.com/rustpunk/clinker/blob/main/docs/explain/E361.md)).
Multi-record CSV sources reject `split_values` and `multiple: true` columns
with [E358](https://github.com/rustpunk/clinker/blob/main/docs/explain/E358.md)
and E361; these input options require a single-schema source.
See [`split_values`](../nodes/source.md#several-values-in-one-cell-split_values)
in the Source reference for the full grammar.

## Writing CSV

On output, the writer emits one row per record with cells in the
**output schema's column order** — the same order as the header row —
regardless of how an upstream node ordered the record's fields. An
output-schema column the record does not carry emits an empty cell,
the same as an explicit null; with `include_unmapped: false` a record
field the output schema does not name is not written. See
[Sink Nodes](../nodes/sink.md) for header control, field mapping,
and null handling.

```yaml
- type: sink
  name: export
  input: orders
  config:
    name: export
    type: csv
    path: ./out/orders.csv
    options:
      encoding: iso-8859-1
```

Each output operation is prepared completely before any of its bytes reach the
destination. The first body row and its automatic header are one operation. An
unrepresentable cell therefore writes neither a partial row nor a stray header,
and leaves previously accepted bytes and format state intact. Explicit document
start and end operations are prepared separately. An I/O failure during delivery
can leave a destination prefix and prevents further writes; successful preparation
alone is not a delivered row. File publication is a separate contract described
in [Storage & Spill Location](../ops/storage.md#output-publication-and-retained-attempts).

CSV quoting needs each complete cell. Its encoding workspace is admitted against
the run's resource budget before allocation and released after that cell; there
is no authored field-size or record-size ceiling. A cell that cannot fit fails as
a resource error rather than being truncated or dropped. Joined or embedded JSON
cells account for the rendered text and encoded bytes while both are live.
The complete operation also needs storage for its prepared bytes: it stays in
memory unless an explicit spill location is available. Spill does not eliminate
the minimum memory needed for a cell, policy, or schema mapping. See
[Memory Tuning](../ops/memory.md#what-the-budget-measures) for the accounting
boundary and [output preparation](../ops/storage.md#output-preparation) for
storage and cleanup behavior.

Split output captures only the header actually delivered by a successful
operation. With `repeat_header: true`, later files replay those same names;
with `repeat_header: false`, only the first file emits the automatic header.
`include_header: false` suppresses that header throughout. Reconstructed
envelope rows do not become an automatic column header.

An empty stream emits no automatic header. A single empty or null cell is written
as `""` followed by a newline; adjacent empty cells are separated by the delimiter.
Reading an ordinary empty cell yields an empty string, so CSV does not distinguish
an empty string from a null unless the pipeline applies its own schema policy.

### Writing multi-value cells (`join_values`)

A `multiple:` field is joined into one delimited cell on write — the write-side
inverse of [`split_values`](#multi-value-cells-split_values). The default needs
no configuration: values join with `;`, and a value that itself contains the
delimiter is a hard error rather than a cell that would split back wrongly.
The planner carries the exact output-facing `multiple: true` column set through
mapping and exclusion into the writer. An array reaching any other CSV column
is rejected as a routing/type-contract error rather than joined implicitly.

```yaml
- type: sink
  name: report
  input: orders
  config:
    name: report
    type: csv
    path: ./out/report.csv
    join_values:
      - tags                              # delimiter ";", on_conflict: error
      - { field: notes, delimiter: "|", on_conflict: escape, escape: "\\" }
```

A field with no `join_values` entry still joins, with the defaults. An entry
overrides, per field:

- **`delimiter`** — the separator written between values (default `;`).
- **`on_conflict`** — what to do when a value contains the delimiter:
  - `error` (default) — reject the record with the field and element position,
    preserving its original value for the DLQ, rather than emit a cell that splits
    back wrongly. This is what makes a defaulted delimiter safe.
    Under `error_handling.strategy: continue`, the offending record goes to the
    [dead-letter queue](../pipelines/error-handling.md) (category
    `multi_value_join_collision`) and the run continues; under `fail_fast` it
    aborts.
  - `escape` — prefix each delimiter (and each escape character) inside a value
    with `escape` (default `\`), so a matching `split_values` `escape:` recovers
    the original. Lossless. `delimiter` and `escape` must each be a single
    character.
  - `encode_json` — encode the whole field as an embedded JSON array, recovered
    by a matching `split_values` `json: true`. Preserves every value's text
    exactly, including ones carrying the delimiter, quotes, or newlines — nothing
    is lost or mis-split. (A decimal/date/datetime element serializes as its JSON
    string form and reads back as a string, re-typed by the column's declared
    `type:`, the same round trip every CSV cell takes.)

An empty field emits an empty cell — and, under the delimited policies (`error`,
`escape`), a single empty-string value `[""]` emits an empty cell too, which
reads back as zero values: the delimited encoding cannot tell an empty field from
one empty value. Use `encode_json` when that distinction matters. A single
non-empty value emits that value with no delimiter. The joined cell is quoted by
the normal CSV rules when it contains the field delimiter, a quote, or a newline.
Declaring `join_values` on a non-CSV output is rejected at compile
([E362](https://github.com/rustpunk/clinker/blob/main/docs/explain/E362.md)).

**Round trip.** `on_conflict: escape` and `encode_json` are recovered exactly by
a matching source `split_values` entry:

```yaml
# write side
join_values:
  - { field: tags, on_conflict: escape, escape: "\\" }
# read side (a later pipeline)
split_values:
  - { field: tags, escape: "\\" }
```

### Header widening under auto-widen

When [`auto_widen`](auto-widen.md) is in effect and the Sink leaves
`include_unmapped` at its default of `true`, different records can carry
different carried-along columns. The header must still be shared by every
row, so Clinker widens it to the **union of every record's columns** in
first-seen order: a column that first appears on a later record still gets
its own header slot, and the earlier rows write an empty cell for it. This
pre-scan runs on the buffered output path, where the record batch is
materialized.

An output that streams under a bounded-memory budget cannot pre-scan the
whole batch: a CSV output fused directly after a `Merge`/`Transform`, a
single-branch `Route`, a streaming-strategy `Aggregate`, or the probe side of
a hash-build-probe `Combine`, or one reconstructing an envelope (which
suppresses the shared header and streams a headerless body), commits its
columns to the first record. A later record
carrying a column that first record lacked then fails the run with a
`SchemaDrift` error naming the column, rather than silently dropping it.
Declare the column in the source or output `schema:` so every record carries
it, or route to a self-describing format (JSON / NDJSON / XML). A
`reconstruct_envelope` CSV output therefore requires a stable body shape —
every record must carry the same columns.

## Multi-record files (header / trailer / body)

Some CSV exports interleave **multiple record types** in one file — a
header row, many body rows, and a trailer row — each distinguished by a
discriminator column. Declare these with a **map-form `schema:`** carrying a
`discriminator:` and a `records:` list, instead of the single column-list
`schema:`. Each record type names its `tag` (the discriminator value that
identifies it) and its own `columns:`; the discriminator field must sit at the
same column in every type (usually the first). The reader derives the runtime
superset schema (a lead `record_type` column plus the union of every record
type's columns) automatically.

```yaml
- type: source
  name: payments
  config:
    name: payments
    type: csv
    path: "./data/payments.csv"
    schema:                                     # one multi-record schema (map form)
      discriminator: { field: rec_type }        # the physical column carrying the type tag
      records:
        - { id: header,  tag: H, columns: [ { name: rec_type, type: string }, { name: batch_id, type: string } ] }
        - { id: detail,  tag: D, columns: [ { name: rec_type, type: string }, { name: id, type: int }, { name: amount, type: int } ] }
        - { id: trailer, tag: T, columns: [ { name: rec_type, type: string }, { name: count, type: int } ] }
      structure:
        - { record: trailer, count: count }     # validate T's count against the body count
    envelope:
      sections:
        head:
          extract: { record_type: H }          # the H record type surfaces as $doc.head.*
          fields:
            batch_id: string
```

The reader emits **one record per CSV row**, including quoted multiline cells,
on a single superset schema
whose lead `record_type` column carries the matched type's `id`. A
downstream [Route](../nodes/route.md) discriminates on that column.
Rows of different record types may carry different column counts (ragged
rows) — the reader validates the column count per record type, not
file-wide. A textual column-header row is skipped when `has_header` is
`true` (the default), so a leading `record_type,name,amount` line is not
mistaken for a record of an unknown type. Each declared field honors its
own `type` / `trim` / `pad`, the same as a single-record CSV field.

- **Header rows** declared as an `envelope:` section via the
  `record_type` extract surface as `$doc.<section>.*` and are excluded
  from the body stream (see
  [Envelopes & Document Context](../pipelines/envelope-and-doc-context.md)).
- **Trailer rows** named by a `structure:` constraint are validated as
  they stream — the declared `count` field is checked against the actual
  body-record count at document close — and excluded from the body
  stream. A declared trailer that never appears is an incomplete-document
  error; a body row after the trailer is rejected as content past the
  document close.
- **Blank lines** (empty or whitespace-only, common after concatenation)
  are skipped rather than parsed.
- An **unknown discriminator value** (a tag no `records:` entry declares)
  is a structural-integrity failure, classified separately from a trailer
  count mismatch. It [aborts the run](https://github.com/rustpunk/clinker/blob/main/docs/explain/E345.md)
  under `fail_fast`; under `continue` with the default record granularity it
  dead-letters only that physical row and continues with the next row; under
  `dlq_granularity: document` it condemns the whole file. A record-grained DLQ
  row carries a JSON array of the decoded CSV cells in `_cxl_dlq_source_record`,
  preserving empty cells without guessing which declared layout the unknown
  tag meant.
