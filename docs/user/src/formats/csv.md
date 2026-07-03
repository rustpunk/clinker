# CSV Format

CSV is the default file format and the most common Clinker input. The
reader decodes each line into a record whose fields are matched
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
| `delimiter` | `,` | Field separator. Set to `\t` for TSV, `;` for semicolon-delimited exports. |
| `quote_char` | `"` | Quote character that escapes delimiters and newlines inside a field. |
| `has_header` | `true` | When `true`, the first line names the columns and is consumed, not emitted. When `false`, fields bind to `schema:` positionally. |
| `encoding` | `utf-8` | Character set each field — including the header row — is decoded through. Supported values are `utf-8` (the default) and `iso-8859-1` (aliases `latin-1`, `latin1`). See [Encoding](#encoding). |

## Encoding

The reader decodes every field through the source's declared `encoding`:

- **`utf-8`** (the default) is strict — a byte sequence that is not valid
  UTF-8 fails the run loudly rather than substituting replacement
  characters, so a mis-declared encoding is caught instead of silently
  corrupting data.
- **`iso-8859-1`** (Latin-1; also spelled `latin-1` or `latin1`) maps each
  byte `0xNN` to codepoint `U+00NN`, so high bytes such as `0xE9` (`é`)
  from legacy exports decode correctly.

An **unsupported** encoding is rejected at startup with a precise error
naming the value and the supported set — it is never silently ignored.

> Multi-record CSV sources (those whose `schema:` is a map with a
> `records:` list, described below) are decoded as UTF-8 only. Declaring a
> non-UTF-8 `encoding` on such a source is rejected at startup; split the
> file into a single-schema CSV source if it needs a non-UTF-8 charset.

## Header handling

With `has_header: true`, the header row's names bind input columns to the
`schema:` entries — column order in the file may differ from the schema.
With `has_header: false`, binding is strictly positional, so the schema
order **must** match the file's column order.

Input columns the schema does not name are governed by the source's
[`on_unmapped`](../formats/auto-widen.md) policy, the same as every other
format.

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

The reader streams **one record per line** on a single superset schema
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
  is a structural-integrity failure: it [aborts the run](../../explain/E345.md)
  by default, or under `dlq_granularity: document` condemns the whole
  file to the dead-letter sink and the run continues.
