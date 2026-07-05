# Fixed-Width Format

Fixed-width files carry no delimiters — each field occupies a fixed column
range on every line, the layout common to mainframe extracts and legacy
COBOL exports. Because the byte layout is not self-describing, each column in
a fixed-width source's `schema:` carries its **byte layout** (`start` +
`width`) alongside its CXL `type` — one unified declaration drives both the
physical slice and compile-time type checking. See
[Source Nodes](../nodes/source.md) for the shared transport rules.

```yaml
- type: source
  name: legacy_data
  config:
    name: legacy_data
    type: fixed_width
    path: "./data/mainframe.dat"
    schema:
      - { name: account_id,  type: string, start: 0,  width: 12 }
      - { name: balance,     type: float,  start: 12, width: 10 }
      - { name: status_code, type: string, start: 22, width: 2 }
    options:
      line_separator: crlf    # line-ending style
```

## The column layout

Each column pins itself to a byte range with `start` (a 0-based offset) and
`width` (a byte count); `end` (exclusive) may be given instead of `width`.
Optional per-column formatting keys — `justify`, `pad`, `trim`, `truncation`
— control padding and trimming on read and write. Because the same column
declaration carries both the byte range and the CXL type, the physical layout
and the types can never drift apart. A layout shared across pipelines can live
in an external `.schema.yaml` file referenced by `schema: layout.schema.yaml`.

## Writing fixed-width output

A fixed-width **output** node declares the same column layout in its
`schema:`. The writer places every field at its declared byte range —
`start` plus `width` (or `end`), resolved exactly as the reader slices —
regardless of the order the columns are declared in, so a file written
with a schema reads back under that same schema. Byte ranges the layout
leaves undeclared (a gap between fields) are filled with spaces. A column
that omits `start` continues at the previous column's end, so a
width-only schema lays its fields out sequentially. Two columns whose
byte ranges overlap have no consistent layout; the writer rejects such a
schema when the output opens, naming both columns and their ranges.

Widths are **byte counts**, matching how the reader slices. When a value
is longer than its field, truncation cuts at a UTF-8 character boundary at
or below the width, so a multi-byte character is never split: the emitted
cell is always valid UTF-8 of exactly `width` bytes (it may hold fewer
*characters* than the width when a trailing multi-byte character does not
fit, with the freed bytes pad-filled). Because padding fills exact byte
counts, `pad` must be a single-byte (ASCII) character; a multi-byte `pad`
is rejected when the output opens. Under `truncation: error` an over-long
value is still a hard error before any slicing.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `line_separator` | platform | Line-ending style (`lf` / `crlf`) used to split the file into records. |

Under `lf` or `crlf`, the reader buffers each physical line only up to the
declared record width plus a line-terminator allowance. A physical line wider
than the declared width — trailing filler beyond the last declared field, or a
schema that maps only a prefix of a wider fixed-length record — reads its
declared-width portion; the remaining bytes are discarded up to the next line
terminator and the reader continues with the following record. Because the
buffered portion is capped, a malformed file (a corrupt or missing newline)
cannot grow a single record until end of input: memory stays bounded regardless
of how long the physical line runs. A final line with no trailing newline reads
normally as long as its declared fields fit within the width.

## Schema drift

Fixed-width is **inert** with respect to
[auto-widen](../formats/auto-widen.md): because every byte is accounted
for by the format schema, there are no "unmapped" trailing columns to
absorb. The `on_unmapped` policy has no effect on a fixed-width source.

## Multi-record files (header / trailer / body)

Mainframe and banking extracts often interleave **multiple record types**
in one file — a header line, many body lines, and a trailer line — each
identified by a discriminator at a fixed byte position (commonly the
first character). Declare these with a **map-form `schema:`** carrying a
`discriminator:` byte range and a `records:` list, instead of a flat column
list. Each record type names its `tag` (the discriminator value) and its own
byte-positioned `columns:`; the reader synthesizes the lead `record_type`
column automatically.

```yaml
- type: source
  name: payments
  config:
    name: payments
    type: fixed_width
    path: "./data/payments.dat"
    schema:                                    # one multi-record schema (map form)
      discriminator: { start: 0, width: 1 }    # the type tag occupies byte 0
      records:
        - { id: header,  tag: H, columns: [ { name: batch_id, type: string, start: 1, width: 9 } ] }
        - { id: detail,  tag: D, columns: [ { name: id, type: int, start: 1, width: 5 }, { name: amount, type: int, start: 6, width: 4 } ] }
        - { id: trailer, tag: T, columns: [ { name: count, type: int, start: 1, width: 5 } ] }
      structure:
        - { record: trailer, count: count }     # validate T's count against the body count
    envelope:
      sections:
        head:
          extract: { record_type: H }          # the H line surfaces as $doc.head.*
          fields:
            batch_id: string
```

The reader streams **one record per line** on a single superset schema
whose lead `record_type` column carries the matched type's `id`. A
downstream [Route](../nodes/route.md) discriminates on that column; the
file is never buffered.

- **Header lines** declared as an `envelope:` section via the
  `record_type` extract surface as `$doc.<section>.*` and are excluded
  from the body stream (see
  [Envelopes & Document Context](../pipelines/envelope-and-doc-context.md)).
- **Trailer lines** named by a `structure:` constraint are validated as
  they stream — the declared `count` field is checked against the actual
  body-record count at document close — and excluded from the body
  stream. A declared trailer that never appears is an incomplete-document
  error; a body line after the trailer is rejected as content past the
  document close.
- **Blank lines** (empty or whitespace-only, common after concatenation)
  are skipped rather than rejected; a line whose declared field range is
  cut off mid-value is a truncation error, not a silently-partial read.
  Field parsing — type coercion, padding strip, justification — is shared
  with the single-record fixed-width reader, so a declared `type` parses
  identically on both paths.
- An **unknown discriminator value** (a tag no `records:` entry declares)
  is a structural-integrity failure, classified separately from a trailer
  count mismatch: it [aborts the run](../../explain/E345.md) by default,
  or under `dlq_granularity: document` condemns the whole file to the
  dead-letter sink and the run continues.
