# Fixed-Width Format

Fixed-width files carry no delimiters — each field occupies a fixed column
range on every line, the layout common to mainframe extracts and legacy
COBOL exports. Because the byte layout is not self-describing, a
fixed-width source needs **two** schemas: the `schema:` on the source body
declares CXL types for compile-time checking, and a separate format schema
(`.schema.yaml`) defines the physical layout — each field's start, width,
and padding. See [Source Nodes](../nodes/source.md) for the shared
transport rules.

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
      line_separator: crlf    # line-ending style
```

## The format schema

The `.schema.yaml` file is the physical layout: it pins each field to a
column range and declares its padding so the reader can slice every line
identically. The source-body `schema:` and the format schema must name the
same fields — the former gives them CXL types, the latter gives them
positions.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `line_separator` | platform | Line-ending style (`lf` / `crlf`) used to split the file into records. |

## Schema drift

Fixed-width is **inert** with respect to
[auto-widen](../formats/auto-widen.md): because every byte is accounted
for by the format schema, there are no "unmapped" trailing columns to
absorb. The `on_unmapped` policy has no effect on a fixed-width source.

## Multi-record files (header / trailer / body)

Mainframe and banking extracts often interleave **multiple record types**
in one file — a header line, many body lines, and a trailer line — each
identified by a discriminator at a fixed byte position (commonly the
first character). Declare these under `format_schema:` with a
`discriminator:` byte range and a `records:` list instead of a flat
`fields:` layout. Each record type names its `tag` (the discriminator
value) and its own byte-positioned `fields:`.

```yaml
- type: source
  name: payments
  config:
    name: payments
    type: fixed_width
    path: "./data/payments.dat"
    schema:                                   # superset CXL types: record_type + every field
      - { name: record_type, type: string }
      - { name: batch_id, type: string }
      - { name: id, type: int }
      - { name: amount, type: int }
      - { name: count, type: int }
    format_schema:
      discriminator: { start: 0, width: 1 }   # the type tag occupies byte 0
      records:
        - { id: header,  tag: H, fields: [ { name: batch_id, type: string,  start: 1, width: 9 } ] }
        - { id: detail,  tag: D, fields: [ { name: id, type: integer, start: 1, width: 5 }, { name: amount, type: integer, start: 6, width: 4 } ] }
        - { id: trailer, tag: T, fields: [ { name: count, type: integer, start: 1, width: 5 } ] }
      structure:
        - { record: trailer, count: count }    # validate T's count against the body count
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
  stream.
- An **unknown discriminator value** (a tag no `records:` entry declares)
  fails the run with [E345](../../explain/E345.md).
