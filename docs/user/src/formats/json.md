# JSON Format

The JSON reader turns a JSON document into a record stream. It handles
three physical shapes — a single array of objects, newline-delimited
objects (NDJSON), or a wrapper object that nests the records under a
path — and auto-detects the shape when you do not declare it. Each object
is matched against the source's declared `schema:`; see
[Source Nodes](../nodes/source.md) for the shared schema and transport
rules.

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
      record_path: "$.data"   # JSONPath to the records array (object format)
      max_index_bytes: 64MB   # cap on retained envelope sections (optional)
```

## Physical shapes

| `format` | Layout |
|----------|--------|
| `array` | The file is a single JSON array of objects. |
| `ndjson` | One JSON object per line (newline-delimited JSON). |
| `object` | A single top-level object; `record_path` locates the records array within it. |

If `format` is omitted, Clinker auto-detects the shape from the file
content. Declare it explicitly when the file is large enough that you want
to skip detection, or when an `object` wrapper needs a `record_path`.

## Nested arrays

JSON records frequently embed arrays — line items on an invoice, tags on a
product. Three source-level declarations decide what happens to them, all
documented on the
[Source Nodes](../nodes/source.md#multi-value-fields) page:

- `split_to_rows` fans the array out to one record per element. `mode: extract`
  (the default) hoists an object element's keys onto the output record;
  `mode: split` keeps the record shape, flattening the element back under the
  field name (`orders.id`). An array of scalars keeps the value under the
  field's own name under both modes.
- A schema column declared `multiple: true` keeps the array as an array, and
  normalizes a lone scalar into a one-element array so the column's shape never
  depends on what a particular document happened to carry.
- `split_values` parses a delimited string cell into several values.

A record whose declared field holds an empty array, or carries no such field at
all, is preserved by default — `keep_empty` defaults to `true`. A field holding
a non-array value has nothing to fan out and passes through untouched.

Two declared fan-out fields apply in declaration order and multiply; a
duplicated or nested pair is rejected at compile (`E358`).

## Bounding envelope retention: `max_index_bytes`

When a source declares an `envelope:` and a pipeline reads `$doc.*` paths
from it, the JSON reader runs a streaming pre-scan that walks the document
once and retains only the declared section subtrees — every other key,
including a multi-megabyte body array, is parsed-and-skipped without being
stored. The retained sections live in a bounded document index.

`max_index_bytes` caps that index. It is charged incrementally as each
section is parsed, so even a single oversized declared section aborts
mid-parse (naming the section and the cap) rather than risking an
out-of-memory failure. It accepts a decimal size string (`64MB`, `500KB`)
or a bare byte count; optional, defaulting to **64MB**. Only the declared
sections a program actually reads are retained, so envelope metadata sits
far below this ceiling in practice — the cap exists to convert an unbounded
mistake into a clear error. See
[Document Envelope Context](../pipelines/envelope-and-doc-context.md) for
the full model.

## Non-finite floats

JSON numbers cannot represent `NaN`, `+infinity`, or `-infinity`. Writing a
record (or an envelope section field) that holds a non-finite float to a
JSON output fails with a JSON error naming the value, rather than silently
substituting `null` — a substituted `null` would be indistinguishable from
a genuine source null on read-back. Filter such records or replace the
value in a transform before the JSON output.
