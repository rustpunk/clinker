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

A record whose declared field holds an empty array, is explicitly `null`, or
carries no such field at all, is preserved by default — `keep_empty` defaults to
`true`, and setting it to `false` drops such a record. An explicit null is how
many producers write "no value", so it counts as no occurrence rather than one;
for the same reason a `multiple: true` column holding an explicit null stays
null rather than becoming `[null]`.

A field that IS present but holds a single object or scalar rather than an array
is one occurrence, projected exactly as a one-element array would be. Producers
routinely unwrap a lone element, so a feed where some documents carry
`"line_items": [{…}, {…}]` and others carry `"line_items": {…}` fans both out
the same way and every output record ends up with the same columns. The XML
reader, where a document cannot express the difference at all, already behaved
this way.

Two declared fan-out fields apply in declaration order and multiply. A nested
pair (`orders` then `orders.items`) produces the two-level expansion when the
outer entry declares `mode: split`:

```yaml
    split_to_rows:
      - { field: orders, mode: split }
      - { field: orders.items, mode: split }
```

Under `mode: extract` the outer entry lifts the occurrence's keys to the top
level, which removes the `orders.items` path the inner entry addresses — so that
pairing is rejected at compile (`E358`) rather than silently fanning out only
one level. A duplicated field is rejected too.

## Flattened-name collisions

The reader dissolves nested objects into dotted keys, so `{"a": {"b": 1}}`
becomes the field `a.b`. When two distinct keys flatten to the **same** name —
for example a nested `{"a": {"b": 1}}` alongside a literal `{"a.b": 2}` in the
same record — only one value could survive, and keeping one while dropping the
other is silent data loss. The reader refuses the record instead, naming the
colliding field. This mirrors the XML reader's treatment of a repeated element:
both formats now fail loud on an undeclared collision rather than one keeping the
first value and the other the last. If the collision is intentional (both values
belong together), declare the column `multiple: true` to collect them into an
array in document order; otherwise rename one of the source keys so they no
longer collide. As with XML, detection is per document at read time, so the run
aborts under `fail_fast` and dead-letters the document under `continue` /
`best_effort` with `dlq_granularity: document`.

Detection covers two **distinct** source keys that flatten to the same dotted
name — the nested `{"a": {"b": 1}}` plus literal `{"a.b": 2}` case above. It does
**not** cover a key that is *literally duplicated* within one JSON object
(`{"tags": "x", "tags": "y"}`): the JSON parser collapses such duplicates
last-wins (keeping `"y"`) before the record reaches collision detection, so that
repeat is silently dropped rather than reported. A collision *inside* an array
element that a `split_to_rows: extract` fan-out lifts to the top level (one
element key clashing with a parent field or with another element key) is
likewise not yet detected and still resolves last-wins — tracked by
[issue 920](https://github.com/rustpunk/clinker/issues/920).

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
