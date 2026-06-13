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
product. The [`array_paths`](../nodes/source.md#array-paths) field on the
source controls whether each nested array **explodes** into one record per
element or **joins** into a delimited string. That field is shared with
the XML reader and documented on the Source Nodes page.
