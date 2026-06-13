# XML Format

The XML reader selects record elements by XPath and maps each one onto the
source's declared `schema:`. Child elements bind to fields by name;
attributes bind under a configurable prefix. Namespaces are stripped by
default so schema field names stay clean. See
[Source Nodes](../nodes/source.md) for the shared schema and transport
rules.

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
      attribute_prefix: "@"             # prefix for XML attribute fields
      namespace_handling: strip         # strip | qualify
      max_index_bytes: 64MB             # cap on retained envelope sections (optional)
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `record_path` | — | XPath selecting the elements that each become one record. |
| `attribute_prefix` | `@` | Prefix that distinguishes an element's attributes from its child elements when both map to schema fields. |
| `namespace_handling` | `strip` | `strip` removes namespace prefixes from element and attribute names; `qualify` preserves the namespace-qualified names. |
| `max_index_bytes` | `64MB` | Cap on the bytes the envelope pre-scan retains while extracting declared `$doc.*` sections. |

## Nested arrays

When a record element contains repeated child elements, the
[`array_paths`](../nodes/source.md#array-paths) field on the source
controls whether each repetition **explodes** into its own record or
**joins** into a delimited string. That field is shared with the JSON
reader and documented on the Source Nodes page.

## Bounding envelope retention: `max_index_bytes`

When a source declares an `envelope:` and a pipeline reads `$doc.*` paths
from it, the XML reader runs an event-driven streaming pre-scan that walks
the document once and retains only the declared section subtrees — every
other element, including a multi-megabyte body, is event-walked and dropped
without being flattened into memory. The retained sections live in a bounded
document index.

`max_index_bytes` caps that index. It is charged incrementally as each
section is built, so even a single oversized declared section aborts
mid-parse (naming the section and the cap) rather than risking an
out-of-memory failure. It accepts a decimal size string (`64MB`, `500KB`)
or a bare byte count; optional, defaulting to **64MB**. Only the declared
sections a program actually reads are retained, so envelope metadata sits
far below this ceiling in practice — the cap exists to convert an unbounded
mistake into a clear error.

The reader still buffers the raw source bytes for the lifetime of the read
(one disk read backs both the body parser and the pre-scan); the pre-scan
no longer materializes the section subtrees themselves. See
[Document Envelope Context](../pipelines/envelope-and-doc-context.md) for
the full model.
