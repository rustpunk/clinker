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

## Writing XML

The XML writer expands dotted field names to nested elements and applies
the same `attribute_prefix` convention in reverse: a field whose final
path segment carries the prefix is emitted as an XML **attribute** of its
enclosing element instead of a child element. A top-level `@id` attaches
to the record element's start tag; a nested `Address.@type` attaches to
the `<Address>` element. Records read from an XML source therefore
round-trip — `<Record id="7"><name>A</name></Record>` reads and writes
back unchanged, and the writer never emits an `@`-named element.

```yaml
- type: output
  name: xml_out
  input: processed
  config:
    name: xml_out
    type: xml
    path: "./output/result.xml"
    options:
      root_element: "Root"              # default Root
      record_element: "Record"          # default Record
      attribute_prefix: "@"             # matches the source-side prefix
```

| Option | Default | Description |
|--------|---------|-------------|
| `root_element` | `Root` | Name of the document root element wrapping all records. |
| `record_element` | `Record` | Name of the element emitted per record. |
| `attribute_prefix` | `@` | Prefix marking a field as an attribute of its enclosing element. Set it to the same value as the source-side prefix when round-tripping; an empty string disables attribute classification (every field emits as an element). |

Attribute handling details:

- A **null** attribute field is dropped even under `preserve_nulls: true` —
  a null element round-trips as a self-closing tag, but an attribute has
  no form that reads back as null.
- A field with children nested under an attribute-prefixed segment
  (e.g. `@a.b`) is rejected with a format error: an XML attribute is a
  leaf and cannot contain elements.
- An element with only attribute fields and no children self-closes:
  `Address.@type` alone emits `<Address type="home"/>`.

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

The reader holds no whole-document buffer: the body walks the document
element-at-a-time, and the envelope pre-scan opens the source a second time
to walk it independently — a file source is read twice, never buffered. Peak
memory is the bounded section index plus a single live record, not the input
size. See
[Document Envelope Context](../pipelines/envelope-and-doc-context.md) for
the full model.
