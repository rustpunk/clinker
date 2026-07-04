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
**joins** into a delimited string. The field is shared with the JSON
reader; the XML-specific matching rules are below.

An entry's `path` is the repeated element's dotted path **relative to the
record element** — the same form the flattened field names use. For a
record element `<Order>` containing repeated `<Item>` children, the path
is `Item`; for `<Order><Items><Item>…`, it is `Items.Item`.

```yaml
- type: source
  name: orders
  config:
    name: orders
    type: xml
    path: "./data/orders.xml"
    options:
      record_path: "Orders/Order"
    schema:
      - { name: id, type: int }
      - { name: "Item.name", type: string }
      - { name: "Item.qty", type: int }
      - { name: "Tag", type: string }
    array_paths:
      - path: "Item"
        mode: explode          # one output record per <Item> occurrence
      - path: "Tag"
        mode: join             # <Tag>a</Tag><Tag>b</Tag> -> "a,b"
        separator: ","
```

**`explode`** emits one output record per occurrence of the element. Each
output carries that occurrence's fields — which keep their full dotted
names (`Item.name`, `Item.@sku`), including the element's attributes —
plus every field outside the path, duplicated onto each record. An
occurrence with no content (`<Item></Item>`) still emits a record, one
carrying only the fields outside the path. A record with **no** occurrence
of the element passes through unchanged: XML cannot distinguish an empty
repetition from an absent element.

**`join`** concatenates values instead of fanning out. Every repeated
flattened field at or under the path is collapsed independently into a
single field whose value joins the occurrences' values with `separator`
(default `,`), in document order: repeated `<Tag>` text joins under `Tag`,
and repeated `<Item><name>` children join under `Item.name`.

Entries apply in declaration order, so a join's collapsed value lands on
every record an earlier explode fanned out, and two exploded paths
multiply. Paths must name **disjoint** element groups — a duplicated path,
or one path extending another (`Item` and `Item.part`), is rejected when
the source opens.

Repeated fields *not* named by any array path keep the default
duplicate-key collapse: the first value wins and later repetitions are
dropped.

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
