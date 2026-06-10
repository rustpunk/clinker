# EDIFACT Format

Clinker reads and writes UN/EDIFACT interchanges alongside CSV, JSON,
XML, and fixed-width. An interchange is a finite file: it opens with an
optional `UNA` service-string advice and a mandatory `UNB` header, wraps
one or more `UNH..UNT` messages, and closes with a `UNZ` trailer. The
reader streams one segment at a time and the writer reconstructs the
envelope around emitted records. The reader decodes release-escape
sequences into clean data values and the writer re-escapes them on
output, so a reader → writer → reader round-trip preserves the data
values and the envelope control references.

## Delimiters and the UNA service string

Each segment is terminated by the segment terminator; within a segment,
data elements split on the element separator and components on the
component separator. A release character escapes a delimiter that occurs
as literal data.

When the file begins with a 9-byte `UNA` prefix, its six service
characters override the defaults in this fixed order: component,
element, decimal, release, repetition, terminator. When `UNA` is absent,
the syntax Level-A defaults apply:

| Role               | Level-A default |
| ------------------ | --------------- |
| Component separator | `:`            |
| Element separator   | `+`            |
| Decimal notation    | `.`            |
| Release / escape    | `?`            |
| Repetition          | space (inactive)|
| Segment terminator  | `'`            |

`UNA` is optional — a parser that requires it would fail on the common
no-`UNA` interchange, so Clinker assumes Level-A when it is absent.

### Release character

The release character (default `?`) marks the following byte as literal
data rather than a delimiter: `?+` is a literal `+` inside an element,
`?'` is a literal apostrophe (not a terminator), and `??` is a literal
`?`. The reader **decodes** these sequences into clean data values, so a
downstream CSV/JSON sink, a CXL string comparison, or a `$doc` field sees
`O'BRIEN`, never the wire form `O?'BRIEN`. The writer **re-escapes** on
output: any element value that carries the element separator, the segment
terminator, or the release character is release-escaped automatically, so
a value computed by a Transform or sourced from CSV — never
EDIFACT-escaped to begin with — does not corrupt the interchange. A
reader → writer → reader round-trip therefore preserves the data values
exactly.

The component separator inside an element (e.g. the `:` in the composite
`UNOA:1`) is kept as part of the element's text and is not escaped — the
positional element model works above component resolution, so a composite
element round-trips unchanged. A literal colon in free-text data is the
one ambiguity this introduces: because components are not split into
separate fields, a `:` in a value re-reads as a component boundary.
Repeating elements ride inside one element string intact and are likewise
never truncated to their first repetition.

### Newlines between segments

Some producers insert CR/LF after each segment terminator for
readability. Those bytes are insignificant and are stripped between
segments; CR/LF that appears inside an element is preserved.

## Record shape

Each non-service segment becomes one record under a fixed positional
schema:

| Column     | Meaning                                              |
| ---------- | ---------------------------------------------------- |
| `seg_id`   | The segment tag (`BGM`, `NAD`, …)                    |
| `msg_ref`  | The enclosing message reference (the `UNH` element 1)|
| `msg_type` | The message type (the `UNH` element 2, full composite)|
| `e01`, `e02`, … | The segment's positional data elements (release sequences decoded) |

Service segments (`UNB`, `UNZ`, `UNH`, `UNT`) are consumed by the reader
to drive envelope state and validation — they are never emitted as body
records. The `UNH` segment that opens a message **is** emitted as a body
record (its `seg_id` is `UNH`), carrying the message reference and type.

The number of `eNN` columns is controlled by the source `max_elements`
option (default 32). A segment carrying more data elements than that is
rejected with guidance rather than silently truncated. Absent trailing
elements read as `null`.

```yaml
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: edifact
      glob: ./inbox/*.edi
      options:
        max_elements: 48      # widen the positional schema for exotic segments
      schema:
        - { name: seg_id, type: string }
        - { name: msg_ref, type: string }
        - { name: e01, type: string }
```

## Envelope sections over UNB

The interchange header `UNB` is extractable as a document envelope
section, exposing its positional elements to CXL as
`$doc.<section>.<field>`. Use the `segment` extract rule with the section
field names matching the positional keys `e01`, `e02`, …:

```yaml
envelope:
  sections:
    interchange:
      extract: { segment: "UNB" }
      fields:
        e05: string          # interchange control reference (UNB element 5)
```

A Transform can then read `$doc.interchange.e05` on every body record.

Only the `UNB` header is extractable as an envelope section. Trailer
segments (`UNT`, `UNZ`) arrive after the body and cannot become `$doc`
fields without buffering the whole interchange — their control counts
are instead validated inline by the reader (see below). A `segment`
extract naming any tag other than `UNB`, or an `xml_path` / `json_pointer`
extract against an EDIFACT source, is rejected at startup.

## Control-count validation

The reader validates the structural integrity claims carried in the
trailers as they arrive, failing the run on a mismatch (a truncation or
corruption signal):

- **`UNT` segment count** — must equal the actual number of segments in
  the message, counting the `UNH` and `UNT` themselves.
- **`UNT` message reference** — must echo the opening `UNH` reference.
- **`UNZ` message count** — must equal the actual number of `UNH`
  messages in the interchange.
- **`UNZ` control reference** — must echo the `UNB` control reference.

The `UNB` control reference (data element 0020) is located by its
structural position — the first data element after the four mandatory
leading composites (syntax identifier, sender, recipient, date/time) —
rather than at a fixed element index. An interchange that carries an
empty optional element ahead of the control reference (shifting it past
the fifth position) therefore validates and round-trips correctly: the
reader reads the real reference and the writer echoes the same one into
`UNZ`, so the trailer never contradicts its own header.

A missing `UNZ` at end of input is a truncation error; content after the
`UNZ` trailer is rejected.

## Writing EDIFACT

An EDIFACT Output node reconstructs the envelope around emitted records.
Records map by the same positional columns (`seg_id`, `msg_ref`,
`msg_type`, `eNN`); trailing `null`/empty elements are trimmed so no
fabricated delimiters appear, and a column the writer does not recognize
is an error (project the record to the EDIFACT columns first).
Engine-internal `$`-namespaced columns are excluded automatically.

```yaml
nodes:
  - type: output
    name: out
    input: messages
    config:
      name: out
      type: edifact
      path: ./out/result.edi
      options:
        interchange: ["UNOA:1", "SENDER", "RECEIVER", "240101:1200", "REF1"]
        message_type: "ORDERS:D:96A:UN"
        write_una: false
        segment_newline: true
```

Output options:

| Option                  | Meaning                                                                 |
| ----------------------- | ----------------------------------------------------------------------- |
| `interchange`           | Literal `UNB` data elements (release-escaped as needed on write).        |
| `interchange_from_doc`  | Name of a `$doc` section to echo the `UNB` elements from (round-trip).   |
| `message_type`          | Fallback `UNH` message type when a record carries no `msg_type` value.   |
| `write_una`             | Emit a leading `UNA` segment (default `false`).                          |
| `segment_newline`       | Write a newline after each segment terminator (default `true`).         |

Consecutive records are grouped into `UNH..UNT` messages on `msg_ref`
transitions. The writer recomputes the `UNT` segment count and `UNZ`
message count, and echoes the message and interchange control references,
so the output passes its own count validation on re-read.

`interchange_from_doc` echoes the header from a record's document
context. That context is populated by a source's `UNB` envelope section
(declare a `segment: "UNB"` envelope section on the source) and travels
with every body record through the pipeline — including to a sink that
sits directly downstream of the source with no intervening Transform. The
reader stashes the complete, ordered `UNB` element list (empty middle
elements included), so the reconstructed header is faithful even when a
middle element is empty and the user declares only the fields they care
about. Supply `interchange` literal elements instead when the records
have no source `UNB` section to echo.

## Limitations

- **Charset.** Element text is decoded as UTF-8. Non-UTF-8 interchanges
  (UNOA/UNOB/Latin-1 high bytes) are rejected explicitly rather than
  silently corrupted.
- **Functional groups.** A single `UNB..UNZ` interchange is supported;
  `UNG`/`UNE` functional-group segments are rejected with a precise
  error.
- **UNH composite fidelity.** The reader stamps the `UNH` reference
  (element 1) and the full message-type composite (element 2). A `UNH`
  carrying additional elements (e.g. a common access reference) is
  reconstructed as a two-element `UNH` on round-trip.
- **Output splitting.** An interchange is a single `UNB..UNZ` envelope and
  cannot be divided across files. An `edifact` output combined with a
  `split:` block is rejected at config-validation time (diagnostic
  `E323`) rather than emitting a structurally corrupt interchange.
