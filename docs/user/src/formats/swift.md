# SWIFT MT Format

Clinker reads and writes SWIFT MT (FIN) messages alongside CSV, JSON, XML,
fixed-width, EDIFACT, X12, and HL7 v2. A SWIFT MT message is a finite file
built from brace-balanced blocks. The reader streams the message body one
field at a time and surfaces the service blocks as document-envelope
sections; the writer inverts the reader exactly, re-framing the block
structure around emitted records so a read → write → read round-trip is
byte-faithful.

## Block structure

A SWIFT MT message is a sequence of top-level blocks, each `{n:...}` where
`n` is a numeric block id:

| Block | Role                | Contents                                          |
| ----- | ------------------- | ------------------------------------------------- |
| `1`   | Basic header        | A fixed header string (application id, BIC, …)    |
| `2`   | Application header  | Input/output direction, message type, recipient   |
| `3`   | User header         | Optional; may carry nested `{tag:value}` sub-blocks |
| `4`   | Message text        | The message body — a run of `:tag:value` fields    |
| `5`   | Trailer             | Optional; may carry nested `{tag:value}` sub-blocks |

Unlike the flat delimiter-structured EDI formats (HL7, X12, EDIFACT), SWIFT
framing is **brace-balanced** rather than terminator-delimited. Because of
that, the nested sub-blocks of blocks 3 and 5 (for example
`{3:{108:MSGREF}}`) are kept intact inside their parent block rather than
mistaken for top-level blocks.

### The `-}` text-block trailer

Block 4 is special. Its body is opaque line-structured free text — a field
value (a `:77E:` envelope, a `:79:` narrative, an `:86:` information line)
legitimately contains `{`, `}`, and even `-}` as data. So braces inside
block 4 are treated as data, not framing: the block closes only on a
*line-anchored* `-}` trailer — a `-}` that begins a line. An interior `{`,
`}`, or a `-}` in the middle of a value is data, not a frame boundary. Both
the framing braces and the closing `-}` trailer are stripped from the stored
values, so a record carries clean tag/value data.

### Whitespace between blocks

Producers insert CR/LF (and the `\r\n` that separates block-4 fields) for
readability. Inter-block whitespace is insignificant and is skipped; the
line breaks inside block 4 delimit the `:tag:value` fields.

## Record shape

Each `:tag:value` line of block 4 becomes one record under a fixed
positional schema — the same one-line-one-record model the X12 and HL7
readers use:

| Column  | Meaning                                                       |
| ------- | ------------------------------------------------------------- |
| `block` | The block id the line came from (always `4` for body fields)  |
| `tag`   | The SWIFT field tag without its surrounding colons (`20`, `32A`, `61`) |
| `value` | The field value, with continuation lines folded in verbatim   |

A multi-line field (a `:50K:` ordering-customer block, a `:77E:` / `:86:`
narrative) keeps its continuation lines: any line of block 4 that does not
begin a new `:tag:` is folded into the current field's value with its line
break preserved — including a blank line **inside** the value, so a narrative
with an internal blank line round-trips faithfully. A repeated tag (the
`:61:` / `:86:` statement lines of an MT940, for instance) streams as one
record per occurrence, in order.

The service blocks (`1`, `2`, `3`, `5`) are consumed by the reader to serve
envelope sections and drive the message-level document context — they are
never emitted as body records.

```yaml
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: swift
      glob: ./inbox/*.swift
      options:
        max_fields: 20000   # raise the block-4 field ceiling for large messages
      schema:
        - { name: block, type: string }
        - { name: tag, type: string }
        - { name: value, type: string }
```

The `max_fields` option caps the number of block-4 field lines a single
message may carry (default 10000). A message exceeding it is rejected with
guidance rather than streamed unbounded — a corruption guard, since a real
MT message is well under the cap.

## Envelope sections over the service blocks

A SWIFT MT message is a single envelope: the four service blocks surface as
file-level `$doc` sections, exposing each block's text to CXL as
`$doc.<section>.body`. Every body record can read the enclosing message's
headers through a `$doc.<section>.body` lookup.

Declare the sections on the source with the `segment` extract rule naming the
block id. The whole block body surfaces under the field name `body`, because
a SWIFT service block carries free-form text (a header string, nested
`{sub:tag}` blocks) rather than positional elements:

```yaml
envelope:
  sections:
    basic:
      extract: { segment: "1" }   # block 1, the basic header
    app:
      extract: { segment: "2" }   # block 2, the application header
    user:
      extract: { segment: "3" }   # block 3, the user header (nested sub-blocks kept verbatim)
    trailer:
      extract: { segment: "5" }   # block 5, the trailer
```

A Transform on any body record can read every enclosing block at once:

```cxl
emit tag       = tag
emit value     = value
emit basic_hdr = $doc.basic.body     # block 1 header string
emit app_hdr   = $doc.app.body       # block 2 header string
emit user_hdr  = $doc.user.body      # block 3 body, nested {108:...} kept verbatim
```

The section names are entirely your choice — the engine reserves none. A
`segment` extract may name a block either by its numeric id (`"1"`, `"3"`) or
by the stable default label (`"basic_header"`, `"app_header"`,
`"user_header"`, `"trailer"`); both resolve the same block.

Block 4 is the message-text body streamed as records, not an envelope
section — a `segment: "4"` extract is rejected at startup. An `xml_path` or
`json_pointer` extract against a SWIFT source is likewise rejected, because
those rules belong to the tree formats.

## Malformed-message handling

A structurally broken message fails the run with a precise `SWIFT` error
rather than producing garbled records:

- **Unbalanced brace** — a block that never closes (or whose brace depth
  never returns to zero) is a truncation error naming the offending block.
- **Missing `-}` trailer** — a block 4 that runs to end of input without its
  `-}` trailer is a truncation error.
- **Missing or non-numeric block id** — a block without a numeric id after
  the `{` (or with no `:` separating the id from the body) is rejected.
- **Malformed `:tag:value` line** — a block-4 line with no second colon
  closing the tag, or an empty tag, is rejected.
- **Repeated service block** — a second `{1:...}` (or any repeated service
  block) in one message is rejected.

A header-only message (no block 4, or an empty block 4) is valid: it
produces no body records and drains cleanly.

## Writing SWIFT MT

A SWIFT Output node inverts the reader exactly. It re-emits each block-4
record as a `:tag:value` line and re-frames the single message envelope
around them: the service blocks 1/2/3 first, then block 4 (`{4:` … `-}`),
then the optional block-5 trailer. Block-4 free text is opaque, so values are
written **verbatim with no escaping** — an interior `{`, `}`, a mid-line `-}`,
a folded continuation break, and an interior blank line all reproduce as data.
The reader strips exactly the structural separators (braces, the leading `:`,
the `-}` trailer, the line breaks) and keeps every other byte; the writer
re-adds exactly those separators and nothing else, so a read → write → read
round-trip returns byte-identical field values.

Records map by the `tag` and `value` columns. The `block` column is the
constant `4` discriminator (an empty `block` is treated as block 4, so a
Transform that projects only `tag`/`value` writes fine); a record carrying a
`block` other than `4` is rejected, because service blocks are never emitted
as records — they ride the document context.

```yaml
nodes:
  - type: output
    name: out
    input: messages
    config:
      name: out
      type: swift
      path: ./out/message.swift
      options:
        basic_header_from_doc: basic
        app_header_from_doc: app
        user_header_from_doc: user
        trailer_from_doc: trailer
```

Each service block is written from a literal body or echoed from a
user-declared `$doc` section:

| Option                   | Meaning                                                          |
| ------------------------ | ---------------------------------------------------------------- |
| `basic_header`           | Literal block-1 body, written verbatim as `{1:<body>}`.          |
| `basic_header_from_doc`  | Name of a `$doc` section to echo the block-1 body from.          |
| `app_header`             | Literal block-2 body.                                            |
| `app_header_from_doc`    | Name of a `$doc` section to echo the block-2 body from.          |
| `user_header`            | Literal block-3 body (nested `{sub:tag}` content kept verbatim). |
| `user_header_from_doc`   | Name of a `$doc` section to echo the block-3 body from.          |
| `trailer`                | Literal block-5 body, written after block 4 closes.             |
| `trailer_from_doc`       | Name of a `$doc` section to echo the block-5 body from.          |

The `*_from_doc` options name the section the user declared on the **source**
— the engine reserves no section name. A literal `*_header` wins over its
`*_from_doc` companion when both are set; a service block with neither is
omitted. The `_from_doc` echo reads the block body verbatim from the section's
`body` field — the same single-field shape the reader writes — so a SWIFT
source's service blocks (declared as `segment` envelope sections) round-trip
unchanged when their section names are passed back to the writer here.

A SWIFT MT message is a single indivisible envelope, so a `swift` output
cannot be combined with a byte-limit `split:` block — the pairing is rejected
at config-validation time (diagnostic `E342`).

## Limitations

- **UTF-8 only.** SWIFT MT messages are decoded as UTF-8; a non-UTF-8 block
  body is rejected explicitly rather than corrupted silently.
- **Field-content parsing.** The reader exposes each `:tag:value` line as a
  `tag`/`value` pair verbatim. Parsing a field's internal structure (the
  sub-fields of a `:32A:` value-date/currency/amount, say) is a CXL concern
  downstream of the source, not a reader responsibility.
