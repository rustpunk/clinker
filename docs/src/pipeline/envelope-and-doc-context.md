# Document Envelope Context (`$doc.*`)

Many enterprise file formats wrap their record body in an **envelope**:
named sections that surround the records and carry document-level
metadata — a batch header with a run date and batch id, a trailer with
a record count and checksum, or arbitrary sibling sections. Clinker
exposes these sections to CXL through the `$doc.<section>.<field>`
namespace.

```yaml
sources:
  - name: payments
    path: data/payments.xml
    format: xml
    envelope:
      sections:
        BatchInfo:
          extract: { xml_path: "/payments/BatchInfo" }
          fields:
            batch_id: string
            run_date: date
        Summary:
          extract: { xml_path: "/payments/Summary" }
          fields:
            record_count: int
            checksum: string
```

A downstream transform reads any declared section field on every body
record:

```yaml
nodes:
  - transform: tag
    inputs: { in: payments }
    project:
      - batch: $doc.BatchInfo.batch_id
      - expected_total: $doc.Summary.record_count
      - amount: amount
```

## Section names are yours

The engine reserves **no** section names. `BatchInfo` and `Summary`
above are arbitrary identifiers chosen by the pipeline author — `Head`
/ `Foot`, `preamble` / `trailer`, `batch_metadata` / `eob_summary` are
all equally valid. A section name is whatever string you put in the
`sections:` map; CXL exposes it verbatim as `$doc.<that_name>.<field>`.

## All sections are available everywhere in the body stream

Before the first body record streams from a file, the reader runs a
one-time **envelope pre-scan** that extracts *every* declared section —
no matter where it physically sits in the file. A header at the top and
a trailer at the bottom are both pulled out up front. The result: every
body record sees every `$doc.<section>.<field>` value, from the first
record to the last.

This means a trailer field is available *during* body streaming, not
just at end-of-file. A pipeline can compute, on every row, a ratio
against the trailer's total:

```yaml
project:
  - running_fraction: row_index / $doc.Summary.record_count
```

The pre-scan reads the envelope-bearing segments of the file before
body streaming begins. Envelope payloads are small (a few hundred bytes
per document is typical), but reaching a trailing section requires the
reader to have buffered the file — so envelope-aware sources hold the
source file's bytes in memory for the lifetime of the read. Body
records still stream one at a time; only the envelope sections (not the
body) live in the document context.

`$doc.*` is **not** the file in memory. It holds the parsed envelope
sections only — body records flow through the pipeline one at a time,
and the only stages that buffer multiple records are the usual blocking
operators (Aggregate, Sort, grace-hash Combine) under the standard RSS
budget.

## Extract rules per format

Each section declares how the reader locates its payload:

| Format  | `extract:` key   | Value                                            |
| ------- | ---------------- | ------------------------------------------------ |
| XML     | `xml_path`       | Slash-path to the section element, e.g. `/doc/Head` |
| JSON    | `json_pointer`   | RFC 6901 pointer, e.g. `/Head`                   |
| EDIFACT | `segment`        | A service-segment tag — only `UNB`               |
| X12     | `segment`        | A service-segment tag — only `ISA` (GS/ST surface as nested levels) |
| HL7 v2  | `segment`        | A header-segment tag — only `FHS` (BHS/MSH surface as nested levels) |

Declaring an `xml_path` section against a JSON source (or vice versa),
or a `segment` extract against XML/JSON, is a configuration error and
fails fast when the source opens, rather than silently producing empty
sections. CSV and fixed-width sources do not yet support envelope
extraction; declaring envelope sections on those formats is a no-op
today.

### EDIFACT `segment` extract

An EDIFACT source exposes its interchange header `UNB` as an envelope
section. The section's field names are the positional element keys
`e01`, `e02`, … :

```yaml
envelope:
  sections:
    interchange:
      extract: { segment: "UNB" }
      fields:
        e05: string          # interchange control reference
```

Only the `UNB` header is extractable. EDIFACT is scanned as a flat byte
stream with only the header pre-read, so trailer segments (`UNT`, `UNZ`)
that arrive after the body are **not** envelope sections — their control
counts are validated inline by the reader instead. A `segment` extract
naming any tag other than `UNB` is rejected at startup. See
[EDIFACT Format](edifact.md) for the full reference.

A JSON example:

```yaml
sources:
  - name: payments
    path: data/payments.json
    format: json
    record_path: records
    envelope:
      sections:
        Head:
          extract: { json_pointer: "/Head" }
          fields:
            batch_id: string
        Foot:
          extract: { json_pointer: "/Foot" }
          fields:
            count: int
```

against:

```json
{
  "Head": { "batch_id": "RUN-001" },
  "records": [ { "amount": 10 }, { "amount": 20 } ],
  "Foot": { "count": 2 }
}
```

## Typed fields

Each section's `fields:` map declares the field name and its type, drawn
from the same small vocabulary as source schemas: `string`, `int`,
`float`, `bool`, `date`, `date_time`. The extracted raw value is coerced
to the declared type at pre-scan time; a value that cannot coerce
(e.g. a non-numeric string declared `int`) fails the source with a
diagnostic naming the section, field, and offending value.

A field that the document does not carry resolves to `null` — `$doc.*`
follows the same missing-value convention as `$source.*` and
`$pipeline.*`. A section that the document does not carry at all is
simply absent from the context; any `$doc.<missing_section>.<field>`
resolves to `null`.

## One document per file

Each source file is its own document with its own envelope context.
When a source matches multiple files (via `glob:` / `paths:`), each file
gets a fresh document context with its own section values. Records from
different files never share a context — a record's `$doc.*` always
reflects the file that record came from.

Document boundaries flow through the pipeline as inline punctuation
signals (one when a document opens, one when it closes). These signals
let document-scoped operators fire at exactly the right point. Today the
signals propagate through Source, Transform, Route, and Sort, and are
reconciled at the multi-input operators — Merge and Combine. A document
opens downstream once (on first sighting) and closes downstream once —
after **every input that opened the document has closed it** (its
per-document close-count reaches its open-count). A document carried by
one input closes after that input's single close; a document that
genuinely spans several inputs closes only after the last of them, and
emits one downstream close either way. So a downstream document-scoped
operator fires exactly once regardless of how many inputs the document
spanned.

### Per-document aggregation

A grouped or global `Aggregate` reading a multi-document source produces
**one set of grouped rows per document**, not a single aggregate spanning
every document. When a document closes, the Aggregate finalizes and emits
the groups belonging to that document, then drops their state before the
next document accumulates — so a `glob:` source over twelve monthly files
through a `group_by` Aggregate yields twelve independent monthly
roll-ups, and only one document's groups are ever live at once (the
others have already been emitted and freed, or have not yet started).

This applies only when a document boundary actually reaches the
Aggregate. A plain single-file source is one document, so it still emits
one aggregate. A `Merge` that combines several distinct single-document
sources now forwards **each** source document's close (each document has
open-count == close-count == 1 on its single branch), so those sources
flush **independently** downstream — one roll-up per source document,
exactly as feeding each source to its own Aggregate would. This holds for
**every** `Merge` mode: `mode: concat`, `mode: interleave` over
non-Source inputs, `mode: interleave` with an explicit `interleave_seed`,
and the fused all-Source `mode: interleave` fast path with no seed. The
roll-up split holds **regardless of whether the Aggregate's upstream
streams or materializes** its output — both paths flush per document.

A `Combine` (join) carries reconciled boundaries on every strategy, so a
per-document Aggregate downstream of a join also rolls up per driver
document.

## Nested (multi-level) envelopes

Some formats wrap their records in *several* envelope levels, one inside
another. EDI X12 is the canonical example and the first format that
implements this: an **interchange** (ISA/IEA) contains one or more
**functional groups** (GS/GE), each containing one or more **transaction
sets** (ST/SE), each containing the records. A single file can carry
multiple interchanges back to back. See [X12 Format](x12.md) for the full
reference.

HL7 v2 is the second multi-level format: an optional **file** (`FHS`/`FTS`)
contains optional **batches** (`BHS`/`BTS`), each containing one or more
**messages** (`MSH..`), each containing the segment records. The tiers map
onto the same nested levels — the `FHS` file header is a declared
`segment: "FHS"` section, while the `BHS` batch and the `MSH` message
surface automatically as the reader-supplied sections `batch` and
`transaction_set`. Every tier is optional, so a bare `MSH`-led file simply
opens one message level. See [HL7 v2 Format](hl7.md) for the full reference.

A reader for such a format opens and closes each nested level as it
crosses the corresponding envelope boundary mid-file. Each level
contributes its own sections to `$doc`. There is no new `$doc` syntax for
nesting — every level's sections are read through the same two-level
`$doc.<section>.<field>` lookup. A record inside the innermost level sees
every enclosing level's sections at once. For X12 the interchange header is
a *declared* `segment: "ISA"` envelope section (you choose its name), while
the `GS` group and `ST` set surface automatically as the reader-supplied
sections `functional_group` and `transaction_set`, each keyed by positional
`eNN` elements:

```yaml
project:
  - interchange_control: $doc.interchange.e13        # ISA13, declared section
  - functional_id:       $doc.functional_group.e01   # GS01 (reader-supplied)
  - transaction_type:    $doc.transaction_set.e01     # ST01 (reader-supplied)
  - claim_amount:        amount                       # body field
```

A record streamed inside the ST level resolves the ST section, the
enclosing GS section, and the outermost ISA section, all at once: each
inner level inherits every enclosing level's sections as siblings in one
flat namespace. If two levels declare a section with the *same* name, the
innermost wins for records inside it — the same shadowing rule a nested
scope follows in any language. Picking distinct per-level names (as above)
keeps every level independently visible.

The reader-supplied default names are not the only option: an X12 source
can name the `GS` and `ST` levels itself and give each a typed field
schema, so a nested level is addressable under a chosen name with coerced
fields exactly like the declared `ISA` section. The declaration lives on
the source's `options` (not the `envelope:` block, which is reserved for
the pre-scannable file-level header), and each nested level is named
independently:

```yaml
type: x12
options:
  group_section:
    name: functional_group       # your choice — the engine reserves no name
    fields: { e06: int }         # GS06 group control number, typed
  set_section:
    name: transaction_set        # your choice
    fields: { e01: int }         # ST01 transaction-set id, typed
```

Omit a level's declaration and it falls back to its reader-supplied
default name keyed by untyped positional `eNN` strings. See
[X12 Format](x12.md#naming-and-typing-the-nested-levels) for the full
reference.

Boundaries nest correctly through the pipeline: each level opens before
the records inside it and closes after them, in strict innermost-first
order. A level that fans in through several branches is still reconciled
once at a multi-input operator (Merge or Combine) — it opens downstream on
first sighting and closes downstream once every input that opened it has
closed it — exactly like a single-level document.

## Header-only interchanges

A multi-level envelope file can legitimately carry an interchange whose
body is empty — envelope structure (an interchange header, and possibly
inner group headers) with zero records inside. Such an interchange still
opens a document and emits its open/close boundaries, so downstream
operators and trailer-count validation observe it just like any other
document. The interchange's `$doc.*` sections are extracted and the
boundaries flow even though no body record ever streams from it.

The same holds for an empty *inner* envelope — an open/close pair with no
records between — and for an inner envelope that opens or closes after the
file's last body record. Every envelope boundary a reader signals is
applied, whether or not a record follows it, so the document frame stays
balanced end to end.
