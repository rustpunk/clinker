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

Like the rest of the pipeline config, the `envelope:` block is strict:
an unknown key at any level (a misspelled `sections:`, `extract:`, or
`fields:`) is rejected at plan parse time with a diagnostic naming the
bad key, rather than being silently ignored.

A downstream transform reads any declared section field on every body
record:

```yaml
nodes:
  - type: transform
    name: tag
    input: payments
    config:
      cxl: |
        emit batch = $doc.BatchInfo.batch_id
        emit expected_total = $doc.Summary.record_count
        emit amount = amount
```

## Section names are yours

The engine reserves **no** section names. `BatchInfo` and `Summary`
above are arbitrary identifiers chosen by the pipeline author — `Head`
/ `Foot`, `preamble` / `trailer`, `batch_metadata` / `eob_summary` are
all equally valid. A section name is whatever string you put in the
`sections:` map; CXL exposes it verbatim as `$doc.<that_name>.<field>`.

## Extracted sections are available throughout the body stream

Every extracted section is available to *every* body record. JSON and XML
can pre-scan declared sections anywhere in the document, so a header at the
top and a trailer at the bottom are both visible from the first record to
the last. SWIFT likewise scans its service blocks before emitting fields.
Multi-record CSV and fixed-width `record_type` extraction captures the
leading header region only; it does not extract arbitrary trailing sections.

For formats with trailing-section extraction, a trailer field is available
*during* body processing, not just at end-of-file. A pipeline can compute,
on every row, a ratio
against the trailer's total:

```yaml
project:
  - running_fraction: row_index / $doc.Summary.record_count
```

Note that an *extracted* trailer section you read via `$doc.*` is distinct
from the **structural counts** an EDI reader validates internally (the X12
`SE`/`GE`/`IEA`, EDIFACT `UNT`/`UNZ`, HL7 `BTS`/`FTS` segment counts). Those
trailer counts are checked by the reader against the body it streamed, and a
mismatch is a structural-integrity failure — see [Malformed
envelopes](error-handling.md#malformed-envelopes-structural-validation) for
how `dlq_granularity: document` dead-letters a malformed file instead of
aborting the run.

The pre-scan reads the envelope-bearing segments of the file before
body streaming begins. The amount of retained data depends on the declared
sections, their payloads, and the format's path to a trailing section:

- **JSON** streams the pre-scan. The reader walks the document once and
  deserializes *only* the subtrees the declared sections point at —
  every other key (including a multi-megabyte body array) is
  parsed-and-skipped without being stored. The retained sections live in
  a bounded **document index** capped by `max_index_bytes` (see below),
  so retained section memory scales with the declared sections, not the
  document size.
- **XML** streams the pre-scan too. The reader event-walks the document
  once and flattens *only* the declared section subtrees — every other
  element, including a multi-megabyte body, is event-walked and dropped
  without being materialized. The retained sections live in the same
  bounded **document index** capped by `max_index_bytes`. The reader still
  holds the file's raw bytes for the lifetime of the read (one disk read
  backs both the body parser and the pre-scan), but the pre-scan no longer
  materializes the undeclared section subtrees.

For multi-record CSV, retained section names, field names, values and document
containers carry allocation ownership against the run's finite memory budget.
Their charge lasts through downstream document use and any surviving aliases.
A repeated header does not create an unbounded history of section snapshots;
resource refusal remains fatal even under `strategy: continue`. CSV accepts the
same UTF-8 and Latin-1 policy for section cells as for body cells.

Only the envelope sections live in the document context. This separation does
not mean the executor retains only one body record: downstream Source and
Combine paths can still materialize whole inputs. See
[what the memory budget measures](../ops/memory.md#what-the-budget-measures).
Other formats' existing document-index limits remain as described below.

### Bounding envelope retention with `max_index_bytes`

For JSON and XML sources, the document index is capped so a
pathologically large declared section fails loud rather than exhausting
memory. The cap is charged incrementally *as each section is parsed* —
byte by byte while the section's subtree is built — so even a single
oversized declared section aborts mid-parse, before its whole subtree
materializes, naming the offending section and the cap. (Undeclared
siblings, including a multi-megabyte body, are skipped without being
parsed into the index at all.)

```yaml
- type: source
  name: events
  config:
    name: events
    type: json
    path: "./data/events.json"
    options:
      record_path: data.rows
      max_index_bytes: 64MB   # cap on retained envelope sections
```

The same `max_index_bytes` option applies to an XML source's `options:`
block, capping its envelope pre-scan identically.

`max_index_bytes` accepts a decimal size string (`64MB`, `500KB`) or a
bare byte count. It is optional; when omitted the reader applies a
documented finite default of **64MB**. Only the declared sections a
program actually reads are retained, so envelope metadata sits far below
this ceiling in practice — the cap exists to convert an unbounded
mistake into a clear error.

## Extract rules per format

Each section declares how the reader locates its payload:

| Format  | `extract:` key   | Value                                            |
| ------- | ---------------- | ------------------------------------------------ |
| XML     | `xml_path`       | Slash-path to the section element, e.g. `/doc/Head` |
| JSON    | `json_pointer`   | RFC 6901 pointer — empty (whole document) or leading `/`, e.g. `/Head` |
| EDIFACT | `segment`        | A service-segment tag — only `UNB`               |
| X12     | `segment`        | A service-segment tag — only `ISA` (GS/ST surface as nested levels) |
| HL7 v2  | `segment`        | A header-segment tag — only `FHS` (BHS/MSH surface as nested levels) |
| SWIFT MT | `segment`      | A service block: `"1"`, `"2"`, `"3"`, or `"5"` (or its default label) |
| Multi-record CSV / fixed-width | `record_type` | A header record-type tag, e.g. `H` |

`xml_path` and the source-level `record_path` option are both slash-paths over
XML but root differently: `xml_path` tolerates a leading `/` (`/doc/Head` is its
documented form), while `record_path` rejects one. They locate different things
and are deliberately not aligned — see
[XML Format → `record_path` and `xml_path` root differently](../formats/xml.md#record_path-and-xml_path-root-differently).

Declaring an `xml_path` section against a JSON source (or vice versa),
a `segment` extract against XML/JSON, a `record_type` extract against
any format other than multi-record CSV / fixed-width, or any `envelope:`
block at all on a plain (single-schema) CSV or fixed-width source is a
configuration error that fails fast rather than silently producing empty
sections.

A `json_pointer` must be a valid RFC 6901 pointer: either empty (`""`,
naming the whole document) or a `/`-introduced path such as `/Head` or
`/batch/summary`. A slashless value like `Head` is a typo — it would
decode to zero segments and silently match the root document — so it is
**rejected at validation** rather than resolving to the wrong metadata.

A **plain** (single-schema) CSV or fixed-width source carries no envelope
— there is no header/trailer structure to extract. Declaring `envelope:`
sections on one is a configuration error (`E356`): the sections would
never be populated and every `$doc.<section>.<field>` against the source
would resolve to `null`, so the compiler **rejects** it rather than
accepting an inert declaration. Envelope extraction on a flat file — the
`record_type` extract — applies only to a
[multi-record](../formats/csv.md#multi-record-files-header--trailer--body)
source, one declaring a `discriminator:` + `records:` block; declare that
schema if the file genuinely carries header/trailer records.

### Network (REST) sources carry no `$doc` context

A `rest` source pulls its records page by page over paginated HTTP — it
has no single buffered document with head and tail sections, so it
carries no envelope context. Envelope sections are a file-document
concept, so the compiler **rejects** them on a REST source rather than
letting them silently resolve to `null`:

- Declaring an `envelope:` block on a `rest` source is an error (`E349`) —
  the declaration would be inert.
- Reading `$doc.<section>.<field>` from a node fed by a `rest` source is an
  error (`E349`) — the access can never resolve.

Pull the document-level metadata into record fields through the API's own
response shape (`record_path`, `split_to_rows`) instead, so the value travels
as a normal field rather than as document-envelope context.

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

Only the `UNB` header is extractable. Trailer segments (`UNT`, `UNZ`)
that arrive after the body are **not** envelope sections — their control
counts are validated by the reader instead. A mismatch between a trailer's
declared count and the body the reader streamed is a structural-integrity
failure: by default it aborts the run, and under a source's
`dlq_granularity: document` opt-in it dead-letters the whole file to the DLQ
(see [Malformed envelopes](error-handling.md#malformed-envelopes-structural-validation)).
A `segment` extract naming any tag other than `UNB` is rejected at startup. See
[EDIFACT Format](../formats/edifact.md) for the full reference.

### Multi-record `record_type` extract

A [multi-record](../formats/csv.md#multi-record-files-header--trailer--body)
CSV or fixed-width source — one that declares a `discriminator:` and a
`records:` list — exposes a **header record type** as an envelope section
through the `record_type` extract. The tag names which of the source's
declared record types carries the section's payload; the matched header
row's named fields become the section's fields.

```yaml
schema:
  discriminator: { start: 0, width: 1 }
  records:
    - { id: header,  tag: H, columns: [ { name: batch_id, type: string, start: 1, width: 9 } ] }
    - { id: detail,  tag: D, columns: [ { name: amount,   type: int, start: 1, width: 9 } ] }
    - { id: trailer, tag: T, columns: [ { name: count,    type: int, start: 1, width: 9 } ] }
  structure:
    - { record: trailer, count: count }
envelope:
  sections:
    head:
      extract: { record_type: H }       # the H header row → $doc.head.*
      fields:
        batch_id: string
```

Only a **header** record type — one whose rows precede the body at the
file head — is extractable as a `$doc` section; the reader captures the
first such row in a bounded pre-scan and excludes it from the body
stream. A **trailer** record type (one named by a `structure:`
constraint) arrives after the body it closes, so it is **not** an
envelope section — its declared `count` is validated against the streamed
body count instead, the same structural-integrity check the EDI trailers
use. See [CSV](../formats/csv.md#multi-record-files-header--trailer--body)
and [Fixed-Width](../formats/fixed-width.md#multi-record-files-header--trailer--body)
for the full reference.

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

## Declared-path validation

Every `$doc.<section>.<field>` reference is cross-checked at compile time
against the schema the feeding source's reader will actually serve. A
reference that can never resolve — almost always a typo — is rejected at
compile time, pointing at the node that made it, rather than resolving
silently to `null`. How the path is checked depends on the source:

**Closed-schema sources (XML, JSON)** — the `envelope:` block is the
complete schema: the reader extracts exactly the sections and fields it
declares. A reference naming a section the source does not declare, or a
field the declared section does not declare, is rejected with error `E341`
(`$doc.Summry.total` against a declared `Summary`). Run
`clinker explain --code E341` for the full write-up.

**Multi-record CSV / fixed-width sources** — a source whose schema declares
`discriminator:` + `records:` exposes a header record type as a `$doc`
section through the `record_type` extract. The reader coerces the matched
header record's columns through the section's declared `fields:` and serves
exactly those fields, so the section is closed just like an XML/JSON one: a
reference naming an undeclared section, or a field the section does not
declare, is rejected with error `E341`. (A plain single-schema CSV /
fixed-width source has no such structure — declaring an `envelope:` on one
is rejected with error `E356`.)

**Segment/positional sources (X12, EDIFACT, HL7)** — the file-level header
(`ISA`/`UNB`/`FHS`) is declared through `envelope:` and is closed, but the
reader *also* synthesizes nested envelope levels the config never names —
X12's `functional_group` / `transaction_set`, HL7's `batch` /
`transaction_set` — keyed by positional `eNN` / `fNN` elements bounded by
the source's `max_elements` / `max_fields`. A `$doc` path is checked
against that synthesized vocabulary plus any section/field you declared, so
a legitimate wire-derived path (`$doc.functional_group.e06`) is accepted
while a misspelled section (`$doc.functonal_group.e06`) or an out-of-range
positional element (`$doc.transaction_set.e99`) is rejected with error
`E348`. Run `clinker explain --code E348` for the full write-up.

**REST sources** carry no document and reject `$doc` outright — see
[Network (REST) sources carry no `$doc` context](#network-rest-sources-carry-no-doc-context)
above (`E349`).

Plain (single-schema) CSV / fixed-width and SWIFT MT sources are not
statically checked. A plain flat file synthesizes no `$doc` sections, and
SWIFT serves declared sections under user-chosen *or* default block labels —
neither fits the closed or positional model cleanly. (A *multi-record* CSV /
fixed-width source is checked — see above.)

## Indexed `$doc` access

A section field that holds an array or a map can be indexed inline, the
same way any record value is — see [Nested paths](../cxl/nested-paths.md)
for the full bracket-index reference. Integer indices select array
elements; string keys select map entries; the two compose into a chain:

```yaml
project:
  - first_line:   $doc.Header.line_items[0]      # array element
  - run_date:     $doc.Header.meta["run_date"]   # map entry
  - first_sku:    $doc.Header.line_items[0]["sku"]  # array-of-maps chain
```

An out-of-range array index or a missing map key resolves to `null` — it
never errors or panics, and a `null` mid-chain short-circuits the rest of
the chain to `null` rather than failing. This is the same missing-value
convention `$doc.<missing_field>` and `$source.*` follow.

### Only literal paths are readable

Every index segment must be a **literal** — a constant integer or string
written in the program text. The section and field are always literal
identifiers (the grammar requires it), so a literal index is the last
piece a reader needs to know, before reading any input, exactly which
envelope paths a run will consume. The pre-scan extracts precisely those
statically-resolvable paths and nothing else.

A **computed** index — one derived from runtime data, such as
`$doc.Header.line_items[row_index]` — is not statically resolvable: the
reader cannot pre-scan a row-dependent element. clinker rejects it at
compile time with a diagnostic pointing at the offending index, rather
than reading it at run time. Use a literal index, or pull the value into
a record field upstream and index that instead.

This compiles together with the declared-path rule above: a `$doc` read
of a section or field the source does not declare is a **compile-time
error** (`E341`), and a `$doc` read with a computed index is a
**compile-time diagnostic** — neither reaches run time as a silent
`null`. Only a literal path over a *declared* section is pre-scanned and
readable.

## One document per file

Each source file is its own document with its own envelope context.
When a source matches multiple files (via `glob:` / `paths:`), each file
gets a fresh document context with its own section values. Records from
different files never share a context — a record's `$doc.*` always
reflects the file that record came from.

Document boundaries flow through the pipeline so that document-scoped
operators fire at exactly the right point. A document-scoped operator
fires exactly once per document, even when that document arrives across
several inputs that a Merge or Combine brings together.

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
sources flushes those sources **independently** downstream — one roll-up
per source document, exactly as feeding each source to its own Aggregate
would. This holds for every `Merge` mode.

A `Combine` (join) preserves document boundaries on every strategy, so a
per-document Aggregate downstream of a join also rolls up per driver
document.

## Nested (multi-level) envelopes

Some formats wrap their records in *several* envelope levels, one inside
another. EDI X12 is the canonical example and the first format that
implements this: an **interchange** (ISA/IEA) contains one or more
**functional groups** (GS/GE), each containing one or more **transaction
sets** (ST/SE), each containing the records. A single file can carry
multiple interchanges back to back. See [X12 Format](../formats/x12.md) for the full
reference.

HL7 v2 is the second multi-level format: an optional **file** (`FHS`/`FTS`)
contains optional **batches** (`BHS`/`BTS`), each containing one or more
**messages** (`MSH..`), each containing the segment records. The tiers map
onto the same nested levels — the `FHS` file header is a declared
`segment: "FHS"` section, while the `BHS` batch and the `MSH` message
surface automatically as the reader-supplied sections `batch` and
`transaction_set`. Every tier is optional, and a level's section exists
only when its header segment is present in the input — the reader
synthesizes a `batch` section only where it reads a `BHS`, and the message
level only where it reads an `MSH`.

A bare `MSH`-led stream — messages with no `BHS` batch and no `FHS` file
wrapper — therefore opens only the message level. `$doc.transaction_set.*`
resolves against each message, but **`$doc.batch.*` resolves to `null`**:
no `BHS` was read, so no `batch` section was synthesized. Whether a file
carries a `BHS` batch wrapper is a property of the input bytes, not the
config, so a `$doc.batch.*` path is never a compile error — it follows the
same missing-value convention an absent `$doc` section follows (resolve
`null`, never error), and populates as soon as a `BHS` wrapper is present:

```yaml
# Bare MSH stream (no BHS/FHS) — only the message level opens:
- msg_type: $doc.transaction_set.f08   # MSH-9 message type — populated
- batch_id: $doc.batch.f01             # null — no BHS, so no batch tier

# BHS-wrapped stream — the BHS opens a batch level, so both populate:
- msg_type: $doc.transaction_set.f08   # MSH-9 message type — populated
- batch_id: $doc.batch.f01             # BHS field — now populated
```

See [HL7 v2 Format](../formats/hl7.md) for the full reference.

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
[X12 Format](../formats/x12.md#naming-and-typing-the-nested-levels) for the full
reference.

Boundaries nest correctly through the pipeline: each level opens before
the records inside it and closes after them, in strict innermost-first
order. A level that arrives across several branches is still handled
once where a Merge or Combine brings those branches together — exactly
like a single-level document.

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

## Fixed-width document output

A fixed-width Sink can echo any extracted section as its header or footer.
The names select document context; they do not declare new input extraction
rules. For example, these Sink config keys select sections named `manifest`
and `totals`:

```yaml
reconstruct_envelope: true
options:
  envelope:
    header_from_doc: manifest
    footer_from_doc: totals
```

Both names are author-defined. With a multi-record flat-file source, both
sections must have been captured in the leading header region, even though
`totals` is rendered at the end of the output document. A structural trailer
count check does not make that trailing input record an extracted section.

Header and footer fields concatenate in stored order without the body's
fixed-width padding. Strings remain verbatim, numbers and booleans use scalar
spellings, dates use `YYYYMMDD`, datetimes use `YYYYMMDDhhmmss`, and null adds
no text. Arrays and maps are unsupported: a structured field anywhere in the
section rejects that entire header or footer before delivery. A missing
section emits nothing; a present empty section emits only the configured
LF/CRLF separator, or no bytes with `line_separator: none`.

Document start, each record, and document end are separate prepared operations.
Only successful delivery advances document state or the body count; opening
the next document starts its own count and selects its own sections. Earlier
successful operations remain delivered if a later footer fails. A computed
footer record-count field is unsupported for fixed-width (E346). See
[fixed-width output](../formats/fixed-width.md#scalar-document-headers-and-footers)
and [output preparation](../ops/storage.md#output-preparation).

## Native JSON and XML output boundaries

JSON/XML reconstructed envelopes prepare document start, each body record,
document end and finalization as separate complete operations. A document's
body count advances only when a record is delivered. Ending one document and
opening the next resets that count and restores the next document's own
sections. Section names remain the names declared in your pipeline.

Reconstructed JSON envelopes retain their document grammar: array mode wraps
the envelope documents in an array; NDJSON mode separates complete envelope
documents with LF. Unlike ordinary compact NDJSON records, reconstructed
`pretty: true` documents can span lines. XML places each `Document` wrapper inside one configured root, retaining
header/footer section wrappers and emitting no XML declaration.

Explicitly opened empty documents still carry framing and a zero body count.
In the CLI, a native source with no body records does not open a writer, so its
output is an empty file even with reconstruction enabled. Do not infer this
behavior from header-only message interchanges, whose readers emit explicit
boundaries even without a body.

Ordinary native output supports correlation, splitting and per-file fan-out.
Reconstructed envelopes combined with splitting, per-file fan-out, correlation
or document-grain DLQ are rejected before execution (E347). These combinations
cannot establish the required single document boundary.

Library writers distinguish draining already delivered bytes from finalizing
a document. A byte drain adds no closing syntax. Once delivery fails, neither a
drain, finalization nor teardown resumes the failed operation. See
[output preparation](../ops/storage.md#output-preparation) for failure effects.
