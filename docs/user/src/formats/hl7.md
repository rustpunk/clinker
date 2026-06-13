# HL7 v2 Format

Clinker reads and writes HL7 v2.x pipe-and-hat messages alongside CSV,
JSON, XML, fixed-width, EDIFACT, and X12. An HL7 v2 file is a finite stream
of carriage-return-terminated segments. The smallest unit is one message,
which always begins with an `MSH` (message header) segment; messages may
optionally be wrapped in a `BHS..BTS` batch and an `FHS..FTS` file
envelope. The reader streams one segment at a time and the writer re-emits
the segments, optionally reconstructing the batch/file envelopes.

The optional envelope tiers surface as nested document-context levels: an
`FHS` file header becomes the file-level `$doc` document, and each `BHS`
batch and `MSH` message opens a nested level whose `$doc` sections layer
over the enclosing tiers. A body record therefore sees every enclosing
tier's fields through one `$doc.<section>.<field>` lookup. All tiers are
optional — a bare stream of `MSH` messages with no batch or file wrapping
is a valid HL7 v2 file.

## Delimiters and the MSH header

HL7 declares its delimiters in the `MSH` header rather than assuming a
fixed set. The byte immediately after the `MSH` tag is the field separator
(`MSH-1`), and the four bytes that follow are the encoding characters
(`MSH-2`), in this fixed order:

| Role                  | Source in MSH-2          | Conventional byte |
| --------------------- | ------------------------ | ----------------- |
| Component separator   | first encoding character | `^`               |
| Repetition separator  | second encoding char     | `~`               |
| Escape character      | third encoding char      | `\`               |
| Sub-component sep.    | fourth encoding char     | `&`               |

The reader reads these bytes from the header, so a message that uses the
conventional `|^~\&` or any other producer-chosen delimiters parses
correctly. The segment terminator is **always** a carriage return
(`0x0D`) — unlike the field and encoding delimiters, it is never
producer-chosen.

When a file opens with an `FHS` or `BHS` header instead of `MSH`, the
delimiters are read from that header; HL7 requires the file's `FHS`/`BHS`
encoding characters to match its messages' `MSH`.

### The MSH off-by-one

`MSH-1` *is* the field separator, so it is implicit — it never appears as a
data field. When the `MSH` segment is split on the field separator, the
encoding-characters field (`MSH-2`) is the first data field. As a result,
for any `MSH` field number N ≥ 2, the positional column is `f<N-1>`:
`MSH-2` is `f01`, the sending application `MSH-3` is `f02`, the message
type `MSH-9` is `f08`, and the message control id `MSH-10` is `f09`. The
same off-by-one applies to `FHS` and `BHS`.

### Escape sequences

Field data escapes a literal delimiter character with an escape sequence
`\X\`, where `X` names the delimiter: `\F\` field separator, `\S\`
component separator, `\T\` sub-component separator, `\R\` repetition
separator, `\E\` the escape character itself. The reader decodes these into
their literal data byte, so downstream consumers — CSV/JSON output, CXL
string predicates, `$doc` fields — see clean data, never the wire escapes.
The writer re-escapes any delimiter byte in field data on output, so the
reader → writer → reader round-trip is byte-faithful.

An application escape the positional reader does not decode (e.g. the
formatting escape `\.br\`) is kept verbatim rather than dropped, so no data
is lost.

The component, repetition, and sub-component separators inside a field
(e.g. the `^` in a composite `PATID^^^HOSP^MR`) are kept as part of the
field's text and are not split — the positional field model works above
component resolution, so a composite field round-trips unchanged.

### Newlines between segments

Some producers (and CRLF-normalizing transports) add a line feed after each
carriage-return terminator. Those bytes are insignificant and are stripped
between segments. A producer that omits the trailing carriage return on the
final segment is accepted — that shape is common in practice.

## Record shape

Each segment becomes one record under a fixed positional schema:

| Column     | Meaning                                                       |
| ---------- | ------------------------------------------------------------- |
| `seg_id`   | The segment tag (`MSH`, `PID`, `OBX`, …)                      |
| `set_ref`  | The enclosing message's control id (`MSH-10`)                |
| `set_type` | The enclosing message's type (`MSH-9`, e.g. `ADT^A01`)       |
| `f01`, `f02`, … | The segment's positional data fields                    |

The `MSH` header segment **is** emitted as a body record (its `seg_id` is
`MSH`), carrying the message's fields positionally. Batch/file envelope
segments (`FHS`, `FTS`, `BHS`, `BTS`) are consumed by the reader to drive
the document levels and validate counts — they are never emitted as body
records.

The number of `fNN` columns is controlled by the source `max_fields`
option (default 64). A segment carrying more data fields than that is
rejected with guidance rather than silently truncated. Absent trailing
fields read as `null`.

```yaml
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./inbox/*.hl7
      options:
        max_fields: 128       # widen the positional schema for large OBX segments
      schema:
        - { name: seg_id, type: string }
        - { name: set_ref, type: string }
        - { name: f01, type: string }
```

## Component splitting (optional)

By default a composite field rides inside one `fNN` column with its
component (`^`), repetition (`~`), and sub-component (`&`) separators intact
— the positional model deliberately works above component resolution. When
you want component-level access (the message code `MSH-9.1` vs the trigger
event `MSH-9.2`) without writing CXL string-splitting downstream, opt one or
more fields into splitting with `split_fields`. The reader explodes the
named field into structured columns, and an HL7 Output re-assembles the
exact wire field from them, so an HL7→HL7 round-trip stays byte-identical.

```yaml
options:
  split_fields:
    - { field: f08, components: 2 }                       # MSH-9 → message code + trigger
    - { field: f03, components: 5 }                       # PID-3 (CX) → its components
    - { field: f04, components: 2, subcomponents: 3 }     # also expose sub-components
    - { field: f13, components: 1, repetitions: 4 }       # repeating field → per-repetition
```

Each split fixes the column width on three structural axes: `components`
(required, the `^` axis), `subcomponents` (default 1, the `&` axis), and
`repetitions` (default 1, the `~` axis). The schema stays static — it never
varies with per-record data. A field whose data carries more structure on
any axis than the declaration reserves is rejected with guidance, the same
posture as a `max_fields` overflow; raise the axis count or leave the field
unsplit.

The exploded columns name the path from the field down to a leaf with the
axis letters `r`, `c`, `s`, all 1-based, eliding the default index (`1`) on
the repetition and sub-component axes so the common component-only case
stays clean:

| Declaration                                | Columns for `f08`            |
| ------------------------------------------ | ---------------------------- |
| `components: 2`                            | `f08_c1`, `f08_c2`          |
| `components: 1, subcomponents: 2`          | `f08_c1_s1`, `f08_c1_s2`    |
| `components: 1, repetitions: 2`            | `f08_r1_c1`, `f08_r2_c1`    |

The verbatim `fNN` column is replaced by the structured columns. Declare the
exploded column names in the source `schema:` block (or rely on
`on_unmapped`) the same way you would any other column:

```yaml
options:
  split_fields:
    - { field: f08, components: 2 }
schema:
  - { name: seg_id, type: string }
  - { name: f08_c1, type: string }   # MSH-9.1 message code
  - { name: f08_c2, type: string }   # MSH-9.2 trigger event
```

```cxl
emit code    = f08_c1
emit trigger = f08_c2
```

Splitting respects the escape rules: an escaped separator (e.g. `\S\`, a
literal `^` in data) is **not** treated as a component boundary — the split
runs on the raw bytes before the escape decodes, so the literal stays inside
one component. On output the writer re-joins the leaves on the separators
verbatim (never escaping `^`/`~`/`&`) and still escapes any field-separator,
escape, or carriage-return byte inside a leaf, so the round-trip is
byte-faithful.

## Envelope sections over the tiers

The file header `FHS` is extractable as a file-level document envelope
section, exposing its positional fields to CXL as `$doc.<section>.<field>`.
Use the `segment` extract rule with the field names matching the positional
keys `f01`, `f02`, … :

```yaml
envelope:
  sections:
    file:
      extract: { segment: "FHS" }
      fields:
        f07: string          # file name / id (FHS-8 under the off-by-one)
```

The `BHS` batch and the `MSH` message surface automatically as the nested
`$doc` sections `batch` and `transaction_set`, each keyed by positional
`fNN` fields — no envelope declaration is needed for them. A Transform on
any body record can read all available tiers at once:

```cxl
emit file_id  = $doc.file.f07            # FHS file id (declared section)
emit batch_id = $doc.batch.f07           # BHS batch id (auto section)
emit mtype    = $doc.transaction_set.f08 # MSH-9 message type (auto section)
emit ctrl     = $doc.transaction_set.f09 # MSH-10 control id (auto section)
```

Only the `FHS` header is extractable as a *declared* envelope section, and
only when the file actually opens with one; a bare `MSH`-led file has no
file-level envelope, so declaring an `FHS` section against it is rejected at
startup. Trailer segments (`BTS`, `FTS`) arrive after the body they close
and cannot become `$doc` fields without buffering the whole file — their
counts are instead validated inline by the reader (see below). A `segment`
extract naming any tag other than `FHS`, or an `xml_path` / `json_pointer`
extract against an HL7 source, is rejected at startup.

## Control-count validation

The reader validates the structural integrity claims carried in the
batch/file trailers as they arrive, failing the run on a mismatch (a
truncation or corruption signal):

- **`BTS` batch message count (`BTS-1`)** — must equal the number of `MSH`
  messages in the batch. An empty `BTS-1` disables the check (the count is
  optional in practice).
- **`FTS` file batch count (`FTS-1`)** — must equal the number of `BHS`
  batches in the file. An empty `FTS-1` disables the check.

Content after the `FTS` file trailer is rejected. A bare `MSH`-led file
needs no trailers and validates with no count checks.

### Routing a count mismatch to the DLQ

By default a `BTS`/`FTS` **count** mismatch aborts the run. A source declaring
`dlq_granularity: document` instead dead-letters the whole file to the DLQ —
the file's records become a `structural_validation` trigger plus
`document_rejected` collaterals, and no record of the malformed file reaches
the sink. The count is only known at the trailer, after the body has streamed,
so the rejection lands at the sink boundary (no record is written out), not
literally before the first record. The grain is the whole file. Other
corruption (truncation, post-trailer content) always aborts, even under the
opt-in. An empty `BTS-1`/`FTS-1` still disables the check entirely. See
[Malformed envelopes](../pipelines/error-handling.md#malformed-envelopes-structural-validation).

## Writing HL7

An HL7 Output node re-emits the `MSH` and body segments from the record
stream, escaping any field data that carries a delimiter byte. Records map
by the same positional columns (`seg_id`, `fNN`); trailing `null`/empty
fields are trimmed so no fabricated delimiters appear, and a column the
writer does not recognize is an error (project the record to the HL7
columns first). Split-leaf columns (`f08_c1`, `f03_r2_c1_s3`) are recognized
too — the writer groups them by field and re-assembles the wire value from
the column names alone, so no output option is needed to round-trip a split
source. The reader-stamped `set_ref`/`set_type` echoes and engine-internal
`$`-namespaced columns are excluded automatically.

```yaml
nodes:
  - type: output
    name: out
    input: messages
    config:
      name: out
      type: hl7
      path: ./out/result.hl7
      options:
        file_header: ["^~\\&", "LAB", "HOSP", "EHR", "HOSP", "20240102", "FILE7"]
        batch_header: ["^~\\&", "LAB", "HOSP", "EHR", "HOSP", "20240102", "BATCH3"]
        segment_newline: true
```

Output options:

| Option                  | Meaning                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| `file_header`           | Literal `FHS` fields; opens an `FHS..FTS` file envelope.               |
| `file_header_from_doc`  | Name of a `$doc` section to echo the `FHS` fields from (round-trip).   |
| `batch_header`          | Literal `BHS` fields; wraps the messages in a `BHS..BTS` batch.        |
| `segment_newline`       | Write a newline after each segment terminator (default `true`).        |

When a file or batch header is configured, the writer recomputes the `BTS`
batch message count and the `FTS` file batch count and emits the trailers
at end of stream, so the output passes its own count validation on re-read.
The `MSH-2` encoding-characters field of each header is written verbatim so
the delimiter declaration round-trips. With no header options set, the
writer emits a bare stream of messages with no batch/file envelope.

`file_header_from_doc` echoes the `FHS` header from a record's document
context. That context is populated by a source's `FHS` envelope section
(declare a `segment: "FHS"` envelope section on the source) and travels
with every body record through the pipeline. The reader stashes the
complete, ordered `FHS` field list, so the reconstructed header is
faithful. Supply `file_header` literal fields instead when the records have
no source `FHS` section to echo.

## Limitations

- **Charset.** Field text is decoded as UTF-8. Non-UTF-8 messages are
  rejected explicitly rather than silently corrupted.
- **Positional fields by default; opt-in component columns.** Components,
  repetitions, and sub-components ride inside one positional `fNN` field
  verbatim unless that field is named in `split_fields` (see *Component
  splitting* above), in which case the reader explodes it into structured
  columns and the writer re-assembles them. The reader always decodes the
  `\X\` delimiter escapes regardless.
- **HL7 v3 and FHIR are out of scope.** HL7 v3 is XML (use the `xml`
  format) and FHIR is JSON/REST (use the `json` format); this format
  handles HL7 v2.x pipe-and-hat encoding only.
- **Output splitting.** A batch/file envelope is a single `FHS..FTS`
  structure and cannot be divided across files. An `hl7` output combined
  with a `split:` block is rejected at config-validation time (diagnostic
  `E339`) rather than emitting a structurally corrupt file.
