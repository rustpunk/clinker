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
let document-scoped operators — for example a future per-document
aggregate flush or trailer-count validation — fire at exactly the right
point. Today the signals propagate through Source, Transform, Route,
Sort, and Combine, and are reconciled at Merge (a document that fans in
through several branches closes downstream exactly once).
