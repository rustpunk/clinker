# Error Handling & DLQ

Clinker provides structured error handling with a dead-letter queue (DLQ) for records that fail processing. The `error_handling:` block at the top level of the pipeline YAML controls the behavior.

## Configuration

```yaml
error_handling:
  strategy: continue
  dlq:
    path: "./output/errors.csv"
    include_reason: true
    include_source_row: true
```

## Strategies

The `strategy:` field controls what happens when a record fails:

| Strategy | Behavior |
|----------|----------|
| `fail_fast` | **Default.** Stop the pipeline on the first error. |
| `continue` | Route bad records to the DLQ and keep processing good records. |
| `best_effort` | Continue processing with partial results, even if some stages produce incomplete output. |

### fail_fast

The safest strategy. Any record-level error (type coercion failure, validation error, missing required field) halts the pipeline immediately. Use this when data quality is critical and you prefer to fix issues before reprocessing.

### continue

The production workhorse. Bad records are written to the DLQ file with diagnostic metadata, and the pipeline continues processing remaining records. After the run completes, inspect the DLQ to understand and correct failures.

A pipeline that completes with DLQ entries exits with **code 2** -- this signals "pipeline completed successfully but some records were rejected." It is not a crash or internal error.

### best_effort

The most lenient strategy. Processing continues even with partial results. Use this for exploratory data analysis where completeness is less important than progress.

## DLQ configuration

The DLQ is always written as CSV, regardless of the pipeline's input/output formats.

```yaml
  dlq:
    path: "./output/errors.csv"
    include_reason: true
    include_source_row: true
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `path` | No | -- | File path for DLQ output. If omitted, DLQ records are logged but not written to file. |
| `include_reason` | No | -- | Include `_cxl_dlq_error_category` and `_cxl_dlq_error_detail` columns. |
| `include_source_row` | No | -- | Include original source fields alongside DLQ metadata. |

## DLQ columns

Every DLQ record includes these metadata columns:

| Column | Description |
|--------|-------------|
| `_cxl_dlq_id` | UUID v7 (time-ordered unique identifier) |
| `_cxl_dlq_timestamp` | RFC 3339 timestamp of when the error occurred |
| `_cxl_dlq_source_file` | Input filename that produced the failing record |
| `_cxl_dlq_source_row` | 1-based row number in the source file |
| `_cxl_dlq_stage` | Name of the transform or aggregate node where the error occurred |
| `_cxl_dlq_route` | Route branch name (if the error occurred after routing) |
| `_cxl_dlq_trigger` | Validation rule name that triggered the rejection |

When `include_reason: true` is set, two additional columns appear:

| Column | Description |
|--------|-------------|
| `_cxl_dlq_error_category` | Machine-readable error classification |
| `_cxl_dlq_error_detail` | Human-readable error description |

## Error categories

The `_cxl_dlq_error_category` column contains one of these values:

| Category | Description |
|----------|-------------|
| `missing_required_field` | A required field is absent from the record |
| `type_coercion_failure` | A value could not be converted to the expected type |
| `required_field_conversion_failure` | A required field exists but its value cannot be converted |
| `nan_in_output_field` | A computation produced NaN |
| `aggregate_type_error` | An aggregate function received an incompatible type |
| `validation_failure` | A declarative validation check failed |
| `aggregate_finalize` | An aggregate function failed during finalization |
| `correlated` | A non-failing record was DLQ'd as collateral because another record in its correlation group failed |
| `group_size_exceeded` | A correlation-key group exceeded the configured `max_group_buffer` limit |
| `document_rejected` | A non-failing record was DLQ'd as collateral because another record in its document failed under a source's `dlq_granularity: document` policy |
| `late_record` | A record arrived at a time-windowed aggregate after its event-time window had already closed |
| `expansion_limit_exceeded` | A transform's `emit each` fan-out produced more output records than its `max_expansion` ceiling allows |
| `combine_output_row` | A Combine output-stage eval failed for one driver row (probe-key, residual, or matched / `on_miss: null_fields` body); the entry carries the contributing-build lineage and rewinds both the driver and matched build source's rollback cursor. Routed to the DLQ under `continue` / `best_effort` across every Combine join mode; `fail_fast` propagates the eval error |
| `structural_validation` | An envelope trailer's declared count did not match the body the reader streamed (X12 `SE`/`GE`/`IEA`, EDIFACT `UNT`/`UNZ`, HL7 `BTS`/`FTS`). The root-cause `trigger: true` entry for a malformed envelope rejected under a source's `dlq_granularity: document` policy; every already-streamed record of the same file is a `document_rejected` collateral |

## Advanced options

### Type error threshold

Abort the pipeline if the fraction of failing records exceeds a threshold:

```yaml
  type_error_threshold: 0.05    # Abort if >5% of records fail
```

This acts as a circuit breaker -- if your input data is unexpectedly corrupt, the pipeline stops early rather than filling the DLQ with millions of entries.

### Correlation key

Group DLQ rejections by a key field. When any record in a correlation group fails, **records from the failing source's contribution to that group** are routed to the DLQ:

```yaml
  correlation_key: order_id
```

For compound keys:

```yaml
  correlation_key: [order_id, customer_id]
```

This is useful for transactional data where partial processing of a group is worse than rejecting the entire group. For example, if one line item in an order fails validation, you may want to reject the entire order.

Under multi-source ingest, the collateral fan-out narrows to the failing source: a `src_b` trigger does NOT DLQ records from `src_a` that share the same correlation key. Single-source pipelines see bit-identical behavior to today's pipeline-wide collateral DLQ. See [Per-source rollback narrowing](correlation-keys.md#per-source-rollback-narrowing) for the full semantic and the two documented exceptions (`max_group_buffer` overflow and Combine output failures).

For the full lifecycle and per-operator semantics (route, merge, aggregate, combine), see [Correlation Keys](correlation-keys.md).

### Max group buffer

Limit the number of records buffered per correlation group:

```yaml
  max_group_buffer: 100000     # Default: 100,000
```

Groups exceeding this limit are DLQ'd entirely with a `group_size_exceeded` summary entry.

### Document-level DLQ

By default a record failure dead-letters only that record (`dlq_granularity: record`). A source can instead reject the **entire document** any record of which fails, by declaring the granularity per source:

```yaml
nodes:
  - type: source
    name: claims
    config:
      name: claims
      type: x12
      glob: ./claims/*.edi
      dlq_granularity: document   # record (default) | document
```

Under `dlq_granularity: document` and the `continue` / `best_effort` strategies, when any record of a document fails:

- the failing record becomes the **root-cause** DLQ entry (`_cxl_dlq_trigger = true`, carrying its original error category);
- every other record of the same document becomes a **collateral** entry (`_cxl_dlq_trigger = false`, category `document_rejected`);
- **no** record of that document reaches the success sink.

Clean documents in the same run stream through untouched, and records from sibling sources still on the default `record` granularity keep per-record semantics — the policy is per source.

This is the document-shaped analogue of [correlation keys](#correlation-key): use it when partial processing of a document (an EDI interchange, a batch file with a header/trailer) is worse than rejecting the whole document. Unlike correlation keys, which group across files by a key value, document-level DLQ scopes rejection to a single document's records.

**Document grain.** The document is the **outermost** level — the source file. For a flat format (CSV, JSON, plain XML) each input file is one document. For a nested-envelope format (an X12 `ISA → GS → ST` interchange, an EDIFACT `UNB → UNG → UNH`) the document is the whole **interchange / file**, not an inner functional group or transaction set: a failure anywhere in the interchange rejects the entire interchange, including the transaction sets that validated cleanly. Reject the inner-level grain instead by partitioning the input so each interchange is its own file is not currently offered — the grain is fixed at the file.

**DLQ rate.** Every emitted entry — the trigger and each collateral — counts toward the DLQ-rate denominator the [type error threshold](#type-error-threshold) circuit breaker measures, matching the correlated-collateral precedent. A rejected 1000-record document contributes 1000 entries, so size any `type_error_threshold` with whole-document rejection in mind.

**Memory.** The engine buffers each open document's records until its boundary, then flushes the document clean to the sink or rejects it and drops the buffer. Peak memory scales with the **concurrently-open** documents, not the total input; a single very large document spills its buffer to disk under the run's memory budget rather than holding everything in RAM. See [Streaming vs blocking](../ops/streaming-vs-blocking.md) for the spill model.

**Output restriction.** Document-level DLQ flushes each whole document to a single output writer, so it cannot be combined with a [per-source-file output](../nodes/output.md) (a `{source_file}` / `{source_path}` path template over a multi-file source). The two are rejected together at compile time (E343); use a single output path, or set `dlq_granularity: record` if per-file output is the requirement.

**Strategy requirement.** `dlq_granularity: document` requires `error_handling.strategy: continue` (or `best_effort`). It is incompatible with the default `fail_fast`: document-level dead-lettering keeps the run going past a bad document, which contradicts fail-fast's abort-on-first-error. The combination is rejected at compile time (E344) — set `strategy: continue` to dead-letter bad documents, or keep `fail_fast` with the default `dlq_granularity: record`.

**Spilling stages.** Document identity survives memory pressure end to end. The per-document buffer identifies each document before buffering and spills under the memory budget, and a blocking stage (Sort, hash Aggregate, grace-hash Combine) between the source and the output preserves each record's document context — including the source file the grain keys on — across its own spill round-trip. A document whose records pass through a spilling stage is therefore still grouped and rejected as one document under memory pressure, exactly as it would be in memory.

### Malformed envelopes (structural validation)

Envelope formats carry their own structural-integrity claims: an X12 interchange declares a segment count in each `SE`/`GE`/`IEA` trailer, EDIFACT in each `UNT`/`UNZ`, HL7 batch/file in each `BTS`/`FTS`. When the declared count does not match the body the reader actually streamed, the file is structurally invalid.

Under `dlq_granularity: document`, such a count mismatch dead-letters the **whole source file** to the DLQ rather than aborting the run:

- the file's records dead-letter as one `structural_validation` root-cause entry (`_cxl_dlq_trigger = true`) plus a `document_rejected` collateral for every other already-streamed record of the file;
- **no** record of the malformed file reaches the success sink.

```yaml
nodes:
  - type: source
    name: claims
    config:
      name: claims
      type: x12
      glob: ./claims/*.edi
      dlq_granularity: document   # reuse the document opt-in — no separate config
```

The opt-in is the same `dlq_granularity: document` knob that governs per-record document rejection above; there is no separate `validation:` block. A malformed envelope is simply one more reason a source under the `document` policy condemns a whole document.

**Honest timing — rejected at the sink boundary, not before the first record.** The trailer that carries the count arrives at the *end* of the file, after every body record it counts has already streamed through the DAG. Clinker is a bounded-memory streaming engine — it does not buffer the whole file up front to pre-validate it (that would defeat the streaming model). So the count mismatch is detected mid-stream, the file is marked failed, and the document-level DLQ buffer rejects every already-streamed record of the file at its close. The user-visible outcome is the same — **no record of a malformed envelope is ever written to the output** — but the rejection lands at the sink boundary, not literally before the file's first record streams.

**Grain — the whole file.** An `SE`-level mismatch (one transaction set inside a larger interchange) rejects the **entire interchange / file**, not just that one transaction set, because the document grain is the outermost source file (see [Document grain](#document-level-dlq) above). Split the input so each interchange is its own file if you need finer rejection.

**Multiple files keep flowing.** When a `glob` / `paths` source matches several files and one is malformed, only that file dead-letters — ingestion continues to the remaining files, so the clean files after a bad one still reach the sink. (This is unlike the default `record` granularity, where a count mismatch aborts the whole run and no file's records are written.) Dead-lettering one malformed file never silently drops the good files around it.

**Default behavior unchanged.** Under the default `dlq_granularity: record`, a structural count mismatch still **aborts the run** exactly as before — the document-DLQ disposition is strictly opt-in. Genuine corruption that is *not* a count mismatch (a truncated stream, a bad delimiter, a control-number echo mismatch, a segment after the interchange trailer) **always aborts**, even under the `document` opt-in: only the trailer-count claims are reclassified to the DLQ; structural corruption that makes the stream un-parseable is never silently dead-lettered.

> **Cryptographic integrity (checksums / signatures) is not yet validated.** Envelope formats can also carry a SHA-256 body hash, a JWS-signed JSON payload, or an XML Signature. Clinker extracts these envelope sections but does not yet verify them. Tracked for a future release.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Pipeline completed successfully, no errors |
| 1 | Pipeline failed (internal error, config error, or `fail_fast` triggered) |
| 2 | Pipeline completed, but DLQ entries were produced |

Exit code 2 is not a failure -- it means the pipeline ran to completion and handled errors according to the configured strategy. Check the DLQ file for details.

## Complete example

```yaml
pipeline:
  name: order_processing
  memory: { limit: "512M" }

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: "./data/orders.csv"
      schema:
        - { name: order_id, type: int }
        - { name: customer_id, type: int }
        - { name: amount, type: float }
        - { name: email, type: string }

  - type: transform
    name: validate_orders
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit customer_id = customer_id
        emit amount = amount
        emit email = email
      validations:
        - field: email
          check: "not_empty"
          severity: error
          message: "Customer email is required"
        - check: "amount > 0"
          severity: error
          message: "Order amount must be positive"

  - type: output
    name: valid_orders
    input: validate_orders
    config:
      name: valid_orders
      type: csv
      path: "./output/valid_orders.csv"

error_handling:
  strategy: continue
  dlq:
    path: "./output/rejected_orders.csv"
    include_reason: true
    include_source_row: true
  type_error_threshold: 0.10
  correlation_key: order_id
```
