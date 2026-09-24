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

`error_handling.strategy` is pipeline-wide -- it is set once at the top level, not per node. It controls what happens when a record fails:

| Strategy | Behavior | Exit code |
|----------|----------|-----------|
| `fail_fast` | **Default.** Abort the run on the first record failure. | Non-zero, by the class of the aborting error (`3` for an evaluation failure, `4` for an I/O failure -- see [Exit Codes](../ops/exit-codes.md)) |
| `continue` | Route the failing record to the DLQ and keep processing. | `2` if any record was dead-lettered, `0` otherwise |

There are exactly two, because the engine makes exactly one decision at each record failure: propagate it and stop, or dead-letter it and carry on.

### fail_fast

The safest strategy. Any record-level error (type coercion failure, validation error, missing required field) halts the pipeline immediately, with a non-zero exit and no DLQ file. Use this when data quality is critical and you prefer to fix issues before reprocessing.

Some failures abort the run under **either** strategy, because they are not record-scoped: an unwritable output path, a config or CXL compile error, and the DLQ-rate ceiling ([`dlq.max_rate`](#bounding-how-much-can-dead-letter), E315/E316) all end the run regardless of the strategy.

CSV, JSON and XML resource failures are also fatal under either strategy. Memory or disk
admission refusal, allocation failure, descriptor exhaustion and temporary-storage
failure are not bad-record errors, so `continue` cannot turn them into successful
output. A typed resource diagnostic preserves the kind of failure rather than
reporting every case as a memory shortage. A failed destination can already have
accepted a prefix; see [output preparation](../ops/storage.md#output-preparation).

Malformed JSON/XML input encoding is a data failure, including when discovered
during schema discovery or envelope pre-scan. Under `fail_fast`, the CLI returns
exit `4` and machine code `source.data.invalid`. It does not report a compilation
error merely because no record has reached the pipeline. A late error can leave
an already delivered prefix; an envelope pre-scan may discover it before any
body records. Failed runs do not publish their staged normal output files.

Explicit cancellation ends an interrupted run with exit `130`; it does not add a
Sink error. If a real I/O or resource failure occurs alongside a shutdown request,
the real failure retains its classification. Record and byte counters describe
established progress, not rows merely attempted or prepared.

An executor invariant failure also aborts under either strategy with exit code
`1`. In particular, if a planned materialized input is unavailable when its
consumer runs, Clinker stops instead of treating that input as a legitimate
zero-row result. The message names the consuming node and planned producer
(including the producer port when applicable) and says the input was not
treated as empty. A source or stage that really emits zero rows remains valid;
it carries an explicit empty buffer and completes normally. Report any missing-
input internal error as an engine defect rather than routing it to the DLQ.

### continue

The production workhorse. Bad records are written to the DLQ file with diagnostic metadata, and the pipeline continues processing remaining records. After the run completes, inspect the DLQ to understand and correct failures.

A pipeline that completes with DLQ entries exits with **code 2** -- this signals "pipeline completed successfully but some records were rejected." It is not a crash or internal error. A `continue` run that dead-letters nothing exits `0`, exactly like a clean `fail_fast` run.

> **Migrating from `best_effort`.** The removed `best_effort` spelling was a third name for the `continue` behavior: it wrote the same DLQ entries and produced the same exit code, because the runtime never distinguished the two. Replace it with `strategy: continue`. A pipeline still carrying `best_effort` is rejected at config-validation time with a message naming the replacement.

Declared source-type failures are deliberately not lossy: under `continue`,
the complete original record is written to the
configured DLQ and no null, raw, or partially converted replacement enters the
pipeline. This strategy therefore requires an `error_handling.dlq` block when
such a failure occurs. `fail_fast` stops on the first failure without emitting a
replacement. This includes fields renamed by a source schema: rejection retains
the original decoded record and its values, even when conversion failed after
other fields had already been examined.

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
| `path` | No | -- | The pipeline-wide DLQ file. It receives the dead letters of every Source without its own [`per_source`](#per-source-dlq-settings) path. A dead letter with neither this `path` nor a `per_source` path for its Source is counted in the run's dead-letter totals, sets exit code 2 and counts toward `max_rate`, but is written nowhere. The `dlq:` block itself is required to continue past a declared source-type failure. |
| `include_reason` | No | `true` | Include `_cxl_dlq_error_category` and `_cxl_dlq_error_detail` columns. |
| `include_source_row` | No | `true` | Include the failing record's columns after the `_cxl_dlq_*` columns. Which record columns each DLQ file carries is fixed when the pipeline compiles; see [How the DLQ columns are chosen](#how-the-dlq-columns-are-chosen). With `false`, only the `_cxl_dlq_*` columns are written. |
| `max_rate` | No | none | Stop the run (E315, exit code 3) once the dead-lettered rows reach this fraction of the source rows read so far, both counted across the whole run. Must be greater than `0.0` and at most `1.0` (E318). Without it, the run is never stopped for its dead-letter rate. See [Bounding how much can dead-letter](#bounding-how-much-can-dead-letter). |
| `min_records` | No | `100` | How many source rows must have been read before `max_rate` is checked, so the first failures of a run cannot trip it on a tiny denominator. Also the default for each `per_source` `min_records`. |
| `per_source` | No | -- | Settings for individual Sources, keyed by Source node name: a separate DLQ file, and a rate ceiling of their own. See [Per-source DLQ settings](#per-source-dlq-settings). |

### Per-source DLQ settings

`per_source` gives a Source its own DLQ file, its own rate ceiling, or both.
Each key is the name of a Source node:

```yaml
error_handling:
  strategy: continue
  dlq:
    path: ./output/errors.csv
    max_rate: 0.05
    per_source:
      vendor_feed:
        path: ./output/vendor_feed_errors.csv
        max_rate: 0.20
        min_records: 500
      orders:
        max_rate: 0.01
```

| Field | Default | Description |
|-------|---------|-------------|
| `path` | -- | A separate DLQ file for this Source's dead letters. They are written only there and do not appear in the pipeline-wide file. Without it, the Source's dead letters go to the pipeline-wide `path`. |
| `max_rate` | none | Stop the run (E316, exit code 3) once this Source's dead-lettered rows reach this fraction of the rows read from this Source so far. Must be greater than `0.0` and at most `1.0` (E318). |
| `min_records` | the pipeline-wide `min_records`, else `100` | How many rows must have been read from this Source before its `max_rate` is checked. |

A Source's own `max_rate` is checked first, so a breach names that Source.
The pipeline-wide `max_rate`, when set, still applies to the run as a whole.
In the example, `vendor_feed` may dead-letter up to 20% of its own rows, but
the run still stops when all dead letters together reach 5% of all rows read.

A key that does not name a declared Source is rejected at compile time
(E317). Two DLQ paths that name one file are rejected too (E318), including
paths that differ only in case on a case-insensitive filesystem, or in being
written relatively and absolutely. A DLQ path that names the same file as a
Sink's path is rejected with E322.

Which record columns each file carries follows from the Sources routed to it;
see [How the DLQ columns are chosen](#how-the-dlq-columns-are-chosen).

### How DLQ output is written

Dead-letter rows are written while the run executes, not collected until it
ends. Each row is formatted under its file's header, which is fixed when the
pipeline compiles (see
[How the DLQ columns are chosen](#how-the-dlq-columns-are-chosen)), and written into
a staged copy of that file in the run's publication attempt: in quarantine
next to the destination by default, or under `local_spool_dir` with
`mode = "local_then_publish"` (see
[Output publication](../ops/storage.md#output-publication-and-retained-attempts)).
Each open DLQ file writes through one fixed 64 KiB buffer, so the memory the
DLQ files use does not grow with the number of failures.

- A DLQ file is created when its first row arrives. A file no row reaches is
  not created, and no empty file is published.
- DLQ files are published only if the run succeeds, by the same publication
  step as the pipeline's other outputs. A failed or interrupted run publishes
  no DLQ file.
- The failures that are counted but have no destination (see `path` above)
  are never formatted or written.
- Three kinds of dead letter are held in memory until the stage that found
  them finishes, and written then:
  - a `join_values` collision at a Sink that writes on its own thread;
  - an Aggregate `add_record` failure found while the Aggregate reads its
    input on its own thread;
  - a Combine output-row failure found while the Combine streams its driver
    on its own thread, or inside a grace-hash, sort-merge or IEJoin join.

  A Sink that writes on its own thread stops the run with an internal error
  once 65,536 collisions are waiting this way.
- Under a [correlation key](#correlation-key) or
  [`dlq_granularity: document`](#document-level-dlq), records are held until
  their group or document is decided. That is those features' own state,
  described in their sections, not DLQ output; the rows they dead-letter are
  then written like any other.

Disk bounds how much DLQ output a run can produce: the free space at the
staging location, and the publication attempt's byte ceiling
(`storage.publication.max_attempt_bytes`, and no more than
`retained_byte_limit`), which every staged file of the run counts toward,
DLQ files included. An attempt larger than that ceiling is refused at
publication and nothing is published.

When the staging location fills, the run fails with an I/O error (exit code
4) and publishes nothing. After the destination's own error text, the message
names the DLQ file, the number of rows dead-lettered so far, and the stage
and category with the most rows, and suggests a breaker:

```text
<destination error>: dead-letter output ./output/errors.csv could not be written after 1048576 dead-lettered rows (most from stage transform:validate_orders, category validation_failure)

help: stop the run before dead letters fill the destination, for example:

  error_handling:
    type_error_threshold: 0.05
    dlq:
      max_rate: 0.05

These breakers bound how many rows can dead-letter; disk at the staging destination bounds their volume.
```

### Bounding how much can dead-letter

Disk bounds the volume of dead-letter output; the breakers bound how many rows
can dead-letter in the first place. Set one wherever a wrong schema could make
most rows fail, for example when a feed can change its columns or types
without notice:

```yaml
error_handling:
  strategy: continue
  type_error_threshold: 0.05
  dlq:
    path: ./output/errors.csv
    max_rate: 0.05
```

- `dlq.max_rate` (E315) and `dlq.per_source.<name>.max_rate` (E316) stop the
  run when the fraction of dead-lettered rows crosses the ceiling, once
  `min_records` rows have been read. Every dead-lettered row counts,
  collateral rows included, whether or not it has a DLQ file to go to.
- [`type_error_threshold`](#type-error-threshold) (E368) stops the run when
  the fraction of declared source-type failures crosses the threshold. It
  catches a schema mismatch at the Source, before the failing rows reach
  later stages.

A rate ceiling is checked each time a dead letter is counted, so the row that
crosses it is itself counted and written before the run stops. A stopped run
exits with code 3 and publishes nothing. No breaker is set by default.

## DLQ columns

Every DLQ record includes these metadata columns:

| Column | Description |
|--------|-------------|
| `_cxl_dlq_id` | UUID v7 (time-ordered unique identifier), unique to the row. It is taken together with `_cxl_dlq_timestamp`, so ids order the same way as timestamps. |
| `_cxl_dlq_trigger_id` | The `_cxl_dlq_id` of the trigger row whose failure produced this row. A trigger row points to itself, so `_cxl_dlq_trigger` is `true` exactly when this value equals `_cxl_dlq_id`; every row one failure produced carries the same value. See [Pairing the rows one failure produced](#pairing-the-rows-one-failure-produced). |
| `_cxl_dlq_timestamp` | RFC 3339 timestamp of when the failure was observed, not of when the row was written. A collateral row (`correlated`, `document_rejected`) and every row of a `group_size_exceeded` group carry the time their correlation group or document was condemned. |
| `_cxl_dlq_source_file` | Input filename carried by that failing record's `$source.file` provenance (or `<merged>` when no source-file provenance exists) |
| `_cxl_dlq_source_name` | Name of the Source the failing record came from (or `<merged>` when the record carries no Source identity) |
| `_cxl_dlq_source_row` | 1-based row number in the source file |
| `_cxl_dlq_triggering_field` | The field whose evaluation failed, when the failure names one; empty for collateral rejections |
| `_cxl_dlq_triggering_value` | The value the failure reported, when it carries one (for example the text that failed to convert) |
| `_cxl_dlq_stage` | Name of the transform or aggregate node where the error occurred |
| `_cxl_dlq_route` | Route branch name (if the error occurred after routing) |
| `_cxl_dlq_trigger` | `true` when the row's own failure dead-lettered it; `false` when another row's failure took it along (a `correlated` or `document_rejected` row, or a Combine build row) |
| `_cxl_dlq_source_record` | One of the record columns rather than a metadata column: present in any file a Source rejection can reach under `strategy: continue`, and filled only for a record-grained E345 rejection. Contains the fixed-width line text or a JSON array of decoded CSV cells, preserving the physical row without assigning it a declared record shape. |

Timestamps need not increase down a file. A failure can be held before its
row is written, for example in a correlation group that commits later, or by
a stage listed under [How DLQ output is written](#how-dlq-output-is-written),
so its row can follow rows observed after it. Sort on `_cxl_dlq_timestamp` or
`_cxl_dlq_id` to read failures in the order they were observed.

When `include_reason: true` is set, two additional columns appear:

| Column | Description |
|--------|-------------|
| `_cxl_dlq_error_category` | Machine-readable error classification |
| `_cxl_dlq_error_detail` | Human-readable error description |

### Pairing the rows one failure produced

One failure can dead-letter several rows:

- a Combine body that fails writes the driver row and its matched build row,
  each under its own Source's `_cxl_dlq_source_name` and `_cxl_dlq_source_row`,
  whichever join strategy ran;
- a failing row in a correlation group takes the rest of its group with it as
  `correlated` rows;
- a group larger than `max_group_buffer` writes a `group_size_exceeded` row
  and its group as `correlated` rows;
- under `dlq_granularity: document`, a failing record rejects the rest of its
  document as `document_rejected` rows.

Every row carries `_cxl_dlq_trigger_id`, the `_cxl_dlq_id` of the trigger row
whose failure produced it. A trigger row points to itself, so `_cxl_dlq_trigger`
is `true` exactly when `_cxl_dlq_trigger_id` equals `_cxl_dlq_id`. Group by
`_cxl_dlq_trigger_id` to see everything one failure took with it.
In this excerpt (other columns omitted), the first two rows are a Combine
driver row and its build row, and the last two are a correlation trigger and
one of its collaterals:

```csv
_cxl_dlq_id,_cxl_dlq_trigger_id,_cxl_dlq_source_name,_cxl_dlq_error_category,_cxl_dlq_trigger
01928f3a-6c10-7b21-8a4e-3f1c2d9e0a01,01928f3a-6c10-7b21-8a4e-3f1c2d9e0a01,orders,combine_output_row,true
01928f3a-6c10-7b22-9f07-51e6a8b4c302,01928f3a-6c10-7b21-8a4e-3f1c2d9e0a01,rates,combine_output_row,false
01928f3a-6c14-7c03-b2d8-0a9e7f615203,01928f3a-6c14-7c03-b2d8-0a9e7f615203,employees,type_coercion_failure,true
01928f3a-6c19-7d40-8c11-6e2b90d3f404,01928f3a-6c14-7c03-b2d8-0a9e7f615203,employees,correlated,false
```

A trigger whose failure wrote nothing else carries its own id and shares it
with no other row. Every row has its own `_cxl_dlq_id`, so the value only
repeats across the rows of one failure.

When a correlation group holds several failing rows, each failing row is a
trigger and keeps its own id as its trigger id. The group's `correlated` rows
carry the trigger id of the group's first failing row, the one whose error
their `_cxl_dlq_error_detail` quotes. A rejected document has one trigger, its
first failing record; its other records, including any that failed after it,
carry that trigger's id.

The rows of one failure can land in different DLQ files: with
`per_source` paths, a Combine build row goes to its own Source's file while
its driver row goes to the driver's.

### How the DLQ columns are chosen

Each DLQ file's header is fixed when the pipeline compiles, before any record
is read. It does not depend on which records failed, or on which stages they
failed in: every time a pipeline writes a given DLQ file, that file has the
same columns in the same order.

A header starts with the `_cxl_dlq_*` metadata columns, always in this order:
`_cxl_dlq_id`, `_cxl_dlq_trigger_id`, `_cxl_dlq_timestamp`, `_cxl_dlq_source_file`,
`_cxl_dlq_source_name`, `_cxl_dlq_source_row`, `_cxl_dlq_triggering_field`,
`_cxl_dlq_triggering_value`, then `_cxl_dlq_error_category` and
`_cxl_dlq_error_detail` when `include_reason` is on, then `_cxl_dlq_stage`,
`_cxl_dlq_route` and `_cxl_dlq_trigger`.

With `include_source_row` on, the record columns follow. They come from every
record shape that can reach that file:

- the declared columns of each Source, and `_cxl_dlq_source_record`, when
  Source rejections are dead-lettered (`strategy: continue`);
- the shape of the records entering each Transform, Route, Reshape, Aggregate,
  Combine and Sink that can dead-letter a record under the pipeline's
  strategy (stages inside a composition count at the composition's place in
  the pipeline);
- the output columns of an Aggregate without `group_by` under
  `strategy: continue`, which go to the pipeline-wide file.

A shape is added to the file of each Source whose records can carry it: that
Source's `per_source.<name>.path` when it has one, otherwise the pipeline-wide
`path`. A shape that carries no Source identity, such as the output of a
Combine using `match: first` or `match: all`, is added to the pipeline-wide
file.

The shapes then combine as follows:

- **One shape** keeps its natural column order.
- **Several shapes** give their first-seen union in plan order: each column
  appears once, at the position where it was first seen. Plan order is the
  node order `--explain` prints, which need not match the order the nodes are
  written in the YAML.
- **A column a record does not carry** is an empty cell in that record's row.
- **Engine-only sidecar columns** (`$widened` and the `$source.*` stamps) are
  never written. Correlation-key columns (`$ck.*`) are.

For example, this pipeline has two Sources with different columns, one
pipeline-wide DLQ file, and a Transform on each Source that can fail:

```yaml
error_handling:
  strategy: continue
  dlq:
    path: rejects.csv
nodes:
- type: source
  name: orders
  config:
    schema:
      - { name: order_id, type: int }
      - { name: amount, type: int }
    # ...
- type: source
  name: refunds
  config:
    schema:
      - { name: refund_id, type: int }
      - { name: order_id, type: int }
      - { name: amount, type: int }
    # ...
# one Transform on each Source, then a Sink on each Transform
```

In this plan `refunds` comes before `orders`, so the record columns of
`rejects.csv` are:

```text
refund_id,order_id,amount,_cxl_dlq_source_record
```

An `orders` row writes an empty `refund_id` cell. `order_id` and `amount`
appear once, although both Sources declare them. `_cxl_dlq_source_record` is
in the header because a Source rejection can reach the file under `continue`.
It is empty for these rows.

To see every DLQ file's columns before a run, use
`clinker run pipeline.yaml --explain`: its `=== Dead-Letter Output ===`
section lists each file, the Sources routed to it and its full header (see
[Explain Plans](../ops/explain.md#dead-letter-output)).

Source-file provenance (`_cxl_dlq_source_file`) is read from each record, so
one file's path is never reused for a later row.

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
| `expansion_limit_exceeded` | Per-input fan-out exceeded its authored ceiling. Transform `max_expansion` rejects before body rows emit; Source `max_output_rows_per_input` emits exactly its ceiling, then DLQs the original input on the first attempted row above it. Neither is silent truncation. |
| `combine_output_row` | A Combine output-stage eval failed for one driver row (probe-key, residual, or matched / `on_miss: null_fields` body); the entry carries the contributing-build lineage and rewinds both the driver and matched build source's rollback cursor. The driver row and the matched build row each report their own Source in `_cxl_dlq_source_name` and their own row in `_cxl_dlq_source_row`, whichever join strategy ran. Routed to the DLQ under `continue` across every Combine join mode; `fail_fast` propagates the eval error |
| `structural_validation` | A structural source rule failed: an envelope trailer's declared count did not match its streamed body, a multi-record body appeared after its closing trailer, or a record type discriminator was unknown. Under `dlq_granularity: document`, the root cause has `trigger: true` and every already-streamed record of that file is `document_rejected` collateral. Under record-grained `continue`, E345 instead emits only the unknown row with `_cxl_dlq_source_record`. |

## Advanced options

### Type error threshold

Abort the pipeline if the fraction of declared source-type failures exceeds a threshold:

```yaml
  type_error_threshold: 0.05    # Abort if >5% of records fail
```

The cumulative ratio is:

```text
declared source-type failures / decoded source rows observed
```

The rejected row appears once in both numerator and denominator. The same
typed-error event is used for strategy routing, DLQ accounting, and this
circuit breaker; unrelated validation, structural-document, and collateral
DLQ entries do not enter the numerator. Equality is allowed: a threshold of
`0.05` stops only when the ratio is strictly greater than 5%. `0.0` stops on
the first type failure, while `1.0` never trips. Values must be finite and in
`[0.0, 1.0]`.

### Correlation key

Declare `correlation_key` on the contributing Source's `config:` block, not on
`error_handling:`. Group DLQ rejections by a key field. When any record in a correlation group fails, **records from the failing source's contribution to that group** are routed to the DLQ:

```yaml
# Inside a Source's config:
correlation_key: order_id
```

For compound keys:

```yaml
# Inside a Source's config:
correlation_key: [order_id, customer_id]
```

This is useful for transactional data where partial processing of a group is worse than rejecting the entire group. For example, if one line item in an order fails validation, you may want to reject the entire order.

Under multi-source ingest, the collateral fan-out narrows to the failing source: a `src_b` trigger does NOT DLQ records from `src_a` that share the same correlation key. Single-source pipelines see bit-identical behavior to today's pipeline-wide collateral DLQ. See [Per-source rollback narrowing](correlation-keys.md#per-source-rollback-narrowing) for the full semantic and the two documented exceptions (`max_group_buffer` overflow and Combine output failures).

When a Combine output row fails under a correlation key, the failing driver
row is the trigger of the driver's correlation group. The dead letter for the
matched build record is held with that group as a collateral
(`_cxl_dlq_trigger: false`, category `combine_output_row`), written right after
its driver's row with its driver's `_cxl_dlq_trigger_id`, and written or rolled
back exactly when that group is. It never condemns the build record's own
correlation group, so another driver that matched the same build record keeps
its output unless its own group failed.

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
      schema: [{ name: seg_id, type: string }]
      dlq_granularity: document   # record (default) | document
```

Under `dlq_granularity: document` and the `continue` strategy, when any record of a document fails:

- the failing record becomes the **root-cause** DLQ entry (`_cxl_dlq_trigger = true`, carrying its original error category);
- every other record of the same document becomes a **collateral** entry (`_cxl_dlq_trigger = false`, category `document_rejected`);
- **no** record of that document reaches the success sink.

Clean documents in the same run stream through untouched, and records from sibling sources still on the default `record` granularity keep per-record semantics — the policy is per source.

This is the document-shaped analogue of [correlation keys](#correlation-key): use it when partial processing of a document (an EDI interchange, a batch file with a header/trailer) is worse than rejecting the whole document. Unlike correlation keys, which group across files by a key value, document-level DLQ scopes rejection to a single document's records.

**Document grain.** The document is the **outermost** level — the source file. For a flat format (CSV, JSON, plain XML) each input file is one document. For a nested-envelope format (an X12 `ISA → GS → ST` interchange, an EDIFACT `UNB → UNG → UNH`) the document is the whole **interchange / file**, not an inner functional group or transaction set: a failure anywhere in the interchange rejects the entire interchange, including the transaction sets that validated cleanly. Reject the inner-level grain instead by partitioning the input so each interchange is its own file is not currently offered — the grain is fixed at the file.

**DLQ rate.** Every emitted entry — the trigger and each collateral — counts toward the configured DLQ `max_rate`, matching the correlated-collateral precedent. A rejected 1000-record document contributes 1000 DLQ entries. It does **not** affect [`type_error_threshold`](#type-error-threshold), whose numerator contains only declared source-type failures.

**Memory.** The engine buffers each open document's records until its boundary, then flushes the document clean to the sink or rejects it and drops the buffer. Peak memory scales with the **concurrently-open** documents, not the total input; a single very large document spills its buffer to disk under the run's memory budget rather than holding everything in RAM. See [Streaming vs blocking](../ops/streaming-vs-blocking.md) for the spill model.

**Sink restriction.** Document-level DLQ flushes each whole document to a single output writer, so it cannot be combined with a [per-source-file Sink](../nodes/sink.md) (a `{source_file}` / `{source_path}` path template over a multi-file source). The two are rejected together at compile time (E343); use a single output path, or set `dlq_granularity: record` if per-file output is the requirement.

**Strategy requirement.** `dlq_granularity: document` requires `error_handling.strategy: continue`. It is incompatible with the default `fail_fast`: document-level dead-lettering keeps the run going past a bad document, which contradicts fail-fast's abort-on-first-error. The combination is rejected at compile time (E344) — set `strategy: continue` to dead-letter bad documents, or keep `fail_fast` with the default `dlq_granularity: record`.

**Correlation restriction.** Document-level and correlation-key rejection are
alternative atomic-disposition models. A document is keyed by its source file;
a correlation group can span files and is keyed by authored field values. The
engine does not define precedence or a combined writer boundary for those two
populations, so a pipeline containing both `dlq_granularity: document` and any
`correlation_key` is rejected at compile time (E370). Remove every
`correlation_key` to keep document rejection, or set `dlq_granularity: record`
to keep correlation rejection.

**Spilling stages.** Document identity survives memory pressure end to end. The per-document buffer identifies each document before buffering and spills under the memory budget, and a blocking stage (Sort, hash Aggregate, grace-hash Combine) between the source and the output preserves each record's document context — including the source file the grain keys on — across its own spill round-trip. A document whose records pass through a spilling stage is therefore still grouped and rejected as one document under memory pressure, exactly as it would be in memory.

### Malformed envelopes (structural validation)

Envelope formats carry their own structural-integrity claims: an X12 interchange declares a segment count in each `SE`/`GE`/`IEA` trailer, EDIFACT in each `UNT`/`UNZ`, HL7 batch/file in each `BTS`/`FTS`, and a multi-record flat file's trailer record declares a body count via its `structure:` constraint. When the declared count does not match the body the reader actually streamed, the file is structurally invalid. A multi-record flat file can also break a **non-count structural rule** — a line whose record-type discriminator matches no declared `records:` entry (E345), or a body record appearing after the trailer that closes the document; these are classified separately from a count mismatch but carry the same disposition.

Under `dlq_granularity: document`, such a structural failure dead-letters the **whole source file** to the DLQ rather than aborting the run:

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
      schema: [{ name: seg_id, type: string }]
      dlq_granularity: document   # reuse the document opt-in — no separate config
```

The opt-in is the same `dlq_granularity: document` knob that governs per-record document rejection above; there is no separate `validation:` block. A malformed envelope is simply one more reason a source under the `document` policy condemns a whole document. E345 is the one structural class that also has a record-grained recovery: under `strategy: continue` and the default `dlq_granularity: record`, only the unknown-tag row is dead-lettered and the reader continues at the next physical row. The DLQ row exposes the unknown tag as `record_type` and the unguessed decoded input as `_cxl_dlq_source_record` (fixed-width line text, or a JSON array of decoded CSV cells).

**Honest timing — rejected at the sink boundary, not before the first record.** The trailer that carries the count arrives at the *end* of the file, after every body record it counts has already streamed through the DAG. Clinker is a bounded-memory streaming engine — it does not buffer the whole file up front to pre-validate it (that would defeat the streaming model). So the count mismatch is detected mid-stream, the file is marked failed, and the document-level DLQ buffer rejects every already-streamed record of the file at its close. The user-visible outcome is the same — **no record of a malformed envelope is ever written to the output** — but the rejection lands at the sink boundary, not literally before the file's first record streams.

**Grain — the whole file.** An `SE`-level mismatch (one transaction set inside a larger interchange) rejects the **entire interchange / file**, not just that one transaction set, because the document grain is the outermost source file (see [Document grain](#document-level-dlq) above). Split the input so each interchange is its own file if you need finer rejection.

**Multiple files keep flowing.** When a `glob` / `paths` source matches several files and one is malformed, only that file dead-letters — ingestion continues to the remaining files, so the clean files after a bad one still reach the sink. (This is unlike the default `record` granularity, where a count mismatch aborts the whole run and no file's records are written.) Dead-lettering one malformed file never silently drops the good files around it.

**Record-grained E345 is narrow.** Under the default `dlq_granularity: record`, `strategy: continue` can recover only from an unknown multi-record discriminator because the reader has consumed exactly one bounded physical row and can resume unambiguously. Trailer-count mismatches and a body record after a document-closing trailer still abort at record granularity: neither belongs to one independently recoverable row. Genuine corruption (a truncated stream, a bad delimiter, a control-number echo mismatch, a segment after an X12/EDIFACT/HL7 envelope trailer) **always aborts**, even under the `document` opt-in. Under `fail_fast`, E345 also aborts at the offending line.

> **Cryptographic integrity (checksums / signatures) is not yet validated.** Envelope formats can also carry a SHA-256 body hash, a JWS-signed JSON payload, or an XML Signature. Clinker extracts these envelope sections but does not yet verify them. Tracked for a future release.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Pipeline completed successfully, no errors |
| 1 | Configuration error -- the pipeline never started |
| 2 | Pipeline completed, but DLQ entries were produced |
| 3 | Data error halted the run: a `fail_fast` evaluation/accumulator failure, or the DLQ-rate ceiling |
| 4 | I/O, format, or spill failure |

Exit code 2 is not a failure -- it means the pipeline ran to completion and handled errors according to the configured strategy. Check the DLQ file for details. See [Exit Codes & Error Diagnosis](../ops/exit-codes.md) for the full reference and the orchestrator retry policy.

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
      correlation_key: order_id
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

  - type: sink
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
```
