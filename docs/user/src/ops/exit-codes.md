# Exit Codes & Error Diagnosis

Clinker uses structured exit codes to communicate the outcome of a pipeline run. These codes are designed for integration with schedulers, cron, CI systems, and monitoring tools.

## Exit code reference

| Code | Meaning | Description |
|------|---------|-------------|
| 0 | Success | Pipeline completed. All records processed successfully. |
| 1 | Configuration error | Invalid YAML, CXL syntax error, type mismatch, or DAG wiring problem. Fix the pipeline configuration. |
| 2 | Partial success | Pipeline ran to completion, but some records were routed to the dead-letter queue. Check the DLQ file. |
| 3 | Evaluation error | CXL runtime error during record processing (e.g., division by zero, type coercion failure). |
| 4 | I/O error | File not found, permission denied, disk full, or input format mismatch. |

## Understanding exit code 2

Exit code 2 is not a crash. It means:

- The pipeline started and ran to completion.
- All viable records were processed and written to output files.
- Some records could not be processed and were diverted to the dead-letter queue.

Your scheduler should treat exit code 2 as a **warning**, not a failure. The DLQ file contains the problematic records along with the error that caused each one to be rejected.

To control when exit code 2 escalates to a hard failure, use `--error-threshold`:

```bash
# Abort if more than 100 records hit the DLQ
clinker run pipeline.yaml --error-threshold 100
```

With a threshold set, the pipeline aborts (exit code 3) when the DLQ count exceeds the threshold, rather than continuing to completion.

## Diagnosing failures

### Exit code 1: Configuration error

The error message includes a span-annotated diagnostic pointing to the exact location of the problem:

```
Error: CXL type error in node 'transform_1'
  --> pipeline.yaml:25:15
   |
25 |   emit total = amount + name
   |                ^^^^^^^^^^^^^ cannot add Int and String
```

**Action:** Fix the YAML or CXL expression indicated in the diagnostic, then re-run with `--dry-run` to confirm the fix.

### Exit code 2: Partial success (DLQ entries)

Check the DLQ file for details:

```bash
# The DLQ path is shown in the run output and in metrics
cat output/errors.csv
```

Common causes:
- Null values in fields that a CXL expression does not handle
- Data that does not match the declared schema (e.g., non-numeric value in an integer column)
- Coercion failures between types

**Action:** Review the DLQ records, fix the data or add null handling to CXL expressions, and re-run.

### Exit code 3: Evaluation error

A CXL expression failed at runtime. The error message includes the failing expression and the record that triggered it:

```
Error: division by zero in node 'compute_ratio'
  expression: emit ratio = total / count
  record: {total: 500, count: 0}
```

**Action:** Add guard conditions to the CXL expression:

```
emit ratio = if count == 0 then 0 else total / count
```

### Exit code 4: I/O error

File system or format errors:

```
Error: file not found: ./data/customers.csv
  --> pipeline.yaml:8:12
```

Common causes:
- Input file does not exist or path is wrong
- Permission denied on input or output directories
- Output file already exists (use `--force` to overwrite)
- Disk full during output writing
- Input file format does not match the declared type (e.g., invalid CSV)

**Action:** Fix file paths, permissions, or disk space, then re-run.

## Plan-time diagnostic codes

The process exit codes above tell a scheduler whether the run
succeeded. The `E###` codes below appear inside the structured
`Error:` messages a configuration error (exit code 1) prints, and
identify the specific compile-time check that rejected the
pipeline. The codes below cover the event-time watermark and
time-windowed aggregate surface
([issue #61](https://github.com/rustpunk/clinker/issues/61));
related code sets live in
[Pipeline Variables](../pipelines/variables.md),
[Channels](../pipelines/channels.md), and
[Correlation Keys](../pipelines/correlation-keys.md).

| Code | Trigger | Remediation |
|------|---------|-------------|
| **E154** | A source declares `watermark.column: <col>` but `<col>` is not present in that source's `schema:` block. | Add the column to `schema:`, or remove the `watermark:` block. |
| **E155** | A source declares `watermark.column: <col>` and the column exists, but its declared CXL type is not `date_time` or `date`. | Change the column's `type:` to `date_time` or `date`, or point `watermark.column` at a column that already has one of those types. |
| **E156** | An aggregate declares `time_window:` but at least one upstream-reachable source does not declare `watermark.column`. | Add `watermark: { column: <event-time-column> }` to each listed source, or remove `time_window:` from the aggregate. Without a watermark on every upstream source, `min_across_sources` never advances past `None` and the window can never close. |
| **E157** | A source declares an external `schema:` file (`schema: path.schema.yaml`) that could not be read or parsed as a `SourceSchema`. | Fix the file path or its contents. A schema file is a bare column list or a multi-record `discriminator:`/`records:` map — it may not itself point at another schema file. |
| **E158** | A source column's declared type is (or wraps) the inference-only `numeric` union. | Declare a concrete `int` or `float`. `numeric` is `int \| float` resolved during type unification and never carries into a compiled source schema. |
| **E159** | A source pairs a `generated` schema with a non-EDI format. | `generated` (engine-synthesized positional columns) is valid only for the EDI-family formats (`edifact`, `x12`, `hl7`, `swift`). Declare an explicit column list for any other format. |

See [Source Nodes → Watermarks](../nodes/source.md#watermarks)
and [Aggregate Nodes → Time-windowed aggregates](../nodes/aggregate.md#time-windowed-aggregates)
for the field semantics each code is enforcing.

### DLQ category: LateRecord

When a time-windowed aggregate sees a record whose event time falls
inside an already-closed window
(`window_end + allowed_lateness < min_across_sources`), the engine
routes the record to the DLQ instead of attempting to fold it into
a finalized accumulator. Mirrors Flink's
[`sideOutputLateData`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/#getting-late-data-as-a-side-output)
and Spark Structured Streaming's late-data drop.

The DLQ row carries:

- `_cxl_dlq_error_category` = `late_record`
- `_cxl_dlq_stage` = `time_window:<aggregate-name>`
- `_cxl_dlq_error_detail` — the closed window's `[start, end)`
  bounds as i64 nanoseconds since the Unix epoch

Tune `watermark.delay` (source-side, applies before any aggregate)
or `allowed_lateness` (operator-side, applies per aggregate) to
absorb expected out-of-order tails before they reach this path.

## Scheduler integration

For running Clinker under a workflow orchestrator (Temporal, Airflow,
Dagster) — mapping these exit codes onto a retry policy, plus the
cancellation and output-atomicity guarantees — see
[Running Under a Workflow Orchestrator](orchestrator-contract.md).

### Cron script

```bash
#!/bin/bash
set -euo pipefail

PIPELINE=/opt/clinker/pipelines/daily_etl.yaml
METRICS_DIR=/var/spool/clinker/

clinker run "$PIPELINE" \
  --memory-limit 512M \
  --log-level warn \
  --metrics-spool-dir "$METRICS_DIR" \
  --force

EXIT=$?

case $EXIT in
  0)
    echo "$(date): Success" >> /var/log/clinker/daily_etl.log
    ;;
  2)
    echo "$(date): Warning - DLQ entries produced" >> /var/log/clinker/daily_etl.log
    mail -s "Clinker ETL Warning: DLQ entries" ops@company.com < /dev/null
    ;;
  *)
    echo "$(date): FAILURE (exit code $EXIT)" >> /var/log/clinker/daily_etl.log
    mail -s "Clinker ETL FAILURE (exit $EXIT)" ops@company.com < /dev/null
    ;;
esac

exit $EXIT
```

### CI pipeline (GitHub Actions)

```yaml
- name: Run ETL pipeline
  run: clinker run pipeline.yaml --dry-run
  # Exit code 1 fails the build on config errors

- name: Smoke test with real data
  run: clinker run pipeline.yaml --dry-run -n 100
  # Catches runtime evaluation errors
```

### Systemd

Systemd `Type=oneshot` services interpret non-zero exit codes as failures. To allow exit code 2 (partial success) without triggering service failure:

```ini
[Service]
Type=oneshot
SuccessExitStatus=2
ExecStart=/opt/clinker/bin/clinker run /opt/clinker/pipelines/daily_etl.yaml --force
```
