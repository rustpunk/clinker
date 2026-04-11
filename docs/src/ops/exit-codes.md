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

## Scheduler integration

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
