# Metrics & Monitoring

Clinker writes per-execution metrics as JSON files to a spool directory. These files can be collected into an NDJSON archive for ingestion into monitoring systems.

## Enabling metrics

There are three ways to enable metrics collection, listed from highest to lowest priority:

**CLI flag:**
```bash
clinker run pipeline.yaml --metrics-spool-dir ./metrics/
```

**Environment variable:**
```bash
export CLINKER_METRICS_SPOOL_DIR=./metrics/
clinker run pipeline.yaml
```

**YAML config:**
```yaml
pipeline:
  metrics:
    spool_dir: "./metrics/"
```

When metrics are enabled, each execution writes one JSON file to the spool directory, named `<execution_id>.json`.

## Metrics schema

Each metrics file follows schema version 1:

```json
{
  "execution_id": "01912345-6789-7abc-def0-123456789abc",
  "schema_version": 1,
  "pipeline_name": "customer_etl",
  "config_path": "/opt/clinker/pipelines/daily_etl.yaml",
  "hostname": "prod-etl-01",
  "started_at": "2026-04-11T10:00:00Z",
  "finished_at": "2026-04-11T10:00:05Z",
  "duration_ms": 5000,
  "exit_code": 0,
  "records_total": 50000,
  "records_ok": 49950,
  "records_dlq": 50,
  "execution_mode": "streaming",
  "peak_rss_bytes": 134217728,
  "thread_count": 4,
  "input_files": ["./data/customers.csv"],
  "output_files": ["./output/enriched.csv"],
  "dlq_path": "./output/errors.csv",
  "error": null
}
```

### Field reference

| Field | Type | Description |
|-------|------|-------------|
| `execution_id` | string | UUID v7 or custom `--batch-id` value |
| `schema_version` | integer | Always `1` for this release |
| `pipeline_name` | string | The `name` from the pipeline YAML |
| `config_path` | string | Absolute path to the config file |
| `hostname` | string | Machine hostname |
| `started_at` | string | ISO 8601 UTC timestamp |
| `finished_at` | string | ISO 8601 UTC timestamp |
| `duration_ms` | integer | Wall-clock duration in milliseconds |
| `exit_code` | integer | Process exit code (see [Exit Codes](exit-codes.md)) |
| `records_total` | integer | Total records read from all sources |
| `records_ok` | integer | Records that reached an output node |
| `records_dlq` | integer | Records routed to the dead-letter queue |
| `execution_mode` | string | `streaming` or `batch` |
| `peak_rss_bytes` | integer | Maximum resident set size during execution |
| `thread_count` | integer | Thread pool size used |
| `input_files` | array | Paths to all source files |
| `output_files` | array | Paths to all output files written |
| `dlq_path` | string/null | Path to the DLQ file, or null if none |
| `error` | string/null | Error message on failure, or null on success |

## Collecting metrics

The spool directory accumulates one file per execution. Use `clinker metrics collect` to sweep them into an NDJSON archive:

```bash
clinker metrics collect \
  --spool-dir ./metrics/ \
  --output-file ./metrics/archive.ndjson \
  --delete-after-collect
```

This appends all spool files to the archive (one JSON object per line) and removes the originals. The NDJSON format is compatible with most log aggregation and monitoring tools.

**Preview without writing:**
```bash
clinker metrics collect \
  --spool-dir ./metrics/ \
  --output-file ./metrics/archive.ndjson \
  --dry-run
```

## Integration with monitoring systems

### Grafana / Prometheus

Parse the NDJSON archive with a log shipper (Promtail, Filebeat, Vector) and create dashboards tracking:

- `duration_ms` -- execution time trends
- `records_dlq` -- data quality over time
- `peak_rss_bytes` -- memory utilization

### Datadog

Ship NDJSON to Datadog Logs, then create metrics from log attributes:

```bash
# Example: tail the archive and ship to Datadog
tail -f ./metrics/archive.ndjson | datadog-agent log-stream
```

### ELK Stack

Filebeat can ingest NDJSON directly:

```yaml
# filebeat.yml
filebeat.inputs:
  - type: log
    paths:
      - /var/log/clinker/metrics.ndjson
    json.keys_under_root: true
```

### Simple alerting with jq

For environments without a full monitoring stack, use `jq` to query the archive directly:

```bash
# Find all runs with DLQ entries in the last 24 hours
jq 'select(.records_dlq > 0)' metrics/archive.ndjson

# Find runs that exceeded 400MB RSS
jq 'select(.peak_rss_bytes > 419430400)' metrics/archive.ndjson

# Average duration by pipeline
jq -s 'group_by(.pipeline_name) | map({
  pipeline: .[0].pipeline_name,
  avg_ms: (map(.duration_ms) | add / length)
})' metrics/archive.ndjson
```

## Operational recommendations

- **Always enable metrics in production.** The overhead is negligible (one small JSON write at the end of each run).
- **Run `metrics collect --delete-after-collect` on a schedule** (e.g., hourly) to prevent spool directory growth.
- **Use `--batch-id`** with meaningful identifiers to correlate metrics across retries and environments.
- **Alert on `records_dlq > 0`** to catch data quality regressions early.
- **Track `peak_rss_bytes` trends** to anticipate when memory limits need adjustment.
