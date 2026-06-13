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

Each metrics file follows schema version 3. The collector rejects spool
files written under an older schema version, so upgrading clinker across a
schema bump means draining the spool first.

```json
{
  "execution_id": "01912345-6789-7abc-def0-123456789abc",
  "schema_version": 3,
  "pipeline_name": "customer_etl",
  "config_path": "/opt/clinker/pipelines/daily_etl.yaml",
  "hostname": "prod-etl-01",
  "started_at": "2026-04-11T10:00:00Z",
  "finished_at": "2026-04-11T10:00:05Z",
  "duration_ms": 5000,
  "exit_code": 0,
  "records_total": 50000,
  "records_ok": 49950,
  "records_written": 49950,
  "records_dlq": 50,
  "execution_mode": "Streaming",
  "peak_rss_bytes": 134217728,
  "thread_count": 4,
  "input_files": ["./data/customers.csv"],
  "output_files": ["./output/enriched.csv"],
  "dlq_path": "./output/errors.csv",
  "error": null,
  "retraction": {
    "groups_recomputed": 0,
    "partitions_dispatched": 0,
    "iterations": 0,
    "degrade_fallback_count": 0,
    "synthetic_ck_columns_emitted_total": 0,
    "synthetic_ck_fanout_lookups_total": 0,
    "synthetic_ck_fanout_rows_expanded_total": 0
  },
  "per_source_record_counts": { "customers": 50000 },
  "per_source_dlq_counts": { "customers": 50 }
}
```

### Field reference

| Field | Type | Description |
|-------|------|-------------|
| `execution_id` | string | UUID v7 or custom `--batch-id` value |
| `schema_version` | integer | Schema version of this payload; currently `3` |
| `pipeline_name` | string | The `name` from the pipeline YAML |
| `config_path` | string | Absolute path to the config file |
| `hostname` | string | Machine hostname |
| `started_at` | string | ISO 8601 UTC timestamp |
| `finished_at` | string | ISO 8601 UTC timestamp |
| `duration_ms` | integer | Wall-clock duration in milliseconds |
| `exit_code` | integer | Process exit code (see [Exit Codes](exit-codes.md)) |
| `records_total` | integer | Total records read from the primary source |
| `records_ok` | integer | Distinct source records that reached at least one output. Under inclusive Route fan-out one input matching N branches counts once |
| `records_written` | integer | Total writes across all sinks. Equals `records_ok` for single-output exclusive pipelines; exceeds it under inclusive Route fan-out or multiple Output sinks |
| `records_dlq` | integer | Records routed to the dead-letter queue |
| `execution_mode` | string | DAG-derived execution summary: `Streaming` (no full-stage materialization required) or `TwoPass` (a blocking stage forces an accumulation pass) |
| `peak_rss_bytes` | integer/null | Peak resident set size in bytes, sampled across chunk boundaries on Linux, macOS, and Windows. `null` on platforms where RSS sampling is unavailable |
| `thread_count` | integer | Thread pool size used |
| `input_files` | array | Paths to all source files |
| `output_files` | array | Paths to all output files written |
| `dlq_path` | string/null | Path to the DLQ file, or null if none |
| `error` | string/null | Error message on exit 1/3/4, or null on success (exit 0) and partial success (exit 2) |
| `retraction` | object | Correlation-key retraction counters (see below). All-zero on strict pipelines, which never enter the relaxed loop |
| `per_source_record_counts` | object | Ingest record count per Source node, keyed by node name |
| `per_source_dlq_counts` | object | DLQ entry count per Source node; sources with zero DLQ entries are absent |

The `retraction` object carries the relaxed correlation-key retraction
orchestrator's counters: `groups_recomputed`, `partitions_dispatched`,
`iterations`, `degrade_fallback_count`,
`synthetic_ck_columns_emitted_total`, `synthetic_ck_fanout_lookups_total`,
and `synthetic_ck_fanout_rows_expanded_total`. Every field is `0` on
strict pipelines and on relaxed pipelines that never trigger a retraction.
See [Correlation Keys](../pipelines/correlation-keys.md) for the underlying
mechanism.

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
