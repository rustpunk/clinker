# CLI Reference

Clinker ships two command-line tools: `clinker` (the pipeline runner) and `cxl` (the expression REPL, covered in the [CXL CLI chapter](../cxl/cxl-cli.md)). This page is the complete reference for `clinker`.

## clinker run

Execute a pipeline.

```
clinker run [OPTIONS] <CONFIG>
```

### Positional arguments

| Argument | Description |
|----------|-------------|
| `<CONFIG>` | Path to the pipeline YAML configuration file (required) |

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--memory-limit <SIZE>` | `256M` | Memory budget for the execution. Accepts `K`, `M`, `G` suffixes. When the limit is approached, aggregation operators spill to disk rather than crashing. CLI value overrides any `memory_limit` set in the YAML. |
| `--threads <N>` | number of CPUs | Size of the thread pool used for parallel node execution. |
| `--error-threshold <N>` | `0` (unlimited) | Maximum number of records routed to the dead-letter queue before the pipeline aborts. `0` means no limit -- the pipeline will run to completion regardless of DLQ volume. |
| `--batch-id <ID>` | UUID v7 | Custom execution identifier. Appears in metrics output and log lines. Use a meaningful value (e.g. `daily-2026-04-11`) for correlation across retries. |
| `--explain [FORMAT]` | `text` | Print the execution plan and exit without processing data. Accepted formats: `text`, `json`, `dot`. See [Explain Plans](explain.md). |
| `--dry-run` | -- | Validate the configuration (YAML structure, CXL syntax, type checking, DAG wiring) without reading any data. |
| `-n, --dry-run-n <N>` | -- | Process only the first `N` records through the full pipeline. Implies `--dry-run`. |
| `--dry-run-output <FILE>` | stdout | Redirect dry-run output to a file instead of stdout. Only meaningful with `-n`. |
| `--rules-path <DIR>` | `./rules/` | Search path for CXL module files referenced by `use` statements. |
| `--base-dir <DIR>` | -- | Base directory for resolving relative paths in the YAML config. Defaults to the directory containing the config file. |
| `--allow-absolute-paths` | -- | Permit absolute file paths in the pipeline YAML. By default, absolute paths are rejected to encourage portable configs. |
| `--env <NAME>` | -- | Set the active environment. Equivalent to setting `CLINKER_ENV`. Used by `when:` conditions in channel overrides. |
| `--quiet` | -- | Suppress progress output. Errors are still printed to stderr. |
| `--force` | -- | Allow output files to be overwritten if they already exist. Without this flag, the pipeline aborts rather than clobbering existing output. |
| `--log-level <LEVEL>` | `info` | Logging verbosity. One of: `error`, `warn`, `info`, `debug`, `trace`. |
| `--metrics-spool-dir <DIR>` | -- | Directory for per-execution metrics files. See [Metrics & Monitoring](metrics.md). |

### Examples

```bash
# Basic execution
clinker run pipeline.yaml

# Production run with memory budget and forced overwrite
clinker run pipeline.yaml --memory-limit 512M --force --log-level warn

# Validate without processing
clinker run pipeline.yaml --dry-run

# Preview first 10 records
clinker run pipeline.yaml --dry-run -n 10

# Show execution plan as Graphviz
clinker run pipeline.yaml --explain dot | dot -Tpng -o plan.png

# Run with a custom batch ID for tracing
clinker run pipeline.yaml --batch-id "daily-2026-04-11" --metrics-spool-dir ./metrics/
```

---

## clinker metrics collect

Sweep per-execution metrics files from a spool directory into a single NDJSON archive.

```
clinker metrics collect [OPTIONS]
```

### Options

| Flag | Description |
|------|-------------|
| `--spool-dir <DIR>` | Spool directory to sweep (required). |
| `--output-file <FILE>` | NDJSON archive destination (required). If the file exists, new entries are appended. |
| `--delete-after-collect` | Remove spool files after they have been successfully written to the archive. |
| `--dry-run` | Preview which files would be collected without writing anything. |

### Examples

```bash
# Collect and archive, then clean up spool
clinker metrics collect \
  --spool-dir /var/spool/clinker/ \
  --output-file /var/log/clinker/metrics.ndjson \
  --delete-after-collect

# Preview what would be collected
clinker metrics collect \
  --spool-dir ./metrics/ \
  --output-file ./archive.ndjson \
  --dry-run
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `CLINKER_ENV` | Active environment name. Equivalent to `--env`. Used by `when:` conditions in channel overrides to select environment-specific configuration. |
| `CLINKER_METRICS_SPOOL_DIR` | Default metrics spool directory. Overridden by `--metrics-spool-dir`. |

**Precedence** (highest to lowest): CLI flag, environment variable, YAML config value.
