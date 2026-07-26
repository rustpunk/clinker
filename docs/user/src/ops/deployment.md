# Production Deployment

Clinker is a single statically-linked binary with no runtime dependencies. Deployment is straightforward: copy the binary to the server.

## Installation

```bash
# Copy the binary
scp target/release/clinker user@server:/opt/clinker/bin/

# Verify it runs
ssh user@server /opt/clinker/bin/clinker --version
```

No JVM, no Python, no container runtime required.

## Recommended directory structure

```
/opt/clinker/
  bin/
    clinker                    # The binary
  pipelines/
    daily_etl.yaml             # Pipeline configs
    weekly_report.yaml
  data/                        # Input data (or symlinks to data locations)
  output/                      # Output files
  rules/                       # CXL module files (for use statements)
  metrics/                     # Metrics spool directory
```

Create a dedicated user:

```bash
sudo useradd --system --home-dir /opt/clinker --shell /usr/sbin/nologin clinker
sudo chown -R clinker:clinker /opt/clinker
```

## Systemd service

For scheduled one-shot execution:

```ini
[Unit]
Description=Clinker ETL - Daily Customer Processing
After=network.target

[Service]
Type=oneshot
ExecStart=/opt/clinker/bin/clinker run /opt/clinker/pipelines/daily_etl.yaml \
  --memory-limit 512M \
  --log-level warn \
  --metrics-spool-dir /var/spool/clinker/ \
  --force
WorkingDirectory=/opt/clinker
User=clinker
Group=clinker
SuccessExitStatus=2

# Resource limits
MemoryMax=1G
CPUQuota=200%

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=clinker-daily

[Install]
WantedBy=multi-user.target
```

Pair with a systemd timer for scheduling:

```ini
[Unit]
Description=Run Clinker daily ETL at 2 AM

[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
sudo systemctl enable --now clinker-daily.timer
```

Note: `SuccessExitStatus=2` tells systemd that exit code 2 (partial success with DLQ entries) is not a service failure. See [Exit Codes](exit-codes.md) for the full reference.

## Cron scheduling

```cron
# Run daily at 2 AM, log to syslog
0 2 * * * /opt/clinker/bin/clinker run \
  /opt/clinker/pipelines/daily_etl.yaml \
  --log-level warn --force \
  2>&1 | logger -t clinker

# Collect metrics hourly
0 * * * * /opt/clinker/bin/clinker metrics collect \
  --spool-dir /var/spool/clinker/ \
  --output-file /var/log/clinker/metrics.ndjson \
  --delete-after-collect
```

## Environment-based configuration

Use the `CLINKER_ENV` variable or `--env` flag to activate environment-specific overrides:

```bash
# Production
CLINKER_ENV=production clinker run pipeline.yaml

# Staging
CLINKER_ENV=staging clinker run pipeline.yaml
```

Combined with channel overrides in the pipeline YAML, this allows a single pipeline definition to target different file paths, connection strings, or thresholds per environment.

## Logging

### Log levels for production

| Level | Use case |
|-------|----------|
| `warn` | Recommended for production cron jobs. Prints warnings and errors only. |
| `info` | Default. Includes progress messages. Useful during initial deployment. |
| `error` | Minimal output. Only prints when something fails. |
| `debug` | Troubleshooting. Generates significant output. |
| `trace` | Development only. Extremely verbose. |

### Directing logs

**To syslog via logger:**
```bash
clinker run pipeline.yaml --log-level warn 2>&1 | logger -t clinker
```

**To a log file:**
```bash
clinker run pipeline.yaml --log-level warn 2>> /var/log/clinker/etl.log
```

**Systemd journal** captures stdout and stderr automatically when running as a service.

## DLQ monitoring

When a pipeline exits with code 2, records that could not be processed are written to the dead-letter queue file. Set up a daily check:

```bash
#!/bin/bash
# Check for DLQ files produced today
DLQ_DIR=/opt/clinker/output/
DLQ_FILES=$(find "$DLQ_DIR" -name "*_errors.csv" -mtime 0 -size +0c)

if [ -n "$DLQ_FILES" ]; then
    echo "DLQ entries found:" | mail -s "Clinker DLQ Alert" ops@company.com <<EOF
The following DLQ files were produced today:

$DLQ_FILES

Review the files and address data quality issues.
EOF
fi
```

## Batch ID for tracing

Use `--batch-id` with a meaningful, consistent naming scheme:

```bash
# Date-based
clinker run pipeline.yaml --batch-id "daily-$(date +%Y-%m-%d)"

# Include environment
clinker run pipeline.yaml --batch-id "prod-daily-$(date +%Y-%m-%d)"
```

The batch ID appears in metrics output and log lines, making it easy to correlate a specific run across logs, metrics, and DLQ files. On retries, use a different batch ID (e.g., append `-retry-1`) to distinguish attempts.

## Upgrades

To upgrade Clinker:

1. Validate the new version against your pipelines:
   ```bash
   /opt/clinker/bin/clinker-new run pipeline.yaml --dry-run
   ```
2. Replace the binary:
   ```bash
   cp clinker-new /opt/clinker/bin/clinker
   ```
3. Verify:
   ```bash
   /opt/clinker/bin/clinker --version
   ```

There is no configuration migration. Pipeline YAML files are forward-compatible within the same major version.
