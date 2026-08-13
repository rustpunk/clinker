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
| `per_source_record_counts` | object | Ingest record count per Source node, keyed by node name. A source that read zero records is present with a count of `0` |
| `per_source_dlq_counts` | object | DLQ entry count per Source node; sources with zero DLQ entries are absent. The values sum to **at most** `records_dlq` — see the note below |

The sum of `per_source_dlq_counts` values is at most `records_dlq`, and can
be less: a failure in a Combine emit or a post-aggregate row is not traceable
to a single declared source, so it is counted in `records_dlq` but not in this
per-source breakdown. For pipelines whose dead-letters all originate at a
declared source, the two match exactly.

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

## Workspace OTLP and lineage policy

Deployment observability is optional and disabled when `clinker.toml` has no
`[observability]` table. It is workspace policy, not pipeline YAML, and does
not participate in the compiled plan's semantic fingerprint. A present table
is one complete policy: callers may supply a complete resolved replacement
only when the workspace table is absent; individual fields are never merged.

The workspace loader validates this policy without opening a source, output,
attempt directory, worker, credential provider, or network connection. It
keeps the Collector endpoint as length-bounded raw text exactly as authored.
The one shape it requires of that text is the shape it requires of every other
authored string in the table: non-empty, within its byte cap, with no
surrounding whitespace and no embedded control character. Padding and a
carriage return are not part of an endpoint under any parse, and refusing them
here names `observability.otlp.endpoint` and hands you a pasteable correction,
where the later network boundary can only report that some endpoint was
unusable.

The network admission boundary parses the text itself later, before any
delivery effect; scheme, authority, credentials, paths, query strings,
fragments, normalization, and the fixed OTLP signal routes are deliberately
not decided by the workspace parser. Collector reachability is not a
configuration admission check.

A complete fixed-capacity example is:

```toml
[observability]
arena_bytes = "4MB"
ordinary_lane_bytes = "3MB"
high_severity_lane_bytes = "1MB"
max_batch_bytes = "256KB"
max_attributes_per_event = 32
max_attribute_bytes = "4KB"
drop_policy = "drop_newest"
sample_every = 1
rate_limit_per_second = 1000
rate_limit_burst = 1000
flush_timeout_ms = 15000

[observability.otlp]
endpoint = "https://collector.example.com"
connect_timeout_ms = 1000
request_timeout_ms = 5000
retry_max_attempts = 3
retry_total_timeout_ms = 10000
max_response_bytes = "64KB"

[observability.otlp.auth]
mode = "none"

[observability.lineage]
queue_bytes = "1MB"
max_event_bytes = "64KB"
drop_policy = "drop_newest"
flush_timeout_ms = 5000
identity_mode = "external"

[[observability.lineage.dataset]]
node = "source_customers"
canonical_datasource = "s3://warehouse/customers"

[[observability.lineage.dataset]]
node = "output_customers"
catalog_namespace = "analytics"
catalog_name = "customers_clean"

[[observability.field_policy]]
event = "run.completed"
field = "records_written"
action = "allow"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "customer_id"
action = "hash"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "email"
action = "replace"
replacement = "[redacted]"
```

Byte-size strings use decimal units (`1KB = 1,000` bytes and
`1MB = 1,000,000` bytes). The fixed defaults and hard ceilings are:

| Key | Default | Hard ceiling or relationship |
|-----|---------|------------------------------|
| `arena_bytes` | `"4MB"` | `"64MB"`; equals the exact sum of both lane caps |
| `ordinary_lane_bytes` | three quarters of the arena | `"64MB"` and disjoint from the high-severity lane |
| `high_severity_lane_bytes` | one quarter of the arena | `"64MB"` and disjoint from the ordinary lane |
| `max_batch_bytes` | `"256KB"` | `"1MB"` and no larger than either lane |
| `max_attributes_per_event` | 32 | 256 |
| `max_attribute_bytes` | `"4KB"` | `"64KB"` |
| `sample_every` | 1 | 1,000,000 |
| `rate_limit_per_second` | 1,000 | 1,000,000 |
| `rate_limit_burst` | 1,000 | 1,000,000 |
| `flush_timeout_ms` | 15,000 | 60,000 |
| `otlp.connect_timeout_ms` | 1,000 | 60,000 and no greater than request timeout |
| `otlp.request_timeout_ms` | 5,000 | 60,000 and no greater than retry total |
| `otlp.retry_max_attempts` | 3 | 10 |
| `otlp.retry_total_timeout_ms` | 10,000 | 60,000 and no greater than flush timeout |
| `otlp.max_response_bytes` | `"64KB"` | `"1MB"` |
| `lineage.queue_bytes` | `"1MB"` | `"64MB"`, reserved independently of the telemetry arena |
| `lineage.max_event_bytes` | `"64KB"` | `"1MB"` and no larger than its lineage queue |
| `lineage.flush_timeout_ms` | 5,000 | 60,000 |

Every byte default above is the quantity its own spelling parses to, so
writing a default out in full changes nothing.

### Sizing the arena

The two lanes partition the arena exactly: no telemetry byte is charged twice,
and none of the arena is unreachable. You may write any of the three and leave
the rest to be worked out from what you wrote:

- **`arena_bytes` alone** — the lanes split it three-to-one, whatever its size.
  `arena_bytes = "8MB"` gives a 6 MB ordinary lane and a 2 MB high-severity one.
- **One lane alone** — the arena stays at its default and the other lane takes
  the remainder.
- **Both lanes** — the arena is their sum.
- **All three** — the equality is checked rather than adjusted, and a
  disagreement is refused before the run starts.

The arena is the budget in every case: a lane is never allowed to grow it. A
lane that does not fit inside the arena is refused, naming both keys.

An exported attribute longer than `max_attribute_bytes` is cut to fit and
marked with a trailing `…`, and the marker is charged against the same cap, so
a marked value is never longer than an unmarked one would have been. The mark
matters because a bare prefix is not obviously a prefix: an amount of
`123456789` cut to four bytes reads as `1234`, and a timestamp cut short is
still a well-formed timestamp. A dashboard reading `1…` fails visibly; one
reading `1234` charts a wrong number. Note that the mark is a signal, not a
guarantee — a free-text value that genuinely ends in `…` is indistinguishable
from a truncated one.

Node names are exported verbatim under the same treatment. A name is never
dropped for the characters it contains, so a Transform named with a space or a
non-ASCII character still produces a span.

Both delivery paths admit with `drop_policy = "drop_newest"`; there is no
blocking, unbounded, or disk-spool spelling. The telemetry arena contains two
disjoint lanes: `trace`, `debug`, and `info` signals occupy the ordinary lane,
while `warn` and `error` occupy the high-severity lane. The lineage queue is a
separate reservation and cannot be expressed as an alias of either telemetry
lane or the arena.

`sample_every = N` keeps one in every N signals, counted **within each lane
separately**. The lanes are disjoint precisely so ordinary volume cannot crowd
out problems, and sampling honours that: with `sample_every = 10`, a Transform
emitting nine per-record `info` events for every `error` still keeps one in ten
of its errors, and that fraction does not change when the `info` volume does.
A run that raises its per-record logging therefore does not quietly thin out
its error reporting.

### What the machine terminal reports about export

Under `--machine ndjson-v1` the terminal event carries an `observability`
object summarising what the exporter did. It holds one counter group per
signal — `logs`, `metrics`, and `traces`, each with `accepted`, `rejected`,
`attempts`, and `failures` — plus `flush_complete`.

Read `flush_complete` before reading the counters. When it is `true` the
counts are the run's final accounting. When it is `false` the exporter did not
get to the end of the flush: either it ran past `flush_timeout_ms`, or it could
not take the signal arena from the pipeline before giving up on it. Either way
the counts are what had been recorded at that point, deliveries may still have
been in flight, signals may remain that were never sent, and a low `accepted`
means "we stopped counting" rather than "the collector refused them". Those call
for different responses, and only one of them is a collector problem.

Any `2xx` answer counts as delivered. A collector's own success status is
`200`, and its body is where a partial success declares the records it refused
— those are what `rejected` counts. A gateway in front of a collector may
answer `202 Accepted` or `204 No Content` instead, and there is no such body to
read: the whole chunk counts under `accepted`, because the answer says it was
taken. A rejection is a `4xx` or a `5xx`.

A delivery cut short after its request was fully sent is not sent again, even
with attempts left in the budget. The collector may already hold that batch —
a reply that is only slow cannot be told apart from one that was lost — and
repeating it would ingest the same log records twice and count the same
monotonic sums twice, which is wrong rather than merely unconfirmed. Such a
batch is counted under `failures`, having spent fewer attempts than
`retry_max_attempts` allowed: its delivery is unconfirmed, not known to have
failed.
A collector that habitually answers slowly needs a larger
`otlp.request_timeout_ms`, not more attempts.

Delivery outcomes never change execution, publication, or the process exit
status; the summary is an observation about the export, not about the run.

### What the machine terminal reports about admission

The per-signal groups above are export-side, and an exporter can only count
what reached it. A run that discarded most of its signals at the arena would
otherwise report `accepted = N, rejected = 0, flush_complete = true` — a clean,
complete-looking export of a silently truncated dataset. The `admission` object
beside them says what the arena took and what it refused, before any export:

```json
"admission": {
  "counts_complete": true,
  "accepted": 9,
  "dropped": {
    "sampled": 8, "rate_limited": 0, "queue_full": 0, "contended": 0,
    "oversize": 0, "invalid_identity": 0, "undecodable": 0
  },
  "lanes": {
    "ordinary":      { "sampled": 4, "queue_full": 0, "retained_bytes": 0, "capacity_bytes": 32000 },
    "high_severity": { "sampled": 4, "queue_full": 0, "retained_bytes": 0, "capacity_bytes": 32000 }
  },
  "fields": { "denied": 0, "truncated": 0, "limit_dropped": 0, "missing": 0 },
  "arena_recoveries": 0,
  "retained_bytes": 0, "peak_retained_bytes": 2477, "capacity_bytes": 64000
}
```

`counts_complete` says whether the rest of the object is a final accounting.
These counters are read from the producer's arena, and the arena keeps changing
while the exporter drains it — `undecodable` is credited at drain. A flush that
ran to completion joined the exporter first, so nothing was left to credit and
`counts_complete` is `true`. A flush that expired on `flush_timeout_ms`
detached an exporter that is still draining, so the read landed mid-drain and
`counts_complete` is `false`: every number below it is whatever had been
reached, and a low one means "we could not finish counting" rather than
"nothing was lost". The flush stays bounded either way — a finishing run does
not wait on an unresponsive collector — so this flag, not a longer wait, is
what keeps a truncated view from looking complete.

`accepted` counts the logs and spans the arena took. Metric points are
coalesced into fixed counters rather than admitted as signals, so none of them
appear here.

Each key under `dropped` is one reason a signal never became exportable. They
are named as OpenTelemetry `error.type` values — a full arena is `queue_full`,
not `full` — so these map onto SDK self-observability metrics without a
rename. `undecodable` is the one member of the set that is also counted in
`accepted`: those signals were admitted, then could not be read back at drain.

`fields` is a different kind of number and must not be added into a loss
total. Those counts describe what became of the fields of records that *were*
accepted — values denied, values truncated, attributes dropped at the per-event
cap, and values a directive requested that the record did not carry. They
reduce what a record says; they never discard one.

The first three are policy doing what you configured it to do. `missing` is
not: a transform's `log` directive asked for a column and the record did not
have one. Most such requests are refused when the pipeline compiles (E374).
What reaches this counter is the case the planner cannot decide — a selector
naming a column that arrives through an open composition port. It is credited
where the signal is built, so under a sampling policy it sees one miss per
sampled event rather than one per record: read it as "this is happening", not
as how often.

`arena_recoveries` is neither a drop nor a quantity of anything lost. It counts
the times the arena resumed from a poisoned lock — telemetry panicked while
holding its own guard, and the arena carried on rather than taking the run down
with it. A non-zero value says every counter beside it was produced by a
subsystem that faulted mid-run. Treat it as a defect report against Clinker,
and read the run's other telemetry numbers with that in mind; the pipeline's
own results are unaffected, because telemetry never changes them.

#### Reconciling admission against export

Every signal counted under `dropped` is one the collector never saw. A
delivery accounts for every item in its chunk — a chunk travels whole, so
`accepted + rejected` equals the items handed to the exporter whatever the
outcome was. That gives an exact identity for a run where `flush_complete` is
`true`:

```
admission.accepted
  = (logs.accepted   + logs.rejected)
  + (traces.accepted + traces.rejected)
  + admission.dropped.undecodable
  - 1
```

The `- 1` is the run-lifecycle span, which the exporter synthesizes at the
final flush rather than drawing from the arena. `metrics` has no term because
metric points are not admitted signals. When `flush_complete` is `false` the
export counters are a partial accounting by definition and the identity does
not apply; `admission.counts_complete` is `false` on that same run, because the
arena side was read while the detached exporter was still draining it.

Any shortfall against that is arena loss, and `dropped` says which kind.

#### Why the lanes are split

`sampled` and `queue_full` are the two refusals that can cost an `error` while
the ordinary lane is the thing under pressure, so both are attributed per lane.
That is what makes the sampling guarantee above checkable from a run's own
accounting: with `sample_every = 10`, `lanes.high_severity.sampled` holds at
one in ten of the high-severity signals produced however far
`lanes.ordinary.sampled` climbs beside it. A single total cannot show that, and
an author reading one would have no way to tell what share of their errors
survived.

`rate_limited` and `contended` have no per-lane spelling. The rate limiter and
the arena lock are properties of the shared arena rather than of a lane.

`retained_bytes` is what the lane still held when the run finished, and
`capacity_bytes` is its reservation. Note that `max_batch_bytes` is the
per-slot bound, so a lane holds `lane_bytes / max_batch_bytes` signals between
drains — that ratio, not the byte size alone, is what `queue_full` is measured
against.

### Telemetry loss without `--machine`

A run without `--machine ndjson-v1` discards the terminal object entirely, so
when anything was dropped Clinker writes one line to standard error:

```
clinker: telemetry admission outcome: accepted=9 dropped=8 sampled=8 rate_limited=0 queue_full=0 contended=0 oversize=0 invalid_identity=0 undecodable=0 ordinary_sampled=4 ordinary_queue_full=0 high_sampled=4 high_queue_full=0 missing_fields=0 arena_recoveries=0 counts_complete=true
```

It mirrors the lineage delivery line, including its suppression rule: a run
that dropped nothing prints nothing. A line that appeared on every run reading
all zeroes is noise an operator learns to skip, and the one run that did lose
signals would be skipped with it.

`missing_fields` and `arena_recoveries` break that silence on their own. Both
sit outside `dropped`, and neither is anything an operator asked for: an
attribute the collector never received, and telemetry having panicked under its
own guard. The `denied` and `truncated` field counters stay silent by contrast,
because they are policy doing exactly what it was configured to do — and they
are not on this line at all.

The suppression is on the counters being final and clean, not on their reading
zero. A run whose flush expired prints the line whatever the numbers say, with
`counts_complete=false`: all-zero counts taken mid-drain are not evidence of a
clean run, and staying silent on them would report a run that may well have
lost signals as one that certainly did not.

Like the lineage line, this is an observation. Telemetry loss does not change
execution, publication, the machine terminal result, or the exit status.

### Runtime ownership and failure isolation

The deployment path keeps capability ownership narrow:

| Boundary | Owned capability |
|---|---|
| Workspace plan/config | Secret-free raw endpoint text plus numeric, capacity, retry, and deadline bounds. |
| Network | The sole endpoint admission, a private admitted-endpoint proof, fixed OTLP signal routes, and transport. |
| Executor | The real log, metric, and trace producers plus the fixed-memory telemetry arena. |
| Lineage | Canonical/catalog dataset identity, authorized subset and symlink facts, and independently bounded event delivery. |
| CLI | Pre-effect composition, one immutable lifecycle-fact source, worker lifecycle, and separate typed delivery outcomes. |

The workspace policy loader owns only the secret-free raw endpoint string.
For an enabled run, the CLI's first capability transition calls the network
crate's sole endpoint-admission API. That API accepts one HTTPS origin and
derives exactly three routes: `/v1/logs`, `/v1/metrics`, and `/v1/traces`.
Relative, malformed, HTTP, credential-bearing, path-bearing, query-bearing,
fragment-bearing, and already signal-specific endpoint text is rejected — and
so is text that is not exactly the origin it names: surrounding whitespace, or
any embedded control character such as a carriage return, is refused rather
than trimmed or ignored, so no endpoint value can smuggle a header into a
request. Each is rejected as
`observability.otlp.endpoint` with a pasteable HTTPS-origin correction before
source discovery, output attempts, arena reservation, worker construction, or
network effects. Rejected text is not echoed.

After admission, the CLI combines the admitted origin with the configured
request, retry, response, arena, and flush bounds in one immutable run-local
bundle. `auth.mode = "none"` is the supported production capability today and
sends no credential headers. `auth.mode = "reference"` remains a logical,
secret-free policy name, but the run fails before exporter effects until the
Phase 4 AUTH-01 credential applicator supplies that capability; the
applicator will not be allowed to change the admitted origin or fixed routes.

Logs, metrics, and traces share one finite telemetry arena and exporter worker,
but retain distinct typed per-signal delivery outcomes and fixed aggregate
counters. Those producers are the executor's actual lifecycle, runtime, and
terminal producers; the transport does not invent equivalent events. The
OpenLineage worker has its own queue, byte cap, sink, deadline, counters, and
typed outcome. The arena allocation and both worker spawns complete before
source discovery, staging, publication-attempt creation, sink writes, or a
lineage `START`; inability to create either worker fails admission without
those effects as `observability.delivery.failed`, with exit `4` and
`retry_with_backoff`. Invalid endpoint, authentication, identity, and bounds
policy remains `observability.configuration.invalid` with `do_not_retry`.
Both paths copy the same batch ID, execution ID, semantic-plan
algorithm/version/digest, and terminal facts from one immutable lifecycle
snapshot; neither path reconstructs or owns those facts. Collector partial
acceptance, rejection, transport failure, shutdown, or flush expiry, and
lineage drop, sink failure, or deadline expiry remain optional observations:
they do not change final or DLQ bytes, process status, the machine terminal
result, publication inventory, visible finals, or retained failed-attempt
evidence. The machine terminal exposes aggregate per-signal counters only; it
does not flatten or replace either typed delivery outcome.

Field policy is applied before telemetry enters the arena. Denied values never
reach Collector request bodies, OpenLineage events, counters, diagnostics, or
machine records. With no `[observability]` table, Clinker performs no endpoint
admission, arena reservation, worker creation, or exporter I/O.

External `--lineage` and `--lineage-events` exports serialize each complete
OpenLineage event within `lineage.max_event_bytes` before attempting immediate
admission to the byte-bounded lineage queue. A full queue drops the newest
event instead of delaying the finite job. One lineage-only synchronous worker
owns the destination and receives no output, DLQ, publication, or machine-mode
authority. At completion Clinker waits no longer than
`lineage.flush_timeout_ms`; dropped events, write or flush failure, and a
deadline-exceeded worker are reported separately on standard error and do not
change the authoritative ETL/publication result. The explicit
`local_diagnostic_paths` compatibility mode remains a local synchronous file
or console export and cannot use this external delivery path.

### Exported signal shapes

Clinker speaks OTLP/JSON over the three fixed routes. What it puts on the wire:

**Traces.** One run is one trace. The lifecycle span `clinker.run` is the trace
root and every Transform span is a child of it, so a collector can reconstruct
the whole run from any single span. Each span carries a trace id, a span id, and
both `startTimeUnixNano` and `endTimeUnixNano`.

A Transform is one span, emitted when the Transform finishes and covering the
interval it ran for. It is not a `start` record followed by an `end` record.
That shape was not exportable: a span requires both timestamps, so a
`start`-only record is not a valid span, and two independently admitted halves
can be sampled or dropped separately, leaving a collector holding one half of a
pair it cannot use. If you want to observe that a Transform has begun while it
is still running, that signal is the `clinker.transform.started` metric below,
which is recorded before the work runs and exported on the normal metric
cadence — the span's `startTimeUnixNano` then tells you exactly when it began.

**Metrics.** The Transform counters — `clinker.transform.started`,
`clinker.transform.completed`, `clinker.transform.records`, and
`clinker.transform.errors` — are exported as monotonic sums with **delta**
aggregation temporality. Each exported point is the count accumulated since the
previous export, not a running total, and carries the `startTimeUnixNano` and
`timeUnixNano` bounding that interval. Sum the deltas to get the run total;
reading any single point as an absolute value will understate the run, often by
a large factor on a long one.

An instrument that recorded nothing in an interval carries no points for it.
That is an ordinary interval, not a malformed export: the batch is delivered
and the points its other instruments did record arrive intact.

**Transforms that a correlated commit re-runs.** A pipeline with a relaxed
correlation-key aggregate converges at commit time: the engine re-runs the
transforms downstream of that aggregate, retracting the rows that turned out to
fail, until the result stops changing. Only the converged result is published,
and the exported signals describe it the same way — one span covering the whole
convergence, one `clinker.transform.started`, one `clinker.transform.completed`,
and record and error counts taken from the converged pass rather than added up
across the discarded ones. An `every:` cadence continues across the passes
rather than restarting on each. So a transform inside a convergence reports the
rows the run actually carried, and its counters stay summable alongside every
other transform's.

A convergence that does not finish — a failure in one of the re-run transforms,
or an interrupting signal — still reports every transform it had passed over.
Each gets one span with an `ERROR` status covering the interval it ran for, one
`clinker.transform.started`, and the record and error counts the interrupted
pass had reached. It gets no `clinker.transform.completed`: nothing completed,
and that counter is what tells you whether everything did.

**Logs.** Each emission of an authored `log:` directive becomes one OTLP log
record: `severityText` from the directive's `level`, the directive's `message`
as the body, the event name as the `clinker.event` attribute, the three
run-correlation attributes described under
[Authentication and privacy](#authentication-and-privacy), and whichever
requested record fields the field policy allowed, hashed, or replaced.

**Request size.** Delivery is bounded per request, not per record. A drained
batch that would exceed one request's byte budget is split across as many
requests as it needs rather than discarded. `max_batch_bytes` bounds one stored
record inside the arena; it is not the request size, and the two are not the
same number.

### Authentication and privacy

Authentication is always explicit. Credential-free delivery uses exactly:

```toml
[observability.otlp.auth]
mode = "none"
```

Referenced delivery retains one provider-neutral logical name:

```toml
[observability.otlp.auth]
mode = "reference"
reference = "telemetry/production"
```

The reference is not a credential. A later run-local authentication provider
must resolve it before effects. Inline headers, bearer/basic values,
environment-variable names, and mixed auth variants are rejected; omission
does not mean anonymous delivery. Diagnostics name the authored field and show
a safe corrected table without echoing an endpoint, credential value, record
value, or physical path.

Event fields are denied by default. Each `[[observability.field_policy]]`
entry selects exactly one dotted event/field pair and one `allow`, `hash`, or
`replace` action. `replacement` is required only for `replace`, and duplicate
rules for the same pair are invalid. A `replacement` is written verbatim into
the exported record, so it is held to the same shape as the other authored
strings here: non-empty, bounded, with no surrounding whitespace and no
embedded control character.

Field policy governs **record fields** — values a Transform selected out of the
data being processed. It does not govern run correlation. Every exported log
record carries `clinker.execution_id`, `clinker.batch_id`, and
`clinker.pipeline_name` unconditionally, with no `[[observability.field_policy]]`
entry required for them.

The reason is worth stating plainly, because the opposite behaviour was a
defect rather than a policy: those three values are identifiers Clinker
generates for the run itself, not data read from a source. A record-field
privacy policy has nothing to decide about them. Subjecting them to it meant a
workspace that declared an event without also writing three correlation rules
exported log records with no correlation at all — telemetry that could not be
joined to the machine stream's `execution_id`, to the lineage events, or to
another run of the same pipeline — and it counted all three as privacy denials
on every event, inflating the arena's denied-field accounting by three per
event. Correlation is now carried outside the policy entirely, so neither
happens.

A `[[observability.field_policy]]` rule whose `field` happens to be named
`execution_id`, `batch_id`, or `pipeline_name` still parses. It governs a
*record column* of that name, if a directive requests one; it has no effect on
the correlation attributes above.

### Lineage identity

`identity_mode = "external"` is the default. Every externally emitted source
or output node needs exactly one binding: either `canonical_datasource`, or
the complete `catalog_namespace`/`catalog_name` pair. Missing, duplicate,
partial, and mixed bindings fail validation; Clinker does not synthesize an
external identity from a working directory, worker path, temporary root, URL,
attempt identifier, or path hash.

The stable collection identity remains the dataset namespace/name. When the
runtime has explicitly authorized a concrete logical partition or location,
lineage represents it with the standard input/output subset facet rather than
changing that collection name. Explicitly authorized aliases use the standard
symlinks facet. The current resolved workspace config has no author-facing
subset or symlink fields, so Clinker does not infer either fact from local
paths, attempt directories, hashes, or process context.

The only path-derived compatibility mode is the exact, explicit value below.
It accepts no external dataset bindings and is for labeled local diagnostics,
not external delivery:

```toml
[observability.lineage]
identity_mode = "local_diagnostic_paths"
```

## Operational recommendations

- **Always enable metrics in production.** The overhead is negligible (one small JSON write at the end of each run).
- **Run `metrics collect --delete-after-collect` on a schedule** (e.g., hourly) to prevent spool directory growth.
- **Use `--batch-id`** with meaningful identifiers to correlate metrics across retries and environments.
- **Alert on `records_dlq > 0`** to catch data quality regressions early.
- **Track `peak_rss_bytes` trends** to anticipate when memory limits need adjustment.
