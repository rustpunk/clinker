# System Variables

CXL provides several system variable namespaces prefixed with `$`. These give CXL expressions access to pipeline execution context, user-defined variables, per-record metadata, and the current time.

## $pipeline.* -- Pipeline context

Pipeline variables are accessed via `$pipeline.member_name`. Some are frozen at pipeline start; others update per record.

### Stable (frozen at pipeline start)

| Variable | Type | Description |
|----------|------|-------------|
| `$pipeline.name` | String | Pipeline name from YAML config |
| `$pipeline.execution_id` | String | UUID v7, unique per pipeline run |
| `$pipeline.batch_id` | String | From `--batch-id` CLI flag, or auto-generated UUID v7 |
| `$pipeline.start_time` | DateTime | Frozen at pipeline start, deterministic within a run |

```bash
$ cxl eval -e 'emit name = $pipeline.name' \
    -e 'emit exec = $pipeline.execution_id'
```

```json
{
  "name": "cxl-eval",
  "exec": "00000000-0000-0000-0000-000000000000"
}
```

### Counters

| Variable | Type | Description |
|----------|------|-------------|
| `$pipeline.total_count` | Int | Total records processed so far |
| `$pipeline.ok_count` | Int | Records that passed successfully |
| `$pipeline.dlq_count` | Int | Records sent to dead-letter queue |
| `$pipeline.filtered_count` | Int | Records excluded by `filter` statements |
| `$pipeline.distinct_count` | Int | Records excluded by `distinct` statements |

```
trace info if $pipeline.total_count % 10000 == 0 then "processed " + $pipeline.total_count.to_string() + " records"
```

## $source.* -- Per-record source lineage

`$source.*` exposes engine-stamped columns that travel with every
record from its origin Source node downstream through merges,
combines, and transforms. They identify *where the record came
from* and *when in event-time it happened*. All three columns are
filtered out of default Output projections — reference them
explicitly with `emit` if you need them in your output schema.

| Variable | Type | Description |
|----------|------|-------------|
| `$source.file` | String | Path of the input file the current record was read from. |
| `$source.name` | String | Name of the Source node that produced the current record. Survives through `merge` / `combine` so downstream nodes can branch on origin. |
| `$source.event_time` | DateTime | Engine-stamped event time, delay-corrected by the source's `watermark.delay`. `Null` when the source has no `watermark:` block, or when the per-record value did not parse. |

```
filter $source.name == "src_web"
emit origin = $source.name
emit ingest_file = $source.file
emit ts = $source.event_time
```

`$source.event_time` is the column a
[time-windowed aggregate](../nodes/aggregate.md#time-windowed-aggregates)
reads to assign records to windows. It is only populated for
records from a source that declares
[`watermark:`](../nodes/source.md#watermarks) — otherwise it
holds `Null`.

## $vars.* -- User-defined variables

User-defined variables are declared in the YAML pipeline config under `pipeline.vars:` and accessed via `$vars.name` in CXL expressions.

### YAML declaration

```yaml
pipeline:
  name: invoice_processing
  vars:
    high_value_threshold: 10000
    tax_rate: 0.21
    output_currency: "USD"
    fiscal_year_start_month: 4
```

### CXL usage

```
filter amount > $vars.high_value_threshold
emit tax = amount * $vars.tax_rate
emit currency = $vars.output_currency
```

Variables provide a clean way to externalize configuration from CXL logic. Combined with [channels](../pipelines/channels.md), different variable sets can parameterize the same pipeline for different environments or clients.

## $record.* -- Per-record scoped state

`$record.*` is a per-record key-value store that travels with the record through the pipeline but never serializes as an output column. It is the mechanism for tagging records with quality flags, routing hints, or audit information that should not appear in the final output unless explicitly re-emitted as a regular column.

Each `$record` variable is declared in the writing Transform's `config.declares:` block (`scope: record`) and written from that Transform's CXL:

### Writing record state

```yaml
- type: transform
  name: classify
  input: orders
  config:
    declares:
      - { name: quality, scope: record, type: string }
    cxl: |
      emit order_id = order_id
      emit $record.quality = if amount < 0 then "suspect" else "ok"
```

### Reading record state

Any downstream node reads it via `$record.<key>`:

```
filter $record.quality == "ok"
emit audit_quality = $record.quality
```

See [Scoped Variables](../pipelines/variables.md) for the full declaration model and the pipeline / source / record lifetimes.

## now -- Current time

The `now` keyword returns the current wall-clock time as a DateTime value. It is evaluated fresh per record, so each record gets the actual time of its processing.

```bash
$ cxl eval -e 'emit timestamp = now'
```

```json
{
  "timestamp": "2026-04-11T15:30:00"
}
```

`now` is useful for timestamping records:

```
emit processed_at = now
emit days_old = now.diff_days(created_date)
```

> **Note:** `now` is a keyword, not a function call. Write `now`, not `now()`.

## Complete example

```yaml
pipeline:
  name: order_enrichment
  vars:
    discount_threshold: 500
    tax_rate: 0.08

  nodes:
    - name: orders
      type: source
      format: csv
      path: orders.csv

    - name: enrich
      type: transform
      input: orders
      cxl: |
        emit order_id = order_id
        emit amount = amount
        emit discount = if amount > $vars.discount_threshold then 0.1 else 0.0
        emit tax = amount * $vars.tax_rate
        emit total = amount * (1 - discount) + tax
        emit processed_at = now
        emit source_file = $source.file
        emit pipeline_run = $pipeline.execution_id

    - name: output
      type: output
      input: enrich
      format: csv
      path: enriched_orders.csv
```
