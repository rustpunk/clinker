# Aggregation & Rollups

This recipe demonstrates grouping records and computing summary statistics. The pipeline filters active sales records, then rolls them up by department.

## Input data

`sales.csv`:

```csv
id,department,amount,status,rep
1,Engineering,5000,active,Alice
2,Marketing,3000,active,Bob
3,Engineering,7000,active,Carol
4,Sales,4000,inactive,Dave
5,Marketing,2000,active,Eva
6,Engineering,9500,active,Frank
7,Sales,6000,active,Grace
8,Marketing,1500,inactive,Hank
```

## Pipeline

`dept_rollup.yaml`:

```yaml
pipeline:
  name: dept_rollup

nodes:
  - type: source
    name: sales
    config:
      name: sales
      type: csv
      path: "./sales.csv"
      schema:
        - { name: id, type: int }
        - { name: department, type: string }
        - { name: amount, type: float }
        - { name: status, type: string }
        - { name: rep, type: string }

  - type: transform
    name: active_only
    input: sales
    config:
      cxl: |
        filter status == "active"

  - type: aggregate
    name: rollup
    input: active_only
    config:
      group_by: [department]
      cxl: |
        emit total = sum(amount)
        emit count = count(*)
        emit average = avg(amount)
        emit maximum = max(amount)
        emit minimum = min(amount)

  - type: output
    name: report
    input: rollup
    config:
      name: dept_totals
      type: csv
      path: "./output/dept_totals.csv"
```

## Run it

```bash
clinker run dept_rollup.yaml --dry-run
clinker run dept_rollup.yaml
```

## Expected output

`output/dept_totals.csv`:

```csv
department,total,count,average,maximum,minimum
Engineering,21500,3,7166.67,9500,5000
Marketing,5000,2,2500,3000,2000
Sales,6000,1,6000,6000,6000
```

One row per department. The inactive records (Dave's $4000, Hank's $1500) are excluded by the filter.

## How aggregation works

### Group-by keys

The `group_by` field lists the columns that define each group. Records with the same values for all group-by columns are aggregated together. The group-by columns appear automatically in the output -- you do not need to emit them.

### Aggregate functions

Available aggregate functions in CXL:

| Function | Description |
|----------|-------------|
| `sum(expr)` | Sum of values |
| `count(*)` | Number of records |
| `avg(expr)` | Arithmetic mean |
| `min(expr)` | Minimum value |
| `max(expr)` | Maximum value |
| `first(expr)` | First value encountered |
| `last(expr)` | Last value encountered |

### Per-document aggregation

Per-document aggregation works after **any upstream that forwards
document boundaries** — a document-aware source (a `glob:` / `paths:`
source that treats each file as its own document, or an enveloped format
like XML or EDI), a `Merge`, or a `Combine`. The Aggregate produces **one
set of grouped rows per document** rather than a single aggregate
spanning every file. Each document's groups finalize and emit at that
document's close boundary, so a glob over twelve monthly files yields
twelve independent monthly roll-ups. A plain single-file source is one
document and still emits a single aggregate. This holds **whether the
Aggregate's upstream streams or materializes** its output — both flush
per document.

A `Merge` of distinct single-document sources forwards each source's
per-document close on **every** mode — `concat`, seeded `interleave`, and
the fused unseeded all-Source `interleave` fast path — so each source
flushes its own roll-up. And per-document aggregation also works **after a
`Combine`** on any join
strategy — for example a driver `glob:` source joined to a lookup table,
then a group-by Aggregate, yields one roll-up per driver document:

```yaml
nodes:
  - type: source            # driver: each monthly file is a document
    name: orders
    config: { name: orders, type: csv, glob: "./orders/*.csv", schema: [ ... ] }
  - type: source            # small lookup table
    name: products
    config: { name: products, type: csv, path: "./products.csv", schema: [ ... ] }
  - type: combine
    name: enrich
    input: { orders: orders, products: products }
    config: { where: "orders.product_id == products.product_id", match: first, on_miss: skip, propagate_ck: driver, cxl: "..." }
  - type: aggregate
    name: monthly_totals    # one roll-up per driver document (per month)
    input: enrich
    config: { group_by: [category], cxl: "..." }
```

See [Envelopes & Document Context](../pipelines/envelope-and-doc-context.md#per-document-aggregation)
for the boundary rules across every `Merge` mode and `Combine` strategy.

### Strategy selection

Clinker offers two aggregation strategies:

- **Hash aggregation** (default): Builds an in-memory hash map keyed by the group-by columns. Works with any input order. Memory usage is proportional to the number of distinct groups.

- **Streaming aggregation**: Processes records in order, emitting each group's result as soon as the next group starts. Requires input sorted by the group-by keys. Uses minimal memory regardless of the number of groups.

The default strategy (`auto`) selects streaming when the optimizer can prove the input is sorted by the group-by keys, and hash otherwise. You can force a strategy:

```yaml
    config:
      group_by: [department]
      strategy: streaming   # requires sorted input
```

See [Memory Tuning](../ops/memory.md) for details on memory implications.

## Variations

### Multiple group-by keys

```yaml
    config:
      group_by: [department, region]
      cxl: |
        emit total = sum(amount)
        emit count = count(*)
```

Produces one row per unique (department, region) combination.

### Pre-aggregation transform

Compute derived fields before aggregating:

```yaml
  - type: transform
    name: prepare
    input: sales
    config:
      cxl: |
        filter status == "active"
        emit department = department
        emit amount = amount
        emit is_large = amount >= 5000

  - type: aggregate
    name: rollup
    input: prepare
    config:
      group_by: [department]
      cxl: |
        emit total = sum(amount)
        emit large_count = sum(if is_large then 1 else 0)
        emit small_count = sum(if not is_large then 1 else 0)
```

### Aggregation followed by routing

Aggregate first, then route the summary rows:

```yaml
  - type: aggregate
    name: rollup
    input: active_only
    config:
      group_by: [department]
      cxl: |
        emit total = sum(amount)

  - type: route
    name: split_by_total
    input: rollup
    config:
      mode: exclusive
      conditions:
        large: "total >= 10000"
      default: small
```

This routes departments with over $10,000 in total sales to one output and the rest to another.

### No group-by (grand total)

Omit `group_by` to aggregate all records into a single output row:

```yaml
    config:
      cxl: |
        emit grand_total = sum(amount)
        emit record_count = count(*)
        emit average_amount = avg(amount)
```

## Time-windowed rollups

To group records into event-time buckets, declare a
[`watermark:`](../nodes/source.md#watermarks) on every source
and a [`time_window:`](../nodes/aggregate.md#time-windowed-aggregates)
on the aggregate. Each window emits one rollup per group when it closes.
Three patterns cover the common shapes; all three
ship as runnable pipelines under `examples/pipelines/`.

### Tumbling: hourly click counts

Non-overlapping one-hour buckets per user. Use when each record
should contribute to exactly one reporting bucket.

`examples/pipelines/tumbling_clicks.yaml`:

```yaml
pipeline:
  name: tumbling_clicks

nodes:
  - type: source
    name: clicks
    description: Per-user click stream with an event-time column.
    config:
      name: clicks
      type: csv
      path: ./data/tumbling_clicks.csv
      options:
        has_header: true
      watermark:
        column: event_ts
      schema:
        - { name: user_id, type: string }
        - { name: event_ts, type: date_time }
        - { name: kind, type: string }

  - type: aggregate
    name: hourly_clicks
    description: Per-user click count, bucketed by event-time hour.
    input: clicks
    config:
      group_by: [user_id]
      time_window:
        tumbling: { size: 1h }
      cxl: |
        emit user_id = user_id
        emit n = count(*)

  - type: output
    name: results
    input: hourly_clicks
    config:
      name: results
      type: csv
      path: ./output/tumbling_clicks.csv

error_handling:
  strategy: fail_fast
```

Run:

```bash
cargo run -p clinker -- run examples/pipelines/tumbling_clicks.yaml
```

Each hour-aligned bucket emits one row per `user_id` once its time
window has passed. Records that arrive out-of-order land
in the DLQ as `late_record` — add `delay:` on the source or
`allowed_lateness:` on the aggregate if the input has a known
out-of-order tail.

### Hopping: 1-hour sums advanced every 5 minutes

Overlapping one-hour windows that move forward every 5 minutes. Use
for moving averages and rolling sums where one record should
contribute to multiple overlapping reports.

`examples/pipelines/hopping_sliding_5m_1h.yaml`:

```yaml
pipeline:
  name: hopping_sliding_5m_1h

nodes:
  - type: source
    name: clicks
    config:
      name: clicks
      type: csv
      path: ./data/hopping_clicks.csv
      options:
        has_header: true
      watermark:
        column: event_ts
        delay: 5s
      schema:
        - { name: user_id, type: string }
        - { name: event_ts, type: date_time }
        - { name: amount, type: int }

  - type: aggregate
    name: sliding_amount
    input: clicks
    config:
      group_by: [user_id]
      time_window:
        hopping:
          size: 1h
          slide: 5m
      allowed_lateness: 30s
      cxl: |
        emit user_id = user_id
        emit total = sum(amount)
        emit n = count(*)

  - type: output
    name: results
    input: sliding_amount
    config:
      name: results
      type: csv
      path: ./output/hopping_sliding_5m_1h.csv

error_handling:
  strategy: fail_fast
```

Run:

```bash
cargo run -p clinker -- run examples/pipelines/hopping_sliding_5m_1h.yaml
```

Each record fans into `ceil(size / slide) = 12` overlapping
windows, so the output row count is roughly 12× the active-window
record count. The source's `delay: 5s` plus the aggregate's
`allowed_lateness: 30s` give the pipeline 35 seconds of total grace
beyond strict event-time order before a record drops to the DLQ.

### Session: per-user multi-source login sessions

Variable-duration windows bounded by inactivity, computed across
two independent sources. Use for activity grouping where the
window length is data-driven rather than clock-aligned.

`examples/pipelines/multi_source_session.yaml`:

```yaml
pipeline:
  name: multi_source_session

nodes:
  - type: source
    name: src_web
    description: Web login events.
    config:
      name: src_web
      type: csv
      path: ./data/session_logins.csv
      options:
        has_header: true
      watermark:
        column: event_ts
      schema:
        - { name: user_id, type: string }
        - { name: event_ts, type: date_time }
        - { name: source, type: string }

  - type: source
    name: src_mobile
    description: Mobile login events.
    config:
      name: src_mobile
      type: csv
      path: ./data/session_mobile.csv
      options:
        has_header: true
      watermark:
        column: event_ts
      schema:
        - { name: user_id, type: string }
        - { name: event_ts, type: date_time }
        - { name: source, type: string }

  - type: merge
    name: all_logins
    inputs: [src_web, src_mobile]

  - type: aggregate
    name: user_sessions
    input: all_logins
    config:
      group_by: [user_id]
      time_window:
        session: { gap: 5m }
      allowed_lateness: 30s
      cxl: |
        emit user_id = user_id
        emit logins = count(*)

  - type: output
    name: results
    input: user_sessions
    config:
      name: results
      type: csv
      path: ./output/multi_source_session.csv

error_handling:
  strategy: fail_fast
```

Run:

```bash
cargo run -p clinker -- run examples/pipelines/multi_source_session.yaml
```

Each source declares its own `watermark.column` independently. A
session can't emit until both `src_web` and `src_mobile` have caught
up past `session_end + allowed_lateness`, so the rollup waits for the
slower source before closing. Drop the `watermark:` block on either
source and the pipeline is rejected at plan time with
[**E156**](../ops/exit-codes.md#plan-time-diagnostic-codes).

### When to pick each

| Kind | Bucket shape | Typical use |
|------|--------------|-------------|
| `tumbling` | Disjoint, clock-aligned, fixed width | Hourly metrics, daily rollups, billing periods. |
| `hopping` | Overlapping, clock-aligned, fixed width | Moving averages, sliding sums, anomaly detection where each record should affect multiple reports. |
| `session` | Variable width, gap-bounded, per-key | User sessions, telemetry burst grouping, activity envelopes where the window length is data-driven. |
