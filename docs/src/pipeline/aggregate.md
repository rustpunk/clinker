# Aggregate Nodes

Aggregate nodes group records by one or more fields and compute summary values using CXL aggregate functions. They consume all input records in a group before emitting a single summary record per group.

## Basic structure

```yaml
- type: aggregate
  name: dept_totals
  input: employees
  config:
    group_by: [department]
    cxl: |
      emit total_salary = sum(salary)
      emit headcount = count(*)
      emit avg_salary = avg(salary)
```

Group-by fields pass through automatically -- you do not need to emit them. In this example, the output records contain `department`, `total_salary`, `headcount`, and `avg_salary`.

## Group-by fields

The `group_by:` field is a list of field names from the input schema. Records sharing the same values for all group-by fields are placed in the same group.

```yaml
    group_by: [region, department]
    cxl: |
      emit total_salary = sum(salary)
      emit max_salary = max(salary)
```

This produces one output record per unique `(region, department)` combination.

## Global aggregation

An empty `group_by` list treats the entire input as a single group, producing exactly one output record:

```yaml
- type: aggregate
  name: grand_totals
  input: orders
  config:
    group_by: []
    cxl: |
      emit grand_total = sum(amount)
      emit record_count = count(*)
      emit avg_order = avg(amount)
```

## Aggregate functions

The following aggregate functions are available in CXL:

| Function | Description |
|----------|-------------|
| `sum(field)` | Sum of all values in the group |
| `count(*)` | Number of records in the group |
| `avg(field)` | Arithmetic mean |
| `min(field)` | Minimum value |
| `max(field)` | Maximum value |
| `collect(field)` | Collect all values into an array |
| `weighted_avg(value, weight)` | Weighted average |

## Strategy hint

The `strategy:` field controls how aggregation is executed:

```yaml
- type: aggregate
  name: totals
  input: sorted_data
  config:
    group_by: [account_id]
    strategy: streaming
    cxl: |
      emit total = sum(amount)
```

| Strategy | Behavior |
|----------|----------|
| `auto` | Default. The optimizer chooses based on whether the input is provably sorted for the group-by keys. |
| `hash` | Force hash aggregation. Works on any input ordering. Holds all groups in memory (with disk spill if memory budget is exceeded). |
| `streaming` | Require streaming aggregation. Processes one group at a time with O(1) memory per group. **Compile-time error** if the input is not provably sorted for the group-by keys. |

### When to use streaming

If your source declares a `sort_order:` that covers the group-by fields, the optimizer will automatically choose streaming aggregation. Use `strategy: streaming` as an explicit assertion -- it turns a silent fallback to hash aggregation into a compile error, which is useful for catching sort-order regressions.

### When to use hash

Hash aggregation works on unsorted input and is the safe default. It uses more memory but handles any data ordering. Memory-aware disk spill kicks in when RSS approaches the pipeline's `memory_limit`.

## Correlation-key interaction

In a pipeline whose sources declare `correlation_key:` fields, the engine inspects each aggregate's `group_by` against the upstream CK lattice (the union of `$ck.*` shadow columns visible at the aggregate's input):

- `group_by` covers every upstream CK field — strict-collateral path. The aggregate emits one row per group, the row inherits the correlation identity of its inputs, and a DLQ trigger anywhere in the group rolls back the whole group including the aggregate output. Zero retraction overhead.
- `group_by` omits any upstream CK field — retraction protocol path. A single correlation group may span multiple aggregate groups; CK fields omitted from `group_by` stop being visible to downstream consumers of this aggregate's output as user-named columns. The engine retracts only the failing records and refinalizes affected groups.

Authors do not configure this — the engine selects the path automatically based on `group_by` content. A retraction-mode aggregate is incompatible with `strategy: streaming` (rejected with `E15Y`, because streaming aggregates emit at group-boundary close before the terminal correlation commit and that defeats the rollback window). See [Correlation Keys](correlation-keys.md#aggregate-interaction) for the full lattice rules.

A retraction-mode aggregate emits one engine-managed `$ck.aggregate.<name>` shadow column on its output schema, alongside `[group_by_columns] ++ [emitted_binding_columns]`. The column carries the aggregator's per-group index at finalize and costs ~16 bytes per emitted row (the `Value::Integer` payload plus its slot overhead); it is hidden from default writer output. The synthetic column is the lineage hook that lifts the post-aggregate retract path: a Transform or Output that fails on an aggregate output row carries the column on the failing record, the orchestrator's detect phase decodes the index back to the contributing source row ids, and the recompute phase retracts those source rows so the failing aggregate row's contributors are removed from the writer payload — matching the upstream-failure DLQ-fan-out semantic. See [Correlation Keys → Where retraction triggers are sourced](correlation-keys.md#where-retraction-triggers-are-sourced) and the runnable demo at [`examples/pipelines/retract-demo/`](https://github.com/rustpunk/clinker/tree/main/examples/pipelines/retract-demo).

The retraction protocol carries a per-aggregate cost — Reversible accumulators use a per-row lineage map, BufferRequired accumulators hold raw contributions until commit. Both paths additionally pay ~16 bytes per output row for the synthetic-CK shadow column. The [operator-by-operator retraction cost reference](correlation-keys.md#operator-by-operator-retraction-cost-reference) has the per-operator breakdown; `clinker run --explain` reports the live per-aggregate detail including the synthetic-CK line.

## Time-windowed aggregates

When `time_window:` is set on the aggregate body, the operator
groups records not just by `group_by` but also by *event-time
window*. Each record is assigned to one or more windows by the
engine-stamped [`$source.event_time`](../cxl/system-variables.md)
column; state accumulates per `(group_by, window)`; a window closes
once `min_across_sources >= window_end + allowed_lateness` and emits
one row per group it saw. The shape parallels Flink SQL
[Window TVFs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-tvf/),
Spark Structured Streaming
[window / session_window](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time),
and Beam
[windowing](https://beam.apache.org/documentation/programming-guide/#windowing).

Every upstream-reachable source must declare a
[`watermark:`](source.md#watermarks). Otherwise `min_across_sources`
stays at `None`, no window ever closes, and the planner rejects the
pipeline with
[**E156**](../ops/exit-codes.md#plan-time-diagnostic-codes).

The engine emits user-declared columns only — window bounds do not
appear in the output unless you compute and emit them yourself. The
emit order is ascending `window_start` (deterministic), so output
rows naturally group by window.

### Tumbling windows

Non-overlapping fixed-size buckets. Each record lands in exactly one
window `[floor(t / size) * size, floor(t / size) * size + size)`.

```yaml
time_window:
  tumbling: { size: 1h }
```

**Input** (`tumbling_demo.csv`):

```csv
user_id,event_ts,kind
u1,2026-05-14T10:05:00,click
u2,2026-05-14T10:30:00,click
u1,2026-05-14T10:42:00,click
u1,2026-05-14T11:03:00,click
u2,2026-05-14T11:15:00,click
u2,2026-05-14T11:50:00,click
```

**Output** with `tumbling: { size: 1h }`, `group_by: [user_id]`,
`emit n = count(*)`:

```csv
user_id,n
u1,2
u2,1
u1,1
u2,2
```

Reading top-to-bottom: the first two rows are the `[10:00, 11:00)`
bucket (u1's 10:05 and 10:42, then u2's 10:30); the next two are
the `[11:00, 12:00)` bucket (u1's 11:03, then u2's 11:15 and 11:50).
Each input record contributes to exactly one window.

### Hopping windows

Overlapping fixed-size buckets advanced by `slide`. Each record
lands in `ceil(size / slide)` windows: `slide < size` produces
overlap, `slide == size` degenerates to tumbling, `slide > size`
produces gaps where some records fall in zero windows.

```yaml
time_window:
  hopping: { size: 1h, slide: 30m }
```

**Input** (`hopping_demo.csv`):

```csv
user_id,event_ts,amount
u1,2026-05-14T10:05:00,10
u1,2026-05-14T10:42:00,20
u1,2026-05-14T11:10:00,15
```

**Output** with `group_by: [user_id]`, `emit total = sum(amount)`,
`emit n = count(*)`:

```csv
user_id,total,n
u1,10,1
u1,30,2
u1,35,2
u1,15,1
```

Three input records, four output rows — each record fans into two
overlapping `size: 1h, slide: 30m` windows:

- `[09:30, 10:30)` — just 10:05 → `total=10, n=1`
- `[10:00, 11:00)` — 10:05 + 10:42 → `total=30, n=2`
- `[10:30, 11:30)` — 10:42 + 11:10 → `total=35, n=2`
- `[11:00, 12:00)` — just 11:10 → `total=15, n=1`

### Session windows

Per-key gap-bounded sessions. A new record extends its key's current
session if its event time is within `gap` of the session's last
event time; otherwise it starts a new session. The boundary is
data-driven, not clock-aligned.

```yaml
time_window:
  session: { gap: 10m }
```

**Input** (`session_demo.csv`):

```csv
user_id,event_ts,action
u1,2026-05-14T10:00:00,login
u1,2026-05-14T10:07:00,click
u1,2026-05-14T10:13:00,click
u1,2026-05-14T10:50:00,login
u1,2026-05-14T10:55:00,click
```

**Output** with `group_by: [user_id]`, `emit n = count(*)`:

```csv
user_id,n
u1,3
u1,2
```

`u1`'s first three rows form one session (10:00 → 10:07 → 10:13,
consecutive gaps ≤ 10m). The 37-minute idle stretch exceeds `gap`,
so 10:50 starts a fresh session that runs through 10:55. Two
sessions, two output rows.

### Allowed lateness

`allowed_lateness` is an operator-side knob, distinct from the
source-side `watermark.delay`. A window with `time_window:` closes
when `min_across_sources >= window_end + allowed_lateness`. Records
arriving after a window's `end + allowed_lateness` route to the DLQ
as `LateRecord` with stage label `time_window:<aggregate-name>`.
See [DLQ category: LateRecord](../ops/exit-codes.md#dlq-category-laterecord)
for the DLQ row layout.

```yaml
- type: aggregate
  name: hourly
  input: clicks
  config:
    group_by: [user_id]
    time_window:
      tumbling: { size: 1h }
    allowed_lateness: 30s
    cxl: |
      emit n = count(*)
```

Default (unset) means no grace beyond the watermark — windows close
the instant `min_across_sources` crosses `window_end`. Set
`allowed_lateness` when the source's `watermark.delay` alone is too
small to absorb the observed out-of-order tail.

### Worked example: multi-source session window

This pipeline merges two independent login feeds and groups per-user
events into gap-bounded sessions. Issue
[#61](https://github.com/rustpunk/clinker/issues/61) tracks the
multi-source synchronisation contract: a window cannot close until
**every** upstream source has advanced its watermark past
`window_end + allowed_lateness`.

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
```

Both sources declare their own `watermark.column` independently. At
ingest, each record gets the engine-stamped `$source.event_time`
column, so the aggregate is column-name-agnostic about which source
delivered any given record. The aggregate's close decision reads
`min_across_sources` across **both** sources' partitions: a session
cannot emit until both `src_web` and `src_mobile` have advanced past
the session's `end + allowed_lateness`. Drop the `watermark:` block
on either source and the planner rejects the pipeline with
[**E156**](../ops/exit-codes.md#plan-time-diagnostic-codes).

Run it from the repo:

```bash
cargo run -p clinker -- run examples/pipelines/multi_source_session.yaml
```

## Complete example

```yaml
- type: source
  name: transactions
  config:
    name: transactions
    type: csv
    path: "./data/transactions.csv"
    schema:
      - { name: account_id, type: string }
      - { name: txn_date, type: date }
      - { name: amount, type: float }
      - { name: category, type: string }
    sort_order:
      - { field: "account_id", order: asc }

- type: aggregate
  name: account_summary
  input: transactions
  config:
    group_by: [account_id]
    strategy: streaming
    cxl: |
      emit total_amount = sum(amount)
      emit txn_count = count(*)
      emit avg_amount = avg(amount)
      emit max_amount = max(amount)
      emit categories = collect(category)

- type: output
  name: summary_output
  input: account_summary
  config:
    name: summary_output
    type: csv
    path: "./output/account_summary.csv"
```
