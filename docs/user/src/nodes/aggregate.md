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

Hash aggregation works on unsorted input and is the safe default. It uses more memory but handles any data ordering. Memory-aware disk spill kicks in when RSS approaches the pipeline's `memory.limit`.

## Correlation-key interaction

When a pipeline's sources declare `correlation_key:` fields, an aggregate behaves one of two ways depending on its `group_by`:

- **`group_by` covers the correlation key** — if any record in a group fails, the whole group (including the aggregate output row) is sent to the DLQ.
- **`group_by` omits a correlation-key field** — only the failing records are dropped and the affected groups are recomputed, so the surviving rows still produce a correct aggregate.

You do not configure this; the engine picks the behavior from your `group_by`. The one restriction is that the second case cannot be combined with `strategy: streaming`. See [Correlation Keys](../pipelines/correlation-keys.md#aggregate-interaction) for the full rules.

## Time-windowed aggregates

When `time_window:` is set on the aggregate body, records are grouped
not just by `group_by` but also by *event-time window*. Each record is
placed into one or more windows by its event time (the
[`$source.event_time`](../cxl/system-variables.md) value derived from
the source's watermark), and each window emits one row per group once
it closes.

Every upstream-reachable source must declare a
[`watermark:`](source.md#watermarks) so the engine knows when a window
is complete. Without one, no window ever closes and the planner rejects
the pipeline with
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

`allowed_lateness` extends how long a window stays open past its end
before it emits, giving late-arriving records a grace period to still
be counted. It is distinct from the source-side `watermark.delay`.
Records that arrive after a window's `end + allowed_lateness` route to
the DLQ as `LateRecord` with stage label `time_window:<aggregate-name>`.
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
events into gap-bounded sessions. When several sources feed one
time-windowed aggregate, a window cannot close until **every** source
has advanced past the window's end — the slowest source paces the
others, so no window emits before all of its records have arrived.

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

Both sources declare their own `watermark.column` independently, and
each source's records carry an event time regardless of which feed
delivered them — so the aggregate does not care which column name a
given source used. A session cannot emit until both `src_web` and
`src_mobile` have advanced past the session's end plus its
`allowed_lateness`. Drop the `watermark:` block on either source and
the planner rejects the pipeline with
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
    type: json
    path: "./output/account_summary.json"
```

The output is JSON because `collect(category)` emits an array. The
CSV, XML, and fixed-width writers reject an array-valued field (a
stray collection reaching a tabular sink is treated as a routing
bug); route such a pipeline to JSON, which serializes arrays
natively, or coerce the array to a scalar first with a downstream
`Transform` (for example `emit categories = categories.join(";")`).
