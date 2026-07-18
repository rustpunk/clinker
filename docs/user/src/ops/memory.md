# Memory Tuning

Clinker is designed to be a good neighbor on shared servers. Rather than consuming all available memory, it works within a configurable budget and reaches for back-pressure or disk spill before it runs out.

## The `memory:` block

All pipeline-level memory tuning lives under a single optional block:

```yaml
pipeline:
  name: my_pipeline
  memory:
    limit: "1G"          # optional — defaults to 512M
    backpressure: pause  # optional — defaults to pause
```

The entire block is optional. A pipeline with no opinions about memory writes nothing:

```yaml
pipeline:
  name: my_pipeline
```

…and gets the runtime defaults (512 MB hard limit, `backpressure: pause`).

Individual fields are also optional. Setting just one is fine:

```yaml
pipeline:
  name: my_pipeline
  memory:
    limit: "2G"
```

## Setting the memory limit

**CLI flag (highest priority):**

```bash
clinker run pipeline.yaml --memory-limit 512M
```

**YAML config:**

```yaml
pipeline:
  memory:
    limit: "512M"
```

When `--memory-limit` is passed it overrides `pipeline.memory.limit` for that run; omit the flag and the YAML value applies unchanged, falling back to the 512 MB default only when neither is set. An empty or whitespace-only flag value — as an ops wrapper produces when it forwards an unset variable, so `--memory-limit "$CLINKER_MEM"` expands to `--memory-limit ""` — is treated exactly like omitting the flag: the YAML value (or default) applies, rather than the run aborting. Suffixes are **binary (1024-based)**: `K` = 1024 bytes, `M` = 1024², `G` = 1024³; a bare integer is bytes. (This differs from the **decimal** `KB`/`MB`/`GB` used by `min_size`/`max_size`, which are 1000-based.)

**Default:** 512 MB.

**Invalid values:** the two entry points treat a malformed limit differently.
An empty or unparseable `memory.limit` *in the YAML* (for example a stray
non-numeric value) falls back to the 512 MB default. A *non-empty* malformed
`--memory-limit` *flag*, by contrast, is rejected up front with a config error
that names `--memory-limit` and echoes the value you passed — so a typo such as
the decimal `4GB` (the binary suffix is `4G`) fails loudly instead of silently
collapsing to the default and shrinking a larger budget set in your YAML. (An
empty or whitespace-only flag value is not malformed: it is treated as if the
flag were omitted, as noted above.)
Either way, a value whose size is well-formed but too large to represent — its
scaled byte count exceeds the maximum a 64-bit counter can hold — is rejected
rather than wrapping to a small budget (the YAML overflow error names
`memory.limit`; the flag overflow error names `--memory-limit`). Pick a limit
that fits your host's real memory.

A well-formed but *undersized* value is a different case: it is not a malformed
flag, so it clears the boundary check, and whether it aborts the run depends on
the `backpressure` policy. Under a producer-pausing policy (`pause`, the
default, or `both`) a value below the process's baseline resident memory is
rejected at startup by the budget gate as `E312`. Under the non-pausing `spill`
policy that startup gate does not fire: the run proceeds and relies on spilling
to stay within the budget rather than aborting. Because `--memory-limit` simply
populates `pipeline.memory.limit`, that `E312` — which names the limit and
echoes the offending byte value — refers to the same limit you passed via the
flag.

## Choosing a backpressure policy

When memory use approaches the limit (the soft threshold is 80 % of `limit`), something has to give up memory. The `backpressure` knob chooses what:

| Value | Behavior |
|-------|----------|
| `pause` (default) | Where possible, pause an upstream reader so it stops producing until pressure eases; when a paused reader is about to be needed, first spill downstream state and then proceed, so a pause never stalls the run. |
| `spill` | Never pause a producer — always free memory by spilling a stage to disk. |
| `both` | Pause where possible, otherwise spill whichever stage is holding the most memory. |

`pause` is the right default for most pipelines: pausing a fast Source feeding a slow downstream stage is cheaper than writing its buffered records to disk. Reach for `spill` or `both` only when you have a specific reason to prefer a different posture — for example, `both` when one large stage dominates the budget and you want it spilled first.

### How pause and resume work

Under `pause` (and `both`), a producer paused because memory crossed the soft threshold is **resumed automatically once memory recedes** — it is never left parked. Pause and resume use two watermarks to avoid flapping (a *hysteresis band*):

- **Pause** when live memory rises above the **soft threshold**, `0.80 × limit`.
- **Resume** when live memory falls back below the lower **resume watermark**, `resume_threshold × limit` (default `0.70 × limit`).

Between the two watermarks nothing changes, so a normal batch-to-batch swing in memory cannot make a producer flap between paused and resumed on every poll.

A paused reader also never blocks the run. When the engine reaches a stage that needs a paused reader's records, it first sheds reclaimable downstream state to disk and then resumes the reader and proceeds — so `pause` throttles producers under pressure but degrades to spill-and-continue at the point it would otherwise wait, rather than stalling.

### `resume_threshold`

`resume_threshold` tunes the low watermark of that band, as a fraction of the hard `limit`:

```yaml
pipeline:
  memory:
    limit: "1G"
    resume_threshold: 0.65   # optional — defaults to 0.70
```

- **Default:** `0.70` (omit the field to take it).
- **Valid range:** strictly greater than `0` and strictly less than the `0.80` soft threshold, so the resume point always sits below the pause point. A value outside `(0, 0.80)` — including `0.0`, a negative, or anything `≥ 0.80` — is rejected at plan time with `E324`. A misspelled key is rejected as an unknown field.
- **Lower** widens the band: a paused producer stays paused longer (smoother, but slower to re-open).
- **Higher** narrows the band: faster to re-open, but more prone to flapping.

Only the pausing policies (`pause`, `both`) use this watermark; under `spill` no producer is ever paused, so it has no effect.

## Streaming batch size (`batch_size`)

`pipeline.batch_size` sets how many events (records plus document-boundary punctuations) a streaming-eligible stage hands off to its downstream consumer at a time over a back-pressured channel. For a fused stage (Source → Transform → Output, Merge.interleave of Sources) it bounds the in-flight working set to one batch rather than the whole stage, because the stage pulls records off a live upstream channel without ever building a full result. The other streaming stages build their full result first and stream it in batches; there the knob sizes only the inter-stage slice, not the producer's footprint. The knob is optional; omit it to use the built-in default of 2048 events. See [Streaming vs. Blocking Stages](streaming-vs-blocking.md) for the distinction.

```yaml
pipeline:
  name: orders_rollup
  batch_size: 1024          # optional; default 2048
```

A per-transform override is available on a Transform's `config.batch_size` (see [Transform Nodes](../nodes/transform.md#batch-size-batch_size)); it takes precedence over the pipeline value for that one stage. A `batch_size` of `0` is rejected at config load. The knob affects only the memory *profile* of streaming stages, never their output — blocking stages (sort, hash Aggregate, Combine build side) ignore it and continue to fully materialize. See [Streaming vs. Blocking Stages](streaming-vs-blocking.md) for the full model.

## Behavior under memory pressure

You don't manage memory by hand — the engine does it within the budget you set. What this means in practice:

- **Spillable stages always complete if disk space is available, regardless of input size.** When a blocking stage (sort, hash Aggregate, large Combine) outgrows the budget, it spills to disk instead of failing.
  - **Range and equality+range Combine also spill.** A Combine whose `where:` joins the two inputs on an inequality (`<`, `<=`, `>`, `>=`) — a pure band join such as `orders.amount >= bands.lo and orders.amount < bands.hi`, or an equality-plus-range join such as `orders.region == bands.region and orders.amount >= bands.lo` — runs the block-band strategy, which is spill-bounded on both axes: it external-sorts each input side to disk and accumulates its matched output in a spillable sort as well, so it completes on inputs and result sizes larger than the budget. When an equality key is present, records are grouped by that key (via its hash) before the range walk and only same-key pairs are joined; a single very common key is spread across many disk-backed blocks and joined a bounded pair at a time, so even a heavily skewed key stays within the budget. The join still aborts with `E310 MemoryBudgetExceeded` only as a last resort — when a single indivisible unit of work (one pair of input blocks plus the join's scratch arrays) cannot fit the hard limit even on its own. If you hit that, give the pipeline more headroom or narrow the predicate.
- **Performance degrades gracefully.** Under pressure you'll see slower execution and possibly disk I/O — not a crash.
- **The limit is a soft ceiling, not a hard wall.** Momentary spikes may briefly exceed it before the engine reacts. Only if memory blows past the limit outright does the run abort with `E310 MemoryBudgetExceeded`, which names the stage that overran.
- **A few handoff buffers are memory-only and can trip `E310`.** A stage that fans out to more than one consumer, or that feeds a composition's input port, must hold its rows in memory for those consumers to read (they cannot share a spilled file), so it never spills. If such a buffer alone would breach the hard limit it aborts with `E310 MemoryBudgetExceeded` rather than exceeding the budget. This is the one case where a larger input cannot be rescued by more disk — raise `memory.limit`, or restructure so the shared stage feeds a single consumer.

Some stages **stream** (they hold only a small in-flight slice of records) and some **materialize** (they hold a whole stage's worth before emitting). `clinker run --explain` annotates each node with `buffer: streaming` or `buffer: materialized` so you can see which stages will dominate the budget before you run. See [Streaming vs. Blocking Stages](streaming-vs-blocking.md) for which is which.

## Sizing guidelines

| Workload | Recommended limit | Notes |
|----------|-------------------|-------|
| Small files (<10 MB) | 128M | Minimal memory pressure |
| Medium files (10–50 MB) | 256M | Covers most ETL jobs |
| Large files or complex aggregations | 512M (default) – 1G | Multiple group-by keys, large cardinality |
| Multiple large group-by keys | 1G+ | High-cardinality distinct values |

**Target workload:** Clinker is optimized for 1–5 input files of up to 100 MB each, processing 10K–2M records per run.

## Aggregation strategy interaction

Memory consumption depends heavily on the aggregation strategy the optimizer selects:

- **Hash aggregation** accumulates state in a hash map. Memory usage is proportional to the number of distinct group-by values. With high-cardinality keys, this can consume significant memory before spill triggers.

- **Streaming aggregation** processes groups in order and emits results as each group completes. Memory usage is minimal (proportional to a single group's state) but requires the input to be sorted by the group-by keys.

- **`strategy: auto`** (the default) lets the optimizer choose based on the declared sort order of the input. If the data arrives sorted by the group-by keys, streaming aggregation is selected automatically.

To influence strategy selection:

```yaml
  - type: aggregate
    name: rollup
    input: sorted_data
    config:
      group_by: [department]
      strategy: streaming    # force streaming (input MUST be sorted)
      cxl: |
        emit total = sum(amount)
```

Only force `streaming` when you are certain the input is sorted by the group-by keys. If the data is not sorted, results will be incorrect. Use `auto` when in doubt.

### Oversized single rows

An aggregate that keeps `min`, `max`, `avg`, or another value-buffering binding holds each contributing row's raw values until the group finalizes. If a **single input row's** buffered footprint is larger than the entire `memory.limit`, no amount of spilling can hold it — spill would only re-read the same oversized row. The engine surfaces this per-row overflow rather than absorbing it:

- With `error_handling.strategy: fail_fast` (the default), the run aborts with `E310 MemoryBudgetExceeded`, naming the aggregate stage and reporting the offending row's byte footprint against the budget.
- With `strategy: continue` (or `best_effort`), the offending record is routed to the dead-letter queue under the `aggregate_finalize` category, and the run proceeds.

This is almost always a sign the budget is set far too low for the record shape — raise `memory.limit` so a typical row fits comfortably.

## Compositions

A composition (a reusable sub-pipeline included via `use:`) does not get its own memory budget — its operators share the parent pipeline's budget and spill to the same temporary directory. If a budget overrun happens inside a composition, the error names the composition's call-site (e.g. `enrich_call`) so you can locate it, prefixing the message with `in composition 'enrich_call': ...` when the overrun is internal to the body.

## Monitoring memory usage

Use the [metrics system](metrics.md) to track `peak_rss_bytes` across runs:

```bash
clinker run pipeline.yaml --metrics-spool-dir ./metrics/
```

The metrics file includes `peak_rss_bytes`, which shows the maximum resident memory during execution. If this consistently approaches your memory limit, consider increasing the budget or restructuring the pipeline to reduce intermediate state.

## Shared server considerations

On servers running JVM applications, memory is often at a premium. Recommendations:

- Set `--memory-limit` or `memory.limit` explicitly rather than relying on the default. Know your budget.
- Use `--threads` to limit CPU contention alongside memory limits.
- Monitor `peak_rss_bytes` in production metrics to right-size the limit over time.
- Schedule large pipelines during off-peak hours when JVM heap pressure is lower.
