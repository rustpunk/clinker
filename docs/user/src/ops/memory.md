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

The CLI flag overrides the YAML value. Suffixes are **binary (1024-based)**: `K` = 1024 bytes, `M` = 1024², `G` = 1024³; a bare integer is bytes. (This differs from the **decimal** `KB`/`MB`/`GB` used by `min_size`/`max_size`, which are 1000-based.)

**Default:** 512 MB.

**Invalid values:** an empty or unparseable limit (for example a stray
non-numeric value) falls back to the 512 MB default. A value whose size is
well-formed but too large to represent — its scaled byte count exceeds the
maximum a 64-bit counter can hold — is rejected at startup with a config error
that names `memory.limit`, rather than wrapping to a small budget. Pick a limit
that fits your host's real memory.

## Choosing a backpressure policy

When memory use approaches the limit (the soft threshold is 80 % of `limit`), something has to give up memory. The `backpressure` knob chooses what:

| Value | Behavior |
|-------|----------|
| `pause` (default) | Where possible, pause an upstream reader so it stops producing until pressure eases; spill to disk only when nothing can be paused. |
| `spill` | Never pause a producer — always free memory by spilling a stage to disk. |
| `both` | Pause where possible, otherwise spill whichever stage is holding the most memory. |

`pause` is the right default for most pipelines: pausing a fast Source feeding a slow downstream stage is cheaper than writing its buffered records to disk. Reach for `spill` or `both` only when you have a specific reason to prefer a different posture — for example, `both` when one large stage dominates the budget and you want it spilled first.

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

- **Pipelines always complete if disk space is available, regardless of input size.** When a blocking stage (sort, hash Aggregate, large Combine) outgrows the budget, it spills to disk instead of failing.
  - **Exception — inequality/range Combine.** A Combine whose `where:` joins the two inputs on an inequality (`<`, `<=`, `>`, `>=`) — for example a band join such as `orders.amount >= bands.lo and orders.amount < bands.hi` — runs a specialized band-join strategy that has no spill path. Instead it enforces the budget on its pre-output working set and aborts with `E310 MemoryBudgetExceeded` if that state overruns, rather than spilling. Give such a pipeline enough headroom, or narrow the predicate so fewer rows enter the join.
- **Performance degrades gracefully.** Under pressure you'll see slower execution and possibly disk I/O — not a crash.
- **The limit is a soft ceiling, not a hard wall.** Momentary spikes may briefly exceed it before the engine reacts. Only if memory blows past the limit outright does the run abort with `E310 MemoryBudgetExceeded`, which names the stage that overran.

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
