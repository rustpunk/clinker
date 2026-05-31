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

The CLI flag overrides the YAML value. Accepted suffixes: `K` (kilobytes), `M` (megabytes), `G` (gigabytes).

**Default:** 512 MB.

## Choosing a backpressure policy

When the soft spill threshold (80 % of `limit`) trips, somebody has to give up memory. The `backpressure` knob picks the policy that decides who:

| Value | Active policy | Behavior |
|-------|---------------|----------|
| `pause` (default) | `BackPressurePreferred -> Priority` | If any consumer can be paused at its inbound channel, pause it. Otherwise pick the lowest-priority consumer (cheapest to spill) and ask it to spill. |
| `spill` | `Priority` | Never pause a producer. Pick the lowest-priority consumer and ask it to spill. Closest to the pre-arbitrator react-only behavior, but with deterministic priority-based selection. |
| `both` | `BackPressurePreferred -> LargestFirst` | Pause when possible, otherwise force the largest holder to spill regardless of priority. Useful when one operator dominates the budget and a fairness override is wanted. |

`pause` is the right default for most pipelines: when a fast Source is feeding a slow Combine through a bounded inter-stage buffer, pausing the Source is strictly cheaper than spilling the buffer to disk (no I/O, no serialization round-trip). `spill` and `both` exist for the rarer cases where you want a different posture.

To inspect which policy is active before running, use `--explain`:

```text
=== Execution Plan ===

Mode: ...
DAG nodes: 7
arbitration: BackPressurePreferred -> Priority
```

The `arbitration:` line shows the composed policy name. A pipeline with `backpressure: spill` prints `arbitration: Priority`; one with `backpressure: both` prints `arbitration: BackPressurePreferred -> LargestFirst`.

## When to override the default

- **Fast Source + slow Combine** → leave on `pause` (the default). The arbitrator pauses the Source when the inter-stage buffer approaches its share of the budget, and no spill files are written.
- **Two parallel Aggregate stages, one much larger than the other** → consider `both`. `BackPressurePreferred -> LargestFirst` pauses where it can, then targets the dominant Aggregate for spill, freeing the most headroom per spill call.
- **Pure react-only with deterministic priority** → `spill`. Pauses are disabled; the arbitrator picks the cheapest-to-spill consumer (`node_buffers` before grace-hash before sort before Aggregate) every time. Closest to the pre-arbitrator behavior.

## How it works

Clinker tracks memory in two layers. RSS (resident set size) is sampled at chunk boundaries and supplies the primary spill / abort signal. Alongside RSS, every memory-touching operator (Source ingest channels, Aggregate hash maps, sort buffers, grace-hash partitions, sort-merge accumulators, IEJoin arrays, inline-Combine hash tables, `node_buffers` slots, and window-runtime arenas) registers a `MemoryConsumer` wrapper with the pipeline-scoped arbitrator. Each operator owns its live byte counter and updates it on every admit / spill transition; the arbitrator queries `current_usage()` per consumer at every policy poll. This pull-mode attribution lets the policy distinguish *reclaimable* bytes (what an operator can give up right now) from currently-held bytes — a grace-hash with on-disk partitions, for instance, reports only its in-memory portion.

Window-runtime arenas (the columnar backing store that analytic-window evaluation reads from) are attributed but not independently spillable: an arena is immutable once built and is freed only indirectly, when the operator that consumes its windows drains to disk. Its wrapper reports the arena's bytes so the arbitrator's attribution is complete, but ranks last among spill victims so a policy never elects an arena while any consumer that can actually pause or spill remains.

When memory pressure crosses the soft threshold (80 % of `limit`), the arbitrator runs the active policy to pick a victim and invokes the corresponding action: `pause()` on a back-pressureable consumer (its producer's hot loop parks on a `Condvar` until `resume`), or `try_spill(target_bytes)` on a spillable consumer (the consumer's wrapper flips a spill-requested flag the operator reads at its next batch boundary). When RSS crosses the hard limit, the engine fails fast with `E310 MemoryBudgetExceeded`.

This means:

- Pipelines always complete if disk space is available, regardless of input size.
- Performance degrades gracefully under memory pressure — you will see slower execution (and possibly disk I/O), not failures.
- The memory limit is a soft ceiling, not a hard wall. Momentary spikes may briefly exceed the limit before the policy fires.

## Bounded-memory contract for non-fused stages

Fused chains (Source → Transform → Output, Merge.interleave fed by Sources) run streaming with no per-stage materialization. Non-fused boundaries — Route fan-out, Merge fan-in, Composition bodies, diamond DAGs — materialize records into per-stage `node_buffers`. Each slot registers a `NodeBufferConsumer` with the arbitrator (priority 0 — the cheapest-to-spill victim class), so the active policy's victim selection is fully attributed.

When a buffer crosses the soft threshold (80 % of the limit) the arbitrator runs the active policy. Under the default `pause`, the producer feeding the buffer is paused at its inbound channel; under `spill` or when no consumer can be paused, the slot spills to disk using the same LZ4 + postcard frame format as grace-hash sort partitions. When RSS crosses the hard limit, the engine fails fast with `E310 MemoryBudgetExceeded { node }` naming the operator whose hot loop polled the abort gate. See [error E310](../explain/E310.html) for the full diagnostic model, including the composition-involved two-shape error model.

Spill fires at the producer side of the first slot whose downstream topology permits it — single-consumer, port-less. For a Source feeding a Route, that's the Source's own slot, not the Route's per-branch slots, because the Source has the one outgoing edge that satisfies the topology rule. Per-branch slots can still spill independently when their own row-distribution drives them past the soft threshold, but the canonical case lands at the producer.

Use `clinker run --explain` to predict which stages will dominate the budget before runtime — each node carries a `buffer: streaming | materialized` annotation, and the materialized nodes are exactly the ones that charge `pipeline.memory.limit` and are spill-eligible.

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

A composition (a reusable sub-pipeline included via `use:`) does not get its own memory budget. Body operators register with the same arbitrator instance as the parent pipeline, admit through the same paths, and spill to the same temporary directory. The recursion is purely structural. Body-scope consumer registrations are unregistered automatically when the body exits, so a body's `NodeBufferConsumer` wrappers do not leak into the parent scope's policy registry.

When a budget exceedance involves a composition, the error message arrives in one of two shapes:

- **At the composition boundary** (records flowing into the body via an input port, or back out into the parent) — the error names the composition's call-site directly (e.g. `enrich_call`).
- **Inside the body** — the error is wrapped so the user-visible call-site name surfaces alongside the body-internal operator that tripped. The rendered message reads `in composition 'enrich_call': ...` followed by the inner detail.

See [error E310](../explain/E310.html) for the full diagnostic model.

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
