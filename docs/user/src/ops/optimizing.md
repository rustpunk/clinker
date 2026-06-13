# Optimizing Pipelines

Clinker keeps memory bounded and spills to disk automatically, so most pipelines run fine with no tuning at all. When you do need a pipeline to run faster or in less memory, a handful of authoring choices do nearly all the work. This page is the practical checklist; the engine mechanics behind each tip live in the separate **Engine Internals** book.

## Let stages stream instead of buffer

The cheapest pipeline is one where records flow straight through without being held in memory. A **Source → Transform → Output** chain streams end to end — no intermediate stage is materialized. You get this automatically; the things that break it are fan-out (a Route with several branches, an output that forks) and blocking operators (sort, hash aggregation, the build side of a Combine).

Practical implication: keep the hot path simple. A filter-and-reshape job that's just Source → Transform → Output already runs at minimal memory. See [Streaming vs. Blocking Stages](streaming-vs-blocking.md) for which operators stream and which block.

## Make aggregation stream with `sort_order`

A hash `Aggregate` holds one entry per distinct group key in memory — fine for low-cardinality keys, expensive for high-cardinality ones. If your input is already sorted on the group-by keys, declare it:

```yaml
- type: source
  name: txns
  config:
    type: csv
    path: ./data/transactions_sorted.csv
    sort_order:
      - { field: account_id, order: asc }
    schema:
      - { name: account_id, type: string }
      - { name: amount, type: float }

- type: aggregate
  name: per_account
  input: txns
  config:
    group_by: [account_id]
    cxl: |
      emit total = sum(amount)
```

With a matching `sort_order`, the optimizer switches the aggregate to **streaming** — it emits each group as the key advances and holds only one group at a time, regardless of cardinality. To make the requirement explicit (and turn a silent fallback to hash aggregation into a compile error), set `strategy: streaming`. See [Aggregate Nodes → Strategy hint](../nodes/aggregate.md#strategy-hint).

> `sort_order` is **trusted, not verified** — if the data isn't actually sorted, streaming aggregation produces wrong results. Only declare it when you're sure.

## Choose the Combine driver side deliberately

A `Combine` holds each non-driving (build-side) input in memory as a lookup table, then streams the driver against it. So:

- Put the **smaller** relation on the build side and **drive** with the larger stream — you iterate the big input once and keep only the small one resident. Plan for roughly 1.5–2× the build file's size in memory.
- The driver also sets output order and which side's correlation identity propagates, so pick it for those reasons too. See [Combine Nodes](../nodes/combine.md) and [Correlation Keys → Combine interaction](../pipelines/correlation-keys.md#combine-interaction).

A large build side isn't a failure — the join spills to disk automatically — but spilling is slower than staying in memory, so sizing the driver right is the main lever.

## Size the memory budget

The default budget is 512 MB. Raise it when a pipeline does high-cardinality aggregation or large joins and you have the RAM; lower it to be a good neighbor on a shared box. The budget is a target, not a hard wall — stages spill rather than fail when they exceed it.

```yaml
pipeline:
  name: my_pipeline
  memory:
    limit: "1G"
```

Full sizing guidance and the `backpressure` knob are in [Memory Tuning](memory.md).

## Reduce intermediate state

Less data in flight means less to buffer and spill:

- **Filter early.** Drop records you don't need in the first Transform, before they reach a blocking stage.
- **Project narrowly.** Emit only the fields downstream stages actually use; carrying wide records through a sort or aggregate costs memory per row.
- **Aggregate before joining** when you can — feeding a small rolled-up relation into a Combine is cheaper than joining raw rows and aggregating after.

## Confirm with `--explain` and metrics

Before running, `clinker run pipeline.yaml --explain` annotates each node with `buffer: streaming` or `buffer: materialized`, so you can see which stages will dominate memory. After a run, the [metrics spool](metrics.md) reports `peak_rss_bytes` — if it consistently approaches your limit, raise the budget or cut intermediate state. See [Explain Plans](explain.md).
