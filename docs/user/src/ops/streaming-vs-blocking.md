# Streaming vs. Blocking Stages

Every node in a pipeline is one of two kinds at runtime, and the difference is what keeps Clinker's memory bounded:

- **Streaming** stages pass records through without holding the whole input. Their memory footprint stays small no matter how large the input is.
- **Blocking** stages must see their entire input before they can produce any output, so they accumulate state. They stay within the memory budget and spill to disk when it gets tight, rather than holding everything in RAM.

A pipeline's peak memory is therefore set by its largest blocking stage, not by the total size of all stages combined.

## Which stages stream

- **Source → Transform → Output** chains — records flow straight from the reader through the transform to the writer.
- **`Output`** — a sink always streams its records to the configured writer.
- **`Route`** — predicate fan-out passes records through.
- **`Merge`** — concatenation or interleaving passes records through.
- **`Aggregate` with `strategy: streaming`** — when the input is pre-sorted on the group key, each group is emitted as soon as the key advances, so the whole input is never held. (See [Aggregate Nodes](../nodes/aggregate.md#strategy-hint).)
- **The probe (driver) side of a hash `Combine`** — the driver streams against the already-built lookup table.

Document boundaries (the signals behind [`$doc.*`](../pipelines/envelope-and-doc-context.md)) flow inline with records through streaming stages, so a document's close always trails its last record.

## Which stages block

A stage blocks when its result depends on records it has not seen yet:

- **`sort`** — the full input must be present before the first sorted record is known.
- **Hash `Aggregate`** — a group's final value depends on every member, so the group table holds the whole input. (A `streaming`-strategy Aggregate over pre-sorted input is the exception above.)
- **A `Combine`'s build side** — the lookup table is built in full before any driver record is matched. The probe side streams; the build side materializes.
- **Time-windowed and correlation-key Aggregates** — these hold their group state for windowing or for the correlation commit, so they materialize.

A blocking stage keeps its accumulated state inside `pipeline.memory.limit` and spills to disk when the budget gets tight.

## Seeing the classification

`clinker run <pipeline>.yaml --explain` annotates every node with its class in the **Physical Properties** section:

```text
output.report:
  buffer: streaming

aggregation.dept_totals:
  buffer: materialized
```

`buffer: streaming` marks a stage that holds only a small in-flight slice; `buffer: materialized` marks one that holds a whole stage's output and may spill it. The annotation comes from the same classifier the executor uses at runtime, so what `--explain` reports is exactly what happens. See [Explain Plans](explain.md) and [Memory Tuning](memory.md).

## Tuning the batch size

The number of records a streaming stage hands downstream at a time is set by [`pipeline.batch_size`](memory.md#streaming-batch-size-batch_size) (default 2048), with an optional [per-transform override](../nodes/transform.md#batch-size-batch_size). Smaller batches lower in-flight memory at the cost of more per-batch overhead; larger batches do the reverse. The batch size changes only the memory *profile* of streaming handoffs — never their output, and never the behavior of blocking stages.
