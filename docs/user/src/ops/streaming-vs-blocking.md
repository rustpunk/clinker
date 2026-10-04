# Streaming vs. Blocking Stages

Every node in a pipeline is one of two kinds at runtime, and the difference is what keeps Clinker's memory bounded:

- **Streaming** stages pass records through without holding the whole input. Their memory footprint stays small no matter how large the input is.
- **Blocking** stages must see their entire input before they can produce any output, so they accumulate state. They stay within the memory budget and spill to disk when it gets tight, rather than holding everything in RAM.

Peak memory includes all concurrently live operator state, source queues, writer buffers, and retained intermediate records. The shared budget and spill policies govern that combined working set; the largest blocking stage alone is not a peak-memory bound.

*Interactive companion: the [streaming vs. blocking explainer](streaming-vs-blocking-explainer.html) classifies every stage of a few pipeline shapes as you change their settings, and shows the `--explain` lines.*

## Which stages stream

A stage streams when two things hold: it is one of the stages listed below, and its output goes to **exactly one** consumer that can take a stream, which is a Sink, the input of an Aggregate, or the driver side of a hash `Combine`. Any other stage, a stage that feeds two consumers, and a stage that roots an analytic window keep their output in a buffer instead. For example, in Source → Transform → Transform → Sink only the first Transform streams.

Two shapes stream *and* hold only one batch at a time, however large the input:

- **Source → Transform → Sink** chains, where the Transform has no window and its Source feeds only it. Records flow straight from the reader through the transform to the writer.
- **`Merge` in `interleave` mode without an `interleave_seed`, whose inputs are all Sources**, each feeding only the Merge.

These hand their output straight to their one consumer, but still build their own result first:

- **`Route` with only one branch wired to a downstream stage.** The `default:` branch counts: a Route with one condition and a wired `default` has two consumers, and gives each its own buffer.
- **`Merge`** in `concat` mode, in seeded `interleave` mode, or in `interleave` mode fed by other stages.
- **`Aggregate` with `strategy: streaming`** — when the input is pre-sorted on the group key, each group is emitted as soon as the key advances. (See [Aggregate Nodes](../nodes/aggregate.md#strategy-hint).)
- **A hash `Combine`'s output**, and its driver side, which streams in against the already-built lookup table.
- **A range `Combine`'s output**, once it has sorted both sides.
- **`Sink`** — a Sink writes each record to its writer as it arrives. A Sink with `sort_order`, `split` or a per-source-file path takes no stream, so the stage before it keeps a buffer.

Document boundaries (the signals behind [`$doc.*`](../pipelines/envelope-and-doc-context.md)) flow inline with records through streaming stages, so a document's close always trails its last record.

## Which stages block

A stage blocks when its result depends on records it has not seen yet:

- **`sort`** — the full input must be present before the first sorted record is known.
- **Hash `Aggregate`** — a group's final value depends on every member, so the group table retains aggregate state for every live group. (A `streaming`-strategy Aggregate over pre-sorted input is the exception above.)
- **A `Combine`'s build side** — the lookup table is built in full before any driver record is matched. The probe side streams; the build side materializes.
- **Time-windowed and correlation-key Aggregates** — these hold their group state for windowing or for the correlation commit, so they materialize.

A blocking stage keeps its accumulated state inside `pipeline.memory.limit` and spills to disk when the budget gets tight.

## Seeing the classification

`clinker run <pipeline>.yaml --explain` annotates every node with its class in the **Physical Properties** section:

```text
sink.report:
  buffer: streaming

aggregation.dept_totals:
  buffer: materialized
```

`buffer: streaming` marks a stage that holds only a small in-flight slice; `buffer: materialized` marks one that holds a whole stage's output and may spill it. The annotation follows the rules the executor applies at runtime, with two known gaps:

- A Sink with `reconstruct_envelope: true` turns streaming output off at runtime, but `--explain` still reports `buffer: streaming` for that Sink and for a Transform feeding it.
- Under a [correlation key](../pipelines/error-handling.md#correlation-key), `--explain` reports each Sink as `buffer: streaming`, though the correlation commit writes its rows.

See [Explain Plans](explain.md) and [Memory Tuning](memory.md).

Under [`dlq_granularity: document`](../pipelines/error-handling.md#document-level-dlq), as under a correlation key, streaming handoffs are off for the whole pipeline: no Transform, Merge, Route, Aggregate or Combine hands its output to a streaming consumer, so none of them reports `buffer: streaming`. Each Sink reports `buffer: materialized`, because it holds every open document's records until the document's verdict is final. A Source read by a single Transform still hands its records straight to that Transform, and keeps `buffer: streaming`.

## Tuning the batch size

The number of records a streaming stage hands downstream at a time is set by [`pipeline.batch_size`](memory.md#streaming-batch-size-batch_size) (default 2048), with an optional [per-transform override](../nodes/transform.md#batch-size-batch_size). Smaller batches lower in-flight memory at the cost of more per-batch overhead; larger batches do the reverse. The batch size changes only the memory *profile* of streaming handoffs — never their output, and never the behavior of blocking stages.
