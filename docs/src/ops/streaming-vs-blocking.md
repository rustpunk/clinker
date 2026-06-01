# Streaming vs. Blocking Stages

Every node in a pipeline plan is one of two kinds at runtime:

- **Streaming** stages process records as they arrive and hand them downstream in bounded batches. They hold at most one batch of in-flight events, so their inter-stage memory does not grow with input size.
- **Blocking** stages must see their whole input before they can produce any output. They accumulate state inside the memory budget and spill to disk when the soft threshold trips, rather than holding everything in RAM.

This distinction is what makes Clinker a bounded-memory executor: a pipeline's peak memory is set by its largest live blocking stage plus one batch per streaming stage, not by the total input size.

## Which stages stream

A stage streams when its output never needs to be held in full for a downstream consumer:

- **Source → Transform → Output** fused chains. A non-windowed Transform whose only upstream is a single Source and whose only downstream is a single sink Output consumes that Source's records directly and hands each batch to the Output's writer thread over a back-pressured channel; neither the Transform nor the Output materializes the whole record set. A Transform that fans out to multiple consumers, feeds another operator, or roots a window keeps the buffered (materialized) path.
- **`Merge` in `interleave` mode** fed entirely by Sources. The merge reads each Source's live stream and forwards records as they arrive.
- **Every `Output`**. A sink writes records to its configured writer and never buffers a whole stage.

Document-boundary punctuations (`DocumentOpen` / `DocumentClose`, the signals behind [`$doc.*`](../pipeline/envelope-and-doc-context.md)) flow inline with records through streaming stages, preserving their order: a document's close always trails the document's last record, even when the document's records span several batches.

## Which stages block

A stage blocks when its result depends on records it has not seen yet:

- **`sort`** — the full input must be present before the first sorted record is known.
- **Hash `Aggregate`** — a group's final value depends on every member, so the group table accumulates the whole input. (A `streaming`-strategy Aggregate over a pre-sorted input is the exception: the planner certifies it can emit a group as soon as the sort key advances.)
- **`Combine` build side** — the build relation is fully indexed before any probe record is matched. The probe side streams against the built index, but the build side materializes.
- **`IEJoin` / sort-merge `Combine`** — both inputs are sorted and buffered before the band/merge step runs.
- **`CorrelationCommit`** — a correlation group is held until its commit decision (flush or dead-letter) is known.

A blocking stage keeps its full-stage accumulation inside `pipeline.memory.limit` and spills to disk past the soft threshold; it does not stream batches.

## Seeing the classification

`clinker run <pipeline>.yaml --explain` annotates every node with its class in the **Physical Properties** section:

```text
output.report:
  buffer: streaming

aggregation.dept_totals:
  buffer: materialized
```

`buffer: streaming` marks a stage whose output is consumed without an inter-stage buffer; `buffer: materialized` marks a stage whose output crosses a `node_buffers` slot that charges the memory budget and is spill-eligible. The explain annotation is derived from the same classifier the executor uses at runtime, so what `--explain` reports is exactly what the dispatcher does. See [Explain Plans](explain.md) and [Memory Tuning](memory.md) for the arbitration model that rides alongside the buffer class.

## Tuning the batch size

The number of events a streaming stage holds per batch is set by [`pipeline.batch_size`](memory.md#streaming-batch-size-batch_size) (default 2048), with an optional [per-transform override](../pipeline/transform.md#batch-size-batch_size). Smaller batches lower a streaming stage's in-flight footprint at the cost of more per-batch bookkeeping; larger batches do the reverse. The batch size changes only the memory *profile* of streaming stages — never their output, and never the behavior of blocking stages.
