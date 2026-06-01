# Streaming vs. Blocking Stages

Every node in a pipeline plan is one of two kinds at runtime:

- **Streaming** stages hand their output downstream in bounded batches over a back-pressured channel, never crossing an inter-stage buffer that charges the memory budget. The two *fused* streaming paths additionally hold at most one batch of in-flight events at a time, so their inter-stage memory does not grow with input size. The other streaming stages still build their own result before handing it off — streaming spares them the *second* copy into a charged buffer and overlaps the writer with downstream work, but their own working set is as large as a blocking stage's would be.
- **Blocking** stages must see their whole input before they can produce any output. They accumulate state inside the memory budget and spill to disk when the soft threshold trips, rather than holding everything in RAM.

This distinction is what makes Clinker a bounded-memory executor: a pipeline's peak memory is set by its largest live blocking-or-non-fused-streaming stage plus one batch per fused streaming stage, not by the cumulative size of every stage at once. A streaming stage's output is never separately buffered between dispatch arms, so it is never charged twice and never spill-eligible.

## Which stages stream

A stage streams when its output is handed straight to a single downstream consumer instead of crossing a charged inter-stage buffer.

Two stages stream *and* bound their own footprint to one batch, because they pull records off a live upstream channel and forward each batch without ever building a full result:

- **Source → Transform → Output** fused chains. A non-windowed Transform whose only upstream is a single Source and whose only downstream is a single sink Output consumes that Source's records directly and hands each batch to the Output's writer thread over a back-pressured channel; neither the Transform nor the Output materializes the whole record set. A Transform that fans out to multiple consumers, feeds another operator, or roots a window keeps the buffered (materialized) path.
- **`Merge` in `interleave` mode** fed entirely by Sources. The merge reads each Source's live stream and forwards records as they arrive.

These stages stream their output to a single downstream Output too — sparing the second copy and overlapping the writer — but each still builds its full result first, so its own working set is not bounded to one batch:

- **Single-branch `Route`**. A Route with exactly one branch feeding one sink Output streams that branch's records to the writer thread. A multi-branch Route forks records across several successor buffers and stays materialized.
- **`Merge` in `concat` mode, or `interleave` fed by non-Source inputs**, feeding one sink Output. The merge drains its predecessors' buffers in order (concat) or round-robin (interleave) into the merged result, then streams it.
- **`streaming`-strategy `Aggregate`** feeding one sink Output. When the planner certifies the aggregate's input is pre-sorted on the group key, it finalizes the group rows and streams them rather than buffering them for a downstream arm.
- **`Combine` probe side** (hash build-probe strategy) feeding one sink Output. The build relation stays fully materialized in the hash table; the matched probe output streams to the writer.

Each of these requires the producer to feed exactly one downstream Output and to root no window; a producer that roots a window keeps the materialized path because the window arena needs the producer's full output to build.

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

The number of events handed downstream per batch is set by [`pipeline.batch_size`](memory.md#streaming-batch-size-batch_size) (default 2048), with an optional [per-transform override](../pipeline/transform.md#batch-size-batch_size). For a fused streaming stage — the only kind whose footprint *is* one batch — smaller batches lower its in-flight footprint at the cost of more per-batch bookkeeping; larger batches do the reverse. For the other streaming stages the batch size sets only the in-flight slice handed across the channel; the producer's own result is built in full regardless, so `batch_size` does not cap their footprint. The batch size changes only the memory *profile* of streaming handoffs — never their output, and never the behavior of blocking stages.
