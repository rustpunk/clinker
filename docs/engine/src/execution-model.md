# Streaming vs. Blocking Stages

*User-facing view: the User Guide's "Streaming vs. Blocking Stages" page.*

This page is the engine-internals reference for the runtime classifier that decides whether a node hands its output downstream in bounded batches or accumulates its whole input before emitting. The streaming/blocking split is the mechanism behind Clinker's bounded-memory guarantee, and the same classifier annotates `--explain` output and drives the dispatcher at runtime, so the model here is exactly what the executor does — not a simplification of it. Read it alongside [Memory Arbitration & Scheduling](memory-arbitration.md), which covers how each in-flight batch and materialized slot charges the budget.

Every node in a pipeline plan is one of two kinds at runtime:

- **Streaming** stages hand their output downstream in bounded batches over a back-pressured channel, never crossing an inter-stage buffer that charges the memory budget. The two *fused* streaming paths additionally hold at most one batch of in-flight events at a time, so their inter-stage memory does not grow with input size. The other streaming stages still build their own result before handing it off — streaming spares them the *second* copy into a charged buffer and overlaps the writer with downstream work, but their own working set is as large as a blocking stage's would be.
- **Blocking** stages must see their whole input before they can produce any output. They accumulate state inside the memory budget and spill to disk when the soft threshold trips, rather than holding everything in RAM.

This distinction is what makes Clinker a bounded-memory executor: a pipeline's peak memory is set by its largest live blocking-or-non-fused-streaming stage plus one batch per fused streaming stage, not by the cumulative size of every stage at once. A streaming stage's output is never separately buffered between dispatch arms, so it is never charged twice: the arbitrator counts each in-flight batch once when the producer flushes it and discharges that charge as the consumer drains it. If RSS still crosses the soft threshold while a single-consumer streaming stage holds batches in flight, the engine spills those batches' records to disk one batch at a time — the streaming handoff is the per-batch counterpart of a blocking stage's full-stage spill, not an exemption from spilling.

## Which stages stream

A stage streams when its output is handed straight to a single downstream consumer instead of crossing a charged inter-stage buffer. The downstream consumer is a sink `Output` writer, an `Aggregate`'s ingest, or a hash build-probe `Combine`'s probe (driver) side — see [Streaming into an Aggregate](#streaming-into-an-aggregate) and [Streaming into a Combine probe](#streaming-into-a-combine-probe) below.

Two stages stream *and* bound their own footprint to one batch, because they pull records off a live upstream channel and forward each batch without ever building a full result:

- **Source → Transform → Output** fused chains. A non-windowed Transform whose only upstream is a single Source and whose only downstream is a single sink Output consumes that Source's records directly and hands each batch to the Output's writer thread over a back-pressured channel; neither the Transform nor the Output materializes the whole record set. A Transform that fans out to multiple consumers, feeds another operator, or roots a window keeps the buffered (materialized) path.
- **`Merge` in `interleave` mode** fed entirely by Sources. The merge reads each Source's live stream and forwards records as they arrive.

These stages stream their output to a single downstream consumer too — sparing the second copy and overlapping the consumer — but each still builds its full result first, so its own working set is not bounded to one batch:

- **Single-branch `Route`**. A Route with exactly one branch feeding one sink Output streams that branch's records to the writer thread. A multi-branch Route forks records across several successor buffers and stays materialized.
- **`Merge` in `concat` mode, or `interleave` fed by non-Source inputs**, feeding one sink Output. The merge drains its predecessors' buffers in order (concat) or round-robin (interleave) into the merged result, then streams it.
- **`streaming`-strategy `Aggregate`** feeding one sink Output. When the planner certifies the aggregate's input is pre-sorted on the group key, it finalizes the group rows and streams them rather than buffering them for a downstream arm.
- **`Combine` probe side** (hash build-probe strategy) feeding one sink Output. The build relation stays fully materialized in the hash table; the matched probe output streams to the writer.

Each of these requires the producer to feed exactly one downstream consumer and to root no window; a producer that roots a window keeps the materialized path because the window arena needs the producer's full output to build.

- **Every `Output`**. A sink writes records to its configured writer and never buffers a whole stage.

Document-boundary punctuations (`DocumentOpen` / `DocumentClose`, the signals behind the `$doc.*` context) flow inline with records through streaming stages, preserving their order: a document's close always trails the document's last record, even when the document's records span several batches.

### Streaming into an Aggregate

The streaming consumer above is usually a sink `Output`. It can also be an `Aggregate`'s *ingest*: when an eligible producer (a fused `Source → Transform`, a single-branch `Route`, a non-fused `Merge`, or a `streaming`-strategy `Aggregate`) feeds exactly one downstream `Aggregate`, the producer streams record-at-a-time into the aggregate's `add_record` over a back-pressured channel rather than the aggregate pre-draining the producer's whole output from a charged buffer. The producer reports `buffer: streaming` and `--explain` shows no `node_buffer` edge between it and the aggregate.

This streams the aggregate's *ingest* half only — the producer no longer needs a charged inter-stage slot, and a slow aggregate (one that is spilling, say) paces the producer through the bounded channel. The aggregate's *finalize* half stays blocking by nature: a `group_by` value depends on every member, so the group table accumulates the whole input and emits only after the channel closes (end of input). Spill stays driven by RSS pressure, never by channel depth, exactly as on the materialized path.

Two aggregate shapes keep the materialized ingest, because their finalize is not a single forward pass: a **time-windowed** aggregate runs a multi-pass per-window algorithm over the whole input, and a **relaxed correlation-key** aggregate retains its group state for the correlation-commit phase. Both show `buffer: materialized` on the edge into them.

### Streaming into a Combine probe

A producer can also stream into a hash build-probe `Combine`'s *probe* (driver) side. When an eligible producer (a fused `Source → Transform`, a single-branch `Route`, a non-fused `Merge`, a `streaming`-strategy `Aggregate`, or another hash build-probe `Combine`) is the Combine's driver input, the producer streams record-at-a-time into the probe kernel over a back-pressured channel rather than the Combine pre-draining the driver's whole output from a charged buffer. The driver producer reports `buffer: streaming` and `--explain` shows no `node_buffer` edge between it and the Combine. Only the `HashBuildProbe` strategy qualifies — the range, sort-merge, and grace-hash kernels re-sort or re-scan the driver and stay materialized.

This streams the Combine's *probe* half only. The build side stays fully materialized: the engine builds the complete hash table on the main thread *before* the driver producer streams its first record, so the probe never matches against an incomplete index. The probe consumer runs on its own thread, so a slow driver paces the probe through the bounded channel and a slow probe (a large fan-out) back-pressures the driver. The build relation's footprint is the hash table, exactly as on the materialized path; the streaming handoff spares only the driver's inter-stage slot. Per-source dead-letter rewind, memory accounting, and output are byte-identical to the materialized path.

## Which stages block

A stage blocks when its result depends on records it has not seen yet:

- **`sort`** — the full input must be present before the first sorted record is known.
- **Hash `Aggregate`** — a group's final value depends on every member, so the group table accumulates the whole input. (A `streaming`-strategy Aggregate over a pre-sorted input is the exception: the planner certifies it can emit a group as soon as the sort key advances.)
- **`Combine` build side** — the build relation is fully indexed before any probe record is matched. The probe side streams against the built index, but the build side materializes.
- **`IEJoin` / sort-merge `Combine`** — both inputs are sorted before the band/merge step runs, and both are **block-spilled** so the input axis stays inside the budget, but by different mechanisms. The IEJoin — pure-range and equi+range alike, which share the one block-band path — external-sorts each side to disk on `(equality-hash, range-key, …)` and slices the sorted stream into min/max-tagged, single-equality-hash blocks, pruning block-pairs on the equality hash and the range bounds before the kernel runs (equality is an added prune axis; each surviving pair re-verifies the canonical equality key, since hashes collide). Its **output axis is spill-bounded too** — matched rows accumulate in a payload-ordered sort buffer that spills on its own byte threshold (charged through the join's consumer handle) and drains incrementally, streamed straight to a downstream Output or folded into a spillable node-buffer, so both axes are bounded with no global-pressure abort. The sort-merge Combine external-sorts each side into runs and merges matching runs; it has no min/max block tags or pruning.
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

`buffer: streaming` marks a stage whose output is consumed without an inter-stage buffer — it charges the budget per in-flight batch and, on a single-consumer edge, spills those batches to disk under pressure; `buffer: materialized` marks a stage whose output crosses a `node_buffers` slot that charges the memory budget as one full-stage slot and spills the whole stage. Both classes are spill-eligible; they differ in granularity, not in whether they can spill. The explain annotation is derived from the same classifier the executor uses at runtime, so what `--explain` reports is exactly what the dispatcher does. See [Memory Arbitration & Scheduling](memory-arbitration.md) for the arbitration model that rides alongside the buffer class.

## Tuning the batch size

The number of events handed downstream per batch is set by `pipeline.batch_size` (default 2048), with an optional per-transform override on a Transform's `config.batch_size`. For a fused streaming stage — the only kind whose footprint *is* one batch — smaller batches lower its in-flight footprint at the cost of more per-batch bookkeeping; larger batches do the reverse. For the other streaming stages the batch size sets only the in-flight slice handed across the channel; the producer's own result is built in full regardless, so `batch_size` does not cap their footprint. The batch size changes only the memory *profile* of streaming handoffs — never their output, and never the behavior of blocking stages.
