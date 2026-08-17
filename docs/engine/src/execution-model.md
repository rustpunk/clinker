# Streaming vs. Blocking Stages

*User-facing view: the User Guide's "Streaming vs. Blocking Stages" page.*

This page is the engine-internals reference for the runtime classifier that decides whether a node hands its output downstream in bounded batches or accumulates its whole input before emitting. The streaming/blocking split is the mechanism behind Clinker's bounded-memory guarantee, and the same classifier annotates `--explain` output and drives the dispatcher at runtime, so the model here is exactly what the executor does — not a simplification of it. Read it alongside [Memory Arbitration & Scheduling](memory-arbitration.md), which covers how each in-flight batch and materialized slot charges the budget.

Every node in a pipeline plan is one of two kinds at runtime:

- **Streaming** stages hand their output downstream in bounded batches over a back-pressured channel, never crossing an inter-stage buffer that charges the memory budget. The two *fused* streaming paths additionally hold at most one batch of in-flight events at a time, so their inter-stage memory does not grow with input size. The other streaming stages still build their own result before handing it off — streaming spares them the *second* copy into a charged buffer and overlaps the writer with downstream work, but their own working set is as large as a blocking stage's would be.
- **Blocking** stages must see their whole input before they can produce any output. They accumulate state inside the memory budget and spill to disk when the soft threshold trips, rather than holding everything in RAM.

## Materialized input invariant

Every planned materialized edge has an occupied node-buffer slot when its
consumer runs. A producer that emitted no rows still admits an explicit empty
slot; absence is not another spelling of an empty input. Once a dispatcher has
excluded its certified streaming/fused path and any explicit alternate slot,
a missing materialized slot is therefore an executor invariant failure. The run
returns `PipelineError::Internal` and stops instead of manufacturing an empty
collection and allowing plausible but incomplete output to commit.

The checked retrieval is stage-level work: it performs the same map lookup and
moves the same buffer as the successful path. It adds no record-rate
allocation, clone, or per-record bookkeeping. Optional lookup remains limited
to body seeding, own-slot-versus-predecessor selection, and cleanup paths where
absence has defined control-flow meaning.

`Aggregate`, `Reshape`, `Cull`, and planner-synthesized `Sort` use one address
rule: first check their own `(consumer, None)` slot, which is where a `Route`
branch or `Cull` port publishes its selected records, then require the incoming
`(producer, producer_port)` slot. A present empty own slot is still
authoritative; only the absence of both valid addresses is an invariant
failure. `Transform` and `Sink` also recognize successor-local slots through
their existing specialized input paths. `Merge` and `Combine` remain
predecessor-slot readers because they select among multiple incoming edges.

### Shared-buffer scans and composition scope

Every published materialized slot carries a remaining-reader count keyed by
its exact `(producer, producer_port)` `NodeBufferKey`. The producer declares
that count when it publishes the slot; readers never rediscover ownership from
node kind, node index, or dispatch order. The common one-reader path removes
the slot and its registration directly with O(1) bookkeeping. With several
readers, each earlier reader opens a sequential scan over shared immutable
backing while the original stays live for the final reader. This applies
uniformly to materialized Transform, Aggregate,
Sort, Reshape, Route, Cull, Envelope, Composition, Merge, Combine, and Sink
inputs, including successor-local Route/Cull slots and both Sink event paths.

`Memory`, `Spilled`, and `Mixed` all support repeatable scans. A memory scan
clones one event at a time; a spill scan opens one chunk at a time, preserving
record and punctuation order with O(1) file descriptors per active scan. A
consumer that collects the scan into a resident vector first registers the
estimated materialized bytes and holds that reservation through its complete
synchronous operation; unwinding an error releases it automatically. The
reader count changes only after the scan is acquired successfully. The final
reader drains the authoritative slot and its ordinary node-buffer
registration. Successful scope completion rejects any residual slot,
registration, or positive reader count as an internal invariant failure.

An adopted `MergeSpilled` run set cannot be scanned repeatedly because its
merger consumes and unlinks the runs. Its first shared acquisition therefore
folds it exactly once into an ordinary spill file. Replacement disk bytes are
charged before the input runs are released; an overlap beyond the spill quota
returns E320 and cleans up every file and registration from the failed fold.

A composition input uses the same scan/materialization boundary, then transfers the live
consumer id and byte handle into the body-local node-buffer registry. Body
Source canonicalization briefly needs the seeded events and its prospective
output together, so it extends that same reservation before allocating the
output and reduces it to the canonicalized footprint as soon as the seed
allocation drops. Slot admission then atomically replaces the transient
consumer wrapper with the ordinary spill-aware wrapper under the same id.
Body execution swaps the parent `node_buffers`, node-buffer registrations,
reader ledger, source-record table, body references, and window state as one
scope. Body dispatch and output harvest are captured before a single cleanup
path unregisters body residue and restores every parent map, so a successful
body, a dispatch error, and a harvest error have the same ownership lifecycle.
The transfer keeps one continuous registration and charges both allocations
only for their real overlap: there is no unregistered interval and no second
consumer charge for the same bytes.

This distinction is what makes Clinker a bounded-memory executor: a pipeline's peak memory is set by its largest live blocking-or-non-fused-streaming stage plus one batch per fused streaming stage, not by the cumulative size of every stage at once. A streaming stage's output is never separately buffered between dispatch arms, so it is never charged twice: the arbitrator counts each in-flight batch once when the producer flushes it and discharges that charge as the consumer drains it. If RSS still crosses the soft threshold while a single-consumer streaming stage holds batches in flight, the engine spills those batches' records to disk one batch at a time — the streaming handoff is the per-batch counterpart of a blocking stage's full-stage spill, not an exemption from spilling.

## Plan admission and runtime entry

The current public entry is typed as a compiled-plan boundary, but its complete
call path re-enters planning:

```text
PipelineConfig::compile -> CompiledPlan
                             |
PipelineExecutor::run_plan_with_readers_writers(&CompiledPlan)
                             |
                        plan.config()
                             |
          run_with_readers_writers[_in_context]
                             |
                    PipelineConfig::compile
                             |
        newly validated plan.dag() -> runtime dispatch
```

The `_in_context` entry supplies a `CompileContext` so file-size estimates are
resolved against the intended workspace instead of the process working
directory. The run body also derives memory admission from the embedded config
before compiling again. `PipelineRunParams` carries the current run-scoped
envelope: execution and batch IDs, pipeline/static/source/record variable
overlays, a shutdown token, and spill root, quota, and compression policy.
These parameters affect a run without serving as a second authoring topology.

This is the observed implementation, not the locked destination. D-01 through
D-11 require Phase 5 / PERF-01 to execute the supplied plan's stored DAG,
composition bodies, bound schemas, compiled expression artifacts, statistics,
and semantic identity directly. Only an explicitly enumerated runtime envelope
may refresh; semantic planning changes require replanning. A `CompiledPlan` is
borrowed and survives a call today, but the current recompilation means
sequential reuse still repeats immutable planning work. Persistent cache
identity, safe misses, atomic replacement, and fresh source-map/provenance
handling are also Phase 5 work. See the canonical
[stored-plan execution and cache identity contract](https://github.com/rustpunk/clinker/blob/main/docs/ai/15_PRODUCTION_CONTRACTS.md#stored-plan-execution-and-cache-identity).

## Frozen execution wiring

The canonical compile completes every structural graph rewrite before it
freezes the two artifacts that runtime dispatch consumes:

- `CompiledConsumerRegistry` is keyed by stable producer identity and optional
  producer port. Each deterministic consumer entry records the consumer's
  stable identity and input port, whether it reads the shared producer slot or
  a pre-forked slot, and whether it crosses a physical writer boundary.
- `ExecutionOrderContract` retains source orders, edge guarantees, consumer
  requirements, terminal guarantees, and topology-derived physical writer
  boundaries. No later planner pass may change topology after this contract is
  frozen.

The registry is the single delivery authority for fan-out. A consumer of a
shared producer port obtains one complete sequential scan: all but the final
observed consumer receive an independent re-readable cursor, and the final
consumer removes the authoritative slot and its one `MemoryArbitrator`
registration. Completion is tracked by compiled identities, not graph indexes
or dispatch order. A missing entry, duplicate read, missing shared slot, or
incomplete final scan is an internal invariant failure instead of a silent
short delivery.

For a Source with `sort_order`, its `CompiledSourceOrder` binds stable source
identity, field indexes and types, direction and null policy, `on_unsorted`,
the sortable event shape, and `PerPhysicalFile` scope. Runtime constructs the
physical-file barrier from that compiled value. Successful rows and declared
type failures share one attempt stream; document punctuation is staged with
them. The barrier emits the complete attempted/rejected population before the
covered attempts. The executor applies that population exactly once and checks
its threshold before routing any covered attempt into the DLQ, downstream
state or counters, or writer effects.

Each `PhysicalWriterBoundary` binds one Sink to the producer port, writer
mode, partition identity, ordering guarantee, and runtime disposition selected
from the finalized topology. The modes are `RecordsOnly`, `PerSourceFile`,
`Envelope`, `DocumentDlq`, `CorrelationDeferred`, and `Streaming`; partition
identity distinguishes a single writer, split sequence, source file, document,
or correlation group. `OrderedWriterBoundary` verifies that the active dispatch
arm matches the compiled mode and that its disposition satisfies the compiled
guarantee. Deferred boundaries use the shared stable authored-key sorter and
bounded-fan-in spill merge; source-row identities and population indexes remain
payload and never become comparison keys. A streaming arm rejects a
complete-population disposition rather than pretending to implement it.

All of this remains synchronous, single-process, finite-batch execution.
Bounded channels provide back-pressure, `MemoryArbitrator` remains the one
run-scoped memory authority, and source repair, terminal ordering, and shared
fan-out reuse the existing spill and merge machinery. Runtime never recounts
Sinks or invents a weaker order when a frozen contract and the selected path
disagree; it returns `PipelineError::Internal`.

## Ordering evidence and test oracles

Ordering is an explicit plan/runtime contract, not a side effect of scheduling.
The frozen `ExecutionOrderContract` records where stable arrival is preserved,
where order is destroyed, and which physical-file sources require
verification. Runtime strategies must satisfy that contract; an unordered
compiled promise is a conservative lower bound and may be served by a stronger
exact runtime path.

| Boundary or strategy | Runtime behavior | Scope and supported oracle |
|---|---|---|
| Source without `sort_order` | Preserves the reader's arrival sequence but claims no sorted keys. | No authored sortedness promise; use multiset and aggregate assertions unless a later operator establishes order. |
| Source with `sort_order` | Strict declared-type coercion precedes adjacent-key verification. A memory-arbitrated barrier releases each physical file only after it is verified; default `on_unsorted: warn` stably repairs and emits one `W307`, while `error` releases no prefix. | `PerPhysicalFile`, never global across files. For one fixed path, resident and spill repair must be byte-identical and stable for equal authored keys. |
| Order-preserving unary paths and `Merge` `concat` | Retain predecessor arrival; concat drains inputs in declaration order. | Exact sequence is valid for the same upstream paths. Matching per-input sorts are not promoted to global order. |
| Seeded `Merge` interleave | Establishes a reproducible stable-arrival schedule for the seed. | Exact sequence is valid for the same seed and paths. |
| Unseeded interleave and every current `Combine` strategy | Cross-input or matched-row arrival is incidental and the plan marks it unordered. | Compare decoded multisets, aggregate values, counters, and identities; never exact incidental row order. |
| Terminal Sink `sort_order` | Uses the shared stable resident/spill kernel and orders all records reaching that terminal by exactly the authored fields. | Exact authored-key order. Equal-key order is stable within a path, but cross-strategy exact bytes require a total authored business key. |

Neither source repair nor terminal sorting adds `SourceRowId`, physical-file,
or canonical-row tie fields. Equal authored keys compare equal. Stability is
maintained by arrival/run position within the selected path, so tests must not
turn an unpromised upstream tie order into a hidden compatibility contract.

The source barrier stages the whole sortable event shape, including successful
and rejected attempts plus file and single-frame punctuation, and charges
resident rows, release state, spill I/O buffers, merge cursors, and queued
verified rows to the run-scoped memory authority. It first releases one
complete population delta, then the attempts covered by that identity. It
preserves the exact `SourceRowId`, source/file provenance, and original
document-context `Arc` while repairing. This is a safe pre-effect barrier; it
does not replay a source after downstream writers, DLQ, counters, metrics,
lineage, or document state have mutated.

## Which stages stream

A stage streams when its output is handed straight to a single downstream consumer instead of crossing a charged inter-stage buffer. The downstream consumer is a `Sink` writer, an `Aggregate`'s ingest, or a hash build-probe `Combine`'s probe (driver) side — see [Streaming into an Aggregate](#streaming-into-an-aggregate) and [Streaming into a Combine probe](#streaming-into-a-combine-probe) below.

Two stages stream *and* bound their own footprint to one batch, because they pull records off a live upstream channel and forward each batch without ever building a full result:

- **Source → Transform → Sink** fused chains. A non-windowed Transform whose only upstream is a single Source and whose only downstream is a single Sink consumes that Source's records directly and hands each batch to the Sink's writer thread over a back-pressured channel; neither the Transform nor the Sink materializes the whole record set. A Transform that fans out to multiple consumers, feeds another operator, or roots a window keeps the buffered (materialized) path.
- **`Merge` in `interleave` mode** fed entirely by Sources. The merge reads each Source's live stream and forwards records as they arrive.

These stages stream their output to a single downstream consumer too — sparing the second copy and overlapping the consumer — but each still builds its full result first, so its own working set is not bounded to one batch:

- **Single-branch `Route`**. A Route with exactly one branch feeding one Sink streams that branch's records to the writer thread. A multi-branch Route forks records across several successor buffers and stays materialized.
- **`Merge` in `concat` mode, or `interleave` fed by non-Source inputs**, feeding one Sink. The merge drains its predecessors' buffers in order (concat) or round-robin (interleave) into the merged result, then streams it.
- **`streaming`-strategy `Aggregate`** feeding one Sink. When the planner certifies the aggregate's input is pre-sorted on the group key, it finalizes the group rows and streams them rather than buffering them for a downstream arm.
- **`Combine` probe side** (hash build-probe strategy) feeding one Sink. The build relation stays fully materialized in the hash table; the matched probe output streams to the writer.

Each of these requires the producer to feed exactly one downstream consumer and to root no window; a producer that roots a window keeps the materialized path because the window arena needs the producer's full output to build.

- **Every `Sink`** writes records to its configured writer and never buffers a whole stage.

Document-boundary punctuations (`DocumentOpen` / `DocumentClose`, the signals behind the `$doc.*` context) flow inline with records through streaming stages, preserving their order: a document's close always trails the document's last record, even when the document's records span several batches.

### Streaming into an Aggregate

The streaming consumer above is usually a `Sink`. It can also be an `Aggregate`'s *ingest*: when an eligible producer (a fused `Source → Transform`, a single-branch `Route`, a non-fused `Merge`, or a `streaming`-strategy `Aggregate`) feeds exactly one downstream `Aggregate`, the producer streams record-at-a-time into the aggregate's `add_record` over a back-pressured channel rather than the aggregate pre-draining the producer's whole output from a charged buffer. The producer reports `buffer: streaming` and `--explain` shows no `node_buffer` edge between it and the aggregate.

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
- **`IEJoin` / sort-merge `Combine`** — both inputs are sorted before the band/merge step runs, and both are **block-spilled** so the input axis stays inside the budget, but by different mechanisms. The IEJoin — pure-range and equi+range alike, which share the one block-band path — external-sorts each side to disk on `(equality-hash, range-key, …)` and slices the sorted stream into min/max-tagged, single-equality-hash blocks, pruning block-pairs on the equality hash and the range bounds before the kernel runs (equality is an added prune axis; each surviving pair re-verifies the canonical equality key, since hashes collide). Its **output axis is spill-bounded too** — matched rows accumulate in a payload-ordered sort buffer that spills on its own byte threshold (charged through the join's consumer handle) and drains incrementally, streamed straight to a downstream Sink or folded into a spillable node-buffer, so both axes are bounded with no global-pressure abort. The sort-merge Combine external-sorts each side into runs and merges matching runs; it has no min/max block tags or pruning.
- **`CorrelationCommit`** — a correlation group is held until its commit decision (flush or dead-letter) is known.

A blocking stage keeps its full-stage accumulation inside `pipeline.memory.limit` and spills to disk past the soft threshold; it does not stream batches.

## Seeing the classification

`clinker run <pipeline>.yaml --explain` annotates every node with its class in the **Physical Properties** section:

```text
sink.report:
  buffer: streaming

aggregation.dept_totals:
  buffer: materialized
```

`buffer: streaming` marks a stage whose output is consumed without an inter-stage buffer — it charges the budget per in-flight batch and, on a single-consumer edge, spills those batches to disk under pressure; `buffer: materialized` marks a stage whose output crosses a `node_buffers` slot that charges the memory budget as one full-stage slot and spills the whole stage. Both classes are spill-eligible; they differ in granularity, not in whether they can spill. The explain annotation is derived from the same classifier the executor uses at runtime, so what `--explain` reports is exactly what the dispatcher does. See [Memory Arbitration & Scheduling](memory-arbitration.md) for the arbitration model that rides alongside the buffer class.

## Tuning the batch size

The number of events handed downstream per batch is set by `pipeline.batch_size` (default 2048), with an optional per-transform override on a Transform's `config.batch_size`. For a fused streaming stage — the only kind whose footprint *is* one batch — smaller batches lower its in-flight footprint at the cost of more per-batch bookkeeping; larger batches do the reverse. For the other streaming stages the batch size sets only the in-flight slice handed across the channel; the producer's own result is built in full regardless, so `batch_size` does not cap their footprint. The batch size changes only the memory *profile* of streaming handoffs — never their output, and never the behavior of blocking stages.
