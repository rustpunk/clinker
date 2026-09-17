# Memory Arbitration & Scheduling

*User-facing view: the User Guide's "Memory Tuning" page.*

This page is the engine-internals reference for how Clinker tracks, attributes, and reclaims memory at runtime, and how it orders simultaneously-runnable nodes to keep the resident working set bounded. It covers the `MemoryConsumer` wrapper registry, pull-mode byte attribution, the per-operator arbitration parameters the active policy reads, the bounded-memory contract for materialized stages, the `predicted_*` values that feed both `--explain` and the scheduler, and the four ranking rules the scheduler applies (with its fallback to topological order). The user-facing knobs — the `memory:` block, the `--memory-limit` flag, the backpressure-policy selection, sizing guidance, and monitoring — live in the User Guide and are intentionally not repeated here. For how each stage's buffer class (`streaming` vs `materialized`) is decided, see [Streaming vs. Blocking Stages](execution-model.md).

## How it works

### Exact allocation admission for prepared output

`clinker_format::preparation` prepares one complete output operation before
delivery. CSV writer construction in the CLI and executor uses this path;
other codecs retain their existing allocation behavior. `MemoryOnlyResources`
requires an explicit nonzero memory budget. `ExecutorResources` shares the run's
`MemoryArbitrator` and takes an explicit optional telemetry producer; disabling
telemetry changes no resource limit. Neither provider offers an unlimited
memory path.

The allocation vocabulary lives in `clinker_record::owned_storage`:
`AllocationAuthority`, `AllocationResources`, `AllocationScope` and the
non-cloneable `AllocationLease`. Format preparation separately supplies the
temporary-storage capability. Both use the same executor admission ledger;
record storage does not depend on the format or executor crates.

`AllocationLease` reserves the complete requested `Layout` before allocation.
`ReservedBuffer` and `ReservedVec` retain that grant until the allocation is
freed. Growth reserves the replacement while the old block remains charged;
allocation failure leaves the old contents and charge intact. Moving a grant
or transferring it within one authority moves ownership without a release and
reacquire gap. Splitting or merging grants preserves the total; cross-authority
transfers are refused. Requested layouts include stage metadata, chunk
inventories, and retained progress space, rather than just encoded lengths.

`OperationStage` retains its metadata lease outside the boxed storage backend.
The backend and its allocation are destroyed before that lease is released,
including on error and unwinding. Sealing moves this same owner into
`PreparedBytes`; it neither reallocates the backend nor detaches its grant.

`FormatWriterHandle` and `WriterFactory` apply the same ordering to the concrete
writer and closure backings. They admit the actual concrete `Layout` before
fallible boxing and keep the lease outside the box. Internal buffers and
captured values retain their own owners; the outer layout cannot account for
their heap allocations. The handles expose neither a detachable box nor its
lease. Tests observe the charge at allocator deallocation, including unwinding,
rather than treating payload destruction or a final zero balance as proof.

The executor admission ledger serializes reservations and limit changes. It
subtracts sampled legacy consumer usage and outstanding writer grants before
admitting another layout. Legacy samples remain estimates, not atomic grants.
`writer_resource_usage()` derives current memory, peak memory, disk and
descriptor usage from this ledger. `set_limit` refuses a limit below outstanding
writer grants and leaves the previous limit unchanged; the disk setter likewise
refuses a quota below the sum of outstanding writer disk and legacy spill bytes.

`WriterResourceConsumer` reports the ledger's exact live grant total through its
`ConsumerHandle`. It is admission-managed and never backpressureable: parking
the synchronous writer would prevent its own release progress. Spill requests
are consumed at chunk boundaries, and cancellation is checked before consulting
pause state. Grants and cleanup debt keep the admission owner registered after
the provider handle drops; the final owner unregisters it on success, error or
cancellation while the run remains open. Closing the run closes admission and
unregisters the consumer even if an allocation escapes the run. Such an
allocation retains only the synchronized release state and a weak arbitrator
reference, so its eventual drop still settles the ledger without retaining the
run or its telemetry producer. Cleanup debt remains visible until the resource
is actually released; closing a run does not manufacture a zero balance.
[Prepared storage](storage-internals.md#prepared-output-storage)
describes the separate disk and descriptor ownership.

These are requested-allocation bounds, not whole-process RSS bounds. Allocator
metadata/rounding, thread stacks and native I/O internals remain outside them.
Named fixed startup allowances are the standalone Arc/mutex control block and
the executor authority, admission, consumer and storage control blocks. The
environment-derived current-directory lookup is a temporary startup allowance;
retained authored paths, path-construction envelopes and descriptor inventories
are admitted separately. CSV's raw parser buffers and the full intermediate
JSON tree used for JSON-encoded cells remain explicit parser allowances.
Unchanged readers and legacy operators retain their existing owners; a later
deep copy or spill reload is a distinct allocation, not an extension of the
original grant.

### CSV decoding and document ownership

Runtime CSV constructors select admitted decoding for both single-schema and
multi-record input. `DecodeWorkspace` borrows valid UTF-8 and uses admitted
scratch for Latin-1 expansion. Final text, keys, arrays, maps, positional values
and schemas use allocation-owned storage. Single-schema repeated-cell parsing
reuses the existing split grammar; final nested JSON values are constructed
fallibly from the parser's intermediate tree. Compiled multi-record CSV still
rejects repeated input declarations; charset support does not widen that
surface.

Multi-record capture retains admitted policy metadata, pending rows, section
values and trailer state. `FormatReader::prepare_document` and the transport
adapter return `OwnedMap`, so section ownership survives the reader boundary.
Ingest moves sections into owned envelope values and a shared
`DocumentContext`; source coercion preserves the original decoded record on
failure and admits changed values before replacing it. A surviving record,
document, string or key alias keeps the corresponding allocation charged.
Unchanged format readers wrap their existing maps as legacy storage; the
common carrier alone does not admit those allocations.

These boundaries do not establish constant-memory Source or Combine execution:
those paths can still materialize whole inputs. Their remaining residency work
is tracked in [#1183](https://github.com/rustpunk/clinker/issues/1183). CSV's
admitted allocations and the existing ownership-relative estimates below must
remain distinct from a claim about all readers or whole-process RSS.

### Allocation-owned record storage

Governed text, positional values, ordered maps and map keys attach their lease
to the allocation's owner. A record or queue entry is not the lifetime boundary:
a detached string or key can remain live after its original container drops.
Shared text clones retain one allocation and one charge. A consuming container
iterator retains the container charge until its backing is destroyed; yielded
children keep their own independent owners.

Complete requested layouts are admitted before allocation, including text
bytes, vector capacity, map entries and hash-table backing, and their owner
holders. Map bounds follow the pinned container implementation and are checked
against actual allocator requests. Growth admits old and replacement backing
simultaneously. Budget refusal or allocation failure preserves the original
container and returns an unconsumed insertion value.

Shared storage uses a sealed final-owner protocol: the last owner frees the
shared allocation before destroying its payload, which in turn releases its
lease after its children. Unique holders follow the same destruction order.
The public APIs cannot extract an ungoverned backing allocation or grow a
governed container without admission.

Physical heap estimates and admission contributions answer different questions.
Physical estimates include governed storage for pressure decisions. Runtime
admission uses `unaccounted_heap_size` with the executing run's live allocation
resources. It excludes a backing allocation only when its lease belongs to that
same ledger, then classifies each child independently. A custom library source
can supply governed storage from a different provider; that foreign allocation
still contributes its physical estimate to this run. The comparison reuses the
existing authority identity and never transfers or releases a grant.

The separate legacy-only traversal describes storage representation and does
not establish which run owns a charge. Neither traversal subtracts a global
managed-byte total from a sampled consumer estimate. An ordinary deep copy or
spill reload creates new legacy storage; it cannot reuse the original
allocation's charge. These primitives alone do not establish complete reader
admission or replace existing parser-buffer allowances.

Fixed-row node-buffer and streaming estimates still count the record/identity
pair and logical value slots; they do not add nested heap to that heuristic.
Actual rows omit their value slots only when the vector itself belongs to the
executing ledger. Each row is classified independently, including mixed-width
batches. Producers and consumers use the same capability and calculate the cost
before moving the row. A successful send transfers the producer reservation;
the consumer may already have discharged its share before that send returns.
Error drains subtract discarded rows individually instead of resetting the
shared counter.

A consuming memory scan moves its original storage. A shared scan needs new
value slots while retaining the original backing, and disk rows need their full
reload forecast. Sort pressure thresholds therefore keep physical size distinct
from resident attribution. During a streaming spill, the original run remains
charged through serialization and destruction; each decoded row then receives
its own charge before publication. Original shared leaves can outlive either
representation under their intrinsic allocation grants.

Range-join output checks its initial spill-merge frontier before returning any
row to a consumer. The physical footprint includes the open readers, their
decoder workspace, merge entries, file inventory and retained metadata. If
that frontier alone exceeds the run's hard limit, execution returns a structured
arena memory-budget error and releases its files and charges. The check applies
to streaming, materialized, delayed and shared output drains, independently of
the sink format. It does not claim pre-allocation admission or a bound on later
decoder-table growth; resident attribution still uses the ownership-relative
queries above.

### Prepared-output telemetry

An executor provider supplied with the existing `TelemetryProducer` emits
closed `WriterAdmission`, `WriterStage`, `WriterSpill` and `WriterCleanup` spans.
Scopes are fixed literals: no filename, field value, owner ID or authored node
becomes a metric dimension. Each scope has started/completed/failed/interrupted
counters. Admission counts memory-grant attempts; a completed admission grants
a layout and does not claim the allocator succeeded. Stage observation spans
creation through successful readback and storage release, including metadata,
progress-buffer and stage-box refusal. A resource failure is reported with its
original kind; cancellation is interrupted, not failed. Cleanup continues under
cancellation so resources can be released.

`WriterStageDropped` counts a stage abandoned without a terminal storage result;
it does not infer destination success or failure. The existing `SinkRecords`,
`SinkErrors`, `SinkBytes` and Sink lifecycle metrics own destination-level
outcomes, so this primitive does not duplicate them. `WriterSpillBytes` counts
bytes actually written to temporary storage, including partial writes. Cleanup
attempt counters include retries; they do not claim remaining debt has been
released. Read current debt and live bytes from the storage and ledger APIs.

Each span is emitted once after its outcome, with both timestamps closed. The
producer's fixed counters coalesce; span admission may shed load. Full, sampled
or contended telemetry never changes preparation, cancellation, delivery or
cleanup, and never grows the arena. Producer clones share the existing
telemetry arena/counter owner; their inline stage handles and observation state
are included in the stage metadata grant. The provider's one retained producer
handle is part of its fixed control block. Format-only providers have no
executor telemetry dependency.

Lineage is unchanged: these primitives alter no column values, row routing or
dataset boundary. Raw temporary bytes are internal storage, not a new dataset.

### Existing consumer attribution

Clinker tracks memory in two layers. RSS (resident set size) is sampled at chunk boundaries and supplies the primary spill / abort signal. Alongside RSS, every memory-touching operator (Source ingest channels, Aggregate hash maps, sort buffers, grace-hash partitions, sort-merge accumulators, IEJoin arrays, inline-Combine hash tables, the Reshape per-group input buffer, `node_buffers` slots and their transient scan materializations, and window-runtime arenas) registers a `MemoryConsumer` wrapper with the pipeline-scoped arbitrator. Each operator owns its live byte counter and updates it on every admit / spill transition; the arbitrator queries `current_usage()` per consumer at every policy poll. This pull-mode attribution lets the policy distinguish *reclaimable* bytes (what an operator can give up right now) from currently-held bytes — a grace-hash with on-disk partitions, for instance, reports only its in-memory portion, and the Reshape buffer reports the live bytes of the groups still resident in memory.

Registrations are scoped to the state they mirror, not to the run: each wrapper is unregistered when the state it attributes drains. A Source's ingest-channel consumer is released the moment its receiver disconnects (whichever arm consumed it — the Source arm, a fused `Merge.interleave`, or a fused Transform); a Combine branch's consumer is released when the branch exits — the IEJoin, grace-hash, and sort-merge branches route their clean return and every internal `?` early-return through a single unregister, and the inline-hash branch unregisters at its clean exit; and a `node_buffers` slot's consumer leaves the registry after its final planned reader. A consumer that collects a sequential scan into a resident vector carries an RAII materialization reservation for its complete synchronous use, so normal completion and every error return unregister it. Composition input seeding transfers that same registration into the body-local node-buffer registry without an unregister/register gap or a second charge. While the body Source canonicalizes its seed, the same byte handle first reserves the prospective output in addition to the still-live seed, then drops back to the output estimate when the seed allocation is gone; admission atomically swaps the wrapper under the existing consumer id. Later stages therefore never see charged bytes from state that has already moved downstream, and the registry the policy polls contains live contributors only.

Window-runtime arenas (the columnar backing store that analytic-window evaluation reads from) are attributed but not independently spillable: an arena is immutable once built and is freed only indirectly, when the operator that consumes its windows drains to disk. Its wrapper reports the arena's bytes so the arbitrator's attribution is complete, but ranks last among spill victims so a policy never elects an arena while any consumer that can actually pause or spill remains.

### Per-operator arbitration parameters

Each registered consumer carries two parameters the active policy reads: a **spill priority** (lower is spilled first under `Priority`) and a **back-pressure flag** (whether its producer can be paused instead). The defaults are:

| Operator class | `spill_priority` | `can_back_pressure` |
|----------------|------------------|---------------------|
| `node_buffers` slot (inter-stage buffer) | 0 | false |
| grace-hash Combine | 10 | false |
| Reshape | 15 | false |
| sort buffer / IEJoin build | 20 | false |
| sort-merge Combine | 25 | false |
| hash Aggregate | 30 | false |
| inline-hash Combine | 30 | false |
| Source ingest | N/A | true |
| streaming Aggregate | N/A | false |
| transient scan materialization | last | false |
| window arena | last | false |

Lower priority is spilled first, so `node_buffers` slots (priority 0) are the cheapest victim class — spilling an inter-stage buffer to disk costs one LZ4 + postcard round-trip and frees the most reclaimable bytes per call. The blocking operators climb from there: a grace-hash Combine (10) is preferred over Reshape (15), which is preferred over a sort buffer (20), which is preferred over a hash Aggregate or inline-hash Combine (30). Reshape sits between grace-hash and sort because its spill round-trip re-runs synthesis on reload — costlier to evict than grace partitions, cheaper than an external-sort merge — and it spills the raw per-group input records rather than post-processed output.

A **Source** and a **streaming Aggregate** show `spill_priority=N/A` because neither *operator* holds spillable accumulated state. A Source's `try_spill` always frees zero bytes — its only real lever is the pause its `can_back_pressure=true` advertises. A streaming Aggregate emits each group as it completes and never accumulates a spillable group table. The `N/A` here is about the operator's own state, not its downstream handoff: when a streaming stage's output rides a per-batch streaming handoff to a single consumer, that handoff registers a priority-0 consumer just like a `node_buffers` slot does, and its in-flight batches are spilled to disk one batch at a time if RSS crosses the soft threshold while they are in flight. So a streaming Aggregate's *group table* is never a spill victim, but the batches it hands downstream can be.

#### Source-order barrier accounting

A Source that declares record-level `sort_order` is the exception to the usual
pause-only Source shape: it inserts a verification barrier around each physical
file before the ingest channel releases that file downstream. The barrier reuses
the Source consumer's live-byte counter. While a file is staged, that counter is
the shared `SortBuffer`'s resident bytes plus one adjacent record retained for
inversion detection, plus any verified records from the preceding file still
queued downstream. During resident release, ownership moves from the sorter to
an explicitly charged release total and then to the bounded-channel estimate;
the transition subtracts a record only after the send succeeds, so the same row
is neither omitted nor charged twice. Document punctuation does not grow with
row count: admission allows only a flat file or one matching inner frame, so the
barrier holds a statically bounded set of open/close events. Different physical
files and different Sources never share a barrier or an authored-key comparison.

The barrier does not expose a second arbitrator victim. At the Source's next
record boundary it honors the shared consumer's spill request, the sort buffer's
resident threshold, or the run-wide soft-pressure signal and calls the existing
`SortBuffer` spill path. `SortedRunMerger` performs any bounded-fan-in cascade;
each intermediate run is charged before its consumed inputs are unlinked and
their exact charges are released. For final release, the barrier writes one
merged spool, charges its exact completed size while the input-run charge is
still live, then releases the input charge. It reads that final spool once to
validate every row before emitting the file, and releases the final charge only
after the second read has drained. Thus a decode or merge failure cannot leak a
prefix of an unverified file, and disk accounting covers the input/output overlap
at every completed-run transition.

The same Source counter remains live during release. A resident result moves
from the remaining sorted-spool estimate to the bounded-channel estimate only
after each record send transfers ownership, so the handoff never reports a
zero-byte gap. A spilled result charges each merge reader's 8 KiB I/O buffer plus
one decoded-record estimate, along with the final writer or validation reader
that overlaps it. Cleanup clears these transient charges and every outstanding
stage disk charge on cancellation or read/write/merge failure.

Records spilled through this path keep their typed `SourceRowId` as the stable
tie-break payload. Spill serialization reconstructs record-owned context, so the
barrier carries the physical file's original shared document-context handle once outside
the row spool and reattaches that exact allocation to every repaired record on
release. This preserves pointer identity without retaining one extra context per
row and without replaying the source.

When memory pressure crosses the soft threshold (80 % of `limit`), the arbitrator runs the active policy to pick a victim and invokes the corresponding action: `pause()` on a back-pressureable consumer (its producer's hot loop parks on a `Condvar` until `resume`), or `try_spill(target_bytes)` on a spillable consumer (the consumer's wrapper flips a spill-requested flag the operator reads at its next batch boundary). When RSS crosses the hard limit, the engine fails fast with `E310 MemoryBudgetExceeded`.

This means:

- Pipelines always complete if disk space is available, regardless of input size.
- Performance degrades gracefully under memory pressure — you will see slower execution (and possibly disk I/O), not failures.
- The memory limit is a soft ceiling, not a hard wall. Momentary spikes may briefly exceed the limit before the policy fires.

## Bounded-memory contract for non-fused stages

A stage runs streaming — no charged per-stage `node_buffers` slot — when it hands its output to a single downstream sink Output and roots no window: fused Source → Transform → Output and Merge.interleave-of-Sources chains, plus single-branch Route, non-fused Merge, `streaming`-strategy Aggregate, and hash-build-probe Combine probe-side feeding one Output (see [Streaming vs. Blocking Stages](execution-model.md)). The remaining boundaries — multi-branch Route fan-out, a Merge or other operator whose output forks to several consumers, Composition bodies, diamond DAGs, and every blocking strategy — materialize records into per-stage `node_buffers`. Each slot registers a `NodeBufferConsumer` with the arbitrator (priority 0 — the cheapest-to-spill victim class), so the active policy's victim selection is fully attributed.

When a buffer crosses the soft threshold (80 % of the limit) the arbitrator runs the active policy. Under the default `pause`, the producer feeding the buffer is paused at its inbound channel; under `spill` or when no consumer can be paused, the slot spills to disk using the same LZ4 + postcard frame format as grace-hash sort partitions. When RSS crosses the hard limit, the engine fails fast with `E310 MemoryBudgetExceeded { node }` naming the operator whose hot loop polled the abort gate. The `explain --code E310` diagnostic covers the full diagnostic model, including the composition-involved two-shape error model.

Every materialized slot is spill-eligible, including slots with several consumers and slots keyed by a producer output port. Pressure can therefore spill whichever live priority-0 slot the policy elects; exact `(producer, producer_port)` keys keep independently spilled Route/Cull branches isolated.

Every materialized slot declares an O(1) remaining-reader count when it is
published. The single-reader path removes the authoritative slot directly.
For several readers, dispatch remains sequential: each earlier reader borrows
the same immutable backing and opens a fresh cursor, while the ledger retains
the slot through its final reader. `Memory` backing clones one event at a time;
`Spilled` and `Mixed` backing opens at most one spill file for the active scan,
so file-descriptor use is O(1) per active scan rather than O(number of
readers). No N-way copy or N-way cursor set is created.

A consumer that needs a full resident vector reserves that materialization
before collecting the cursor. A projected overlap beyond the nonzero hard
limit returns `E310 MemoryBudgetExceeded` with the `NodeBuffer` category and
the consuming node's name. A consumer that stays lazy, such as an Output
writer on the envelope-reconstruction path, reads directly from the cursor
without a full duplicate. The final reader reclaims the authoritative backing
and its existing registration.

`MergeSpilled` is the one destructive spill form: its k-way merger consumes
and unlinks input runs. On the first shared read, the executor folds those runs
once into one ordinary re-readable spill file. It charges the replacement file
before releasing the input-run charges, so the real disk-overlap peak is
enforced; exceeding `max_spill_bytes` returns `E320 SpillCapExceeded` and
removes both replacement and input registrations/files. Later readers reopen
the folded file and do not repeat the fold.

Use `clinker run --explain` to predict which stages will dominate the budget before runtime — each node carries a `buffer: streaming | materialized` annotation. Materialized nodes charge `pipeline.memory.limit` as one full-stage slot and spill the whole stage; streaming nodes charge per in-flight batch and, on a single-consumer edge, spill those batches one at a time. Both classes count against the limit and can spill — the annotation tells you the *granularity* (whole-stage vs. per-batch), not whether a stage is exempt from the budget.

## Reading `--explain` arbitration output

Alongside the `buffer:` class, every node in the **Physical Properties** stanza of `--explain` carries an `arbitration:` line giving the [per-operator parameters](#per-operator-arbitration-parameters) the arbitrator would apply at runtime. The numbers are derived at plan time — `--explain` does no I/O, so there are no live consumers to query — but they mirror the runtime values exactly, so an author can read the spill/pause model before running the pipeline.

For a fast Source feeding a slow Aggregate (the canonical bounded-memory shape), the relevant lines read:

```text
=== Physical Properties ===

source.orders:
  buffer: materialized
  arbitration: spill_priority=N/A, can_back_pressure=true, predicted_peak=1K, predicted_freed=0B, predicted_subtree_reclaim=1K

aggregation.dept_totals:
  buffer: materialized
  arbitration: spill_priority=30, can_back_pressure=false, predicted_peak=1K, predicted_freed=1K, predicted_subtree_reclaim=1K
```

The Source advertises `can_back_pressure=true` and `spill_priority=N/A`: when memory pressure rises, the arbitrator pauses the Source rather than asking it to spill (it has nothing to free). The hash Aggregate advertises the opposite — `spill_priority=30`, `can_back_pressure=false` — so it is a spill victim, ranked behind any cheaper consumer.

The three `predicted_*` values are the scheduler's inputs (see [Scheduling](#scheduling) below). `predicted_peak` is the live volume a node is expected to hold at its peak — seeded at a file-backed Source from its `path:` file's on-disk size and propagated forward. `predicted_freed` is what the node returns to the budget the instant it finishes draining: a blocking Aggregate holds its whole accumulated input (`predicted_peak=1K`) and frees it on drain (`predicted_freed=1K`), while a streaming Source carries the volume through but frees nothing the instant it drains (`predicted_freed=0B`). `predicted_subtree_reclaim` is the largest reclaim the node's downstream chain eventually unlocks: the Source frees nothing itself, but launching it is the only way to reach the point where its downstream Aggregate can drain, so it inherits that Aggregate's reclaim (`predicted_subtree_reclaim=1K`). Propagation of the subtree value stops at a convergence node — the Combine two independent chains feed — so each feeding chain keeps the distinct reclaim it owns up to the join rather than the shared post-join total. All three render `0B` when no file-size seed reached the node — a multi-file (`glob`/`regex`/`paths`) or absent/unreadable Source, or any node downstream of one. The bytes are formatted in the same binary-prefix units as `memory.limit` (`1K`, `64M`, `2G`), and the same three values appear in `--explain --format json` under `node_properties.<name>.predicted_peak_bytes`, `predicted_freed_bytes_on_complete`, and `predicted_subtree_reclaim_bytes`.

A **`=== Buffer Edges ===`** section follows, listing the `node_buffers` slot between each pair of non-fused stages. Every slot is a priority-0, non-back-pressureable `NodeBufferConsumer` — the cheapest victim class — and the `slot=` number is the stable producer index the executor admits into. For a multi-output producer, `port=` completes the exact runtime slot identity. The slot carries the producer's predicted volume (it holds the producer's materialized output and frees that whole buffer once the consumer drains it):

```text
=== Buffer Edges ===

edge source.orders -> aggregation.dept_totals:
  buffer: node_buffer (slot=0)
  arbitration: spill_priority=0, can_back_pressure=false, predicted_peak=1K, predicted_freed=1K (producer: source)
```

Reading top to bottom: under memory pressure the arbitrator first spills the inter-stage buffer (priority 0), then — if the soft threshold is still tripped — pauses the Source before it ever forces the Aggregate (priority 30) to spill. That ordering is exactly what the default `pause` policy (`BackPressurePreferred -> Priority`) encodes. Cross-reference the [per-operator table](#per-operator-arbitration-parameters) to see where any operator in your own pipeline lands.

## Scheduling

When a pipeline has several nodes that are *simultaneously runnable* — every one of their inputs is ready, so the executor could legally run any of them next — the engine picks one deterministically rather than walking topological position blindly. The common case is a single linear chain where only one node is ever runnable at a time, and there is nothing to choose. The choice matters only for a pipeline whose DAG has **multiple independent subgraphs** (for example, two unrelated Source → Aggregate branches that a later Combine or Merge joins): both branches' lead nodes become runnable together.

The engine runs one node to completion before dispatching the next. When two independent chains converge — two Source → Aggregate branches a later Combine joins — both branches' outputs must be materialized and held until the Combine consumes them, so the chain that runs *second* builds its working set while the *first* chain's output already sits in a buffer. Running the memory-heaviest chain first therefore drains and releases its large state before the lighter chain's output has to coexist with it, lowering the peak resident working set; running it last makes its large state coexist with the already-materialized output of every chain that finished before it. What the ranking also buys is *when* the frontier offers a mix of node kinds: with a blocking operator ready to drain (and reclaim its accumulated state) alongside a fresh Source about to charge a new buffer, draining first reclaims headroom before the new charge lands, and under a tight budget the engine prefers the runnable node that fits the remaining headroom over one that would overflow it.

The engine ranks the simultaneously-runnable nodes by these rules, in order:

1. **Headroom fit.** A node whose `predicted_peak` fits within the budget's remaining headroom is preferred over one that does not. Running a node that fits avoids tipping the live working set over the soft threshold and forcing a spill that a different ordering would have avoided. A node with an *unknown* peak (`predicted_peak=0B` — no file-size seed reached it) counts as fitting, because `0` is always within any headroom; this keeps an unestimated pipeline on its topological order rather than deprioritizing every node.

2. **Immediate-freed tiebreak.** Among nodes that fit equally, the one with the larger `predicted_freed` runs first. Finishing a node that returns more bytes to the budget *the instant it completes* maximizes the headroom available to everything still waiting — the same intuition as shortest-remaining-state-first. A ready blocking operator (which reclaims its accumulated state now) therefore wins over a fresh Source (which frees nothing the instant it drains), because the immediate reclaim is the headroom-minimizing choice.

3. **Subtree-reclaim tiebreak.** Among nodes that also tie on immediate freed — most importantly the fresh Sources of independent chains, which all free `0` the instant they drain — the one with the larger `predicted_subtree_reclaim` runs first. This front-loads the chain whose completion eventually frees the most: a Source's value is the reclaim its downstream Aggregate will release, so the heavier chain's Source is dispatched ahead of the lighter one even when it sorts later in topological order. Because it ranks *below* immediate freed, it never elects a fresh heavy Source over a ready light Aggregate (which would raise the peak), only between candidates whose immediate reclaim is equal.

4. **Stable-index tiebreak.** If two nodes still tie (equal fit, equal immediate freed, equal subtree reclaim — including the all-unknown case where all are `0`), the one with the lower stable node index wins. The index is each node's position in the plan's topological order — the exact sequence the executor walks the DAG — so this tiebreak is fully deterministic and independent of the machine, the thread schedule, and the order the runnable set happened to be assembled in.

**Fallback to topological order.** When no node carries a volume estimate (every `predicted_peak` is `0B`), rules 1–3 are no-ops — every node fits and every node frees the same `0` — so rule 4 alone decides, and the engine runs nodes in exactly the lowest-index / topological order it used before any volume estimates existed. This is the load-bearing guarantee: **scheduling never changes record output or branching order.** A pipeline's data output is byte-identical regardless of the predictions; the estimates only steer *which runnable node goes first* to reclaim headroom sooner, front-load the heaviest chain, and prefer fitting nodes under pressure, never *what* each node computes.

Because the predictions are a pure function of the plan shape and the input files' on-disk sizes (resolved against the pipeline file's directory, never the process working directory), the scheduling decision is identical on every machine for an identical plan over identically-sized inputs.
