# Memory Arbitration & Scheduling

*User-facing view: the User Guide's "Memory Tuning" page.*

*Interactive companion: the [memory system explainer](memory-explainer.html) walks through the budget, ledger, arbitration policies, backpressure, spill and scheduler, with a simulator that runs the decision rules on this page. Its script ports `select_victim`, `reconcile_backpressure`, `spill_reclaimable` and `next_runnable`; update it when those change.*

This page is the engine-internals reference for how Clinker tracks, attributes, and reclaims memory at runtime, and how it orders simultaneously-runnable nodes to keep the resident working set bounded. It covers the `MemoryConsumer` wrapper registry, pull-mode byte attribution, the per-operator arbitration parameters the active policy reads, the bounded-memory contract for materialized stages, the `predicted_*` values that feed both `--explain` and the scheduler, and the four ranking rules the scheduler applies (with its fallback to topological order). The user-facing knobs — the `memory:` block, the `--memory-limit` flag, the backpressure-policy selection, sizing guidance, and monitoring — live in the User Guide and are intentionally not repeated here. For how each stage's buffer class (`streaming` vs `materialized`) is decided, see [Streaming vs. Blocking Stages](execution-model.md).

## How it works

### Exact allocation admission for prepared output

`clinker_format::preparation` prepares one complete output operation before
delivery. CSV, JSON, XML, fixed-width and SWIFT writer construction in the CLI
and executor uses this path; EDIFACT, X12 and HL7 retain their existing
allocation behavior. `MemoryOnlyResources`
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

The executor admission ledger is the run's one charged total; it serializes
reservations, consumer charges and limit changes. Governed allocations charge
it through `MemoryArbitrator::reserve`, which checks and charges under the
ledger's one lock and returns a `Grant` or a `Shortfall`. Registering a
consumer (`register_consumer` for run-scoped state, `register_node_consumer`
for a node's state, each with the consumer's `ConsumerHandle` and a label)
binds the handle to the same ledger: from then until the consumer
unregisters, every charge through the handle is a ledger charge under that
lock, and unregistering releases what the handle still holds. A handle charges
for one consumer at a time: registering a second consumer through a handle
still bound to the first is an internal error that registers nothing, and the
caller returns it.
`ConsumerHandle::try_grow` and `try_resize` check a growth against the limit
with every other charge, so a handle growth and a governed allocation, or two
of either, can never together pass the limit; the older `set_bytes` /
`add_bytes` charges are applied unchecked. A `Shortfall` charges nothing and
carries a snapshot taken under the same lock: the limit, the charged total, each labelled holder's
current bytes under its node name and an author-vocabulary surface, and the
bytes no labelled holder owns, so
the holders and that remainder add up to the charged total. A Source's
consumer unregisters once the Source has finished reading, and the drain arm
hands its rows on only after that, so the rows it read can still be charged
in its name once its consumer is gone. `unregister_consumer` reads
`can_back_pressure` once (the predicate that makes a listed holder a
Source), and for a Source whose grants are still live the ledger keeps its
entry, unlabelled and marked as a finished Source's, until the last of those
bytes drops; the snapshot reports them as `retired_source`, always inside the
remainder. Nothing is re-charged. An E310 shows them as memory not held by
any one node and never counts them as state that cannot spill, the same rule
as for a Source still listed; bytes granted in no consumer's name, or in the
name of another consumer that has unregistered, still count. A consumer that
unregisters while it is the walk requester stops being the requester, so the
rest of its arm's walk allocations are charged to no consumer and never join
a finished Source's rows. A reserve made for
a consumer is attributed to it for the life of the grant, and the ledger keeps
each consumer's high-water mark over its handle bytes plus its attributed
bytes; attribution never changes what is admitted. Every release advances a
release epoch. `writer_resource_usage()` derives current memory, peak memory,
disk and descriptor usage from this ledger; its memory figures are what
governed allocation grants hold, not the consumer handle charges beside them. `set_limit` refuses a limit below the
charged total and leaves the previous limit unchanged; the disk setter likewise
refuses a quota below the sum of outstanding writer disk and legacy spill bytes.

On the walk thread a `Shortfall` is not yet a refusal. A `reserve`, a
`Grant::try_grow` or a `ConsumerHandle::try_grow` / `try_resize` made on the
walk that does not fit runs a reclaim pass without holding the ledger lock:
the registered consumers that cannot be paused and hold bytes are taken in the
run's policy order (ties to the older consumer), the requesting consumer last,
each consumer's figures read once when the pass begins (the shipped policies
order them with one sort),
and each whose state the walk owns is spilled there and then (see "How a
pass reaches state" below). A consumer whose state the walk
does not own is skipped and never asked to act; one the running dispatch arm
holds (a slot out of its scope, a cell its owner is borrowing) frees nothing
this pass and has its own spill request raised, which its owner answers at
its next boundary or push. A pass
aims to bring the ledger, with the request charged, down to the resume
watermark, not just to fit the request. The pass's progress is the sum of its
victims' own releases, recorded by the ledger while each victim spills on the
walk (the slot's charge and the governed allocations its records drop), so a
concurrent release by another thread never counts as a victim's progress; it
only marks the pass as having seen a release. The request retries after each
pass. It is refused only when a pass freed nothing with no release during it
and a final pass then freed nothing too; the refusal's snapshot is the one
taken with the retry after that final pass. Every other thread's request is
checked once and never spills. Governed allocations the walk makes while a
dispatch arm runs are charged to that node's first registered consumer, and
release against it however the arm has moved on.

#### How a pass reaches state

A pass reaches an elected consumer's state through the walk reclaim set, in
one of two ways:

- **Node-buffer slots** live in the set's frames: one frame per dispatch
  scope, the top level's and one per composition body running inside it. A
  pass searches the running scope's frame first, then each calling scope's,
  so a body that falls short can spill a resident slot its callers still
  hold, though the body itself never reads, replaces or removes a caller's
  slot.
- **Walk-owned state** lives in a cell of its own (`Rc<RefCell<_>>`) that is
  registered under the consumer it charges through `register_walk_owned`
  (`crates/clinker-exec/src/pipeline/memory/walk.rs`). Registered today,
  each with what its figure leaves out while its owner holds that part:
  - the run's document dead-letter state (`document_dlq.rs`): its held
    rows, resident tails only; a pass that finds the state mid-step raises
    its spill request instead;
  - an Output's per-document buckets (`document_dlq.rs`, one registration
    per bucket): resident records; a pass that finds the buckets borrowed
    raises the bucket's spill request;
  - the rows parked for a deferred consumer (`parked_generations.rs`, one
    registration per edge): resident segments, less any an open replay
    cursor shares;
  - a Cull's and a Reshape's group buffers (`cull_dispatch.rs`,
    `reshape_dispatch.rs`): resident groups, less groups already on disk or
    taken out for routing;
  - a grace-hash Combine's partition table (`grace_hash/mod.rs`): the
    partitions its build is still filling, and nothing once the probe holds
    them;
  - a hash Aggregate's group tables on the strict per-document and
    time-windowed arms (`aggregate_dispatch.rs`, one registration per
    table): the groups each table holds resident, and nothing from the
    moment a finalize takes the table, or ever for a table with no spill
    directory.

  These seven are every production caller of `register_walk_owned`. A pass
  does not reach the following state today:
  - the sort-merge and IEJoin kernels' state, which spills on thresholds of
    its own. Their consumers report 0 reclaimable, so no pass elects them,
    and a refused request's E310 lists them as `cannot spill`. The
    per-operator table and the consumer inventory still class them as
    spillable at priorities 25 and 20: that is their class once they
    register, not what a pass can do today;
  - an authored Sort's buffer, which registers no consumer of its own and
    spills on a threshold of its own, sized from the limit
    (`sort_dispatch.rs`, `operator_memory_limit`);
  - an inline hash join's table, which never spills (grace hash is the
    spillable join strategy): its consumer reports 0 reclaimable;
  - a streaming-ingest Aggregate's tables, which its worker thread owns
    (below);
  - a relaxed-key Aggregate's table, which does not register and ranks by 0
    while it ingests and while the commit keeps it, so no pass and no
    soft-threshold poll elects it. The in-place finalize that ends its
    ingest, and the commit's retract and finalize after it, read only
    resident groups: the in-place finalize fails on a table with spilled
    groups, so its spill would end the run or lose the Aggregate's groups
    rather than free memory
    ([#1288](https://github.com/rustpunk/clinker/issues/1288)).

  The registry is
  run-scoped, outside every frame, and holds only a `Weak` to each cell, so
  an owner dropped on any exit is never reached. Several cells may register
  under one consumer; one cell may serve several consumers and spills only
  what the elected one charges. The owner keeps the registration beside its
  state, so both drop together. It borrows its cell only for one operation
  of its own, never across a call that can charge another consumer, a
  channel wait or a call into another dispatch arm. Its spill never
  reserves.

State owned by a thread other than the walk (a Source reader, a streaming
writer or worker, such as a streaming-ingest Aggregate's tables) is not
reached by a pass: its consumer is skipped, and the pass raises no spill
request for it. A streaming-ingest Aggregate's tables rank by their charge,
so a pass can elect them and find them `NotOwned`; they spill on their own
thresholds, and on a spill request the soft-threshold poll raises, which
the worker reads as it adds its next row
([#1247](https://github.com/rustpunk/clinker/issues/1247)).

Each victim a pass asks ends in one of three outcomes:

- **Spilled.** The walk owns the state and wrote resident state of it to
  disk, now, on the walk. For walk-owned state, at least one free cell
  reported that it wrote (`OwnedSpillResult::Wrote`); a cell's spill never
  reports a write it did not make.
- **Busy.** The walk owns the state but its owner holds it right now (a slot
  out of its scope, a slot whose rows a live reader's cursor or view still
  shares, a cell its owner is mutating or is itself the requester, or a
  free cell that holds state for the consumer but had nothing it could
  write: every part already on disk, taken out for use, or shared with a
  reader). The pass frees nothing from it and raises the consumer's spill
  request, which the owner answers at its next push, yield or batch
  boundary. A shared slot's spill writes nothing and leaves its figure and
  charge as they were; the E310 lists it as `in use`, never at its floor.
  The walk's spill-request sweep at the next node dispatch clears that
  request and writes nothing while the reader still shares the rows; the
  next pass that elects the slot raises it again.
- **NotOwned.** The walk holds no spillable state for the consumer: a slot
  its compiled classification keeps in memory, state another thread owns,
  or a registered owner that is gone or no longer holds that consumer. It is
  skipped and never asked to act. The round keeps it as evidence that no
  spill the walk could make would free that consumer's bytes: when the last
  pass to elect it found it `NotOwned`, a refused request's E310 lists it as
  `cannot spill` and counts its bytes as state that cannot spill. A pass
  that finds it `NotOwned` does not name it as asked; if an earlier pass of
  the same round asked it, the reclaim line still names it from that pass.
  A report with no round has no such evidence, so a consumer another thread
  owns still lists as `in use` there.

Spillable state that no pass can reach is a false E310: a request that does
not fit is refused while megabytes it could have freed stay resident. So
every walk-owned spillable state must register through
`register_walk_owned`, and nothing walk-owned and spillable may be
`NotOwned`; the spillable state listed above as not reached by a pass is
the open exception (the inline hash join's table is listed there too, but
it never spills, so no pass could free it). Registering changes none of a consumer's charge, priority,
spill triggers or admission; it only makes the state reachable. A
registered group buffer (Cull, Reshape) also records on its consumer's
handle what spilling its resident groups frees now, which is the figure
it ranks by. A grace-hash partition table records the bytes of the
partitions its build is still filling; a pass that elects it spills
every one of them. Once the build finishes the probe holds every
in-memory partition, so the figure is 0 and no pass elects the consumer,
though the partitions stay charged until they drop. A hash Aggregate's
table records its charge as its figure while it can spill, and 0 after a
spill wrote its groups, from the moment a finalize takes it (a walk arm
takes it out of its cell first), and always when it has no spill
directory or is a relaxed-key Aggregate's table; a table being finalized
or kept for the commit stays charged until it drops, and a refused
request's E310 lists it as `cannot spill`.

A hash Aggregate's table also spills on a group count of its own, fixed
when the Aggregate starts: the largest count whose bucket array, together
with the half-size array it grows from while it doubles, plus each group's
key and accumulator heap, fits 60% of the limit. The bucket count is a
power of two, at most seven eighths full, with one control byte per bucket,
and a growth allocates the doubled array before it frees the old one, so
the count is taken at the moment both arrays are live. A table that cannot
afford its next doubling spills full and keeps its array instead of growing.
Compared with a flat per-group allowance, that spills sooner for most
shapes and later for some shapes with many aggregates per group. The array
is counted from the table's own sizing rule, which a test pins against the
real allocation. Cull's decision check reads the arrays its decision
Aggregate's tables hold now, plus the same per-group heap. Neither figure
reaches the run's ledger: the Aggregate's charge is its value heap, so the
table's fixed bytes are bounded by the group count rather than charged, a
gap tracked separately.

The execution report samples the arbitrator's spill totals and the ledger's
charged peak after dispatch has finished and every Source worker has joined. Ordered
Sources can still release staged spill charges while unwinding cancellation;
sampling at dispatch close would report those already-released bytes as live.
The total and per-stage spill fields include committed charges minus releases,
not every byte ever written to temporary storage.

Two further report figures answer per-node questions those totals cannot.
`per_stage_spill_bytes_written` adds every spill charge a stage records and is
never lowered by a release, so a sort whose runs were merged and unlinked
before the run ended still shows the bytes it wrote while its on-disk entry is
back at zero. `per_node_peak_charged_bytes` gives, for each node whose state is
registered under the node's name (`register_node_consumer`, whose label names
the node), the highest charge any one of its consumers reached. The ledger
keeps each consumer's mark over its handle's bytes plus the governed
allocations made in its name, and every charge to either raises it, so the
figure is exact per consumer rather than sampled, and another node's state
never raises it. A node with several consumers reports the largest single
consumer's mark. Run-scoped state that no node owns (writer output staging,
the credential registry, the document dead-letter state) registers through
`register_consumer` and has no entry. The report's run-wide charged peak is
the ledger's own: the most bytes charged at one instant, consumer handles and
governed allocations together, raised by every charge rather than taken only
when a streaming batch is charged.

`WriterResourceConsumer`'s `ConsumerHandle` charges nothing: its staging chunks
are governed allocations the ledger already holds, so a handle charge would
count every staged byte twice. It stays registered for its inventory row. It
is never backpressureable: parking
the synchronous writer would prevent its own release progress. Spill requests
are consumed at chunk boundaries, and cancellation is checked before consulting
pause state. Grants and cleanup debt issued through the writer's own admission
keep the admission owner registered after the provider handle drops; the final
owner unregisters it on success, error or cancellation while the run remains
open. A lease issued through a Source's attributed view is the exception: it
releases through its reservation state and holds no release authority, so it
does not keep the writer consumer registered. The accounting stays correct
because that consumer's handle charges nothing. Closing the run closes admission and
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
are admitted separately. Each Source channel's slot array
(`SourceIngestChannel::DEFAULT_CAPACITY` slots, allocated once when the channel
is built) is a fixed allowance, constant in input size: a queued attempt's
fixed shell lives in a slot, so only the heap the attempt holds outside the
ledger is charged to its Source. CSV's raw parser buffers and the full intermediate
JSON tree used for JSON-encoded cells remain explicit parser allowances.
Unchanged readers and legacy operators retain their existing owners; a later
deep copy or spill reload is a distinct allocation, not an extension of the
original grant.

### Native JSON/XML configuration and schema caches

`JsonEncoderConfig` and `XmlEncoderConfig` share immutable admitted configuration.
Names, envelope policies and schema-derived plans use `ReservedText` and
`ReservedVec`; the shared configuration backing remains charged through its
final alias's actual deallocation. Construction admits the concrete writer and
factory closure layouts before boxing. There are no raw `JsonWriter` or
`XmlWriter` constructors: direct callers use the finite prepared APIs described
in [extension seams](extension-seams.md#finite-native-writer-construction).

`SharedStorageIdentity<Schema>` gives each plan cache an opaque identity without
retaining the schema payload. Governed storage uses its existing allocation ID;
legacy storage retains a weak backing identity with a separately admitted,
conservative backing reservation. It cannot upgrade to a strong owner. The
weak backing is dropped before its reservation, so even the final weak alias
retains accounting until physical deallocation. Equal-content schemas with
different storage identities do not share a cached plan accidentally.

A schema change prepares the replacement while the old committed plan remains
charged. Failed preparation drops the pending plan; only complete delivery
commits it. Encoding borrows the record tree. XML scalar formatting uses a fixed
128-byte stack scratch and escaping uses bounded chunks; neither codec retains
a rendered record between operations. Encoded bytes are owned by the shared
operation stage and may spill under the same finite authority.

Strict UTF-8 input adds a four-byte probe/incomplete-scalar buffer per open.
It changes no input-sized allocation ownership. Existing JSON parser/scanner
and XML parser/event/record allocations retain their prior allowance; optional
envelope indexes retain their existing cap. Prepared output does not establish
whole-reader admission or a whole-process RSS ceiling.

### Physical-text configuration, truncation tallies and trailers

`FixedWidthEncoderConfig` retains admitted derived layout policies, field and
group names, and selected envelope names. Layout validation borrows the
caller's column/type trees; the encoder does not clone recursive schemas.
Each `FixedWidthEncoder` owns a `truncation: warn` tally per warn field and a
reused per-record staging array, both allocated once, when the encoder is
built, and sized by the layout: a count, a longest length and eight record
numbers per field. Preparing a record writes its hits into the staging array;
commit folds them into the tally. Neither step allocates or requests budget,
so a warn truncation cannot fail a record, and no value text is retained. A
record that fails or is never delivered leaves only staged hits, cleared by
the next preparation.

`SwiftEncoderConfig` shares admitted service literals or document-section
names; literal precedence avoids retaining an unused section name. Column
indices are resolved from the supplied schema without retaining its payload.
The first successful body operation commits the document-derived trailer to
`SwiftEncoder`; later record contexts cannot replace it. Literal trailers
stay with the shared configuration. Pending trailer growth reserves the old
and replacement backing simultaneously, and commit only moves ownership.

Both encoders borrow record strings and document fields. Scalar formatting
uses fixed 1 KiB stack scratch, and fixed-width padding uses a fixed chunk.
Large strings are not copied into a rendered cell before truncation. Encoded
bytes belong to the shared operation stage; truncation tallies and retained
trailers remain memory charges even when staged bytes spill. Factory and writer boxes
keep the admitted concrete-layout owners described above. There are no raw
fixed-width or SWIFT writer constructors: direct callers provide finite
`WriterResources` to the encoder and `PreparedWriter`.

Strict fixed-width decoding validates each selected physical byte range;
ignored gaps and tails retain their existing behavior. SWIFT validates raw
block UTF-8 and preserves continuation separators. Its failed initialization
releases partial body fields, service text and pending envelope events and
remains terminal. These reader corrections do not admit existing line buffers,
parser allocations, retained SWIFT fields or legacy document maps. Reader
materialization and the remaining EDI writers are outside this writer guarantee.

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
contributes its size to the Source's charge for as long as it is queued in the
Source's channel, and the operator that keeps it downstream charges it under
its own rule. The comparison reuses the existing authority identity and never
transfers or releases a grant.

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

`WriterSpillCompleted` establishes that an operation spilled successfully.
The final execution report's post-Source-join spill snapshot can be zero
after those files have been released; it is not a cumulative spill-event
counter. Source parsing failures remain data failures, while cancellation is
interrupted. Reader value fidelity and failed-row selection are covered by
the Source's declared-column lineage mapping; writer representation and
temporary staging add no dataset or syntax-token edges.

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

Clinker tracks memory in two layers. The run's one ledger of charged bytes decides admission and refusal; RSS (resident set size), sampled at chunk boundaries, is a second reading that the soft-threshold poll and the hard-limit backstops also trip on. Alongside RSS, every memory-touching operator except an authored Sort's buffer, which registers no consumer of its own (Source ingest channels, Aggregate hash maps, grace-hash partitions, sort-merge accumulators, IEJoin arrays, inline-Combine hash tables, the Reshape per-group input buffer, `node_buffers` slots and their transient scan materializations, and window-runtime arenas) registers a `MemoryConsumer` wrapper with the pipeline-scoped arbitrator. Each operator owns its live byte counter and updates it on every admit / spill transition; that counter is the consumer handle's charge on the run's one ledger. Victims are ranked by `reclaimable_bytes()`, each consumer's estimate of what a spill would free now, which the arbitrator reads per consumer at every policy poll and reclaim pass; a pass counts what each spill actually releases toward its target. A Source ingest channel's handle charges exactly the heap its queued attempts hold outside the run's ledger (foreign-provider or legacy storage, and a rejection's box): each attempt carries that charge from just before it is sent until the walk takes it off the channel, or until it is dropped unconsumed. Its records' admitted bytes are charged when they are allocated, in the Source's name, so no byte is counted twice. This pull-mode attribution lets the policy distinguish *reclaimable* bytes (what an operator can give up right now) from currently-held bytes — a grace-hash Combine, for instance, reports only the partitions its build is still filling in memory, and nothing once its probe holds them, and the Reshape and Cull buffers report what spilling the groups still resident would free, each row counted as a `node_buffers` slot counts it, with rows on disk or taken out for processing counting 0, and a hash Aggregate's table reports its charge while it can spill and nothing once its groups are on disk or a finalize has taken it, and never for a relaxed-key Aggregate's table, which the commit retracts from in memory. The sort-merge and IEJoin kernels report nothing reclaimable: no pass can reach them, and each spills on thresholds of its own.

Registrations are scoped to the state they mirror, not to the run: each wrapper is unregistered when the state it attributes drains. A Source's ingest-channel consumer is released when its stream ends: when the walk takes the reader's `Ended` event, which the reader sends only after it has released everything it held (whichever arm consumed it — the Source arm, a fused `Merge.interleave`, or a fused Transform), or, for a Source the walk never drained to its end, when the walk stops; a Combine branch's consumer is released when the branch exits — the IEJoin, grace-hash, and sort-merge branches route their clean return and every internal `?` early-return through a single unregister, and the inline-hash branch unregisters at its clean exit; and a `node_buffers` slot's consumer leaves the registry after its final planned reader. A consumer that collects a sequential scan into a resident vector carries an RAII materialization reservation, and every error return unregisters it. A Combine's inputs keep theirs only until the rows each one charges have a new owner, are written to disk, or are dropped: the Combine's kernel owns its inputs' reservations and ends each one there, never at its own return. Every other operator that collects its input this way holds the reservation for its complete synchronous use: Route, Transform, Sort, Reshape, Aggregate, Cull, Merge and Sink release it when their dispatch returns, and an Envelope releases its body input's when it returns and its header input's once the headers are replaced. Composition input seeding transfers that same registration into the body-local node-buffer registry without an unregister/register gap or a second charge. While the body Source canonicalizes its seed, the same byte handle first reserves the prospective output in addition to the still-live seed, then drops back to the output estimate when the seed allocation is gone; admission atomically swaps the wrapper under the existing consumer id. Later stages therefore never see charged bytes from state that has already moved downstream, and the registry the policy polls contains live contributors only.

Window-runtime arenas (the columnar backing store that analytic-window evaluation reads from) are attributed but not independently spillable: an arena is immutable once built and is freed only indirectly, when the operator that consumes its windows drains to disk. Its wrapper reports the arena's bytes so the arbitrator's attribution is complete, but ranks last among spill victims so a policy never elects an arena while any consumer that can actually pause or spill remains.

Under `dlq_granularity: document` the run-scoped document state registers one consumer for the ledgers that record, per rejected document, which rows have been dead-lettered, so a row held by several Sinks is written once. A ledger is a compressed row set, one per Source and document, keyed by absolute row ordinal. It is exact dedup state, so it cannot spill and ranks last: each admission's worst-case growth is charged through the ledger before the row is recorded, which on the walk first reclaims from every other walk victim; if that falls short the document state flushes its own held rows and retries once, and growth that still does not fit fails the run with an E310 naming the node and its set of rows already dead-lettered. At the end of every rejection pass, and every 65,536 admissions within one, the ledger is compressed and its charge replaced by a bound on the compressed heap; each Sink's pass ends by settling every ledger it grew, the admissions of its late records included, so no per-admission charge outlives the pass that made it. The consumer is unregistered when the run's context drops.

The same consumer carries the document state's held rows (see **Document dead-letter state** under the bounded-memory contract below). While any held row is resident it reports priority 0, alongside the `node_buffers` slots, because it spills with one sequential write per document; its `try_spill` raises its spill request and reports the resident held bytes as what it frees. With no held row resident it reports the last priority and frees nothing, so a state holding only ledgers never shadows a consumer that can spill.

### Per-operator arbitration parameters

Each registered consumer carries two parameters the active policy reads: a **spill priority** (lower is spilled first under `Priority`) and a **back-pressure flag** (whether its producer can be paused instead). The defaults are:

| Operator class | `spill_priority` | `can_back_pressure` |
|----------------|------------------|---------------------|
| `node_buffers` slot (inter-stage buffer) | 0 | false |
| rows parked for a deferred (relaxed-key) consumer | 0 | false |
| output staging (writer resources) | 0 | false |
| grace-hash Combine | 10 | false |
| Reshape | 15 | false |
| Cull | 15 | false |
| sort buffer / IEJoin build | 20 | false |
| sort-merge Combine | 25 | false |
| hash Aggregate | 30 | false |
| inline-hash Combine | 30 | false |
| Source ingest | N/A | true |
| streaming Aggregate | N/A | false |
| credential registry | last | false |
| transient scan materialization | last | false |
| window arena | last | false |
| document dead-letter state | 0 while it holds resident rows, else last | false |

A consumer whose state cannot spill is listed as charged-only in `crates/clinker-exec/tests/memory_consumer_inventory.rs` with the approval that allows it, and a consumer only part of whose charge spills is listed in the partly-charged-only class with its approval. The document dead-letter state is in that class: its held rows spill, while its emitted-row ledgers and each failed document's verdict slot are charged and never spilled.

Lower priority is spilled first. Among the spillable consumers, `node_buffers` slots (priority 0) are the cheapest victim class — spilling an inter-stage buffer to disk costs one LZ4 + postcard round-trip and frees the most reclaimable bytes per call. Output staging (writer resources) also sits at 0, but victims rank by an estimate of what a spill would free now (`reclaimable_bytes`), not by what they have charged, and a consumer whose reclaimable bytes are 0 is never elected: output staging (a fixed floor per writer), the inline-hash build side, the credential registry, the transient scan materialization, the window arena, a Source's queued-event charge and the sort-merge and IEJoin kernels are counted toward the limit but never chosen as victims. A `node_buffers` slot ranks by its resident rows' slot cost and the payload no Source has charged. Text a Source read is charged to that Source and left out of the figure, even when the slot holds its last copy and a spill would free it; the Reshape and Cull buffers, Output's per-document buckets and a parked edge rank the same way, so a pass can spill one holder, find the target not yet met, and spill the next. That costs extra spill I/O, never a refusal: a pass keeps electing until what it measured covers the target. The blocking operators climb from there: a grace-hash Combine (10) is preferred over Reshape and Cull (both 15), which are preferred over a sort buffer (20), which is preferred over a sort-merge Combine (25), which is preferred over a hash Aggregate or inline-hash Combine (30). The sort buffer row (registered today only by the IEJoin kernel) and the sort-merge row give the order those kernels take once a pass can reach them; until then both report 0 reclaimable and are never elected, and the inline-hash row is never elected either. Reshape sits between grace-hash and sort because its spill round-trip re-runs synthesis on reload — costlier to evict than grace partitions, cheaper than an external-sort merge — and it spills the raw per-group input records rather than post-processed output. Cull shares Reshape's priority for a similar reason: its grouped record buffer is costlier to evict than grace partitions, because reload re-splits the group, but cheaper than an external-sort merge.

A **Source** and a **streaming Aggregate** show `spill_priority=N/A` because neither *operator* holds spillable accumulated state. A Source's `try_spill` always frees zero bytes — its only real lever is the pause its `can_back_pressure=true` advertises. A streaming Aggregate emits each group as it completes and never accumulates a spillable group table. The `N/A` here is about the operator's own state, not its downstream handoff: when a streaming stage's output rides a per-batch streaming handoff to a single consumer, that handoff registers a priority-0 consumer just like a `node_buffers` slot does, and its in-flight batches are spilled to disk one batch at a time if RSS crosses the soft threshold while they are in flight. So a streaming Aggregate's *group table* is never a spill victim, but the batches it hands downstream can be.

#### Source-order barrier accounting

A Source that declares record-level `sort_order` is the exception to the usual
pause-only Source shape: it inserts a verification barrier around each physical
file before the ingest channel releases that file downstream. The barrier reuses
the Source consumer's live-byte counter. While a file is staged, that counter is
the shared `SortBuffer`'s resident bytes plus one adjacent record retained for
inversion detection, plus the charges of any verified records from the
preceding file still queued downstream. During resident release, ownership
moves from the sorter to an explicitly charged release total and then to the
queued record's own charge, the same exact per-attempt charge an unordered
Source's attempts carry. That last hand-off is one step on the counter, taken
before the send, so the same row is neither omitted nor charged twice; a failed
send drops the record together with its charge. The barrier moves the counter
only by the change in its own figure, so it never overwrites the charges its
queued records carry. Document punctuation does not grow with
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

The soft-threshold poll (`MemoryArbitrator::should_spill`, called at batch boundaries) trips when the charged total or the process's peak resident reading crosses the soft threshold (80 % of `limit`), and then takes two separate steps. Under a pausing policy (`pause`, `both`), `reconcile_backpressure` reads only the charged total: above the soft threshold it pauses one back-pressureable consumer that is not the Source being drained (its producer's hot loop parks on a `Condvar` until `resume`), and below the resume watermark it resumes every paused one. The spill arm (`poll_arbitration`) asks the active policy for one victim among the consumers that can be paused or have reclaimable bytes and, when that victim cannot be paused, calls its `try_spill`, which raises its spill request for the operator to read at its next batch boundary. The poll runs no reclaim round and frees nothing itself, and a pause frees no charged bytes. A request on the walk that does not fit beside the charged total is refused with E310 only after the reclaim round described above. Three refusals run no round: a request made off the walk, a request larger than the whole limit, and Cull's per-group decision checks. The hard-limit backstops (below) also refuse at once when the process's peak resident reading passes the limit.

This means:

- State that can spill lets a pipeline complete on input larger than the limit when disk space is available; state that cannot spill (see "How a pass reaches state") still ends the run with E310 when it does not fit.
- Performance degrades gracefully under memory pressure: while what the run holds can spill, you see slower execution (and possibly disk I/O), not failures.
- Checked growth (`reserve`, `try_grow`, `try_resize`) never takes the charged total past the limit. Unchecked charges (`set_bytes`, `add_bytes`) and memory outside the ledger can pass it briefly before a poll or a backstop sees it.

## Bounded-memory contract for non-fused stages

A stage runs streaming — no charged per-stage `node_buffers` slot — when it hands its output to a single downstream Sink and roots no window: fused Source → Transform → Sink and Merge.interleave-of-Sources chains, plus single-branch Route, non-fused Merge, `streaming`-strategy Aggregate, and hash-build-probe Combine probe-side feeding one Sink (see [Streaming vs. Blocking Stages](execution-model.md)). The remaining boundaries — multi-branch Route fan-out, a Merge or other operator whose output forks to several consumers, Composition bodies, diamond DAGs, and every blocking strategy — materialize records into per-stage `node_buffers`. Each slot registers a `NodeBufferConsumer` with the arbitrator (priority 0 — the cheapest-to-spill victim class), so the active policy's victim selection is fully attributed.

When a buffer crosses the soft threshold (80 % of the limit) the arbitrator runs the active policy. Under the default `pause`, the producer feeding the buffer is paused at its inbound channel; under `spill` or when no consumer can be paused, the slot spills to disk using the same LZ4 + postcard frame format as grace-hash sort partitions. Every hard-limit backstop (the inline hash build and its finished-table check, the hash and grace probe loops, the grace chunked fallback, the sort-merge emit loop, the range join's finalize) goes through one check, `MemoryArbitrator::check_hard_limit(node, surface, requester, uncharged)`, where `uncharged` is what the site is about to hold that no consumer has charged yet (0 for a check made after the fact). A hash table's build counts its whole footprint, with two exceptions. A grace partition's (`CombineHashTable::build_from_charged`) rows stay charged to the grace consumer, so only the index, chains and key cache it adds are uncharged. The inline hash join's build rows stay charged under its build input's reservation while the table is built (`CombineHashTable::build_from_reserved`): its periodic checks count the whole partial table, because the input and the table coexist, and its finished-table checks count the table less that reservation's charge, which then moves to the table's handle in one ledger step (`ConsumerHandle::take_over`), so each row's slot is charged once. A row's text its Source read stays charged under the Source as well as in the table's figure ([#1394](https://github.com/rustpunk/clinker/issues/1394)). The other Combine inputs end their reservations where their rows leave them, so no backstop counts an input beside the kernel's own charge of the same rows: the grace build's and driver's once the partition build and the probe loop have consumed them, the inline join's materialised driver once its probe loop has, the range join's once each side's drain returns, and the sort-merge join's in the ledger step that first charges each side's rows to its own handle, the driver's null-key rows included, which stay charged there until each miss is dispatched. That sort-merge step is not one of these backstops: what it adds to the input's charge (the rows' heap and the null-key rows) is admitted first as a checked growth of the join's handle (`try_grow`), which reads only the ledger and on the walk reclaims before it refuses, so the ledger never passes the limit through the step, and a refusal is an E310 naming the join with the growth it asked for. The join's own state reports nothing reclaimable, so such a refusal does not spill the side being taken over. The check decides which reading tripped first. When the charged total plus `uncharged` is over the limit it calls `reclaim_before_abort`: on the walk that runs the same reclaim loop a charge does (the requester elected last) and retries, off the walk it returns at once. Only the shortfall that ends that round refuses, as an E310 naming the operator and its surface, whose request is `uncharged`, whose floor is the charged total plus that request rounded up, and whose reclaim line is the round the walk ran. A refusal that asked for nothing reads `the run held X, over memory.limit L, while <node> held <surface>`. When only the process's peak resident reading is over the limit, the check refuses at once with the process-memory form (`LimitReading::ProcessMemory`: the headline states that reading and the charged total instead of a full limit, and the suggested limit is the peak rounded up); no pass can lower a peak. `reclaim_before_abort`'s projection fits only when the charged total plus the projection fits the limit, so a projection of 0 does not fit a ledger an unchecked growth has carried past it. Checks that refuse one piece larger than the whole limit (the window index, the spill merge's fan-in, the range join's loaded block pair, an oversized aggregate row, a giant group) and Cull's decision checks are not backstops and refuse without a round. The `explain --code E310` diagnostic covers the full diagnostic model, including the composition-involved two-shape error model.

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
limit returns an E310 naming the consuming node and its rows collected for a
full scan. A consumer that stays lazy, such as an Output
writer on the envelope-reconstruction path, reads directly from the cursor
without a full duplicate. The final reader reclaims the authoritative backing
and its existing registration.

**Document dead-letter state.** Under `dlq_granularity: document` the run has one document dead-letter consumer, registered by the run-scoped document state. Its ledgers (above) do not spill. Every failing record of a failed document is encoded as its dead-letter row where it fails and held, behind a small header, in a per-document resident tail; no record is kept. The tails leave memory only on the arbitrator's signals, never on a size of their own: a reclaim pass on the walk that elects the consumer while the state is between steps, which flushes them at once; the consumer's election while the state was busy or by a round off the walk (its spill request, read before every held row and at every document decision); the soft threshold (polled every `pipeline.batch_size` held rows and at every decision); and a held row's own admission when the walk's reclaim leaves it short. Any of them flushes every tail to one chained-extent spill file in the run's spill directory: each flush writes a document's tail as one extent at the end of the file and links it from the document's previous extent. A held row, its index entry (one per failed document) and, on a document's first failure, the document's slot are admitted in one checked growth of the consumer's charge, which is their only charge: on the walk it first spills every other walk victim the pass elects, the document state (the requester) last, and only when that falls short does the state flush its own tails and retry once. A row that still does not fit fails the run with an E310 naming the failing node's held failing rows. A flush is credited, in the spill quota and the run's per-stage spill figures, to the failing node (for a pass's flush, the node whose failure was held last); the rejecting Sink for a flush at a decision or a ledger admission; and, at the end-of-run sweep, the node that first failed the document, which the document's failed verdict records. One flush writes every resident tail, so a node's figure can include rows other nodes failed. Past `max_spill_bytes` a flush returns E320 naming the same node.

The Output that runs under the document granularity keeps each open document's records in a bucket of its own, one `NodeBufferConsumer` per bucket registered under the Output's name. A record's bytes (what its run has not already charged) are grown through its bucket's handle before the record is pushed, with nothing of the Output's buckets borrowed, so the pass the growth starts can spill sibling buckets and every other walk victim, the growing bucket last; if that falls short the bucket spills itself and retries once, and a second shortfall is an E310 naming the Output's rows held until their document is decided. A pass that elects a bucket spills its resident records as a new chunk after any it already has, recorded under the Output's name; one that finds the Output's buckets borrowed raises the bucket's spill request, which its next push answers first. Until the soft-threshold poll is retired, a push while the threshold is tripped also spills the bucket. A document's first rejection streams its chain row by row through its ledger into the dead-letter writer, in the order the rows were held. The file is removed with the state, and nothing in it is ever promoted.

**Rows parked for a deferred (relaxed-key) consumer.** A relaxed-key pipeline runs the steps below its relaxed aggregate at the commit, once per retraction iteration. An edge from outside that deferred region into it (a Source, Route branch, Cull port or composition-body node feeding a deferred Combine) cannot hand its rows over on the forward pass, so the producer parks a copy of them in the run's parked-row store, keyed by the edge and the composition body it belongs to. Each edge has its own consumer, registered under the producer at its first park with the surface `rows held between <producer> and <consumer> for commit` (priority 0, `can_back_pressure` false). A park charges, before it copies any row, what the copy allocates or alone may keep alive with no other charge in this run: each row's place and value slots, text a clone copies (unique text, governed or not), and shared text no admission in this run covers (text a computed expression built, or text another allocation authority admitted), since the copy may outlive the row that carried its only charge. Shared text this run admitted is not charged again: its admission travels with the allocation to every copy until the last one drops. The charge is a checked growth of the edge's charge; on the walk that growth first spills what the pass elects, and when it still falls short the edge's own resident rows spill and the growth is retried once, after which the borrowed rows are written straight to disk and no resident copy is made. A park is never refused for memory. The edge's rows are kept as ordered segments, one per park, so arrival order survives any mix of resident and spilled segments; the edge ranks by the resident segments no open cursor shares, and any reclaim pass on the walk that elects it spills them (an election that finds the store busy raises the edge's spill request, which its next park answers). Every retraction iteration publishes a fresh cursor over all the edge's segments, in parking order, as the reading node's input slot: the view adds no charge, the reader charges its own materialization as for any slot, and a spilled segment is read again from its file, its disk charge recorded once. Rows a region member parks during the commit pass, for a member of another region, form a generation of their own that the next iteration discards before it parks again; the commit walks each region after the regions whose members park rows for it. The store is released — every edge's consumer unregistered and its spill files removed — when the commit returns, on success or error, and at the end of a walk that never reached the commit.

`MergeSpilled` is the one destructive spill form: its k-way merger consumes
and unlinks input runs. On the first shared read, the executor folds those runs
once into one ordinary re-readable spill file. It charges the replacement file
before releasing the input-run charges, so the real disk-overlap peak is
enforced; exceeding `max_spill_bytes` returns `E320 SpillCapExceeded` and
removes both replacement and input registrations/files. Later readers reopen
the folded file and do not repeat the fold.

Use `clinker run --explain` to predict which stages will dominate the budget before runtime — each node carries a `buffer: streaming | materialized` annotation. Materialized nodes charge `pipeline.memory.limit` as one full-stage slot and spill the whole stage; streaming nodes charge per in-flight batch and, on a single-consumer edge, spill those batches one at a time. Both classes count against the limit and can spill — the annotation tells you the *granularity* (whole-stage vs. per-batch), not whether a stage is exempt from the budget. Under `dlq_granularity: document` every Sink reports `materialized`: it holds a charged, spillable `NodeBuffer` bucket per open document, registered under the Sink, until the document's verdict.

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

**Sinks after operators under document granularity.** When any Source declares `dlq_granularity: document`, the plan orders every Sink after every other node, and the scheduler does not consider a Sink runnable until every other node of its pass has run. The four rules then rank the operators among themselves and the Sinks among themselves. Every place that condemns a document is an operator, so each document's verdict is final before any Sink writes one of its records. The constraint overrides rule 2 where a Sink would otherwise run early to free its input, so every Sink's input stays in its charged, spillable node buffer until the Sink phase. A composition body that holds a Sink is refused under document granularity (E378), because a body Sink runs inside its composition's dispatch, where this ordering cannot reach it.

**Fallback to topological order.** When no node carries a volume estimate (every `predicted_peak` is `0B`), rules 1–3 are no-ops — every node fits and every node frees the same `0` — so rule 4 alone decides, and the engine runs nodes in exactly the lowest-index / topological order it used before any volume estimates existed. This is the load-bearing guarantee: **scheduling never changes record output or branching order.** A pipeline's data output is byte-identical regardless of the predictions; the estimates only steer *which runnable node goes first* to reclaim headroom sooner, front-load the heaviest chain, and prefer fitting nodes under pressure, never *what* each node computes.

Because the predictions are a pure function of the plan shape and the input files' on-disk sizes (resolved against the pipeline file's directory, never the process working directory), the scheduling decision is identical on every machine for an identical plan over identically-sized inputs.
