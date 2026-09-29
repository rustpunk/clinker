# Observability And Memory Obligations

Read this before adding or changing a plan node, an operator, a dataset
boundary, or anything that retains input-proportional state. AGENTS.md keeps
the one-paragraph summary; this file is the full contract.

OpenLineage lineage, OTLP telemetry, and the memory budget are part of a node's contract, not instrumentation fitted afterwards. Every new node, new feature, and refactor answers all three trigger tests below. "Where appropriate" is decided by the trigger, and an exemption is claimed out loud in the PR rather than left silent — an unstated exemption is indistinguishable from an oversight.

Backfill is in scope for the node types whose contract the change actually alters — where the change adds, removes, or redefines a node's lineage edges, its execution work, or what it retains. Bring those up to the obligations in the same PR; leaving them at the old standard is what keeps the gaps permanent.

Merely editing a file that a node type happens to live in does not trigger backfill. When a change reveals an unmet obligation on a node it does not otherwise alter, file a follow-up issue naming the node and the unmet obligation rather than widening the PR. This bound is deliberate: an unbounded "touched" trigger makes every fix reachable from every other node, and the resulting edits introduce defects whose fixes trigger further backfill.

Deployment configuration for both delivery paths is the workspace `ObservabilityConfig` in `crates/clinker-plan/src/config/observability.rs`, whose `otlp` and `lineage` tables are independently optional, and delivery is wired at the CLI edge in `crates/clinker/src/observability.rs`. Neither path may become a dependency of the engine core.

Three mechanisms make an omission invisible today, which is why these are written as obligations rather than reminders:

- The `clinker-lineage` builder dispatches over plan nodes through catch-all arms, so a new `PlanNode` variant compiles clean and silently produces no lineage.
- `MetricKey` and `SpanName` in `clinker_exec::telemetry` name only `Transform` work, so every other node type is telemetry-blind by construction.
- Registering with `MemoryArbitrator` is a call a consumer makes, not an obligation the compiler enforces. `crates/clinker-exec/tests/memory_consumer_inventory.rs` fails for a `MemoryConsumer` implementation that is not listed with its spill class and engine-table row, but an operator that accumulates without implementing the trait at all still builds and passes its tests.

## Lineage

Trigger: the change affects how a column's value is determined, or whether, where, and in what order a row travels.

- Reading a field to produce a value earns a DIRECT edge; reading it to decide inclusion, routing, grouping, or ordering earns INDIRECT influence.
- A new or renamed dataset boundary — anything that reads or writes external data — earns its `dataset_identity` mapping.
- Adding a `PlanNode` variant earns an explicit arm in the lineage builder, including when the correct arm is a documented no-op. A variant left to a catch-all reads exactly like one that was forgotten.
- `clinker-lineage` stays plan-time and read-only, keeps no dependency on `clinker-exec`, holds no clock, and takes no in-crate HTTP transport. Meet run-lifecycle lineage through `emit::start_event` and `emit::terminal_event`, driven from the CLI edge in `crates/clinker/src/lifecycle.rs`.
- Exempt: a change touching no field values and no row selection, grouping, or ordering — a performance refactor with identical output, an internal error type, diagnostic wording.

## Telemetry

Trigger: the change introduces execution work with a lifecycle, or an outcome worth counting.

- Work that starts, finishes, and can fail earns a `SpanName` variant and an `emit_span`; records, errors, drops, spills, and retries earn a `MetricKey` variant and a `record_metric`. Extending those enums is the ordinary way to meet this obligation, not an escalation.
- Keep both enums closed and fixed-cardinality. A metric keyed by a data value — a group key, a filename, an author-supplied node output — is what makes a startup-sized arena unsizable.
- Emission is admission-controlled and may be dropped. No behavior may depend on a signal being admitted, and no producer may block, grow the arena, or spill to make room.
- Emit a span once, after its work completes, closed at both ends. A collector has no representation for half a span, and independent admission can deliver one half without the other; the live "has begun" signal is the corresponding metric.
- Event fields are deny-by-default and pass field policy before serialization. Carry record values as `SignalValue::Record` so policy sees them typed — formatting a record into a message string moves it past the policy that governs it.
- Exempt: plan-time code that performs no execution work, and a count already derivable from existing metrics.

## Memory budget

Trigger: the change retains anything whose size grows with input — buffered records, hash tables, group state, sort runs, join build sides, spill indexes, retained tails, held or deferred rows, per-document or per-key sets.

A plan or PR that meets the trigger states each item below for every new or changed consumer:

1. Consumer: the type implementing `MemoryConsumer`, where it registers, and how every exit path (success, error, cancellation, drop) unregisters it. Node-owned state registers through `register_node_consumer` under the node's name, the name its spill is recorded under, so the run report attributes its charged peak and spill to that node; `register_consumer` is for run-scoped state no node owns. A consumer backed by a `ConsumerHandle` returns the handle's `peak_bytes` from `peak_charged_bytes`.
2. Bytes: what `current_usage` reports and why it is the true resident size, including collection overhead; and what `reclaimable_bytes` reports, the bytes a spill would free now, which victims are ranked by (the default is `current_usage`; state no spill can free reports 0 and is never elected).
3. `spill_priority` and `can_back_pressure`: the value and the row of the per-operator table in [docs/engine/src/memory-arbitration.md](../engine/src/memory-arbitration.md) it follows; a new row goes into that table in the same change, and the consumer goes into `crates/clinker-exec/tests/memory_consumer_inventory.rs`. A consumer that returns `true` from `can_back_pressure` parks its producer through `wait_while_paused`.
4. Spill: only when the arbitrator asks. Today that is a reclaim pass on the walk that elects the consumer and spills its state there; the consumer's own growth through `try_grow` / `try_resize` falling short; the spill request `try_spill` posts, read with `ConsumerHandle::take_spill_request` at a batch boundary; or `should_spill` / `should_spill_self` reporting the soft threshold crossed when polled at a batch boundary. `spill_reclaimable`, which runs before a paused Source resumes, runs the reclaim round: on the walk it spills walk-owned victims now; with no walk frame it raises their spill requests. Never on a byte, row or count threshold or an RSS reading of the consumer's own. A fixed I/O write buffer is not a spill decision; a size that triggers a flush is. Some existing consumers still spill on a threshold of their own; they are not templates. State the walk owns lives in a cell registered through `register_walk_owned` (`crates/clinker-exec/src/pipeline/memory/walk.rs`), so a pass started by any other request can spill it; the owner borrows the cell only for its own operation. Spillable state no pass can reach is a false E310.
5. Admission: charge growth through the consumer's `ConsumerHandle` with `try_grow` / `try_resize` as it becomes resident (`add_bytes` / `set_bytes` remain only for consumers not yet moved onto them), and no later than the batch boundary where the operator next polls the arbitrator. State collected into memory in one step is reserved through `try_grow` before it is collected, as `reserve_node_buffer_materialization` does. Refuse growth only through the arbitrator: a `Shortfall` from `reserve`, `try_grow` or `try_resize`, or the remaining limit checks `should_abort` and `should_abort_local` for bytes not yet charged. Refusing or aborting on the consumer's own RSS reading, or on a limit of its own, is forbidden. A growth charged on the walk through `reserve` or `ConsumerHandle::try_grow` / `try_resize` runs a reclaim pass before it is refused: the walk spills the state the pass elects, the requesting consumer last, and retries; a refusal comes only after a pass that freed nothing with no release during it, followed by a final pass that also freed nothing.
6. Tests: one proves the state spills and completes under a limit smaller than the state, with output identical to ample memory; one proves nothing reaches disk and the bytes are charged when memory is ample. Use the helper in `crates/clinker-exec/tests/common/memory_pressure.rs`. Neither may be made to pass by shrinking its input, raising its limit or relaxing an assertion — that is a stop, not a deviation.

Exempt: allocation bounded by a constant independent of input size, or state an ancestor consumer already accounts for — name that consumer.

Charged state that cannot spill (`try_spill` frees nothing) is not an exemption an agent may claim: it needs the maintainer's recorded approval, named in the change and in the inventory test. Existing charged-only consumers are not precedent for a new one.

Small inputs in practice, a passing test, and short-lived state are not bounds. The question is whether a bound exists that holds however much data arrives.

Growing a buffer never answers a dropped telemetry signal. The observability arena is fixed and sheds load deliberately, so enlarging it to retain signals converts a reporting gap into a memory-bound violation.
