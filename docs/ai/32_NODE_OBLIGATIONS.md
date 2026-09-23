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
- Registering with `MemoryArbitrator` is a call a consumer makes, not an obligation the compiler enforces, so an operator that accumulates without registering builds and passes its tests.

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

Trigger: the change retains anything whose size grows with input — buffered records, hash tables, group state, sort runs, join build sides, spill indexes, retained tails.

- That state implements `MemoryConsumer`, registers through `register_consumer` on entry and unregisters on every exit path including error and cancellation, reports true bytes through its `ConsumerHandle`, and honors both `take_spill_request` and `wait_while_paused`.
- Exempt: allocation bounded by a constant independent of input size, or state an ancestor consumer already accounts for — name that consumer.
- Small inputs in practice, a passing test, and short-lived state are not bounds. The question is whether a bound exists that holds however much data arrives.
- Growing a buffer never answers a dropped telemetry signal. The observability arena is fixed and sheds load deliberately, so enlarging it to retain signals converts a reporting gap into a memory-bound violation.
