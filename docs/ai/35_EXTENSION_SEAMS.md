# AI Onboarding: Extension Seams

Verified against working tree fb07dd7c (2026-07-26).

Purpose: Map the boundaries an implementer must follow when extending Clinker.
This is a change-impact guide, not a public compatibility promise and not a
plug-in API catalog.

## Source Evidence

Validate this page against:

- `Cargo.toml` and `crates/*/Cargo.toml`
- `crates/clinker-plan/src/config/pipeline_node.rs`
- `crates/clinker-plan/src/config/pipeline.rs`
- `crates/clinker-plan/src/plan/execution/`
- `crates/clinker-exec/src/executor/`
- `crates/clinker-format/src/traits.rs`
- `crates/clinker-exec/src/source/mod.rs`
- `crates/cxl/src/`
- `crates/clinker-core-types/src/diagnostic.rs`
- The relevant crate-local `AGENTS.md`, tests, examples, and user or engine
  documentation

Existing documentation is routing context, not source truth. Recheck every
seam against the current enum arms, trait methods, builders, and tests before
changing it.

## Seam Classes

- **Integration contract:** a typed handoff already used to isolate a
  subsystem, such as `FormatReader`, `RecordSource`, or `CompiledPlan`.
  Implementing the trait alone may not make a feature user-selectable; central
  config and construction wiring can still be required.
- **Coordinated change path:** an intentionally exhaustive enum or phase
  pipeline. Extending it means updating every participating layer rather than
  registering one implementation dynamically.
- **Internal handoff:** a useful boundary inside the engine, but not a reason
  to widen visibility or create a new public abstraction.
- **Unresolved boundary:** current code has a seam, but the intended ownership
  or stability is not settled. Route these through
  [the open-question registry](80_OPEN_QUESTIONS.md) instead of choosing a
  direction during implementation.

Public Rust visibility does not by itself establish a downstream compatibility
guarantee. D-18 and D-19 in the
[production-contract register](15_PRODUCTION_CONTRACTS.md) classify the current
planner and CXL compatibility surface.

## System Handoff

```text
YAML + CXL
    |
    v
clinker-plan: parse -> validate -> bind schemas/typecheck -> lower/enrich
    |
    | CompiledPlan / ExecutionPlanDag
    v
clinker-exec: ingest -> dispatch -> memory/spill -> outputs/DLQ/metrics
    ^                                      |
    | RecordSource                         | FormatWriter
    |                                      v
clinker-net                         clinker-format

Read-only edge consumers: clinker-lineage, explain/config tooling
Declarative overlays: clinker-channel -> compile context / compiled provenance
```

The plan/runtime division is load-bearing: planning owns author input,
validation, schema binding, CXL compilation, and DAG construction; execution
owns record flow and runtime effects. Executor entry points take a
`CompiledPlan`. D-01 through D-11 lock its compiled DAG as the authoritative
runtime input and assign persistent reuse and runtime-envelope repair to
Phase 5 / PERF-01; this page does not claim that downstream work has landed.

## Seam Map

| Change | Class | Primary handoff | Coordinated surfaces |
|---|---|---|---|
| Add a file format | Integration contract plus wiring | `FormatReader` / `FormatWriter` | Format config, reader/writer construction, schema/envelope behavior, docs, tests |
| Add a finite transport | Integration contract plus wiring | `RecordSource` / `SourceInput::Records` | Source config validation, edge construction, shared ingest, docs, tests |
| Add a pipeline node | Coordinated change path | `PipelineNode` -> `PlanNode` -> dispatch | Parsing, validation, binding, lowering, properties, memory, explain/lineage, docs, tests |
| Change CXL | Coordinated phase pipeline | AST plus resolved/typed forms | Parser, resolver, typechecker, analyzer/extractor, evaluator, CLI/docs/tests |
| Add a diagnostic | Coordinated contract | `Diagnostic` and `REGISTRY` | Emission, spans/payload, explanation routing, rendering and registry tests |
| Add a memory-aware runtime facility | Internal handoff | `MemoryConsumer` / `MemoryArbitrator` | Registration lifetime, polling, spill/pause, cleanup, metrics, stress tests |
| Extend channels or compositions | Declarative boundary | Declared ports/config/vars/resources | Overlay precedence, provenance, binding, sealing rules, docs/tests |
| Add a plan consumer | Edge integration | `CompiledPlan` | Keep dependency direction read-only; do not add an executor back-edge |

## Approved Transitional Exceptions

These are the complete approved dependency/parser exception inventory for
CONT-05. Each exception is narrower than the general seam it touches. Full
status and compatibility detail is authoritative in the
[production-contract register](15_PRODUCTION_CONTRACTS.md); the table below is
the contributor-facing change boundary.

The Phase 1 edge probe could not classify any additional boundary. Therefore
D-20 through D-23 are the complete **documentation inventory pending
source-grounded phase verification**, not proof that no other source edge can
exist. CONT-05 closes only when phase verification re-runs the source and
manifest audit against these four permitted/rejected boundaries.

| Decision | Exact permitted boundary | Forbidden expansion | Current evidence and status | Downstream owner |
|---|---|---|---|---|
| D-20 | `clinker-format -> cxl` may use only logical type and document path/index vocabulary: current imports are `cxl::typecheck::Type` and `cxl::analyzer::doc_paths::{DocPath, DocIndex}`. | No CXL parser, resolver, evaluator, planner, or other analyzer dependency; the exception does not authorize pipeline or runtime policy in the format crate. | `crates/clinker-format/Cargo.toml` and imports under `crates/clinker-format/src/` show the current narrow edge. It is implemented as a transitional exception, not a general plug-in boundary. | Phase 1 owns the exception contract. Neutral lower-vocabulary extraction is deferred and requires separately approved work. |
| D-21 | `clinker-exec -> clinker-bench-support` may exist only behind repaired `bench-alloc` instrumentation, with the default and release dependency graphs excluding it. | No default-runtime helper edge and no trusted allocation claim without feature forwarding, verified allocator identity, plausible nonzero end-to-end measurements, and documented measurement distortion. | `crates/clinker-exec/Cargo.toml` and `executor/stage_metrics.rs` show the optional edge. The repair contract is incomplete, so all current allocation measurements are **untrusted**. Phase 1 changes no feature or allocator wiring. | Phase 5 / PERF-07 repairs and qualifies the instrumentation. |
| D-22 | Underlying `serde_saphyr::from_str*` calls are confined to `clinker-plan::yaml` and its parser-specific tests; every other production or test caller uses the canonical wrapper such as `clinker_plan::yaml::from_str`. | No cross-module test bypass, alternate budget, or direct parser call merely because input is a fixture. | `crates/clinker-plan/src/yaml.rs` owns the chokepoint; `crates/clinker-exec/tests/composition_binding_test.rs` still calls `serde_saphyr::from_str` directly. The repair and static gate have not landed. | Phase 2 repairs the parser boundary; Phase 6 / EVID-03 owns qualification. |
| D-23 | A dependency declaration remains only for an implemented, approved use after auditing source, build scripts, generated code, features, tests, and supported API exposure. | No speculative declaration, retention based only on a name match, or reintroduction for an unimplemented async/runtime design. | Root `Cargo.toml` still declares workspace Tokio without an audited source or per-crate use. Other declarations require the same full-graph audit. Phase 1 removes nothing. | Phase 4 owns bounded dependency cleanup under CONT-05; any new runtime coupling still requires approval. |

An exception entry proves only its named boundary. It does not approve adjacent
dependencies, parser call sites, feature forwarding, measurement results, or
manifest cleanup.

## File-Format Seam

**Owner and contract.** `clinker-format` owns byte-to-record and
record-to-byte behavior through the `Send`-only `FormatReader` and
`FormatWriter` traits. Readers and writers are single-thread-owned. Default
document, envelope, source-file, byte-counting, and non-finalizing flush hooks
are part of the contract; wrappers must delegate hooks they do not own.

**Required change path.** A selectable format normally needs:

1. Strict, span-preserving author config in `clinker-plan`, including
   `InputFormat` and/or `OutputFormat` and typed option structs.
2. The reader or writer implementation and `FormatError` behavior in
   `clinker-format`.
3. Construction arms in executor ingest and/or the writer factory. This is
   central wiring, not dynamic discovery.
4. Deliberate decisions for schema inference/coercion, generated columns,
   multi-record input, `ReopenableSource`, document context, output splitting,
   envelopes, and single-document cardinality. Only participate in features
   the format can actually represent.
5. Plan/config tests, format-level round-trip or failure tests, executor
   integration tests, examples, and user documentation.

Do not add a format-specific branch deep in generic operator dispatch or
silently inherit a trait default when the format needs different document
semantics.

**Focused gates.** Start with `cargo test -p clinker-format --locked --offline`
and the relevant `clinker-plan` or `clinker-exec` test target; broaden when the
new format crosses all three crates.

## Transport Seam

**Owner and contract.** `RecordSource: Send` is the transport-neutral row
yielder. It deliberately mirrors the ingest-facing parts of `FormatReader`, is
finite by contract, and is driven by one source thread. `SourceInput::Files`
adapts file readers into this contract; `SourceInput::Records` accepts a direct
implementation. Paginated REST in `clinker-net` is the current non-file model.

**Required change path.** A new transport needs its author config and
plan-time validation, an edge-crate constructor that returns a
`Box<dyn RecordSource>`, CLI or embedding-layer registration as
`SourceInput::Records`, transport tests, and shared-ingest integration tests.
Keep widening, provenance stamping, document-boundary handling, watermarks,
backpressure, and channel handoff in the common ingest path.

Do not route paginated or cursor-based records through fake paths, add
transport arms to node dispatch, or introduce an unbounded polling source.

**Focused gates.** Test the transport crate and
`clinker-exec`'s transport-validation/record-source coverage, then the CLI if
user selection changed.

## Pipeline-Node Seam

A node kind is not a registered plug-in. It is a coordinated extension across
exhaustive representations and passes.

Review each layer, marking non-participation explicitly:

1. **Author surface:** `PipelineNode`, its span-preserving visitor, strict body
   config, names/inputs/ports, config validation, and diagnostics.
2. **Binding and compilation:** schema propagation, CXL typing, composition-body
   parity, compile artifacts, and lowering from `PipelineNode` to `PlanNode`.
3. **Plan enrichment:** topology edges, ordering/partitioning/CK properties,
   streaming classification, scheduling hints, strategy selection, deferred
   regions, cardinality, and explain output.
4. **Runtime:** `dispatch_plan_node`, the node-specific operator, input/output
   ports, inline punctuation, DLQ policy, metrics, cancellation, memory
   consumers, spill, and cleanup.
5. **Edge consumers:** static lineage, field provenance, CLI behavior, channel
   structural overrides, examples, and user/engine documentation where the
   node is visible.
6. **Tests:** parse rejection and diagnostics, compiled DAG shape, property or
   explain snapshots, runtime success/failure, bounded-memory behavior when
   stateful, and composition-body behavior when applicable.

`PlanNode` also contains synthetic execution nodes that authors cannot write.
Do not expose those through YAML merely because runtime dispatch has an arm.

**Focused gates.** Start with `cargo test -p clinker-plan --locked --offline`
and the relevant `clinker-exec` integration test. Include
`clinker-lineage` and CLI tests when their exhaustive plan walks or output
change.

## CXL Seam

CXL changes flow in order:

```text
source -> lexer/parser AST -> resolution -> typecheck -> analysis/extraction
       -> ProgramEvaluator or compiled scalar evaluation
```

Syntax or AST changes must review `NodeId` accounting, recursive walkers,
resolver bindings, type side tables, analyzer visitors, aggregate extraction,
evaluation, formatter/CLI behavior, diagnostics, docs, and tests. A phase may
intentionally ignore a construct only when its input contract guarantees that
an earlier phase removed or rejected it.

Keep CXL independent of pipeline YAML and executor concepts. Planner code may
compile CXL against a bound row, but CXL must not depend back on
`clinker-plan` or `clinker-exec`.

**Focused gate.** Run `cargo test -p cxl --locked --offline`; add planner tests
for schema-bound expressions and `cxl-cli` tests for changed command behavior.

## Diagnostic Seam

`clinker-core-types::diagnostic::REGISTRY` is the single code inventory.
`Diagnostic::error` and `Diagnostic::warning` enforce membership in debug
builds, while `registry_no_orphan_codes` scans workspace source for unlisted
literals.

A new diagnostic may also require:

- a typed `DiagnosticPayload` variant when callers need structured fields;
- an emitting-site test that checks code, primary span, labels, help, and
  corrected input rather than only the rendered message;
- a `docs/explain/<CODE>.md` page and an `explain_code` match arm when
  `clinker explain --code` should expose the code;
- CLI rendering/explanation tests and user documentation if the error belongs
  to a public authoring surface.

Registry membership does not automatically add an explanation page. Check both
surfaces rather than assuming they are generated from one another.

**Focused gates.** Run
`cargo test -p clinker-core-types --test registry_no_orphan_codes --locked --offline`
plus the owning subsystem's test and CLI explanation tests when applicable.

## Runtime Resource And Memory Seams

Run-scoped state is passed explicitly through handles and registries.
`MemoryConsumer` and `ArbitrationPolicy` let the shared `MemoryArbitrator`
observe consumers and choose pause/spill actions. `WriterRegistry` is a public
executor input shape; registries such as `WindowRuntimeRegistry` are
crate-private implementation handoffs. `RunProgress` is a published-counter
seam, not a callback and not a global event bus: the executor advances its
counters and an observer on another thread samples them on its own clock, so
adding a reader costs nothing on the hot path and no producer has to know a
reader exists.

A stateful operator must account for every retained structure, register and
unregister consumers for the correct lifetime, poll spill/abort gates at
bounded intervals, preserve pause/resume liveness, use the run-scoped spill
directory and quota, and clean up on success, error, or interruption. A local
buffer limit is not a substitute for the shared arbitration path, and the
configured memory limit remains a soft control rather than an absolute RSS
guarantee.

Use focused memory/spill tests and explain-output coverage before broad
workspace tests. Follow the file-descriptor guidance in
[the command guide](50_TESTING_AND_COMMANDS.md) for spill-heavy runs.

## Channel, Composition, And Edge Seams

Channels and compositions expose declared boundaries rather than arbitrary
graph mutation:

- Structural overlay operations apply before ordinary compilation, so the
  effective graph passes through normal validation, binding, and lowering.
- Config and vars are layered onto compiled provenance with fixed semantic
  precedence.
- Composition bodies are sealed except for declared inputs, outputs, config,
  scoped vars, and resource slots.
- `clinker-lineage` is a read-only `CompiledPlan` consumer. It must not gain an
  executor dependency merely to observe runtime state; live lifecycle facts
  are supplied at the CLI edge.

Do not turn locked targets into extension recipes. Composition resources and
call-site fields are governed by D-12 through D-16, planner authority by D-17,
API compatibility by D-18 and D-19, and the four exceptions above by D-20
through D-23. Their downstream owners must implement and verify them before the
target behavior is described as current. Route only genuinely new uncertainty
through [the open-question registry](80_OPEN_QUESTIONS.md).

## Review Checklist

Before completing a seam-crossing change:

1. Name the owning subsystem and the typed value crossing each boundary.
2. Classify the change as an integration contract, coordinated path, internal
   handoff, or unresolved boundary.
3. Search every exhaustive match over the affected enum and every wrapper of
   the affected trait.
4. Confirm finite-job, synchronous, bounded-channel, memory-arbitration,
   span-preserving, and dependency-direction invariants still hold.
5. Update public docs and examples in the same change as author-visible
   behavior.
6. Run the smallest boundary tests, then broaden only as far as the changed
   dependency path.
7. Record unresolved ownership or policy in `80_OPEN_QUESTIONS.md`; do not
   encode an inferred answer in implementation.
