# AI Onboarding: Common Patterns

Verified against the working tree on 2026-08-07.

Purpose: Describe practices that recur in current Clinker code without turning
repetition into architecture law. Reviewed invariants live in
[design rules](30_DESIGN_RULES.md); current and locked contract status lives in
the [production-contract register](15_PRODUCTION_CONTRACTS.md).

Every entry is classified as one of:

- **Observed:** repeated current implementation evidence; copy only after
  checking that the new context has the same constraints.
- **Preferred:** the normal choice for the stated context, with named valid
  alternatives or counterexamples.
- **Local:** useful inside a named subsystem; not a project-wide convention.

The verification date belongs to the evidence snapshot, not to a promise of
permanent stability.

## Runtime And Planning Patterns

### Send-only streaming traits

- **Classification:** Observed.
- **Where:** `FormatReader`, `FormatWriter`, and `RecordSource` cross ownership
  boundaries as `Send`; shared lookup or callback contracts such as
  `RecordStorage`, `MemoryConsumer`, and `FieldResolver` use `Send + Sync` only
  where concurrent borrowing requires it.
- **Use:** Match receiver and thread-safety bounds to actual ownership.
  Streaming readers usually use `&mut self`; arbitrator-facing callbacks use
  `&self` with shared handles or atomics.
- **Evidence:** `crates/clinker-format/src/traits.rs`,
  `crates/clinker-exec/src/source/mod.rs`,
  `crates/clinker-record/src/storage.rs`, and
  `crates/clinker-exec/src/pipeline/memory.rs`.
- **Counterexamples / limits:** A single-thread-owned reader does not become
  `Sync` because another trait is shared. The finite synchronous executor is
  not precedent for async traits.
- **Verified:** 2026-07-29.

### Subsystem errors and direct result types

- **Classification:** Preferred.
- **Where:** `PipelineError`, `FormatError`, `EvalError`, `ConfigError`,
  `ChannelError`, and operator-specific error enums.
- **Use:** Add a failure to the subsystem that owns its vocabulary and
  propagate with an explicit `Result<_, OwningError>`. Use
  `PipelineError::Internal` for a plan-time invariant discovered at runtime.
- **Evidence:** `crates/clinker-plan/src/error.rs`,
  `crates/clinker-format/src/error.rs`, `crates/cxl/src/eval/error.rs`, and
  `crates/clinker-channel/src/error.rs`.
- **Counterexamples / limits:** Small local aliases such as `FileFactory` and
  `TransformOutput` exist. They do not support a project-wide `Result<T>`
  alias, string erasure, `anyhow`, or panic-based runtime handling.
- **Verified:** 2026-07-29.

### Typed mismatch guard at dispatcher entry

- **Classification:** Local implementation of the SECU-03 invariant decision.
- **Where:** The twelve specialized executor dispatchers and their one-shot
  finite-attempt qualification selector.
- **Use:** Check the received `PlanNode` kind before touching mutable operator
  or publication context. Return `PipelineError::DispatchMismatch` with fixed
  dispatcher and expected/actual kind tags, bounded logical node identity, the
  registered `runtime.invariant.dispatch_mismatch` code, internal-invariant
  category, policy-required retry advice, and fixed safe guidance.
- **Evidence:** Dispatcher entry modules under
  `crates/clinker-exec/src/executor/`, `crates/clinker-exec/src/executor/dispatch.rs`,
  and `crates/clinker-exec/tests/invariant_errors.rs`.
- **Counterexamples / limits:** Do not include records, paths, runtime state, or
  authored secret-bearing values. Locally proven internal algorithm and Output
  assertions remain assertions; this pattern is not a blanket assertion-removal
  rule and is not a numbered production-contract row.
- **Verified:** 2026-08-07.

### Typed wrappers and proof-bearing values

- **Classification:** Observed.
- **Where:** `FileId`, `Span`, `DocumentId`, `NodeId`, `PlanNodeId`,
  `DottedPath`, `CxlSource`, `ValidatedPath`, `SourceIdentity`, `ConsumerId`,
  and `AdmittedOtlpEndpoint` distinguish identities and validated state.
- **Use:** Keep construction private when the type proves validation or origin;
  expose borrowed accessors and only the traits callers actually need.
- **Evidence:** `crates/clinker-core-types/src/span.rs`,
  `crates/clinker-record/src/document_context.rs`,
  `crates/clinker-plan/src/security.rs`,
  `crates/clinker-channel/src/dotted.rs`, and
  `crates/clinker-net/src/otlp.rs`.
- **Counterexamples / limits:** A wrapper without a domain guarantee is not
  automatically clearer than its underlying value. TOML and JSON retain their
  own typed parsing paths rather than reusing YAML proof types.
- **Verified:** 2026-07-29.

### Raw-to-admitted-to-composed endpoint chain

- **Classification:** Local implementation of a reviewed security boundary.
- **Where:** Workspace policy resolution in `clinker-plan`, endpoint admission
  and fixed routing in `clinker-net`, and run-local composition in the CLI.
- **Use:** Preserve the chain `raw secret-free policy ->
  AdmittedOtlpEndpoint -> OtlpRuntimeBundle`. Pass the admitted proof forward;
  do not reparse, split, normalize, rebuild, or accept raw endpoint text in a
  sibling production entry point.
- **Evidence:** `crates/clinker-plan/src/config/observability.rs`,
  `crates/clinker-net/src/otlp.rs`, and
  `crates/clinker/src/observability.rs`.
- **Counterexamples / limits:** Test-only loopback construction is not a
  production constructor. The borrowed credential applicator runs after
  admission and cannot replace the origin or fixed route; referenced credential
  resolution remains Phase 4 AUTH-01 work.
- **Verified:** 2026-08-07.

### Run-scoped handles and registries

- **Classification:** Preferred.
- **Where:** `MemoryArbitrator`, `ConsumerHandle`, `WriterRegistry`,
  `WindowRuntimeRegistry`, `SourceReaders`, `ScopedVarsRegistry`,
  `ReopenableSource`, and staging guards.
- **Use:** Pass registries explicitly through their owning run boundary. Share
  immutable/runtime state with `Arc`, use narrow handles for hot counters, and
  retain guards for the complete protected lifetime.
- **Evidence:** `crates/clinker-exec/src/pipeline/memory.rs`,
  `crates/clinker-exec/src/executor/registry.rs`,
  `crates/clinker-exec/src/executor/window_runtime.rs`, and
  `crates/clinker-format/src/source.rs`.
- **Counterexamples / limits:** Process-wide signal registration is a bounded
  platform integration, not precedent for global mutable operator state. A new
  registry is not justified when an existing run context already owns the
  resource.
- **Verified:** 2026-07-29.

### One lifecycle snapshot with independent optional bulkheads

- **Classification:** Observed implementation of the Phase 1 D-41 rule.
- **Where:** CLI `RunLifecycleFacts`, OTLP composition, OpenLineage
  event/delivery adapters, and machine correlation checks.
- **Use:** Record start and terminal facts once, then hand consumers bounded
  immutable snapshots. Keep OTLP and lineage queues, byte accounting, workers,
  deadlines, counters, and typed outcomes independent even though both copy the
  same batch ID, execution ID, semantic fingerprint, and terminal facts and
  must match the machine stream's correlation and terminal truth.
- **Evidence:** `crates/clinker/src/lifecycle.rs`,
  `crates/clinker/src/observability.rs`,
  `crates/clinker-lineage/src/emit.rs`, and
  `crates/clinker-lineage/src/delivery.rs`.
- **Counterexamples / limits:** A consumer does not mint or parse a replacement
  lifecycle identity. Machine control, human diagnostics, metrics spool, and
  guaranteed outputs remain separate paths rather than extra consumers of one
  observability queue.
- **Verified:** 2026-08-07.

### Inline record and control events

- **Classification:** Preferred.
- **Where:** `StreamEvent`, `Punctuation`, `StructuralReject`, `EventBatch`,
  source handoff, and streaming dispatch keep record order and document
  boundaries in one channel.
- **Use:** Represent control information inline when its order relative to
  records is semantic; forward events unchanged through operators that do not
  consume them.
- **Evidence:** `crates/clinker-exec/src/executor/stream_event.rs`,
  `batch_handoff.rs`, `source_stream.rs`, and `dispatch.rs`.
- **Counterexamples / limits:** Progress and cancellation are run control, not
  record-ordered data, and use separate bounded mechanisms. Inline events are
  not a reason to count punctuation as a record.
- **Verified:** 2026-07-29.

### File and finite network sources converge on `RecordSource`

- **Classification:** Preferred.
- **Where:** `SourceInput::Files` adapts format readers and
  `SourceInput::Records` accepts direct finite sources; REST follows the shared
  ingest path.
- **Use:** Implement `FormatReader`/`FormatWriter` for byte formats and
  `RecordSource` for finite non-file transports, then wire author selection at
  the edge.
- **Evidence:** `crates/clinker-format/src/traits.rs`,
  `crates/clinker-exec/src/source/mod.rs`, and
  `crates/clinker-net/src/rest.rs`.
- **Counterexamples / limits:** `ReopenableSource` is the local multi-pass byte
  abstraction. A fake filesystem path, transport-specific dispatch arm, or
  unbounded polling loop is not an equivalent source seam.
- **Verified:** 2026-07-29.

### Typed `ExecutionPlanDag` dispatch

- **Classification:** Observed implementation of a reviewed invariant.
- **Where:** Planning lowers `PipelineNode` into `PlanNode` and an enriched
  `ExecutionPlanDag`; runtime dispatch uses exhaustive node-kind arms.
- **Use:** Attach new behavior to the owning plan representation, compile
  artifact, and dispatch path. Keep plan construction in `clinker-plan` and
  effects in `clinker-exec`.
- **Evidence:** `crates/clinker-plan/src/plan/execution/`,
  `crates/clinker-exec/src/executor/dispatch.rs`, and node dispatch modules.
- **Counterexamples / limits:** Synthetic `PlanNode` variants are not author
  syntax. Do not infer a dynamic plug-in model or create a second runtime graph.
  The authoritative-topology rule is specified in
  [design rules](30_DESIGN_RULES.md#compiled-topology-is-authoritative).
- **Verified:** 2026-07-29.

## Construction And Organization Patterns

### `SchemaBuilder` for incremental aligned construction

- **Classification:** Preferred.
- **Where:** Format readers, projection, planning, aggregation, combine,
  benchmarks, and executor tests build fresh `Arc<Schema>` values while
  preserving column/metadata alignment.
- **Use:** Use `SchemaBuilder::new` or `with_capacity`, add fields and metadata,
  then call `build` when a schema is assembled incrementally or metadata must
  stay aligned.
- **Evidence:** `crates/clinker-record/src/schema.rs` and call sites in
  `clinker-format`, `clinker-plan`, and `clinker-exec`.
- **Counterexamples / limits:** Direct `Schema::new` is valid when a caller
  already owns complete aligned vectors or a test needs a compact literal.
  Builder repetition does not prohibit that constructor.
- **Verified:** 2026-07-29.

### Feature/domain modules with curated re-exports

- **Classification:** Preferred.
- **Where:** Record, format, plan, executor, and CXL crates group code by domain
  and expose a smaller root surface.
- **Use:** Put code in the module that owns its behavior, keep executor helpers
  private or `pub(crate)`, and re-export only the curated compatibility facade.
- **Evidence:** crate `lib.rs` files, `crates/clinker-plan/src/config/mod.rs`,
  `crates/clinker-plan/src/plan/mod.rs`, and
  [the crate map](20_CRATE_MAP.md).
- **Counterexamples / limits:** Test support may use an explicit feature-gated
  public seam. Rust `pub` or workspace reuse alone does not make a symbol a
  supported integration API; D-18 and D-19 classify that separately.
- **Verified:** 2026-07-29.

### Narrow shared-failure vocabulary at consumer edges

- **Classification:** Local implementation of the Phase 3 D-41 dependency
  decision.
- **Where:** `clinker-net` and `clinker-lineage` outcomes that expose the
  serialization-neutral taxonomy from `clinker-core-types`.
- **Use:** Keep exactly the normal edges `clinker-net -> clinker-core-types` and
  `clinker-lineage -> clinker-core-types`. Consumers use only
  `FailureClassification`, `FailureCategory`, and `RetryAdvice`, adapt them at
  their own boundary, and do not re-export the taxonomy.
- **Evidence:** The three crate manifests, `crates/clinker-net/src/otlp.rs`,
  and `crates/clinker-lineage/src/emit.rs`.
- **Counterexamples / limits:** This does not authorize a feature, serializer,
  package, identity type, transport type, or any fourth shared type. Semantic
  plan identity stays in `clinker-plan`; dataset identity stays in
  `clinker-lineage`.
- **Verified:** 2026-08-07.

### External derives and test macros

- **Classification:** Observed.
- **Where:** Serde and Clap derives, `thiserror::Error`, static assertions,
  `proptest!`, and `insta::assert_snapshot!` remove local boilerplate.
- **Use:** Reuse the established macro for its existing serialization, CLI,
  error, assertion, property-test, or snapshot role.
- **Evidence:** `crates/cxl/src/ast.rs`, `crates/cxl/src/builtins.rs`,
  `crates/clinker-channel/src/error.rs`, and executor/CXL tests.
- **Counterexamples / limits:** Repeated use of external derives is not evidence
  for a local macro DSL around config parsing or dispatch; prefer functions and
  types when they remain clear.
- **Verified:** 2026-07-29.

### Localized unsafe code

- **Classification:** Preferred.
- **Where:** Compact string layout, allocator instrumentation, OS process and
  filesystem probes, and limited benchmark/test helpers.
- **Use:** Keep unsafe code in the owning module, target-gate platform FFI, and
  document each unsafe block or implementation with its `SAFETY:` contract.
- **Evidence:** `crates/clinker-record/src/field_str.rs`,
  `crates/clinker-bench-support/src/alloc.rs`, and executor/plan system probes.
- **Counterexamples / limits:** Safe standard-library code remains preferred
  when no representation or FFI boundary requires unsafe. Benchmark unsafe
  code is not production-runtime precedent.
- **Verified:** 2026-07-29.

## Verification And Corpus Patterns

### Layered tests at the owning boundary

- **Classification:** Preferred.
- **Where:** Module tests, crate integration tests, fixtures, snapshots,
  property tests, scenario goldens, and Criterion benchmarks.
- **Use:** Test local algorithms in-module, public pipeline behavior through
  compiled-plan entry points, stable text/DAGs with snapshots, and equivalence
  or determinism with property tests.
- **Evidence:** `crates/clinker-plan/src/plan/tests/`,
  `crates/clinker-exec/src/executor/tests/`, `crates/clinker-exec/tests/`,
  `crates/cxl/src/eval/compiled/tests.rs`, and `examples/scenarios/`.
- **Counterexamples / limits:** Do not expose production internals only to make
  an integration test convenient. Snapshot acceptance requires explaining the
  intended behavior change; a golden is not proof of correctness by itself.
- **Verified:** 2026-07-29.

### Authoritative oracle for optional delivery

- **Classification:** Preferred for optional observability and lineage fault
  matrices.
- **Where:** CLI observability isolation and bounded lineage delivery tests.
- **Use:** Capture exact authoritative output/DLQ bytes, exit status, projected
  machine terminal, publication inventory, visible finals, and retained
  manifest/quarantine evidence from a no-fault run. For each optional-delivery
  fault, compare that oracle independently from the path-specific typed outcome
  and counters; canonicalize only unavoidable run-local IDs and timestamps.
- **Evidence:** `crates/clinker/tests/observability_isolation.rs` and
  `crates/clinker/tests/lineage_cli.rs`.
- **Counterexamples / limits:** A green exporter assertion, matching counter,
  or process exit alone is not authoritative artifact equivalence. The pattern
  does not claim set-wide atomic publication or lossless observability.
- **Verified:** 2026-08-07.

### Pipeline YAML as runnable documentation

- **Classification:** Local.
- **Where:** `examples/pipelines`, `examples/scenarios`, and
  `benches/pipelines` hold author-facing, golden-tested, and performance
  pipeline corpora respectively.
- **Use:** Place a pipeline in the corpus matching its purpose and keep nearby
  data, compositions, channels, manifests, and expected output together.
- **Evidence:** those directories plus CLI example and scenario integration
  tests.
- **Counterexamples / limits:** Cargo `examples/` targets are a different Rust
  mechanism. A pipeline under a future/reserve directory is not active coverage
  until discovery and a gate include it.
- **Verified:** 2026-07-29.

## Local Subsystem Patterns

### `ValidatedPath` capability

- **Classification:** Local to trusted filesystem boundaries.
- **Where:** `clinker-plan::security::ValidatedPath` and consumers such as
  source loading.
- **Use:** Keep construction behind canonical validation and accept the token
  where the path must already be trusted.
- **Evidence:** `crates/clinker-plan/src/security.rs` and
  `crates/clinker-plan/src/span.rs`.
- **Counterexamples / limits:** It is not a generic replacement for every
  internal `PathBuf`, and must not gain an unchecked `From<PathBuf>`.
- **Verified:** 2026-07-29.

### Stable collection identity plus standard facets

- **Classification:** Local to external OpenLineage identity.
- **Where:** `LineageIdentityContext`, dataset emission, and CLI lineage
  preflight.
- **Use:** Bind each source/output node to canonical datasource or exact catalog
  namespace/name identity. Keep that collection identity stable while placing
  authorized concrete input/output locations in standard subset facets and
  authorized aliases in the standard symlinks facet.
- **Evidence:** `crates/clinker-lineage/src/logical_identity.rs`,
  `crates/clinker-lineage/src/emit.rs`, and
  `crates/clinker-lineage/tests/logical_identity.rs`.
- **Counterexamples / limits:** Do not infer subsets or aliases from worker,
  attempt, or process paths. The current author config has no subset/symlink
  fields; exact `local_diagnostic_paths` is a separately labeled synchronous
  compatibility path and cannot enter external delivery.
- **Verified:** 2026-08-07.

### `FieldStr` compact storage

- **Classification:** Local to record string representation.
- **Where:** `Value::String` uses `FieldStr` because string width affects
  record memory and spill accounting.
- **Use:** Use its public string API and preserve layout/size invariants when
  modifying the representation.
- **Evidence:** `crates/clinker-record/src/field_str.rs` and the `smol_str`
  workspace manifest rationale.
- **Counterexamples / limits:** This does not justify a second compact-string
  type or make storage hints part of serialized equality/content semantics.
- **Verified:** 2026-07-29.

### `bench-alloc` allocation accounting

- **Classification:** Local, conditional, and currently untrusted as evidence.
- **Where:** `clinker-bench-support::alloc` and
  `clinker-exec::executor::stage_metrics` behind feature gates.
- **Use:** Do not use existing allocation numbers for conclusions until the
  D-21 forwarding, allocator-identity, plausibility, and distortion contract is
  repaired and verified.
- **Evidence:** `crates/clinker-bench-support/src/alloc.rs`,
  `crates/clinker-exec/src/executor/stage_metrics.rs`, and Cargo feature edges.
- **Counterexamples / limits:** Default runtime code and release graphs may not
  acquire this helper edge. Wall-clock timings collected under allocation
  instrumentation are not production throughput evidence.
- **Verified:** 2026-07-29.

## Applying A Pattern

Before copying a pattern:

1. Recheck its evidence and verification date against the current tree.
2. Confirm its classification and the stated context match the change.
3. Preserve the counterexample or limit; do not convert a preferred or local
   practice into a prohibition.
4. If correctness, security, bounded resources, layering, or compatibility
   requires the behavior, cite the reviewed design rule or contract instead of
   claiming repetition made it mandatory.
