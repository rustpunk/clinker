# AI Onboarding: Common Patterns

Purpose: Identify repeated implementation and documentation patterns that AI
agents should verify and reuse when making future changes.

## Source Evidence

Validate this page against:

- `Cargo.toml`
- `crates/*/Cargo.toml`
- `crates/*/src/lib.rs`
- `rg "^pub trait |^trait " crates --glob '*.rs'`
- `rg "enum .*Error|struct .*Error|type Result" crates --glob '*.rs'`
- `rg "Spanned<|serde\\(deny_unknown_fields\\)|Visitor" crates/clinker-plan/src`
- `rg "cfg\\(|feature =" Cargo.toml crates --glob '*.rs'`
- `rg "unsafe\\b|unsafe impl|unsafe fn" crates --glob '*.rs'`
- `find crates -path '*/tests/*.rs' -o -path '*/src/**/tests.rs' -o -path '*/benches/*.rs'`

Existing files under `docs/*` may be stale. Treat them as secondary context
only; any pattern claim found in `docs/*` must be validated against code,
manifests, tests, examples, or safe local commands before being copied here.

## Trait Seams For Runtime Boundaries

1. Pattern name: Send-only streaming traits and callback traits.
2. Where it appears: `FormatReader`, `FormatWriter`, `RecordSource`,
   `ProgressReporter`, `MemoryConsumer`, `ArbitrationPolicy`,
   `AccumulatorOp`, `SchedulingHint`, `Clock`, `RecordStorage`,
   `FieldResolver`, and `WindowContext`.
3. Rationale: traits mark real subsystem boundaries: byte-format
   IO, transport-agnostic source ingestion, memory arbitration, progress
   reporting, expression evaluation, and storage lookup. Many are `Send` or
   `Send + Sync` because executor ownership crosses `std::thread`, Rayon, or
   shared `Arc` boundaries.
4. How to copy it correctly: add a trait only when there is a real runtime,
   test, or planning boundary. Match the existing receiver style: streaming
   readers use `&mut self`; arbitrator-facing traits use `&self` plus shared
   atomics/handles; callback traits take borrowed update structs. Document
   default no-op methods when wrappers must delegate them.
5. Common mistakes an AI agent might make: adding `Sync` to single-threaded
   stream traits; using async traits for the synchronous executor path; adding
   a broad trait where an enum or function would do; forgetting wrapper
   delegation for optional/default methods.
6. Evidence: `crates/clinker-format/src/traits.rs`; `crates/clinker-exec/src/source/mod.rs`;
   `crates/clinker-exec/src/progress.rs`; `crates/clinker-exec/src/pipeline/memory.rs`;
   `crates/clinker-record/src/storage.rs`; `crates/clinker-record/src/resolver.rs`;
   `crates/clinker-exec/src/aggregation/mod.rs`; `crates/clinker-plan/src/plan/scheduling_hint.rs`;
   `crates/cxl/src/eval/context.rs`.

## SchemaBuilder Construction

1. Pattern name: Local pattern: central `SchemaBuilder` for `Arc<Schema>`
   creation.
2. Where it appears: `SchemaBuilder` is defined in `clinker-record` and used
   across format readers, projection, planning, aggregation, combine, benches,
   and executor tests.
3. Rationale: schema construction is common and must keep field
   metadata aligned with column order while materializing fresh schemas as
   `Arc<Schema>`.
4. How to copy it correctly: use `SchemaBuilder::new()` or
   `SchemaBuilder::with_capacity(n)`, chain `with_field`,
   `with_field_meta`, or `extend`, then call `.build()`. Prefer this over
   hand-building `Arc::new(Schema::new(...))` in new code.
5. Common mistakes an AI agent might make: constructing `Schema` manually at a
   call site; forgetting field metadata length must match columns; cloning a
   schema when a builder should produce the intended output schema once.
6. Evidence: `crates/clinker-record/src/schema.rs`; uses in
   `crates/clinker-format/src/csv/reader.rs`, `crates/clinker-format/src/json/reader.rs`,
   `crates/clinker-plan/src/config/pipeline.rs`, `crates/clinker-plan/src/plan/bind_schema.rs`,
   `crates/clinker-exec/src/projection.rs`, and
   `crates/clinker-exec/src/aggregation/hash.rs`.

## Error Enums And Direct Result Types

1. Pattern name: Subsystem-owned error enums, usually spelled directly in
   `Result`.
2. Where it appears: `PipelineError`, `FormatError`, `EvalError` plus
   `EvalErrorKind`, `ConfigError`, `SchemaError`, `ChannelError`,
   `StagingError`, `HashAggError`, `CombineError`, `ArenaError`, and many
   smaller parser/validation errors.
3. Rationale: each subsystem owns its diagnostic vocabulary and the
   top-level runtime error aggregates failures through variants or `From`
   conversions. Some errors manually implement `Display`/`Error`; `thiserror`
   is used where that is simpler and local.
4. How to copy it correctly: add variants to the owning subsystem's error type
   and propagate with explicit `Result<_, ThatError>` or
   `Result<_, PipelineError>`. Use `PipelineError::Internal` for violated
   plan-time/runtime invariants instead of `panic!`. Do not introduce a
   crate-wide `Result<T>` alias unless there is a strong local precedent.
5. Common mistakes an AI agent might make: hiding error context in
   `Box<dyn Error>` or `anyhow`; adding a new top-level `Result` alias; using
   `panic!` for recoverable runtime errors; flattening specific format or
   eval errors into plain strings too early.
6. Evidence: `crates/clinker-plan/src/error.rs`; `crates/clinker-format/src/error.rs`;
   `crates/cxl/src/eval/error.rs`; `crates/clinker-channel/src/error.rs`;
   `crates/clinker-exec/src/aggregation/error.rs`;
   `crates/clinker-exec/src/pipeline/combine.rs`;
   `rg "type .*Result" crates --glob '*.rs'` shows local aliases such as
   `FileFactory` and `TransformOutput`, not a project-wide result alias.

## Span-Aware YAML Deserialization

1. Pattern name: `serde-saphyr` chokepoint plus `Spanned<T>` and manual
   visitors for diagnostics.
2. Where it appears: `clinker-plan::yaml`, `PipelineConfig`,
   `PipelineNode`, `NodeHeader`, `MergeHeader`, `CombineHeader`,
   composition raw config, sort specs, schema-source specs, transform inputs,
   aggregate correlation keys, and resources.
3. Rationale: user-facing config errors need YAML source spans,
   strict unknown-field rejection, and bounded parser behavior. Manual
   visitors preserve spans and variant-specific `deny_unknown_fields` where a
   `serde_json::Value` intermediate would erase span data.
4. How to copy it correctly: parse YAML through `clinker_plan::yaml::from_str`.
   Keep `#[serde(deny_unknown_fields)]` on user-facing structs. Use
   `Spanned<T>` at fields that need diagnostics. If a shape needs custom
   dispatch, follow the existing `Visitor` pattern and stay on the native
   serde-saphyr path.
5. Common mistakes an AI agent might make: calling `serde_saphyr::from_str`
   directly in production code; routing through `serde_json::Value`; adding
   `#[serde(default)]` to hide mandatory fields; losing spans by moving a field
   into a tagged/flattened context without tests.
6. Evidence: `crates/clinker-plan/src/yaml.rs`;
   `crates/clinker-plan/src/config/pipeline.rs`;
   `crates/clinker-plan/src/config/pipeline_node.rs`;
   `crates/clinker-plan/src/config/node_header.rs`;
   `crates/clinker-plan/src/config/composition/raw.rs`;
   `crates/clinker-plan/src/config/sort.rs`;
   `crates/clinker-plan/src/config/format.rs`.

## Newtype Wrappers And Proof Tokens

1. Pattern name: Small semantic wrappers for IDs, spans, document grains, and
   security proofs.
2. Where it appears: `FileId`, `Span`, `DocumentId`, `DocumentGrain`,
   `NodeId`, `TailVarId`, `DottedPath`, `CxlSource`, `ValidatedPath`,
   `SourceIdentity`, `ConsumerId`, and local wrapper structs like
   `SharedByteCounter`.
3. Rationale: wrappers prevent mixing unrelated integers or
   strings, encode validation state, preserve origin spans, and provide a
   narrow public API around internal representation.
4. How to copy it correctly: use a newtype when the type carries a domain
   guarantee that raw `u64`, `String`, or `PathBuf` cannot express. Keep inner
   fields private when construction must be controlled. Provide borrowed
   accessors such as `as_path`/`as_ref` and consuming accessors only where
   ownership transfer is intentional.
5. Common mistakes an AI agent might make: replacing wrappers with raw strings
   or integers; making proof-token fields public; accepting a raw `PathBuf`
   where a `ValidatedPath` is required; adding new IDs without deriving the
   small trait set callers need (`Copy`, `Clone`, `Eq`, `Hash`, `Debug`).
6. Evidence: `crates/clinker-core-types/src/span.rs`;
   `crates/clinker-record/src/document_context.rs`;
   `crates/cxl/src/ast.rs`; `crates/cxl/src/typecheck/row.rs`;
   `crates/clinker-channel/src/binding.rs`;
   `crates/clinker-plan/src/yaml.rs`;
   `crates/clinker-plan/src/security.rs`;
   `crates/clinker-format/src/source.rs`;
   `crates/clinker-exec/src/pipeline/memory.rs`.

## Resource Handles And Registries

1. Pattern name: Run-scoped handles and registries instead of global mutable
   state.
2. Where it appears: `ConsumerHandle`, `MemoryArbitrator`, `WriterRegistry`,
   `WindowRuntimeRegistry`, `SourceReaders`, `ScopedVarsRegistry`,
   `BuiltinRegistry`, `DocArenaIndex`, `SourceDb`, `ReopenableSource`,
   `SourceStager`, and staging lock/manifest helpers.
3. Rationale: the executor coordinates many shared resources
   without hidden globals: memory accounting, writer ownership, window arena
   lookup, source inputs, scoped variables, builtins, document indexes, loaded
   source text, and stable multi-pass input handles.
4. How to copy it correctly: pass registries explicitly through the boundary
   that owns them. Use `Arc` for shared immutable/runtime state and use small
   handles with atomics when hot loops update shared counters. Keep run-scoped
   guards alive for the duration they protect.
5. Common mistakes an AI agent might make: adding a global registry; sharing
   mutable state through `Mutex` when the existing pattern uses atomics; losing
   liveness guards while keeping only a path; creating a second registry that
   duplicates one already threaded through executor context.
6. Evidence: `crates/clinker-exec/src/pipeline/memory.rs`;
   `crates/clinker-exec/src/executor/registry.rs`;
   `crates/clinker-exec/src/executor/window_runtime.rs`;
   `crates/clinker-exec/src/executor/mod.rs`;
   `crates/cxl/src/resolve/scoped_vars.rs`;
   `crates/cxl/src/builtins.rs`;
   `crates/clinker-format/src/doc_index.rs`;
   `crates/clinker-plan/src/span.rs`;
   `crates/clinker-format/src/source.rs`;
   `crates/clinker-channel/src/staging_copy.rs`.

## Inline Stream Events

1. Pattern name: Records and control messages share the same ordered stream.
2. Where it appears: `StreamEvent`, `Punctuation`, `PunctuationKind`,
   `StructuralReject`, `EventBatch`, `EventBatcher`, `SourceStream`, and
   executor dispatch arms that preserve or reconcile punctuations.
3. Rationale: document-boundary signals must preserve strict order
   relative to records. The code explicitly rejects out-of-band signaling for
   these events.
4. How to copy it correctly: carry new stream-control information as a typed
   event only when ordering with records matters. Ensure operators that do not
   consume a punctuation forward it unchanged, and multi-input operators
   reconcile document boundaries once.
5. Common mistakes an AI agent might make: adding side channels for record
   boundaries; dropping punctuation in transform/route paths; batching records
   and punctuations separately; treating punctuation as a record for memory
   accounting.
6. Evidence: `crates/clinker-exec/src/executor/stream_event.rs`;
   `crates/clinker-exec/src/executor/batch_handoff.rs`;
   `crates/clinker-exec/src/executor/source_stream.rs`;
   `crates/clinker-exec/src/executor/dispatch.rs`;
   `crates/clinker-exec/src/executor/streaming.rs`.

## Transport And Loading Abstractions

1. Pattern name: File and network sources converge on `RecordSource`.
2. Where it appears: `FormatReader`, blanket `RecordSource` impl for
   `Box<dyn FormatReader>`, `SourceInput::Files` / `SourceInput::Records`,
   `SourceReaders`, `ReopenableSource`, `RestRecordSource`, and
   `build_rest_source`.
3. Rationale: source ingestion should be transport-agnostic after
   records are yielded. File inputs decode bytes through format readers;
   network sources implement the row-yielding trait directly and still flow
   into the same ingest channel.
4. How to copy it correctly: add a new transport by implementing
   `RecordSource` and registering it as `SourceInput::Records`, or add a new
   file format by implementing `FormatReader` / `FormatWriter`. Use
   `ReopenableSource` when a reader needs multi-pass bytes. Keep sources
   finite.
5. Common mistakes an AI agent might make: special-casing a transport in
   dispatcher logic; forcing network data through fake filesystem paths;
   buffering whole files when `ReopenableSource` supports lazy or re-openable
   access; adding unbounded polling sources.
6. Evidence: `crates/clinker-format/src/traits.rs`;
   `crates/clinker-exec/src/source/mod.rs`;
   `crates/clinker-exec/src/executor/mod.rs`;
   `crates/clinker-format/src/source.rs`;
   `crates/clinker-net/src/lib.rs`;
   `crates/clinker-net/src/rest.rs`.

## Composition Resource Declarations

1. Pattern name: Local pattern: typed composition resource declarations.
2. Where it appears: `ResourceDecl`, `ResourceKind`, `Resource`,
   `resources_schema`, and composition call-site `resources`.
3. Rationale: compositions declare resource slots separately from
   config params, while resolved runtime resources carry typed payloads and
   YAML spans for diagnostics. Evidence currently shows only file resources,
   with validation still partly stubbed.
4. How to copy it correctly: keep declaration-level tags (`ResourceKind`) and
   runtime payloads (`Resource`) separate. Preserve spans on paths and add new
   resource variants only when there is a real implementation and tests.
5. Common mistakes an AI agent might make: treating arbitrary JSON resources as
   already fully validated; merging resources into config params; adding a
   stringly typed resource kind; claiming non-file resource support before code
   exists.
6. Evidence: `crates/clinker-plan/src/config/composition/mod.rs`;
   `crates/clinker-plan/src/config/composition/resource.rs`;
   `crates/clinker-plan/src/config/composition/raw.rs`;
   `crates/clinker-plan/src/config/composition/tests.rs`;
   `crates/clinker-plan/src/plan/bind_schema.rs`;
   `crates/clinker-plan/src/config/pipeline_node.rs`.

## DAG Dispatcher, Not ECS

1. Pattern name: Typed DAG execution and dispatch arms, not an ECS/system
   framework.
2. Where it appears: `ExecutionPlanDag`, `PlanNode`, `PlanEdge`,
   `PipelineExecutor`, `dispatch`, and per-node dispatch modules such as
   `source_dispatch`, `transform_dispatch`, `aggregate_dispatch`,
   `combine_dispatch`, `route_dispatch`, `merge_dispatch`, `output_dispatch`,
   `reshape_dispatch`, and `cull_dispatch`.
3. Rationale: planning builds a typed execution DAG, and runtime
   walks it synchronously with node-kind-specific operators. There is no
   evidence of Bevy-style ECS resources/systems/components.
4. How to copy it correctly: new runtime behavior should normally attach to a
   plan node, compile artifact, or dispatch arm. Keep compile-time plan
   construction in `clinker-plan` and runtime execution in `clinker-exec`.
5. Common mistakes an AI agent might make: introducing an ECS abstraction;
   adding ad hoc runtime node maps outside `ExecutionPlanDag`; making executor
   APIs accept raw `PipelineConfig` instead of compiled plans.
6. Evidence: `crates/clinker-plan/src/plan/execution/mod.rs`;
   `crates/clinker-plan/src/plan/execution/dag.rs`;
   `crates/clinker-exec/src/executor/mod.rs`;
   `crates/clinker-exec/src/executor/dispatch.rs`;
   `crates/clinker-exec/src/executor/*_dispatch.rs`.

## Module Organization And Re-Exports

1. Pattern name: Feature/domain modules with curated crate-root exports.
2. Where it appears: `clinker-record`, `clinker-format`, `clinker-plan`,
   `clinker-exec`, `cxl`, and smaller crates.
3. Rationale: public crates expose core vocabulary at the crate
   root while keeping implementation modules organized by domain or operator.
   The executor uses many `pub(crate)` dispatch modules and only exports the
   public entry points and support types needed by callers.
4. How to copy it correctly: place new code in the domain module that owns the
   behavior. Re-export only stable public API types. Prefer `pub(crate)` or
   private modules for executor internals. Follow local suffixes such as
   `*Config`, `*Body`, `*Error`, `*_dispatch`, `Registry`, and `Handle`.
5. Common mistakes an AI agent might make: putting runtime code in
   `clinker-plan`; making helper modules `pub` just for tests; adding public
   root exports for implementation details; creating type-based folders where
   the crate is organized by feature.
6. Evidence: `crates/clinker-record/src/lib.rs`;
   `crates/clinker-format/src/lib.rs`;
   `crates/clinker-plan/src/config/mod.rs`;
   `crates/clinker-plan/src/plan/mod.rs`;
   `crates/clinker-exec/src/executor/mod.rs`;
   `docs/ai/20_CRATE_MAP.md`.

## Feature Flags And Target Gates

1. Pattern name: Feature flags are narrow and mostly for benchmarks/tests;
   platform gates isolate OS-specific code.
2. Where it appears: `bench-alloc`, `bench-xlarge`, `test-utils`, target
   dependencies for Unix/Windows/native signal handling, and platform-specific
   RSS/CPU/IO/filesystem probes.
3. Rationale: default runtime builds should not depend on
   benchmark instrumentation or test-only surfaces. OS-specific APIs are kept
   behind `cfg` arms with fallback behavior where possible.
4. How to copy it correctly: keep optional behavior behind explicit features
   only when it is not part of normal runtime. Use `#[cfg(test)]` for local
   test seams and `#[cfg(any(test, feature = "test-utils"))]` only for public
   test helpers needed by downstream integration crates. Put platform
   dependencies in target-specific Cargo sections.
5. Common mistakes an AI agent might make: enabling benchmark helpers by
   default; adding a feature that changes normal semantics; exposing test
   helpers without a gate; using `cfg` arms without a conservative unsupported
   target fallback.
6. Evidence: `crates/clinker-exec/Cargo.toml`;
   `crates/clinker-bench-support/Cargo.toml`;
   `crates/clinker-benchmarks/Cargo.toml`;
   `crates/clinker-exec/src/executor/stage_metrics.rs`;
   `crates/clinker-exec/src/progress.rs`;
   `crates/clinker-exec/src/pipeline/memory.rs`;
   `crates/clinker-exec/src/pipeline/sysstats.rs`;
   `crates/clinker-plan/src/config/fs_type.rs`.

## Macro Usage

1. Pattern name: Macros are mostly external derives/assertions/tests, not
   project-local architecture.
2. Where it appears: `serde` derives/attributes, `thiserror::Error`,
   `static_assertions::assert_impl_all!`, `proptest!`, `insta::assert_snapshot!`,
   and Clap derives in CLI crates.
3. Rationale: macros reduce boilerplate for serialization, CLI
   parsing, error displays, compile-time trait assertions, snapshots, and
   property tests. There is little evidence of local `macro_rules!` DSLs.
4. How to copy it correctly: prefer the existing derive/test macros for their
   established purposes. Add local macros only if there is repeated code that
   cannot be cleanly expressed with functions or types.
5. Common mistakes an AI agent might make: creating a macro DSL for dispatch or
   config parsing; replacing clear functions with macros; forgetting snapshot
   and proptest macros belong in tests.
6. Evidence: `crates/cxl/src/ast.rs`; `crates/cxl/src/builtins.rs`;
   `crates/cxl/src/eval/compiled/tests.rs`;
   `crates/clinker-exec/tests/path_a_parity.rs`;
   `crates/clinker-exec/tests/state_node_diagnostics.rs`;
   `crates/clinker-channel/src/error.rs`;
   `crates/clinker/src/main.rs`; `crates/cxl-cli/src/main.rs`.

## Unsafe Boundaries

1. Pattern name: Unsafe is localized, commented, and platform/performance
   motivated.
2. Where it appears: `FieldStr` layout/union and auto-trait impls, benchmark
   allocation accounting, process RSS/CPU/IO probes, Windows filesystem probes,
   UTF-8 generation in bench helpers, and test/env-var code required by Rust
   2024.
3. Rationale: unsafe is used where Rust cannot express a compact
   in-memory representation, global allocator instrumentation, or OS FFI.
4. How to copy it correctly: keep unsafe in the narrow module that owns the
   invariant. Add a `SAFETY:` comment explaining the contract at every unsafe
   impl/block. Prefer safe standard-library APIs when no layout or FFI boundary
   requires unsafe.
5. Common mistakes an AI agent might make: spreading raw pointer logic outside
   `FieldStr`; adding FFI calls without target gates; omitting `SAFETY:`
   comments; treating benchmark unsafe code as production runtime precedent.
6. Evidence: `crates/clinker-record/src/field_str.rs`;
   `crates/clinker-bench-support/src/alloc.rs`;
   `crates/clinker-exec/src/pipeline/memory.rs`;
   `crates/clinker-exec/src/pipeline/sysstats.rs`;
   `crates/clinker-plan/src/config/fs_type.rs`;
   `crates/clinker-bench-support/src/lib.rs`;
   `crates/clinker-bench-support/src/combine.rs`.

## Test Organization

1. Pattern name: Layered tests: in-module unit tests, crate integration
   tests, fixtures, snapshots, property tests, and benches.
2. Where it appears: `#[cfg(test)] mod tests` in most modules; extensive
   `crates/clinker-exec/tests`; shared helpers in
   `crates/clinker-exec/tests/common`; YAML fixtures under
   `crates/clinker-exec/tests/fixtures`; snapshots under
   `crates/clinker-exec/tests/snapshots`; `proptest` in CXL and IEJoin tests;
   Criterion benches under crate `benches/`.
3. Rationale: narrow unit tests cover local parsing/formatting/data
   structures; integration tests pin pipeline behavior and public executor
   entry points; snapshots preserve user-visible explain/diagnostic output;
   property tests cover broad algorithm/evaluator invariants.
4. How to copy it correctly: put pure local behavior in module tests; put
   public pipeline behavior in integration tests using `PipelineExecutor`'s
   compiled-plan entry point; add fixtures under the owning test fixture tree;
   use snapshots for stable text or DAG shapes; use property tests for
   algorithmic equivalence or determinism.
5. Common mistakes an AI agent might make: exposing crate-private helpers only
   for tests; testing executor behavior through non-public seams from
   integration tests; hardcoding large YAML repeatedly instead of using shared
   fixture builders; updating snapshots without explaining the behavior change.
6. Evidence: `crates/clinker-exec/tests/common/mod.rs`;
   `crates/clinker-exec/tests/common/branch_fixtures.rs`;
   `crates/clinker-exec/tests/path_a_parity.rs`;
   `crates/clinker-exec/tests/combine_iejoin_prop.rs`;
   `crates/cxl/src/eval/compiled/tests.rs`;
   `crates/clinker-plan/src/plan/tests/`;
   `crates/clinker-exec/src/executor/tests/`;
   `crates/*/benches/*.rs`.

## Examples And Benchmark Configs

1. Pattern name: YAML examples and benchmark pipelines live outside Cargo
   example targets.
2. Where it appears: `examples/pipelines`, `examples/pipelines/data`,
   `examples/pipelines/channels`, `examples/pipelines/compositions`,
   `examples/pipelines/tests/baseline`, and `benches/pipelines`.
3. Rationale: examples double as user-facing pipeline material and
   integration/benchmark fixtures. Bench configs are grouped by feature area
   rather than by Rust crate.
4. How to copy it correctly: add runnable pipeline examples under
   `examples/pipelines` with nearby data/compositions/channels when needed.
   Add benchmark YAML under the appropriate `benches/pipelines` category.
   Do not create Cargo example targets unless the workspace intentionally adds
   one.
5. Common mistakes an AI agent might make: placing pipeline examples in
   `crates/*/examples`; adding benchmark YAML to runtime test fixtures; moving
   example data away from the example that uses it; treating `future/`
   benchmark configs as active benchmark coverage without checking discovery.
6. Evidence: `rg --files examples/pipelines benches/pipelines`;
   `crates/clinker-bench-support/src/lib.rs`;
   `crates/clinker-benchmarks/src/lib.rs`;
   `crates/clinker-exec/tests/path_a_parity.rs`;
   `docs/ai/20_CRATE_MAP.md`.

## Local Patterns To Treat Carefully

These patterns are useful evidence but should not be promoted to
project-wide conventions without more support.

1. Pattern name: Local pattern: `ValidatedPath` proof token.
2. Where it appears: `clinker-plan::security::ValidatedPath` and
   `SourceDb::load`.
3. Rationale: callers that need trusted paths are forced through
   validation before file loading.
4. How to copy it correctly: consume `ValidatedPath` by value at APIs that
   require the guarantee. Keep construction private.
5. Common mistakes an AI agent might make: adding `From<PathBuf>`; accepting
   raw paths at protected boundaries; treating it as a general path wrapper.
6. Evidence: `crates/clinker-plan/src/security.rs`; `crates/clinker-plan/src/span.rs`.

1. Pattern name: Local pattern: `FieldStr` compact string storage.
2. Where it appears: `clinker-record::field_str`.
3. Rationale: `Value::String` is common enough that its memory
   width affects spill/RSS accounting.
4. How to copy it correctly: use `FieldStr` through the public string API; do
   not duplicate its union layout elsewhere.
5. Common mistakes an AI agent might make: adding a second compact-string type;
   assuming the heap arm affects equality; changing layout without checking
   size assertions and value-cost assumptions.
6. Evidence: `crates/clinker-record/src/field_str.rs`; `Cargo.toml` comments
   around `smol_str`.

1. Pattern name: Local pattern: `bench-alloc` global allocator accounting.
2. Where it appears: `clinker-bench-support::alloc` and
   `clinker-exec::executor::stage_metrics`.
3. Rationale: benchmarks need scoped allocation deltas without
   enabling allocation accounting in normal runtime builds.
4. How to copy it correctly: keep it behind the `bench-alloc` feature and use
   `Region` deltas for benchmark measurements.
5. Common mistakes an AI agent might make: enabling it by default; using its
   timings as production throughput; adding allocator instrumentation to
   runtime code paths without the feature gate.
6. Evidence: `crates/clinker-bench-support/src/alloc.rs`;
   `crates/clinker-bench-support/Cargo.toml`;
   `crates/clinker-exec/src/executor/stage_metrics.rs`;
   `crates/clinker-exec/Cargo.toml`.

## Review Notes

- Confirm whether the "DAG dispatcher, not ECS" wording matches project
  owner terminology.
- Confirm whether any future documentation should call `SchemaBuilder`
  project-wide, or describe it more narrowly as a shared utility with many
  call sites.
- Confirm whether any direct `serde_saphyr::from_str` usage in tests should be
  documented as an allowed test-local exception or cleaned up later.
