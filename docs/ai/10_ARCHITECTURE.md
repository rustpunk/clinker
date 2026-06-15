# AI Onboarding: Architecture

Purpose: Give a senior Rust engineer or AI coding agent a practical, source-backed architecture overview before changing Clinker.

## Source Evidence

Primary evidence used for this pass:

- Workspace and crate boundaries: `Cargo.toml`, `crates/*/Cargo.toml`, `docs/ai/20_CRATE_MAP.md`.
- Planning and config: `crates/clinker-plan/src/lib.rs`, `crates/clinker-plan/src/config/pipeline.rs`, `crates/clinker-plan/src/config/pipeline_node.rs`, `crates/clinker-plan/src/yaml.rs`, `crates/clinker-plan/src/plan/compiled.rs`, `crates/clinker-plan/src/plan/execution/mod.rs`.
- Runtime and IO: `crates/clinker-exec/src/executor/mod.rs`, `crates/clinker-exec/src/executor/params.rs`, `crates/clinker-exec/src/source/mod.rs`, `crates/clinker-exec/src/pipeline/memory.rs`, `crates/clinker-exec/src/pipeline/shutdown.rs`, `crates/clinker-format/src/traits.rs`, `crates/clinker-net/src/lib.rs`.
- Data model and language: `crates/clinker-record/src/lib.rs`, `crates/clinker-record/src/record/mod.rs`, `crates/clinker-record/src/storage.rs`, `crates/clinker-record/src/value.rs`, `crates/cxl/src/lib.rs`.
- Edge surfaces: `crates/clinker/src/main.rs`, `crates/clinker-channel/src/lib.rs`, `crates/clinker-schema/src/lib.rs`, `examples/pipelines/customer_etl.yaml`.
- Tests and CI: `crates/clinker-exec/tests/*`, `crates/clinker-plan/src/plan/tests/*`, `crates/clinker-format/tests/*`, `crates/clinker-net/tests/*`, `crates/clinker-channel/tests/*`, `.github/workflows/ci.yml`.

## What Clinker Appears To Be

Verified facts:

- Clinker is a Rust workspace with 13 active members. The main executable is `crates/clinker`; the standalone CXL tool is `crates/cxl-cli`; lower layers are split into records, CXL, format, planning, execution, channel, network, schema, and benchmark support crates.
- Pipelines are YAML documents using a unified top-level `nodes:` list. `PipelineConfig` has `nodes: Vec<Spanned<PipelineNode>>`, and comments say legacy top-level `inputs:` / `outputs:` / `transformations:` are rejected by serde.
- The executable workload is finite batch-style pipeline execution. `RecordSource::next_record` is explicitly finite by contract, `clinker-net` describes REST as a finite-pull source with `max_pages` / `max_records`, and `PipelineExecutor` says no async runtime is required.
- CXL is the per-record expression language layer. The `cxl` crate exposes parser, resolver, typechecker, analyzer, aggregate extraction, and evaluator modules; plan and exec compile and evaluate CXL-bearing nodes.
- Clinker is not currently an editor application in this repository. Tooling-facing surfaces appear to be data/API outputs such as `ExplainFormat::Json`, `CompiledPlan::provenance`, `CompiledPlan::typed_output_row`, and `clinker-schema`.

Hypothesis:

- The project is best described as a bounded-memory, single-process DAG executor for finite ETL jobs, with YAML configuration, CXL expression programs, streaming readers/writers, and an explicit plan/runtime boundary. This matches crate docs and runtime contracts, but maintainers should confirm whether "ETL engine" or "batch DAG executor" is the preferred public wording.

## Major Subsystems

Verified facts:

- **Record model:** `clinker-record` owns `Value`, `Record`, `Schema`, `RecordStorage`, `RecordView`, provenance, document context, grouping keys, counters, and accumulator state. `Record` stores positional `Vec<Value>` data behind an `Arc<Schema>`.
- **Expression engine:** `cxl` owns AST, lexing/parsing, module evaluation, resolution, typechecking, static analysis, aggregate planning, and runtime evaluation. Public symbols used downstream include `Parser`, `resolve_program`, `type_check`, `ProgramEvaluator`, and aggregate extraction/planning APIs.
- **Format layer:** `clinker-format` owns streaming `FormatReader` / `FormatWriter` traits plus CSV, JSON/NDJSON, XML, fixed-width, HL7, X12, EDIFACT, SWIFT, multi-record, envelope, document index, source reopening, writer counting, and splitting modules.
- **Planning layer:** `clinker-plan` parses YAML, validates topology and paths, resolves schemas, scans composition signatures, typechecks CXL, lowers unified nodes into `ExecutionPlanDag`, and returns `CompiledPlan`.
- **Runtime layer:** `clinker-exec` owns `PipelineExecutor`, executor dispatch arms, source ingestion, node buffers, streaming handoff, DLQ, metrics, memory arbitration, spill handling, joins/combines, aggregation, merge, reshape, cull, envelope, output, and shutdown.
- **Network source layer:** `clinker-net` currently exposes `build_rest_source`, adapting REST pages into `Box<dyn clinker_exec::source::RecordSource>`.
- **Channel/deployment layer:** `clinker-channel` owns channel binding, overlays, dotted paths, and source staging copies. Channels override declared config/resources only.
- **Schema workspace layer:** `clinker-schema` parses `.schema.yaml`, discovers schema files, builds `SchemaIndex`, and validates pipeline schema references.
- **CLI layer:** `crates/clinker/src/main.rs` exposes `run`, `metrics`, and `explain` commands through Clap and calls into plan/exec/channel/net/format code.
- **Benchmark/test layer:** `clinker-bench-support` and `clinker-benchmarks` own generators, cached data, benchmark runners, and optional allocation instrumentation.

## Data Flow

Verified end-to-end shape:

1. A user-facing pipeline YAML looks like `examples/pipelines/customer_etl.yaml`: `pipeline:` metadata, a `nodes:` list with `type: source`, `type: transform`, `type: output`, and optional `error_handling:`.
2. Config loading goes through `clinker-plan`. `load_config` / `load_config_with_vars` parse YAML into `PipelineConfig`, and `clinker_plan::yaml::from_str` is the parser chokepoint over `serde-saphyr` with a 32 MiB input cap and depth/node/alias budgets.
3. `PipelineNode` deserialization is hand-written around the `type:` discriminator. It intentionally preserves per-node spans and per-variant `deny_unknown_fields` behavior.
4. `PipelineConfig::compile_topology_only` checks duplicate names, self-loops, general cycles, undeclared input references, path validation, dotted-name restrictions, and log directive sanity.
5. `PipelineConfig::compile` / `compile_with_diagnostics` scan composition signatures when needed, bind schemas, typecheck CXL, lower nodes to `PlanNode`, build an `ExecutionPlanDag`, and wrap it in `CompiledPlan`.
6. Source inputs enter runtime as `SourceInput::Files(Vec<FileSlot>)` or `SourceInput::Records(Box<dyn RecordSource>)`. File transports reach `RecordSource` through a blanket impl for `Box<dyn FormatReader>`; REST uses `build_rest_source`.
7. Source ingest runs per declared source, resolves schemas with `schema(&mut self)`, calls finite `next_record`, attaches document/provenance context, and pushes events to bounded crossbeam channels.
8. Runtime dispatch walks the plan DAG, executing `PlanNode` variants through focused dispatch modules such as `transform_dispatch`, `aggregate_dispatch`, `combine_dispatch`, `route_dispatch`, `merge_dispatch`, `reshape_dispatch`, `cull_dispatch`, `envelope_dispatch`, and `output_dispatch`.
9. Records are schema-indexed. Transform and aggregate CXL programs use typechecked artifacts from planning; runtime writes only fields already present in the widened output schema.
10. Outputs consume `FormatWriter` implementations, optionally with envelope begin/end document hooks, byte counting, splitting, and metrics.
11. `ExecutionReport` returns counters, DLQ entries, execution summary, peak RSS, CPU/IO totals, stage metrics, watermarks, rollback cursors, per-source counts, spill totals, streaming charge peaks, and interrupted status.

Important nuance:

- Public executor APIs take `&CompiledPlan`, but `PipelineExecutor::run_plan_with_readers_writers` currently delegates through `plan.config()` into the shared run path, where the runtime performs the canonical compile path again in context before dispatch. Treat `CompiledPlan` as the public proof boundary, but inspect the current executor body before assuming the stored DAG is always consumed directly.

## Architectural Boundaries

Verified boundaries:

- `clinker-record` and `clinker-core-types` are lower-level vocabulary crates. They should not depend on planning or execution.
- `cxl` depends on records but does not depend on plan or exec.
- `clinker-plan` sits below execution. Its crate docs say it turns YAML and CXL into a typed, validated `ExecutionPlanDag` "without depending on any runtime operator."
- `clinker-exec` consumes plan/config artifacts and owns runtime operator behavior. Executor public docs include a `compile_fail` doctest showing `&PipelineConfig` is not accepted by `run_plan_with_readers_writers`.
- `clinker-format` is the streaming IO layer. It depends on `cxl` today, which `20_CRATE_MAP.md` flags as a current but potentially worth-reviewing layering edge.
- `clinker-net` is not a low-level HTTP-only crate; it depends on `clinker-exec` because REST readers implement executor `RecordSource`.
- `clinker-channel`, `clinker-net`, `clinker-schema`, `clinker`, and `cxl-cli` are edge/integration crates.
- Benchmark helpers must remain outside default runtime paths. The `clinker-exec -> clinker-bench-support` edge is optional and feature-gated for `bench-alloc`.

Practical guidance:

- Do not move raw YAML parsing into executor code.
- Do not let executor APIs casually accept unvalidated config when a `CompiledPlan` or typed plan artifact is the established boundary.
- Do not special-case transports inside operator dispatch. `SourceInput::Files` and `SourceInput::Records` are intentionally normalized before dispatch.
- Keep path security APIs proof-oriented. `ValidatedPath` has private internals and should remain the handoff type for trusted filesystem paths.
- Keep channel and composition overrides at declared boundaries. `clinker-channel` docs explicitly forbid mid-graph patching and sealed composition internals access.

## Public API Surfaces

Verified API surfaces future agents should recognize:

- `clinker_plan::config::PipelineConfig::{compile, compile_with_diagnostics, compile_topology_only, source_configs, output_configs}`.
- `clinker_plan::config::{load_config, load_config_with_vars}` and `clinker_plan::yaml::{from_str, to_string, Spanned, CxlSource}`.
- `clinker_plan::plan::CompiledPlan::{dag, config, artifacts, body_of, typed_output_row, provenance, provenance_mut, channel_identity, pipeline_hash}`.
- `clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode, PlanEdge, NodeExecutionReqs}`.
- `clinker_exec::executor::{PipelineExecutor, PipelineRunParams, ExecutionReport, WriterRegistry, SourceReaders, SourceInput, RecordSource, single_file_reader}`.
- `clinker_exec::source::{RecordSource, SourceInput}` for non-file source integration.
- `clinker_format::{FormatReader, FormatWriter, FormatError, EnvelopeConfig, EnvelopeEvent, EnvelopeFramer, ReopenableSource}`.
- `clinker_record::{Record, RecordPayload, Value, Schema, SchemaBuilder, RecordStorage, RecordView, DocumentContext, PipelineCounters}`.
- `clinker_channel::{ChannelBinding, ChannelTarget, DottedPath, scan_workspace_channels, validate_channel_bindings, apply_channel_overlay, SourceStager}`.
- `clinker_net::build_rest_source`.
- `clinker_schema::{parse_schema, parse_schema_file, build_workspace_schema_index, validate_pipeline}`.
- CLI commands in `crates/clinker/src/main.rs`: `run`, `metrics collect`, and `explain`.

## Ownership And Lifetime Patterns

Verified patterns:

- Records own their values but share schema and document context: `Record { schema: Arc<Schema>, values: Vec<Value>, doc_ctx: Arc<DocumentContext> }`.
- `RecordStorage` returns borrowed `&Value` and requires `Send + Sync`, allowing zero-copy field resolution in window/evaluator paths.
- `Value::String(FieldStr)` optimizes short strings inline and longer strings through shared or unique storage hints; serialization intentionally loses the storage hint and preserves content.
- `RecordPayload` is the spill wire form. It omits schema and full document context; spill files write schema/context side tables and records carry positional values plus a document id.
- Runtime resources are grouped by ownership. `DagExecInputs<'a>` borrows config, transforms, plan, artifacts, and params; `DagExecResources` owns source receivers, writers, routes, spill dir guard, watermarks, and `Arc<MemoryArbitrator>`.
- `FormatReader::schema(&mut self)` and `RecordSource::schema(&mut self)` take `&mut self` because some readers discover schema only after peeking or querying.
- Traits passed across worker threads are `Send` but generally not `Sync` (`FormatReader`, `FormatWriter`, `RecordSource`). Each source or writer is single-thread-owned once execution starts.
- `MemoryArbitrator` is shared as `Arc<MemoryArbitrator>` with interior mutability through atomics, mutexes for cold-path maps, and an `ArcSwap` copy-on-write consumer registry.

## Async And Concurrency

Verified facts:

- The core runtime is synchronous. `PipelineExecutor` docs say it uses `std::thread` workers, bounded `crossbeam_channel`s, and a shared Rayon pool; no async runtime is required.
- Source ingestion uses one OS thread per declared source. Non-file network readers are still driven synchronously by the source ingest thread.
- CPU-heavy kernels such as sort, grace-hash, IEJoin, and sort-merge run under one run-scoped Rayon `ThreadPool`, sized by `pipeline.concurrency.threads` when configured.
- Streaming output and some streaming producer/consumer paths use bounded crossbeam channels plus `std::thread::JoinHandle`s.
- Shutdown uses per-run `ShutdownToken` values backed by `Arc<AtomicBool>`. A process-wide `ctrlc` handler broadcasts to registered live tokens through a `Weak` registry.
- Memory arbitration is concurrent but centralized. Registered consumers expose usage/pause/spill hooks; operators poll `should_spill` / `should_abort` at chunk boundaries.

Hypothesis:

- `tokio` exists in workspace dependencies, but current core pipeline execution should be treated as non-async. Adding async transport or a Tokio-driven executor would be an architectural change, not a local refactor.

## Serialization, Configuration, And Resource Loading

Verified facts:

- Pipeline and composition configuration is YAML via `serde-saphyr`, routed through `clinker_plan::yaml`.
- User-facing config structs commonly use `#[serde(deny_unknown_fields)]`; `PipelineConfig`, `PipelineMeta`, `MemoryConfig`, `MetricsConfig`, and many node bodies participate in this strict config style.
- `PipelineNode` uses a custom serde visitor instead of a `serde_json::Value` intermediate to keep spans and variant-specific validation.
- `CxlSource` carries source string plus YAML span metadata for diagnostics.
- `PipelineConfig.source_hash` and `CompiledPlan.pipeline_hash` store BLAKE3 of post-env-var-interpolated YAML when loaded from file.
- `Value` implements custom serde for postcard-compatible spill encoding and textual tagged output; production JSON output uses format/executor conversion helpers rather than relying on the raw enum shape.
- File readers and writers stream through `FormatReader` / `FormatWriter`. Envelope-aware readers and writers add document hooks without buffering whole documents.
- Workspace storage configuration uses TOML (`clinker.toml` / storage config in `clinker-plan`), not the YAML parser chokepoint.
- Channels load `.channel.yaml`, content-hash the raw file with BLAKE3, and apply bounded provenance layers.
- Source staging uses `clinker-channel::SourceStager` and path matching/resource reuse logic; filesystem path validation lives in `clinker-plan::security`.

## Extension, Plugin, And Scripting Boundaries

Verified facts:

- CXL is the scripting boundary for row expressions, filters, emits, aggregates, route predicates, and related computed behavior. It is compiled/typechecked before runtime evaluation.
- Pipeline extension is by node variants in `PipelineNode` / `PlanNode` plus corresponding plan lowering, schema binding, executor dispatch, docs, examples, and tests.
- Format extension is by implementing `FormatReader` and/or `FormatWriter` and wiring it through format config and executor reader/writer construction.
- Transport extension is by implementing `RecordSource` and registering `SourceInput::Records`; REST is the current concrete non-file example.
- Composition extension is declarative through `.comp.yaml`, signatures, ports, params, resources, and bound bodies. Composition internals are sealed from channel overlays except through declared surfaces.
- Channel extension is declarative through `.channel.yaml`, `DottedPath`, config/resource overrides, and source staging.

No verified general-purpose plugin system was found in this repository. Treat "plugin" as unsupported unless maintainers identify a specific extension mechanism.

## Error Handling Strategy

Verified facts:

- Compile-time structured diagnostics live in `clinker-core-types` (`Diagnostic`, `Severity`, spans, payloads). `PipelineConfig::compile` returns `Result<CompiledPlan, Vec<Diagnostic>>`.
- Runtime and subsystem failures aggregate through `clinker_plan::error::PipelineError`, which has variants for config, schema, format, eval, compilation, I/O, spill, thread pool, multiple writer errors, internal invariant violations, accumulator failures, schema mismatch, composition, memory, combine, envelope, and other runtime cases.
- `PipelineError::Internal` is explicitly for plan-time invariant violations found at runtime and should abort regardless of `ErrorStrategy::Continue`.
- `FormatError` is `#[non_exhaustive]`, returned per record or setup operation, and wraps format-specific errors plus structural count, invalid record, schema inference, undeclared field, and unserializable map cases.
- Some runtime data failures can route to DLQ depending on error strategy; other classes always abort. Examples called out in source comments include memory budget errors, unsatisfiable memory budgets, sort-order violations, internal invariant failures, and specific envelope conflicts.
- CLI exit codes distinguish success, config/schema/CXL errors, partial DLQ completion, fatal data/eval errors, I/O/format errors, and interrupted runs.

Practical guidance:

- Prefer subsystem error enums and `PipelineError` conversions over panics in runtime paths.
- Use `PipelineError::Internal` for "should never happen after planning" cases.
- If an error should be recoverable into DLQ under `continue`, verify the relevant dispatch/commit path already handles that class.

## Testing Strategy

Verified facts:

- CI runs `cargo fmt --all --check`, two Clippy passes, `cargo test --workspace`, bench compile/smoke checks, native Windows/macOS tests, selected cross-target checks, and `cargo deny`.
- Planner tests live under `crates/clinker-plan/src/plan/tests/` and cover DAGs, CK lattice/aligned partitions, deferred regions, route ports, cull/reshape validation, doc paths, watermark validation, envelope synthesis, and explain output.
- Executor integration tests dominate runtime coverage under `crates/clinker-exec/tests/`, covering aggregates, combine strategies, channels/compositions, retraction/correlation, DLQ, memory arbitration, spill/storage, streaming, formats, document context, REST transport validation, provenance, scheduling, and user docs.
- Executor white-box tests live under `crates/clinker-exec/src/executor/tests/` for scheduling, overshoot, deferred dispatch, spill-dir behavior, aggregation internals, and multi-output.
- Format integration tests cover streaming document indexes for JSON/XML; many format pipelines are covered through executor tests.
- `cxl` has unit tests and property tests, including compiled evaluator proptests.
- Snapshot tests use `insta` for explain/diagnostic output such as `pre_lift_baselines`, cull explain, retraction explain, and state-node diagnostics.
- Benchmarks are first-class for hot paths: record ops, CXL parse/eval, format IO, executor sort/arena/window/parallel/provenance/combine/memory/spill, and end-to-end benchmark matrices.

Practical gates:

- For docs-only architecture edits, run `git diff --check`.
- For Rust behavior changes, follow `docs/ai/50_TESTING_AND_COMMANDS.md` and match tests to the touched boundary: plan/config in `clinker-plan`, runtime in `clinker-exec`, format in `clinker-format`, language in `cxl`, channels in `clinker-channel`, CLI in `clinker`.

## Architectural Invariants

These are supported by current code structure or source comments:

- Pipelines compile before they execute. Planning/config validation belongs in `clinker-plan`; runtime operator execution belongs in `clinker-exec`.
- Executor public entry points should consume `CompiledPlan`, not raw YAML config.
- The runtime is finite and synchronous. Do not introduce unbounded streams, daemon/service loops, distributed execution, or async-runtime assumptions without architecture review.
- All declared source transports must normalize into `RecordSource` / `SourceInput` and feed the same ingest path.
- Memory arbitration is shared per run. Spill-capable operators and node buffers should poll the same `MemoryArbitrator` instead of inventing independent budget decisions.
- Records are schema-indexed. Runtime field writes should target fields already widened/bound by plan/schema compilation.
- YAML parsing goes through `clinker_plan::yaml`; span-aware config parsing and `Spanned<T>` diagnostics are load-bearing.
- User-facing config should remain strict where `deny_unknown_fields` is already established.
- Channels and compositions are explicit-boundary mechanisms. Channel overlays cannot patch arbitrary internals.
- Path trust should flow through validation/proof types such as `ValidatedPath`, not raw strings or unchecked `PathBuf`s.
- Benchmark/test support should not leak into default runtime paths.
- Public behavior changes need matching docs/examples/tests at the boundary they affect.

## Areas Of Uncertainty

- `clinker-format -> cxl` is a current dependency edge, apparently for CXL-aware envelope/extraction behavior, but `20_CRATE_MAP.md` flags whether this is intended as a permanent layering rule.
- `clinker-net -> clinker-exec` is current and deliberate for `RecordSource`, but it means network transport is an executor integration crate rather than a low-level IO layer.
- `PipelineExecutor::run_plan_with_readers_writers` requires `CompiledPlan`, yet the run body currently re-enters compilation through the embedded config. Future changes around plan reuse should inspect this carefully before assuming the stored DAG is the sole runtime input.
- The repo has `tokio` in workspace dependencies, but core execution is synchronous. It is unclear whether `tokio` is reserved for external/editor tooling or stale dependency surface.
- `reserve/` appears to be a crates.io name-reservation package, not runtime code, but this is inferred from local files.
