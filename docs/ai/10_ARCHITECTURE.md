# AI Onboarding: Architecture

Verified against the current working tree (2026-08-07).

Purpose: Give a senior Rust engineer or AI coding agent a practical, source-backed architecture overview before changing Clinker.

## Source Evidence

Primary evidence used for this pass:

- Workspace and crate boundaries: `Cargo.toml`, `crates/*/Cargo.toml`, `docs/ai/20_CRATE_MAP.md`.
- Planning and config: `crates/clinker-plan/src/lib.rs`, `crates/clinker-plan/src/config/pipeline.rs`, `crates/clinker-plan/src/config/pipeline_node.rs`, `crates/clinker-plan/src/config/sink.rs`, `crates/clinker-plan/src/resources/mod.rs`, `crates/clinker-plan/src/yaml.rs`, `crates/clinker-plan/src/plan/compiled.rs`, `crates/clinker-plan/src/plan/execution/consumer_registry.rs`, `crates/clinker-plan/src/plan/execution/scheduling.rs`.
- Runtime and IO: `crates/clinker-exec/src/executor/mod.rs`, `crates/clinker-exec/src/executor/params.rs`, `crates/clinker-exec/src/executor/source_stream.rs`, `crates/clinker-exec/src/executor/sink_dispatch.rs`, `crates/clinker-exec/src/executor/stream_event.rs`, `crates/clinker-exec/src/source/mod.rs`, `crates/clinker-exec/src/source/order_barrier.rs`, `crates/clinker-exec/src/pipeline/memory.rs`, `crates/clinker-exec/src/pipeline/shutdown.rs`, `crates/clinker-exec/src/telemetry.rs`, `crates/clinker-format/src/traits.rs`, `crates/clinker-net/src/otlp.rs`.
- Data model and language: `crates/clinker-record/src/lib.rs`, `crates/clinker-record/src/record/mod.rs`, `crates/clinker-record/src/storage.rs`, `crates/clinker-record/src/value.rs`, `crates/cxl/src/lib.rs`.
- Edge surfaces: `crates/clinker/src/main.rs`, `crates/clinker/src/lifecycle.rs`, `crates/clinker/src/observability.rs`, `crates/clinker-lineage/src/logical_identity.rs`, `crates/clinker-lineage/src/delivery.rs`, `crates/clinker-channel/src/lib.rs`, `crates/clinker-channel/src/discovery.rs`, `crates/clinker-channel/src/group.rs`, `crates/clinker-channel/src/resolve.rs`, `crates/clinker-schema/src/lib.rs`, `examples/pipelines/customer_etl.yaml`.
- Tests and CI: `crates/clinker-exec/tests/*`, `crates/clinker-plan/src/plan/tests/*`, `crates/clinker-format/tests/*`, `crates/clinker-net/tests/*`, `crates/clinker-channel/tests/*`, `crates/clinker/tests/machine_supervision.rs`, `crates/clinker/tests/observability_isolation.rs`, `.github/workflows/ci.yml`.

Locked production targets, their current implementation status, compatibility
posture, and downstream owners are indexed in the
[production-contract register](15_PRODUCTION_CONTRACTS.md). This architecture
page remains implementation-first: a locked target is not evidence that the
current binary implements it.

## What Clinker Is

Verified facts:

- Clinker is a Rust workspace with 15 active members. The main executable is `crates/clinker`; the standalone CXL tool is `crates/cxl-cli`; lower layers are split into records, CXL, format, planning, execution, channel, network, schema, lineage, scenario, and benchmark support crates.
- Pipelines are YAML documents using a unified top-level `nodes:` list. `PipelineConfig` has `nodes: Vec<Spanned<PipelineNode>>`, and comments say legacy top-level `inputs:` / `outputs:` / `transformations:` are rejected by serde.
- The executable workload is finite batch-style pipeline execution. `RecordSource::next_record` is explicitly finite by contract, `clinker-net` describes REST as a finite-pull source with `max_pages` / `max_records`, and `PipelineExecutor` says no async runtime is required.
- CXL is the per-record expression language layer. The `cxl` crate exposes parser, resolver, typechecker, analyzer, aggregate extraction, and evaluator modules; plan and exec compile and evaluate CXL-bearing nodes.
- Clinker is not currently an editor application in this repository. Tooling-facing surfaces appear to be data/API outputs such as `ExplainFormat::Json`, `CompiledPlan::provenance`, `CompiledPlan::bound_schemas`, OpenLineage NDJSON from `clinker-lineage`, and `clinker-schema`.

Current description:

- Clinker is a bounded-memory, single-process DAG executor for finite ETL jobs, with YAML configuration, CXL expression programs, streaming readers/writers, and an explicit plan/runtime boundary. Locked production wording and target deltas are tracked in the [production-contract register](15_PRODUCTION_CONTRACTS.md).

## Major Subsystems

Verified facts:

- **Record model:** `clinker-record` owns `Value`, `Record`, `Schema`, `RecordStorage`, `RecordView`, provenance, document context, grouping keys, counters, and accumulator state. `Record` stores positional `Vec<Value>` data behind an `Arc<Schema>`.
- **Expression engine:** `cxl` owns AST, lexing/parsing, module evaluation, resolution, typechecking, static analysis, aggregate planning, and runtime evaluation. Public symbols used downstream include `Parser`, `resolve_program`, `type_check`, `ProgramEvaluator`, and aggregate extraction/planning APIs.
- **Format layer:** `clinker-format` owns streaming `FormatReader` / `FormatWriter` traits plus CSV, JSON/NDJSON, XML, fixed-width, HL7, X12, EDIFACT, SWIFT, multi-record, envelope, document index, source reopening, writer counting, and splitting modules.
- **Planning layer:** `clinker-plan` parses YAML, validates topology and paths, resolves schemas and the workspace catalog, enumerates every CXL-bearing field through `PipelineNode::visit_cxl_fields`, admits and typechecks the bounded transitive module/declaration closure, lowers unified nodes into `ExecutionPlanDag`, and returns `CompiledPlan`. After all structural rewrites, the finalized DAG retains a `CompiledConsumerRegistry` keyed by `ProducerPortKey` and an immutable `ExecutionOrderContract` containing typed source orders, edge/consumer requirements, terminal promises, and physical-writer boundaries. `CompiledPlan::cxl_modules()` retains the parsed registry needed by execution. Its workspace observability config owns only strict secret-free raw endpoint/auth intent and numeric telemetry/lineage bounds; it does not parse a URI or hold network-auth state.
- **Runtime layer:** `clinker-exec` owns `PipelineExecutor`, executor dispatch arms, the unified `SourceAttemptEvent` stream and `AttemptPopulationDelta` accounting, `SourceRowId`, per-physical-file source-order verification/repair, shared-port replay, node buffers, streaming handoff, DLQ, metrics, memory arbitration, spill handling, joins/combines, aggregation, merge, reshape, cull, envelope, ordered physical-writer boundaries, output, and shutdown. It also owns the fixed-memory producers for real logs, metrics, and traces; it does not own OTLP transport.
- **Network source and OTLP layer:** `clinker-net` exposes finite REST sources plus the sole structured `http::Uri` admission/normalization boundary for one OTLP origin. The crate is split by whether a piece needs an HTTP client: endpoint admission and the public OTLP vocabulary compile unconditionally, while the request loops and the `rest` reader live behind its `transport` feature, which is what carries the `ureq` and `rustls-graviola` edges. `clinker-net` installs the graviola crypto provider on each agent it builds rather than through rustls's process-global default. Its admitted proof has private fields, derives only `/v1/logs`, `/v1/metrics`, and `/v1/traces`, and feeds finite synchronous transport with a post-admission borrowed authentication applicator.
- **Channel/deployment layer:** `clinker-channel` owns catalog-backed channel target discovery, explicit group target sets, selector/forced-group admission, typed overlay resolution, dotted paths, and source staging copies. Every applied layer is canonicalized and contained beneath its admitted root; parsing and identity hashing consume one bounded buffer from the same open file. `CompiledPlan::channel_identity()` records the complete ordered pipeline/group/channel/per-target layer stack, not merely a channel name or one overlay hash.
- **Schema workspace layer:** `clinker-schema` parses `.schema.yaml`, discovers schema files, builds `SchemaIndex`, and validates pipeline schema references.
- **Lineage layer:** `clinker-lineage` walks a `CompiledPlan` to compute OpenLineage column-level lineage (DIRECT value derivation plus dataset-level INDIRECT influence, traced through composition bodies), owns canonical/catalog collection identity with standard subset and symlinks facets, and emits run events as NDJSON. External delivery has its own capped nonblocking queue, sink-owned worker, deadline, counters, and typed outcome; explicit `local_diagnostic_paths` remains a synchronous local-only compatibility mode.
- **CLI layer:** `crates/clinker/src/main.rs` exposes `run`, `metrics`, `explain`, `channels`, `refactor`, and `config` commands through Clap and calls into plan/exec/channel/net/format/lineage code. At the observability edge it composes the admitted endpoint with plan-owned bounds before effects, owns one immutable `RunLifecycleFacts` source, and keeps OTLP and lineage workers and outcomes separate.
- **Benchmark/test layer:** `clinker-bench-support` and `clinker-benchmarks` own generators, cached data, benchmark runners, and optional allocation instrumentation.

## Data Flow

Verified end-to-end shape:

1. A user-facing pipeline YAML looks like `examples/pipelines/customer_etl.yaml`: `pipeline:` metadata, a `nodes:` list with `type: source`, `type: transform`, `type: sink`, and optional `error_handling:`.
2. Config loading goes through `clinker-plan`. `load_config` / `load_config_with_vars` parse YAML into `PipelineConfig`, and `clinker_plan::yaml::from_str` is the parser chokepoint over `serde-saphyr` with a 32 MiB input cap and depth/node/alias budgets.
3. `PipelineNode` deserialization is hand-written around the `type:` discriminator. It intentionally preserves per-node spans and per-variant `deny_unknown_fields` behavior.
4. `PipelineConfig::compile_topology_only` checks duplicate names, self-loops, general cycles, undeclared input references, path validation, dotted-name restrictions, and log directive sanity.
5. `PipelineConfig::compile` / `compile_with_diagnostics` use the variant-exhaustive `PipelineNode::visit_cxl_fields` traversal to find direct CXL roots, load only their admitted bounded module/declaration closure, bind schemas, typecheck CXL, lower nodes to `PlanNode`, and apply all structural rewrites. The finalized `ExecutionPlanDag` freezes its complete producer-port consumer registry and ordering/writer contract before `CompiledPlan` is returned.
6. Source inputs enter runtime as `SourceInput::Files(Vec<FileSlot>)` or `SourceInput::Records(Box<dyn RecordSource>)`. File transports reach `RecordSource` through a blanket impl for `Box<dyn FormatReader>`; REST uses `build_rest_source`.
7. Source ingest runs per declared source, resolves schemas with `schema(&mut self)`, calls finite `next_record`, assigns an attempt-local monotonic `SourceRowId`, attaches document/provenance context, and emits both successful records and recoverable type failures through one bounded `SourceAttemptEvent` stream. `AttemptPopulationDelta` carries the same attempt population into success, DLQ, and accounting paths. A retained `CompiledSourceOrder` supplies stable source identity, typed key positions/types, event shape, and unsorted policy to the memory-arbitrated barrier; the barrier verifies or stably repairs each physical file before release and never asserts global order across files.
8. Runtime dispatch walks the plan DAG, executing `PlanNode` variants through focused dispatch modules such as `transform_dispatch`, `aggregate_dispatch`, `combine_dispatch`, `route_dispatch`, `merge_dispatch`, `reshape_dispatch`, `cull_dispatch`, `envelope_dispatch`, and `sink_dispatch`. Fan-out consults the planning-owned `CompiledConsumerRegistry`; a shared producer port is materialized once and replayed independently to every compiled consumer, including spill-backed replay when memory pressure requires it.
9. Records are schema-indexed. Transform and aggregate CXL programs use typechecked artifacts from planning; runtime writes only fields already present in the widened output schema.
10. Sinks consume `FormatWriter` implementations through the planning-owned `PhysicalWriterBoundary`. `OrderedWriterBoundary` performs terminal sorting with the shared `MemoryArbitrator`, `SortBuffer`, and `SortedRunMerger`, preserves the authored keys as the whole ordering guarantee, and owns finish/error/temporary-spill cleanup. Writers may also provide envelope begin/end document hooks, byte counting, splitting, and metrics.
11. `ExecutionReport` returns counters, DLQ entries, execution summary, peak RSS, CPU/IO totals, stage metrics, watermarks, rollback cursors, per-source counts, spill totals, streaming charge peaks, and interrupted status.

Important nuance:

- Public executor APIs take `&CompiledPlan`, but `PipelineExecutor::run_plan_with_readers_writers` currently delegates through `plan.config()` into the shared run path, where the runtime performs the canonical compile path again in context before dispatch. When the supplied plan has CXL modules, the executor seeds that compile context from `plan.cxl_modules()`; admitted module source files are not reopened. Treat `CompiledPlan` as the public proof boundary, but inspect the current executor body before assuming the stored DAG is always consumed directly.

### Execution correctness authority map

The compile/runtime division is deliberate: planning proves and freezes
author-authored meaning; execution consumes those proofs while enforcing finite,
bounded operation.

| Surface | Planning authority retained by `CompiledPlan` | Runtime enforcement | Invariant |
|---|---|---|---|
| CXL in any node variant | `PipelineNode::visit_cxl_fields`, typed direct roots, bounded module/declaration closure, parsed `CompiledModuleRegistry` | Evaluator registry built from retained modules; admitted files are not reopened | No hidden CXL-bearing field or runtime filesystem admission |
| Channel/group overlays | Catalog target admission plus complete ordered `ChannelIdentity::layers` with exact-byte hashes | The resolved config enters the ordinary compile/run boundary | Every layer is contained, parsed, and identified from the same single-open buffer |
| DAG fan-out | `CompiledConsumerRegistry` keyed by `(PlanNodeId, producer port)` after structural rewrites | One shared resident or spill-backed replay source supplies independent consumers | No node-kind heuristic may omit a consumer or make one consumer drain another |
| Source typing and ordering | `CompiledSourceOrder`, typed key positions/types, per-file scope, unsorted policy | Unified attempt stream, population accounting, and `SourceFileOrderBarrier` | Records and recoverable type failures share one attempt order; separate files never imply a global sort |
| Downstream and terminal order | Edge requirements, output promises, and `PhysicalWriterBoundary` in `ExecutionOrderContract` | Strategy assertions and `OrderedWriterBoundary` using shared sort/spill machinery | Authored fields are the entire order key; no hidden source-identity tie-breaker |

These enforcement paths remain under the one run-scoped `MemoryArbitrator`.
Source repair, shared-port replay, and writer sorting can spill and merge, but do
not create private memory budgets, native helpers, async runtimes, or a second
ordering implementation.

### Current execution path and locked target

The current call path is explicit and non-conforming with the locked target:

```text
PipelineConfig::compile -> CompiledPlan
                             |
PipelineExecutor::run_plan_with_readers_writers(&CompiledPlan)
                             |
                        plan.config()
                             |
       CompileContext + PipelineConfig::compile
                             |
                 newly validated plan.dag()
                             |
                         dispatch
```

The supplied `CompiledPlan` is the locked execution boundary, but direct
execution of its stored DAG, composition bodies, schemas, compiled artifacts,
and statistics has **not** landed. D-01 through D-11 assign that correction and
sequential in-process reuse to Phase 5 / PERF-01; only an enumerated runtime
envelope may refresh. See
[stored-plan execution and cache identity](15_PRODUCTION_CONTRACTS.md#stored-plan-execution-and-cache-identity).

### Current terminal node

The terminal writer is authored only as `type: sink`, deserializes to
`PipelineNode::Sink` with `SinkConfig` from `config/sink.rs`, lowers to
`PlanNode::Sink`, and runs through `executor/sink_dispatch.rs`. The retired
`type: output` spelling is rejected with E376 and the paste-ready correction
`type: sink`. Output-port maps, produced artifacts and paths, serialization
formats, stdout, command and machine output, writer results, and OpenLineage
output datasets remain valid output vocabulary.

Sink work uses bounded streaming handoffs or the shared memory-arbitrated
physical-writer sort/spill boundary; it has no private unbounded accumulator.
Real synchronous, streaming, and correlation-deferred work units record the
closed `Sink*` metric set and one complete `SpanName::Sink` outcome without
making telemetry admission part of execution. Lineage handles
`PlanNode::Sink` explicitly, retaining direct mapping edges and indirect
filter/order influence while publishing the terminal role as an OpenLineage
output dataset. See [Sink Nodes](../user/src/nodes/sink.md), [Sink
Internals](../engine/src/sink-internals.md), and [terminal destination
vocabulary](15_PRODUCTION_CONTRACTS.md#terminal-destination-vocabulary).

## Architectural Boundaries

Verified boundaries:

- `clinker-record` and `clinker-core-types` are lower-level vocabulary crates. They should not depend on planning or execution.
- `cxl` depends on records but does not depend on plan or exec.
- `clinker-plan` sits below execution. Its crate docs say it turns YAML and CXL into a typed, validated `ExecutionPlanDag` "without depending on any runtime operator." Its public `resources` module owns workspace catalog identity, rules-root selection, bounded module loading, and `CompiledModuleRegistry`.
- `clinker-exec` consumes plan/config artifacts and owns runtime operator behavior. Executor public docs include a `compile_fail` doctest showing `&PipelineConfig` is not accepted by `run_plan_with_readers_writers`.
- `clinker-format` is the streaming IO layer. Its current dependency on `cxl` is the reviewed D-20 exception for logical types and document path/index behavior, not general permission to move expression evaluation into formats.
- `clinker-net` is not a low-level HTTP-only crate; it depends on `clinker-exec` because REST readers implement executor `RecordSource`, and it separately owns the opaque OTLP endpoint-admission proof and synchronous transport.
- The shared failure edge is deliberately exact: only `clinker-net` and
  `clinker-lineage` add normal dependencies on `clinker-core-types`, and they
  consume only `FailureClassification`, `FailureCategory`, and `RetryAdvice`.
  They do not re-export the taxonomy or move identity and serialization policy
  into the shared crate.
- `clinker-channel`, `clinker-net`, `clinker-schema`, `clinker`, and `cxl-cli` are edge/integration crates.
- Benchmark helpers must remain outside default runtime paths. The `clinker-exec -> clinker-bench-support` edge is optional and feature-gated for `bench-alloc`.

Practical guidance:

- Do not move raw YAML parsing into executor code.
- Do not let executor APIs casually accept unvalidated config when a `CompiledPlan` or typed plan artifact is the established boundary.
- Do not special-case transports inside operator dispatch. `SourceInput::Files` and `SourceInput::Records` are intentionally normalized before dispatch.
- Keep path security APIs proof-oriented. `ValidatedPath` has private internals and should remain the handoff type for trusted filesystem paths.
- Keep channel and composition overrides at declared boundaries. `clinker-channel` docs explicitly forbid mid-graph patching and sealed composition internals access.

### Optional machine process boundary

The ordinary architecture is still `clinker run <CONFIG>` as one finite
synchronous process. `--machine ndjson-v1 --batch-id <ID>` is an opt-in CLI-edge
serializer in `crates/clinker/src/machine.rs`, not a worker runtime. One owner
assigns sequence, bounds advisory progress, and emits terminal truth only after
the existing cancellation/publication decision is known.

An external parent owns concurrent bounded stdout/stderr drains, the overall
deadline, independent heartbeat, platform process-tree policy, fresh-process
retry, and direct-child reaping. Cancellation delivers the real platform
graceful signal while both drains remain active, waits a distinct bounded grace
interval, forces once only after expiry, reaps the direct child, and only then
joins the drains. It accepts success only when the supported terminal, actual
process status, and current-attempt artifact truth reconcile. Progress is not a
heartbeat or resume cursor, and individually atomic artifacts do not imply
set-wide atomic publication. The Linux direct-child proof covers cooperative
SIGTERM and uncooperative forced fallback; process groups and descendants
remain adapter-owned and outside that proof. The executable contract is
covered by `crates/clinker/tests/machine_protocol_cli.rs` and
`crates/clinker/tests/machine_supervision.rs`.

### Optional observability and lineage boundary

Deployment observability is disabled when the workspace policy is absent. The
enabled capability crosses a single chain: `clinker-plan` retains exact raw
secret-free endpoint/auth intent and finite bounds; `clinker-net` alone admits
and normalizes the endpoint into its opaque proof; the CLI composes that proof
with the bounds before source, output, worker, or network effects; and
`clinker-exec` supplies the real log, metric, and trace producers. There is no
second URI parser or admitted-endpoint type.

Credential-free HTTPS with no headers is production-reachable. A
provider-neutral referenced mode is accepted as secret-free intent but fails
before exporter effects until D-13/D-15 and AUTH-01 supply the run-local
credential handle and applicator. That later applicator cannot change the
admitted origin or fixed signal routes.

Machine, OTLP, and OpenLineage correlation copies batch ID, execution ID,
semantic plan fingerprint, and terminal facts from one CLI-owned immutable
lifecycle source. OTLP and OpenLineage retain independent capacities,
deadlines, workers, counters, and typed outcomes. Privacy policy is applied
before telemetry queue admission; optional delivery outcomes never redefine
output/DLQ bytes, process status, machine truth, publication inventory, visible
finals, or retained failed-attempt evidence.

## Public API Surfaces

The following are reachable surfaces future agents should recognize. Rust
visibility does not by itself make each one a supported integration API. Apply
the D-18/D-19 classification in
[the crate map](20_CRATE_MAP.md#rust-reachability-and-compatibility) before
making a compatibility claim or changing a re-export:

- `clinker_plan::config::PipelineConfig::{compile, compile_with_diagnostics, compile_topology_only, source_configs, output_configs}`.
- `clinker_plan::config::{load_config, load_config_with_vars}` and `clinker_plan::yaml::{from_str, to_string, Spanned, CxlSource}`.
- `clinker_plan::plan::CompiledPlan::{dag, config, composition_bodies, statistics, body_of, provenance, provenance_mut, channel_identity, pipeline_hash, bound_schemas, schema_provenance, cxl_modules}` and planning-owned module resources under `clinker_plan::resources`.
- `clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode, PlanEdge, NodeExecutionReqs}`.
- `clinker_exec::executor::{PipelineExecutor, PipelineRunParams, ExecutionReport, WriterRegistry, SourceReaders, SourceInput, RecordSource, single_file_reader}`.
- `clinker_exec::source::{RecordSource, SourceInput}` for non-file source integration.
- `clinker_format::{FormatReader, FormatWriter, FormatError, EnvelopeConfig, EnvelopeEvent, EnvelopeFramer, ReopenableSource}`.
- `clinker_record::{Record, RecordPayload, Value, Schema, SchemaBuilder, RecordStorage, RecordView, DocumentContext, PipelineCounters}`.
- `clinker_channel::{resolve, OverlayResolution, resolve_channel_overlay, scan_channels, scan_groups, DottedPath, ChannelManifest, OverlayFile, Group, GroupTargetSet, ValidatedGroupTargets, ChannelOverlayResult, ResolvedChannelConfig, SourceStager}`.
- `clinker_net::build_rest_source`.
- `clinker_schema::{parse_schema, parse_schema_file, build_workspace_schema_index, validate_pipeline}`.
- `clinker_lineage::{column_lineage, dataset_identity, run_events, LiveRunEmitter, write_ndjson}`.
- CLI commands in `crates/clinker/src/main.rs`: `run`, `metrics collect`, `explain`, `channels`, `refactor`, and `config`.

## Ownership And Lifetime Patterns

Verified patterns:

- Records own their values but share schema and document context: `Record { schema: Arc<Schema>, values: Vec<Value>, doc_ctx: Arc<DocumentContext> }`.
- `SourceRowId` lives in executor stream vocabulary because it combines a planning-owned `PlanNodeId` with an attempt-local source ordinal. It is minted once at ingest and preserved through resident/spilled handoffs, fan-out, structural carriers, DLQ/commit state, and terminal accounting; `clinker-record` does not acquire a planner dependency.
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
- Memory arbitration is concurrent but centralized. Registered consumers expose usage/pause/spill hooks; operators poll `should_spill` / `should_abort` at chunk boundaries, and the resume controller un-pauses paused producers once usage falls back below the configured `resume_threshold` fraction of the budget.

Current guidance:

- `tokio` exists in workspace dependencies, but current core pipeline execution is non-async. Adding async transport or a Tokio-driven executor is an architectural change, not a local refactor.

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
- Channels load `channel.cfg.yaml` plus one catalog-selected per-target `.channel.yaml`, content-hash their raw files with BLAKE3, and apply bounded provenance layers.
- Planning owns CXL module filesystem admission. It resolves the bounded transitive import closure into a `CompiledModuleRegistry`, records direct program roots and exports, and stores that immutable registry on `CompiledPlan`; runtime evaluation consumes the compiled registry rather than reopening admitted module files. The `cxl_module_resolution` integration gate removes those files after planning and verifies that the run still succeeds.
- Groups declare explicit pipeline/composition targets. Catalog validation produces `ValidatedGroupTargets`; selector-derived groups are restricted to the already admitted target-intersected subset, and forcing a group bypasses label selection but not target admission. Channel overlay resolution then runs per target and its resolved composition closure.
- Source staging uses `clinker-channel::SourceStager` and path matching/resource reuse logic; filesystem path validation lives in `clinker-plan::security`.

## Extension, Plugin, And Scripting Boundaries

Verified facts:

- CXL is the scripting boundary for row expressions, filters, emits, aggregates, route predicates, and related computed behavior. It is compiled/typechecked before runtime evaluation.
- Pipeline extension is by node variants in `PipelineNode` / `PlanNode` plus corresponding plan lowering, schema binding, executor dispatch, docs, examples, and tests.
- Format extension is by implementing `FormatReader` and/or `FormatWriter` and wiring it through format config and executor reader/writer construction.
- Transport extension is by implementing `RecordSource` and registering `SourceInput::Records`; REST is the current concrete non-file example.
- Composition extension is declarative through `.comp.yaml`, signatures, ports, params, resources, and bound bodies. Composition internals are sealed from channel overlays except through declared surfaces.
- Channel extension is declarative through `.channel.yaml`, explicit group target sets, `DottedPath`, config/resource overrides, and source staging. New channel behavior must preserve catalog-backed target admission and per-target composition-closure confinement.

No verified general-purpose plugin system was found in this repository. Treat "plugin" as unsupported unless maintainers identify a specific extension mechanism.

## Error Handling Strategy

Verified facts:

- Compile-time structured diagnostics live in `clinker-core-types` (`Diagnostic`, `Severity`, spans, payloads). `PipelineConfig::compile` returns `Result<CompiledPlan, Vec<Diagnostic>>`.
- Runtime and subsystem failures aggregate through `clinker_plan::error::PipelineError`, which has variants for config, schema, format, eval, compilation, I/O, spill, thread pool, multiple writer errors, internal invariant violations, accumulator failures, schema mismatch, composition, memory, combine, envelope, and other runtime cases.
- `PipelineError::Internal` is explicitly for plan-time invariant violations found at runtime and should abort regardless of `ErrorStrategy::Continue`.
- The twelve production-reachable specialized dispatcher entry boundaries
  return a bounded `DispatchMismatch` instead of unwinding. It classifies
  directly as `runtime.invariant.dispatch_mismatch`,
  `FailureCategory::InternalInvariant`, and `RetryAdvice::PolicyRequired`
  before mutable operator or publication effects. This SECU-03 boundary is a
  Phase 3 runtime-invariant decision, not a numbered production-contract row;
  locally proven internal algorithm and Sink assertions remain assertions.
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

## Open Question Routing

Unresolved or explicitly deferred architecture questions are tracked in
`docs/ai/80_OPEN_QUESTIONS.md`. Locked decisions about the `clinker-format ->
cxl` edge, plan reuse, public API compatibility, unused Tokio declaration, and
terminal Sink migration are in the
[production-contract register](15_PRODUCTION_CONTRACTS.md); do not reopen them
as implementation-local choices. `clinker-net` layering and any genuinely new
async-runtime proposal still require source review and architecture approval.
