# AI Onboarding: Crate Map

Verified against the current working tree (2026-08-07).

Purpose: Give future AI agents a factual map of the current Cargo workspace, with dependency direction, crate roles, and evidence anchors for safe code changes.

## Workspace Overview

The root workspace has 15 members: `clinker-record`, `cxl`, `cxl-cli`, `clinker-format`, `clinker-core-types`, `clinker-plan`, `clinker-exec`, `clinker-net`, `clinker-channel`, `clinker`, `clinker-schema`, `clinker-lineage`, `clinker-bench-support`, `clinker-benchmarks`, and `clinker-scenarios` (`Cargo.toml`). A separate non-member Cargo package named `clinker` (a crates.io name-reservation placeholder, directory `reserve/`) is maintained outside the tracked tree — it is untracked, absent from fresh clones, and nothing in the workspace depends on it.

No Cargo `examples` targets were found. The repository does contain YAML pipeline examples and fixtures under `examples/pipelines/`, the executed scenario corpus under `examples/scenarios/`, plus benchmark pipeline configs under `benches/pipelines/`.

`clinker-scenarios` sits at the edge alongside the benchmark crates: it depends on nothing in the workspace and nothing in the runtime depends on it. It generates the input data for `examples/scenarios/`, which `crates/clinker/tests/scenarios.rs` executes against committed goldens. Scenario inputs are generated rather than committed; the expected output is committed.

## Dependency Direction Summary

Normal workspace dependencies, from lower-level vocabulary toward applications, currently appear to be:

```text
clinker-core-types
clinker-record
  -> cxl
  -> clinker-format
  -> clinker-plan
  -> clinker-exec
  -> clinker-net
  -> clinker-channel
  -> clinker-schema
  -> clinker / cxl-cli

clinker-core-types -> clinker-net + clinker-lineage

clinker-bench-support -> clinker-record
clinker-benchmarks -> clinker-bench-support + clinker-exec + clinker-plan + cxl
clinker-scenarios -> (no workspace dependencies; dev-dependency of clinker)
```

Important normal dependency edges from `cargo metadata --no-deps`: `cxl -> clinker-record`; `clinker-format -> clinker-record, cxl`; `clinker-plan -> clinker-core-types, clinker-format, clinker-record, cxl`; `clinker-exec -> clinker-core-types, clinker-format, clinker-plan, clinker-record, cxl`; `clinker-channel -> clinker-core-types, clinker-plan, clinker-record`; `clinker-net -> clinker-core-types, clinker-exec, clinker-format, clinker-plan, clinker-record`; `clinker -> clinker-channel, clinker-core-types, clinker-exec, clinker-format, clinker-plan, clinker-record`, plus `clinker-lineage` and `clinker-net` under the default features; `cxl-cli -> cxl, clinker-record`; `clinker-schema -> clinker-plan`; `clinker-lineage -> clinker-core-types, clinker-plan, clinker-record, cxl`.

### Shared failure taxonomy boundary

`clinker-core-types::failure` owns the serialization-neutral failure taxonomy.
The only approved root types for the network and lineage consumers are
`FailureClassification`, `FailureCategory`, and `RetryAdvice`. The approved
normal dependency edges are `clinker-net -> clinker-core-types` and
`clinker-lineage -> clinker-core-types`. Consumer crates adapt these values at
their edges and do not re-export the shared taxonomy. This decision adds no
feature, serialization policy, package, or additional shared type.

Semantic plan identity remains in `clinker-plan`; dataset identity remains in
`clinker-lineage`. The shared vocabulary therefore owns neither identity nor
wire-format serialization policy.

## Current Layering Rules Inferred From Source

- `clinker-core-types` appears intended as a leaf crate: its crate docs say it holds spans, diagnostics, graph, and DLQ vocabulary and "deliberately holds no executor, config, or schema types" (`crates/clinker-core-types/src/lib.rs`).
- `clinker-record` is the shared data model leaf for row values, schemas, storage traits, grouping keys, document context, and accumulators (`crates/clinker-record/src/lib.rs`).
- `cxl` sits above records but below planning/execution: it parses, resolves, type-checks, plans aggregates, and evaluates expressions against `clinker-record` values (`crates/cxl/src/lib.rs`).
- `clinker-format` owns streaming readers/writers and document/envelope framing. It depends on `cxl`, so it is not a pure serialization leaf (`crates/clinker-format/Cargo.toml`; `crates/clinker-format/src/lib.rs`).
- `clinker-plan` is the sole execution-admission authority below the runtime executor. It discovers every typed CXL root, freezes the complete producer-port consumer registry and execution-order/writer contract after structural rewrites, and returns those proofs on `CompiledPlan`. Its observability config retains only strict secret-free raw endpoint/auth intent and numeric telemetry/lineage bounds; endpoint parsing and network-auth state stay out (`crates/clinker-plan/src/lib.rs`; `crates/clinker-plan/src/config/observability.rs`; `crates/clinker-plan/src/plan/execution/consumer_registry.rs`; `crates/clinker-plan/src/plan/execution/scheduling.rs`).
- `clinker-exec` is runtime orchestration and operators. It consumes planning-owned proofs, owns the unified source-attempt and population-accounting stream, enforces shared-port replay plus source/writer ordering under one run-scoped memory arbitrator, and produces real fixed-memory logs, metrics, and traces. Binaries and network transports depend on it rather than the reverse (`crates/clinker-exec/src/lib.rs`; `crates/clinker-exec/src/executor/mod.rs`; `crates/clinker-exec/src/telemetry.rs`).
- `clinker-channel`, `clinker-net`, `clinker-schema`, `clinker`, and `cxl-cli` appear to be edge/application or integration crates around the plan/exec/language core. In addition to finite REST sources, `clinker-net` alone parses and normalizes an OTLP origin into an opaque admitted proof with private fields, derives the three fixed signal routes, and owns bounded synchronous transport plus the post-admission borrowed credential applicator.
- `clinker-lineage` is a plan-time, read-only consumer of `clinker-plan`: it maps Source/Output nodes to canonical datasource or exact catalog identities and walks the compiled DAG to emit DIRECT (per-column) and INDIRECT (whole-dataset influence) column-level lineage facets (OpenLineage `2-0-2` / `ColumnLineageDatasetFacet` `1-2-0`). Stable collection identities carry explicitly authorized standard input/output subset and symlinks facets; path identity exists only in explicit `local_diagnostic_paths`. It reads typed/compiled programs off plan nodes via `cxl`; it does not run pipelines, mint identities, hold a clock, or own lifecycle state. The CLI supplies immutable bounded start/terminal snapshots from one `RunLifecycleFacts` source (`crates/clinker/src/lifecycle.rs`; `crates/clinker-lineage/src/emit.rs`). `run --lineage <path>` writes a static START/COMPLETE pair; `run --lineage-events <path>` emits live START and terminal events with shared `clinker_batch` run and `clinker_semanticPlan` job correlation, real `clinker_runStats`, and sanitized standard/clinker failure facets. External delivery owns an independent byte-capped nonblocking queue, sink worker, deadline, counters, and typed outcome. Both paths preflight the resolved workspace identity policy before opening lineage or execution outputs. The engine core (`clinker-exec`) has no lineage dependency; lifecycle truth remains at the CLI edge.
- Benchmark crates appear intended to stay outside the runtime layer. `clinker-benchmarks/src/lib.rs` says it houses a runner needing both `clinker-exec` and `clinker-bench-support` to avoid a circular dependency.

## Cycles And Suspicious Coupling

- No normal workspace dependency cycle was found in `cargo metadata --no-deps` or `cargo tree --workspace --depth 1`.
- `clinker-exec` has an optional normal dependency on `clinker-bench-support` for `bench-alloc` (`crates/clinker-exec/Cargo.toml`). That may be intentional for allocation measurement, but future agents should avoid letting benchmark helpers leak into default runtime code.
- `clinker-net` depends on `clinker-exec` to implement `RecordSource` (`crates/clinker-net/src/lib.rs`; `clinker_exec::source::RecordSource`). This couples network source readers to executor source traits; it appears deliberate but means network transport is not a low-level IO crate.

## Rust Reachability And Compatibility

Rust `pub` controls whether a path is reachable. It does not establish that
Clinker supports the path as an integration API. D-18 locks four compatibility
classes: supported integration API, workspace-internal exposed API, test
support, and deprecated cleanup debt; D-19 also distinguishes a deprecated
route whose supported replacement already exists. Structural facade work is
owned by Phase 4. See
[validation authority and Rust API compatibility](15_PRODUCTION_CONTRACTS.md#validation-authority-and-rust-api-compatibility).

Every D-19 seed has exactly one class:

| Reachable symbol | D-19 class | Compatibility posture | Evidence and route |
|---|---|---|---|
| `clinker_record::FieldResolver` | Supported integration API | Changes require an explicit compatibility decision and migration note. | Root re-export in `crates/clinker-record/src/lib.rs` |
| `clinker_record::HashMapResolver` | Supported integration API | Changes require an explicit compatibility decision and migration note. | Root re-export in `crates/clinker-record/src/lib.rs` |
| `clinker_record::WindowContext` | Supported integration API | Changes require an explicit compatibility decision and migration note. | Root re-export in `crates/clinker-record/src/lib.rs` |
| `cxl::resolve::HashMapResolver` | Deprecated route | Migrate consumers to `clinker_record::HashMapResolver`; removal follows a bounded migration. | Re-export from `cxl::resolve::test_double` in `crates/cxl/src/resolve/mod.rs` |
| `cxl::resolve::test_double` | Deprecated cleanup debt | Public module reachability is accidental debt, not test-support compatibility. | Public module declaration in `crates/cxl/src/resolve/mod.rs` |
| `cxl::typecheck::Row` | Workspace-internal exposed API | Compiler representation; workspace use does not create downstream support. | Re-export in `crates/cxl/src/typecheck/mod.rs` |
| `cxl::typecheck::RowTail` | Workspace-internal exposed API | Compiler representation; workspace use does not create downstream support. | Re-export in `crates/cxl/src/typecheck/mod.rs` |
| `cxl::typecheck::TailVarId` | Workspace-internal exposed API | Compiler representation; workspace use does not create downstream support. | Re-export in `crates/cxl/src/typecheck/mod.rs` |
| `cxl::typecheck::ColumnLookup` | Workspace-internal exposed API | Compiler representation; workspace use does not create downstream support. | Re-export in `crates/cxl/src/typecheck/mod.rs` |
| `cxl::typecheck::QualifiedField` | Workspace-internal exposed API | Compiler representation; workspace use does not create downstream support. | Re-export in `crates/cxl/src/typecheck/mod.rs` |
| `clinker_plan::config::RouteConfig` | Deprecated cleanup debt | Legacy config reachability may be removed through bounded cleanup; it is not a supported facade. | `route::*` re-export in `crates/clinker-plan/src/config/mod.rs` |
| `clinker_plan::config::RouteBranch` | Deprecated cleanup debt | Legacy config reachability may be removed through bounded cleanup; it is not a supported facade. | `route::*` re-export in `crates/clinker-plan/src/config/mod.rs` |

**Test support** remains a valid D-18 class for deliberately gated helpers, but
none of the D-19 seed symbols is assigned to it. In particular,
`cxl::resolve::test_double` is cleanup debt, not a supported test facade.

## Terminal Node Vocabulary

The current planner and runtime use `PipelineNode::Output`, `OutputConfig`,
Output-oriented dispatch, and public YAML `type: output`. D-56 assigns the
terminal-node-only migration to Sink to Phase 4 / AUTH-09, wholly before Phase
4.1 endpoint work. Phase 1 does not change Rust, YAML, examples, fixtures, or
tests. Output ports, artifacts, paths, formats, stdout, machine output, writer
results, and OpenLineage output datasets keep their existing vocabulary. See
[terminal destination vocabulary](15_PRODUCTION_CONTRACTS.md#terminal-destination-vocabulary).

## Crates

### clinker-record

- Crate name: `clinker-record`
- Path: `crates/clinker-record`
- Role: Library crate plus `record_ops` Criterion bench.
- Purpose: Defines Clinker's core in-memory data model: `Value`, `Record`, `Schema`, field strings, coercion, grouping keys, provenance, document context, storage traits, pipeline counters, and aggregate accumulator state.
- Important public modules: `accumulator`, `coercion`, `counters`, `decimal_serde`, `document_context`, `field_str`, `group_key`, `minimal`, `provenance`, `record`, `record_view`, `resolver`, `schema`, `schema_def`, `storage`, `value`.
- Internal dependencies: none for normal build; dev-depends on `clinker-bench-support`.
- Architecturally important external dependencies: `serde`, `serde_json`, `chrono`, `ahash`, `indexmap`, `smol_str`; `postcard` and `criterion` for dev/bench.
- Known tests/examples/benches: unit tests across module files; `crates/clinker-record/src/accumulator/tests.rs`; `crates/clinker-record/benches/record_ops.rs`. Many downstream `clinker-exec` tests exercise records indirectly.
- Confidence: High.
- Evidence: `crates/clinker-record/Cargo.toml`; `crates/clinker-record/src/lib.rs` re-exports `Value`, `Record`, `Schema`, `RecordStorage`, `PipelineCounters`, `RetractionCounters`, and accumulator APIs.

### clinker-core-types

- Crate name: `clinker-core-types`
- Path: `crates/clinker-core-types`
- Role: Library crate.
- Purpose: Provides leaf vocabulary shared by planning, execution, diagnostics, channels, network delivery, and lineage: source spans, structured diagnostics, name-keyed graph utilities, DLQ categories/stage helpers, and the serialization-neutral failure taxonomy.
- Important public modules: `diagnostic`, `dlq`, `failure`, `graph`, `span`. The failure module owns only `FailureClassification`, `FailureCategory`, and `RetryAdvice` for the approved network and lineage consumer boundary; it owns no identity or wire policy.
- Internal dependencies: none.
- Architecturally important external dependencies: `miette`, `petgraph`, `serde-saphyr`.
- Known tests/examples/benches: unit tests in `diagnostic.rs`, `dlq.rs`, `failure.rs`, `graph.rs`, and `span.rs`; one integration test, `tests/registry_no_orphan_codes.rs`, which scans the workspace's Rust sources for diagnostic code literals and fails on any the registry does not list; no benches listed by Cargo metadata.
- Confidence: High.
- Evidence: `crates/clinker-core-types/src/lib.rs` explicitly describes the crate as leaf vocabulary and re-exports `Diagnostic`, `NameGraph`, `Span`, `DlqErrorCategory`, `FailureClassification`, `FailureCategory`, and `RetryAdvice`; `crates/clinker-core-types/Cargo.toml`.

### cxl

- Crate name: `cxl`
- Path: `crates/cxl`
- Role: Library crate plus `eval` and `parse` benches.
- Purpose: Owns the CXL expression language pipeline: AST, lexer/parser, module evaluation, name resolution, type checking, static analysis, aggregate extraction, and runtime evaluation.
- Important public modules: `analyzer`, `ast`, `builtins`, `eval`, `lexer`, `module_eval`, `parser`, `plan`, `resolve`, `typecheck`.
- Internal dependencies: `clinker-record`; dev-depends on `clinker-bench-support`.
- Architecturally important external dependencies: `miette` for diagnostics, `regex`, `indexmap`, `ahash`, `tracing`, `static_assertions`, `serde`, `chrono`; `proptest`, `criterion`, and `tracing-subscriber` for dev/bench.
- Known tests/examples/benches: many unit tests embedded under `src`; `crates/cxl/src/eval/tests.rs`; benches `crates/cxl/benches/eval.rs` and `crates/cxl/benches/parse.rs`.
- Confidence: High.
- Evidence: `crates/cxl/Cargo.toml` description "CXL language parser, type checker, and evaluator"; `crates/cxl/src/lib.rs`; symbols `Parser`, `resolve_program`, `type_check`, `ProgramEvaluator`, and `extract_aggregates`.

### cxl-cli

- Crate name: `cxl-cli`
- Path: `crates/cxl-cli`
- Role: Binary crate (`cxl-cli` package target; command name in Clap is `cxl`).
- Purpose: Provides a standalone language tool for checking, evaluating, and formatting CXL files or inline expressions.
- Important public modules: none; all code is in `src/main.rs`. Main command symbols include `Cli`, `Command`, `cmd_check`, `cmd_eval`, and `cmd_fmt`.
- Internal dependencies: `cxl`, `clinker-record`.
- Architecturally important external dependencies: `clap`, `miette`, `serde_json`, `indexmap`, `chrono`.
- Known tests/examples/benches: unit tests in `crates/cxl-cli/src/main.rs`; no integration tests or benches listed.
- Confidence: High.
- Evidence: `crates/cxl-cli/Cargo.toml`; `crates/cxl-cli/src/main.rs` command help and calls into `cxl::parser::Parser`, `cxl::resolve`, `cxl::typecheck`, and `cxl::eval`.

### clinker-format

- Crate name: `clinker-format`
- Path: `crates/clinker-format`
- Role: Library crate plus integration tests and `io_throughput` bench.
- Purpose: Owns streaming format IO, including CSV, JSON/NDJSON, XML, fixed-width, HL7, X12, EDIFACT, SWIFT, multi-record support, document indexes, source reopenability, output envelopes, counting writers, BOM handling, and output splitting.
- Important public modules: `bom`, `charset`, `counting`, `csv`, `doc_index`, `edifact`, `envelope`, `envelope_writer`, `error`, `fixed_width`, `hl7`, `json`, `multi_record`, `schema`, `source`, `splitting`, `swift`, `traits`, `x12`, `xml`. `segment_tokenizer` is crate-private.
- Internal dependencies: `clinker-record`, `cxl`; dev-depends on `clinker-bench-support`.
- Architecturally important external dependencies: `csv`, `quick-xml`, `serde`, `serde_json`, `miette`, `tracing`, `indexmap`, `chrono`.
- Known tests/examples/benches: `crates/clinker-format/tests/streaming_doc_index_json.rs`; `crates/clinker-format/tests/streaming_doc_index_xml.rs`; `crates/clinker-format/src/splitting/tests.rs`; `crates/clinker-format/benches/io_throughput.rs`.
- Confidence: High.
- Evidence: `crates/clinker-format/Cargo.toml`; `crates/clinker-format/src/lib.rs` re-exports `FormatReader`, `FormatWriter`, `FormatError`, `DocArenaIndex`, `EnvelopeFramer`, and EDI/HL7 defaults.

### clinker-plan

- Crate name: `clinker-plan`
- Path: `crates/clinker-plan`
- Role: Library crate with in-crate plan/config tests.
- Purpose: Parses YAML pipeline/composition configuration, resolves schemas, discovers sources and workspace resources, validates configs, and produces typed execution DAGs consumed by `clinker-exec`. Planning uses the variant-exhaustive `PipelineNode::visit_cxl_fields` traversal to own direct CXL roots and the bounded transitive module/declaration closure; the parsed `CompiledModuleRegistry` remains on `CompiledPlan`. After every structural rewrite it also freezes `CompiledConsumerRegistry` and `ExecutionOrderContract`, including source-order proofs and physical-writer boundaries. Workspace observability resolution owns strict secret-free raw endpoint/auth intent and fixed numeric telemetry plus independent-lineage bounds, but no URI admission or network credential handle.
- Important public modules: `config`, `error`, `overlay_ops`, `plan`, `resources`, `runtime_error`, `schema`, `security`, `span`, `validation`, `yaml`. `config` exposes aggregate/canonical/compile-context/composition/discovery/format/output/patch/pipeline/route/sort/source/storage/transform surfaces; `plan` exposes binding, combine, compiled plans, composition bodies, deferred regions, entities, envelope synthesis, execution, provenance, extraction/index, properties, row types, scheduling, statistics, streaming eligibility, and plan types. `plan::execution` owns `ProducerPortKey`, `CompiledConsumerRegistry`, `CompiledSourceOrder`, `PhysicalWriterBoundary`, and `ExecutionOrderContract`; `resources` owns `CompiledModuleRegistry`, parsed module entries, export metadata, and evaluator-registry construction.
- Internal dependencies: `clinker-core-types`, `clinker-format`, `clinker-record`, `cxl`.
- Architecturally important external dependencies: `serde`, `serde_json`, `serde-saphyr`, `toml`, `indexmap`, `miette`, `petgraph`, `regex`, `tracing`, `walkdir`, `glob`, `blake3`, `postcard`, `lz4_flex`, `tempfile`, platform `nix`/`windows-sys`.
- Known tests/examples/benches: in-crate rename gates in `src/lib.rs`; plan tests under `crates/clinker-plan/src/plan/tests/` for DAGs, consumer registries, frozen ordering contracts, CK lattice/aligned partitions, cull validation, deferred regions, route ports, watermark validation, source type diagnostics, doc paths, envelope synthesis, and explain output; config composition tests in `crates/clinker-plan/src/config/composition/tests.rs`.
- Confidence: High.
- Evidence: `crates/clinker-plan/src/lib.rs` states it sits below execution and produces `plan::execution::ExecutionPlanDag`; `crates/clinker-plan/src/config/mod.rs`; `crates/clinker-plan/src/plan/mod.rs`; `crates/clinker-plan/src/plan/compiled.rs`; `crates/clinker-plan/src/resources/mod.rs`; `crates/clinker-plan/src/plan/execution/mod.rs` symbols `PlanNode`, `PlanEdge`, `ExecutionPlanDag`, and `PlanError`.

### clinker-exec

- Crate name: `clinker-exec`
- Path: `crates/clinker-exec`
- Role: Library crate with the largest integration-test and benchmark surface.
- Purpose: Executes compiled pipeline DAGs: unified successful/type-error source attempts and population accounting, attempt-local row identity, per-physical-file source-order verification/repair, compiled shared-port replay, dispatch for node kinds, transforms, aggregations, combines/joins, route/merge/reshape/cull/output dispatch, physical-writer ordering and cleanup, DLQ, metrics, memory arbitration, spill handling, record sources, and progress. CXL and channel filesystem admission, the consumer graph, and ordering/writer proofs are planning-owned; execution consumes the retained artifacts. The crate also owns the preallocated privacy-gated logs/metrics/traces arena and real execution producers; it does not admit endpoints or deliver OpenLineage.
- Important public modules: `aggregation`, `dlq`, `executor`, `exit_codes`, `metrics`, `output`, `partial`, `pipeline`, `progress`, `projection`, `sketch`, `source`, `telemetry`; transform dispatch remains private. The `executor` module exposes `PipelineExecutor`, `PipelineRunParams`, `ExecutionReport`, `WriterRegistry`, `RecordSource`, `SourceInput`, and validation types; internal `source_stream` owns `SourceAttemptEvent` and `AttemptPopulationDelta`, `stream_event` owns `SourceRowId`, and `output_dispatch` owns `OrderedWriterBoundary`. The `pipeline` module exposes sort, combine, grace hash, IEJoin, memory, spill, streaming merge, and window context helpers. Source repair and writer ordering both reuse `MemoryArbitrator`, `SortBuffer`, and `SortedRunMerger` rather than defining private budgets or another ordering engine.
- Internal dependencies: `clinker-core-types`, `clinker-format`, `clinker-plan`, `clinker-record`, `cxl`; optional normal dependency on `clinker-bench-support`; dev-depends on `clinker-bench-support` and `clinker-channel`.
- Architecturally important external dependencies: `crossbeam-channel`, `rayon`, `arc-swap`, `hashbrown`, `lz4_flex`, `postcard`, `fs4`, `petgraph`, `miette`, `tracing`, `serde-saphyr`, `serde_json`, `csv`, `glob`, `uuid`, `ctrlc` on native targets, `windows-sys` on Windows, `criterion`, `insta`, `proptest`, `serial_test`.
- Known tests/examples/benches: many integration tests in `crates/clinker-exec/tests/`, including `multi_output`, `source_order_verification`, `source_type_errors`, `ordering_contract`, `output_envelope_seam`, `streaming_output`, and `document_dlq`, plus aggregate, combine, composition, correlation/retraction, format, storage, memory, streaming, docs, and fixture coverage; white-box tests under `crates/clinker-exec/src/executor/tests/`; benches under `crates/clinker-exec/benches/` including `sort`, `arena`, `window`, `pipeline`, `parallel`, `provenance`, `composition`, `combine`, `combine_iejoin`, `combine_nary_3input`, `combine_grace_hash`, `deferred_buffer_pruning`, `arbitration_poll`, and `spill_compression`.
- Confidence: High.
- Evidence: `crates/clinker-exec/Cargo.toml`; `crates/clinker-exec/src/lib.rs`; `crates/clinker-exec/src/executor/mod.rs` symbols `PipelineExecutor`, `SourceReaders`, `single_file_reader`, `PipelineRunParams`; `crates/clinker-exec/src/source/mod.rs` symbols `RecordSource` and `SourceInput`.

### clinker-channel

- Crate name: `clinker-channel`
- Path: `crates/clinker-channel`
- Role: Library crate plus integration tests and `channel_merge` bench.
- Purpose: Manages channel files for multi-tenant pipeline/composition launches: discovering catalog targets, validating explicit pipeline/composition group target sets, deriving selector or forced groups within the admitted target subset, resolving one target's channel overlay and composition closure, validating config override paths, applying overlays, and staging source copies with reuse/crash-safety logic. Each applied layer is canonicalized and contained beneath its admitted root, then read, parsed, and hashed from one bounded buffer on the same open handle. Resolution stamps the complete ordered pipeline/group/channel/per-target layer identity on `CompiledPlan`.
- Important public modules: `derivation`, `discovery`, `dotted`, `error`, `group`, `manifest`, `overlay`, `resolve`, `selector`, `staging_copy`.
- Internal dependencies: `clinker-core-types`, `clinker-plan`, `clinker-record`.
- Architecturally important external dependencies: `serde-saphyr`, `serde`, `serde_json`, `blake3`, `indexmap`, `tracing`, `thiserror`, `walkdir`, `uuid`, `tempfile`, `fs4`, Unix `nix`.
- Known tests/examples/benches: `crates/clinker-channel/tests/overlay_resolution_test.rs`, `discovery_test.rs`, `channel_manifest_test.rs`, `group_parse_test.rs`, `source_patch_parse_test.rs`, `scoped_overlay_validation.rs`, `staging_reuse_concurrent.rs`; `scoped_overlay_validation` includes containment, single-open, target-admission, and complete-identity gates; `crates/clinker-channel/benches/channel_merge.rs`; the multitenant overlay workspace under `examples/multitenant/`.
- Confidence: High.
- Evidence: `crates/clinker-channel/src/lib.rs` channel/group overlay guide; re-exports `resolve`, `OverlayResolution`, `resolve_target_channel`, `resolve_channel_overlay`, `scan_channels`, `scan_groups`, `DottedPath`, `ChannelManifest`, `OverlayFile`, `Group`, `GroupTargetSet`, `ValidatedGroupTargets`, and `SourceStager`.

### clinker-net

- Crate name: `clinker-net`
- Path: `crates/clinker-net`
- Role: Library crate plus REST and OTLP integration tests.
- Purpose: Provides finite-pull REST sources and the sole OTLP endpoint-admission/normalization and finite synchronous delivery boundary. `admit_otlp_endpoint` accepts one HTTPS origin, returns an opaque `AdmittedOtlpEndpoint` whose fields are private, and fixes `/v1/logs`, `/v1/metrics`, and `/v1/traces`; authentication can only be applied through the borrowed post-admission request boundary.
- Important public modules: no public submodules; `rest`, `otlp`, and `tls` are private. The root re-exports `admit_otlp_endpoint`, the opaque admitted proof, and the signal/budget/outcome and credential-applicator types unconditionally; `build_rest_source` and `send_otlp_json` exist only under the `transport` feature, so a caller compiled without it gets a resolution error rather than a call that does nothing.
- Internal dependencies: `clinker-core-types`, `clinker-exec`, `clinker-format`, `clinker-plan`, `clinker-record`; dev-depends on `clinker-bench-support` and `clinker-exec` with `test-utils`.
- Features: `transport` (not default) switches on the HTTP client and the TLS provider — the `rest` request loop plus its continuation policy, and the OTLP send/retry loop. Endpoint admission and the public OTLP vocabulary are outside it.
- Architecturally important external dependencies: `http` (URI and header vocabulary, unconditional); behind `transport`, `ureq` with `rustls-no-provider` + `rustls-webpki-roots` and `rustls-graviola` as the crypto provider — deliberately not ureq's `rustls` feature, which brings ring and with it a C build step. Also `serde_json`, `indexmap`, `tracing`.
- Known tests/examples/benches: `crates/clinker-net/tests/rest_executor_e2e.rs`; `crates/clinker-net/tests/rest_pagination.rs`; `crates/clinker-net/tests/otlp_http.rs`; executor transport coverage also appears in `crates/clinker-exec/tests/transport_validation.rs` and `record_source_transport.rs`.
- Confidence: High.
- Evidence: `crates/clinker-net/src/lib.rs` documents the finite-pull model and returns `Box<dyn clinker_exec::source::RecordSource>` from `build_rest_source`.

### clinker

- Crate name: `clinker`
- Path: `crates/clinker`
- Role: Binary crate for the main CLI.
- Purpose: Provides the user-facing ETL CLI that runs pipelines, performs dry-run/explain flows, applies channels, resolves memory/threads/output behavior, collects metrics, explains diagnostic codes, lists channels/groups, applies workspace-wide refactors (node rename), and prints resolved config (`config --resolved` expands multi-value shorthand to canonical form). It owns pre-effect observability composition, one immutable lifecycle-fact source, and separate finite OTLP and OpenLineage workers and outcomes.
- Important public modules: `refactor` (workspace-wide rename support in `src/refactor.rs` + `src/refactor/`); the rest of the CLI lives in `src/main.rs`. Main symbols include `Cli`, `Commands`, `RunArgs`, `MetricsCommands`, `CollectArgs`, `ExplainArgs`, `ConfigArgs`, `RenameNodeArgs`, `ResolveArgs`, and `LintArgs`.
- Internal dependencies: `clinker-channel`, `clinker-core-types`, `clinker-exec`, `clinker-format`, `clinker-plan`, `clinker-record`; plus `clinker-net` and `clinker-lineage`, which are optional.
- Features: `default = ["rest", "otlp", "lineage"]`. `rest` and `otlp` each pull `clinker-net` with its `transport` feature; `lineage` pulls `clinker-lineage`, which has no transport dependency and is a separate axis. `src/capability.rs` refuses a construct a build cannot carry out, at config validation and by name.
- Architecturally important external dependencies: `clap`, `miette`, `tracing`, `tracing-subscriber`, `serde-saphyr`, `serde_json`, `indexmap`, `chrono`, `num_cpus`, `uuid`, `tempfile`.
- Known tests/examples/benches: integration tests `crates/clinker/tests/atomic_output_test.rs`, `explain_provenance_test.rs`, `miette_rendering.rs`, `storage_config_cli.rs`; unit tests in `src/main.rs`; YAML examples under `examples/pipelines/` are primarily CLI-facing runnable examples.
- Confidence: High.
- Evidence: `crates/clinker/Cargo.toml`; `crates/clinker/src/main.rs` Clap command help for `run`, `metrics`, `explain`, `channels`, `refactor`, and `config`; uses `clinker_exec::executor::PipelineExecutor`.

### clinker-schema

- Crate name: `clinker-schema`
- Path: `crates/clinker-schema`
- Role: Library crate.
- Purpose: Parses `.schema.yaml` files, discover schemas and pipelines in a workspace, build schema indexes, and validate pipeline source/schema references.
- Important public modules: `discovery`, `model`, `parse`, `validate`.
- Internal dependencies: `clinker-plan`.
- Architecturally important external dependencies: `serde`, `serde_json`, `serde-saphyr`, `ahash`; `tempfile` for dev tests.
- Known tests/examples/benches: unit tests in `parse.rs`, `discovery.rs`, and `validate.rs`; schema examples/fixtures under `examples/pipelines/retract-demo/*.schema.yaml`.
- Confidence: High. The crate is an advisory edge/authoring support crate; D-17 keeps `clinker-plan` as the sole execution-admission authority, so schema-index validation cannot substitute for compilation.
- Evidence: `crates/clinker-schema/src/lib.rs`; public symbols `build_workspace_schema_index`, `parse_schema`, `parse_schema_file`, `validate_pipeline`, `SourceSchema`, and `SchemaIndex`.

### clinker-lineage

- Crate name: `clinker-lineage`
- Path: `crates/clinker-lineage`
- Role: Library crate plus composition-lineage integration test.
- Purpose: Serializes pipeline lineage as OpenLineage events: maps Source/Output nodes to canonical/catalog collection identities with standard subset and symlinks facets, walks a `CompiledPlan` DAG to compute DIRECT per-column lineage and dataset-level INDIRECT influence (traced through composition bodies and `$doc` reads), assembles static or live events from caller-owned immutable lifecycle facts, and owns independently bounded external delivery.
- Important public modules: `builder`, `dataset`, `delivery`, `emit`, `logical_identity`, `openlineage` (`event`, `facet`, `ndjson`).
- Internal dependencies: `clinker-core-types`, `clinker-plan`, `clinker-record`, `cxl`.
- Architecturally important external dependencies: `petgraph`, `serde`, `serde_json`.
- Known tests/examples/benches: `crates/clinker-lineage/tests/composition_lineage.rs`, `logical_identity.rs`, and `lifecycle_delivery.rs` plus fixtures; unit tests in module files; CLI-level behavior exercised through `crates/clinker` (`run --lineage`, `run --lineage-events`).
- Confidence: High.
- Evidence: `crates/clinker-lineage/src/lib.rs` (pinned to OpenLineage core `2-0-2`, `ColumnLineageDatasetFacet` `1-2-0`; re-exports immutable lifecycle input types, `run_events`, and `write_ndjson`); `crates/clinker-lineage/Cargo.toml`.

### clinker-bench-support

- Crate name: `clinker-bench-support`
- Path: `crates/clinker-bench-support`
- Role: Library support crate for tests/benchmarks, plus `bench_alloc` integration test gated by feature usage.
- Purpose: Provides deterministic test/benchmark data generation, workspace and benchmark pipeline discovery, IO capture helpers, synthetic readers, reusable cached benchmark data, combine data generation, and optional allocation accounting.
- Important public modules: `alloc` behind `bench-alloc`, `cache`, `combine`, `generators`, `io`.
- Internal dependencies: `clinker-record`.
- Architecturally important external dependencies: `fastrand`, `blake3`, `glob`, `serde`, `serde_json`, `chrono`; dev `quick-xml`, `tempfile`, `serial_test`.
- Known tests/examples/benches: unit tests in module files; `crates/clinker-bench-support/tests/bench_alloc.rs`; used by benches in `clinker-record`, `cxl`, `clinker-format`, `clinker-exec`, and `clinker-benchmarks`.
- Confidence: High.
- Evidence: `crates/clinker-bench-support/src/lib.rs` crate docs say it consolidates utilities for integration tests and Criterion benches and is "not shipped in release builds"; symbols `workspace_root`, `discover_pipeline_configs`, `RecordFactory`, `Scale`, `FieldKind`, `DataSpec`, and format generators.

### clinker-benchmarks

- Crate name: `clinker-benchmarks`
- Path: `crates/clinker-benchmarks`
- Role: Library benchmark harness plus `e2e_matrix` and feature-gated `e2e_xlarge` benches.
- Purpose: Runs end-to-end pipeline benchmarks over YAML benchmark configs, map pipeline input formats to generated data formats, execute pipelines through `clinker-exec`, and report benchmark output including CI JSON.
- Important public modules: `format_mapping`, `report`, `runner`.
- Internal dependencies: `clinker-bench-support`, `clinker-exec`, `clinker-plan`, `cxl`.
- Architecturally important external dependencies: `criterion`, `indexmap`, `serde`, `serde_json`, `chrono`, `tempfile`.
- Known tests/examples/benches: benches `crates/clinker-benchmarks/benches/e2e_matrix.rs` and `e2e_xlarge.rs`; benchmark configs under `benches/pipelines/`; unit tests in `report.rs` and `runner.rs`.
- Confidence: High.
- Evidence: `crates/clinker-benchmarks/src/lib.rs` states it breaks a circular dependency by housing the runner needing both exec and bench support; `crates/clinker-benchmarks/Cargo.toml` features `bench-alloc` and `bench-xlarge`.

### reserve package

- Crate name: `clinker`
- Path: `reserve` (untracked; not part of the tracked repository tree)
- Role: Separate non-workspace library package used as a crates.io name-reservation placeholder.
- Purpose: Reserves the published crate name while implementation continues in the main workspace.
- Internal dependencies: none; nothing in the workspace depends on it.
- Confidence: High that it is intentionally untracked local-only scaffolding — it does not exist in fresh clones, so do not cite its files as repository evidence.

## Examples, Tests, Benches, And CI

- YAML examples live under `examples/pipelines/` and include runnable pipeline configs, data, channels, compositions, baseline test configs, and README files for per-source CK and retract demos.
- Benchmark pipeline configs live under `benches/pipelines/` and are grouped by `combine`, `cxl_ops`, `execution_mode`, `features`, `format`, `realistic`, and `scale`; `future/` configs are explicitly skipped by `clinker-bench-support::discover_pipeline_configs`.
- CI runs `cargo fmt --all --check`, two Clippy passes (`cargo clippy --workspace -- -D warnings` and `cargo clippy --workspace --all-targets -- -D warnings`), `cargo test --workspace`, bench compile/smoke checks, native Windows/macOS tests, selected cross-target checks, and `cargo deny` (`.github/workflows/ci.yml`).
- Release workflow builds CLI tools `clinker` and `cxl-cli` (`.github/workflows/release.yml`).

## Contract Routing

Locked crate-map decisions are centralized in
[the production-contract register](15_PRODUCTION_CONTRACTS.md), not the
open-question ledger. In particular, D-17 makes `clinker-plan` the sole
execution-admission authority and `clinker-schema` advisory; D-20 bounds the
`clinker-format -> cxl` exception; D-21 permits only repaired feature-gated
`clinker-exec -> clinker-bench-support` instrumentation; D-22 forbids parser
bypasses; and D-23 routes unused dependency cleanup. Use
[docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md) only for uncertainty that
remains unresolved or explicitly deferred.
