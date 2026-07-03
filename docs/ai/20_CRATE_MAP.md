# AI Onboarding: Crate Map

Purpose: Give future AI agents a factual map of the current Cargo workspace, with dependency direction, crate roles, and evidence anchors for safe code changes.

## Workspace Overview

The root workspace has 14 members: `clinker-record`, `cxl`, `cxl-cli`, `clinker-format`, `clinker-core-types`, `clinker-plan`, `clinker-exec`, `clinker-net`, `clinker-channel`, `clinker`, `clinker-schema`, `clinker-lineage`, `clinker-bench-support`, and `clinker-benchmarks` (`Cargo.toml`). `reserve/` is a separate non-member Cargo package named `clinker` with its own `[workspace]` table; `reserve/src/lib.rs` identifies it as a crates.io name-reservation placeholder, not runtime code.

No Cargo `examples` targets were found. The repository does contain YAML pipeline examples and fixtures under `examples/pipelines/` plus benchmark pipeline configs under `benches/pipelines/`.

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

clinker-bench-support -> clinker-record
clinker-benchmarks -> clinker-bench-support + clinker-exec + clinker-plan + cxl
```

Important normal dependency edges from `cargo metadata --no-deps`: `cxl -> clinker-record`; `clinker-format -> clinker-record, cxl`; `clinker-plan -> clinker-core-types, clinker-format, clinker-record, cxl`; `clinker-exec -> clinker-core-types, clinker-format, clinker-plan, clinker-record, cxl`; `clinker-channel -> clinker-core-types, clinker-plan, clinker-record`; `clinker-net -> clinker-exec, clinker-format, clinker-plan, clinker-record`; `clinker -> clinker-channel, clinker-core-types, clinker-exec, clinker-format, clinker-lineage, clinker-net, clinker-plan, clinker-record`; `cxl-cli -> cxl, clinker-record`; `clinker-schema -> clinker-plan`; `clinker-lineage -> clinker-plan, clinker-record, cxl`.

## Current Layering Rules Inferred From Source

- `clinker-core-types` appears intended as a leaf crate: its crate docs say it holds spans, diagnostics, graph, and DLQ vocabulary and "deliberately holds no executor, config, or schema types" (`crates/clinker-core-types/src/lib.rs`).
- `clinker-record` is the shared data model leaf for row values, schemas, storage traits, grouping keys, document context, and accumulators (`crates/clinker-record/src/lib.rs`).
- `cxl` sits above records but below planning/execution: it parses, resolves, type-checks, plans aggregates, and evaluates expressions against `clinker-record` values (`crates/cxl/src/lib.rs`).
- `clinker-format` owns streaming readers/writers and document/envelope framing. It depends on `cxl`, so it is not a pure serialization leaf (`crates/clinker-format/Cargo.toml`; `crates/clinker-format/src/lib.rs`).
- `clinker-plan` is compile-time orchestration and DAG construction below the runtime executor; its crate docs explicitly say execution consumes its plan (`crates/clinker-plan/src/lib.rs`).
- `clinker-exec` is runtime orchestration and operators; binaries and network transports depend on it rather than the reverse (`crates/clinker-exec/src/lib.rs`; `crates/clinker-exec/src/executor/mod.rs`).
- `clinker-channel`, `clinker-net`, `clinker-schema`, `clinker`, and `cxl-cli` appear to be edge/application or integration crates around the plan/exec/language core.
- `clinker-lineage` is a plan-time, read-only consumer of `clinker-plan`: it maps Source/Output nodes to OpenLineage dataset identities and walks the compiled DAG to emit DIRECT (per-column) and INDIRECT (whole-dataset influence) column-level lineage facets (OpenLineage `2-0-2` / `ColumnLineageDatasetFacet` `1-2-0`). It reads typed/compiled programs off plan nodes via `cxl`; it does not run pipelines (`crates/clinker-lineage/src/lib.rs`). The `clinker` CLI consumes it via `run --lineage <path>`, which compiles the plan and writes a static START/COMPLETE OpenLineage NDJSON pair without reading data (mirroring `--explain`).
- Benchmark crates appear intended to stay outside the runtime layer. `clinker-benchmarks/src/lib.rs` says it houses a runner needing both `clinker-exec` and `clinker-bench-support` to avoid a circular dependency.

## Cycles And Suspicious Coupling

- No normal workspace dependency cycle was found in `cargo metadata --no-deps` or `cargo tree --workspace --depth 1`.
- `clinker-exec` has an optional normal dependency on `clinker-bench-support` for `bench-alloc` (`crates/clinker-exec/Cargo.toml`). That may be intentional for allocation measurement, but future agents should avoid letting benchmark helpers leak into default runtime code.
- `clinker-net` depends on `clinker-exec` to implement `RecordSource` (`crates/clinker-net/src/lib.rs`; `clinker_exec::source::RecordSource`). This couples network source readers to executor source traits; it appears deliberate but means network transport is not a low-level IO crate.

## Crates

### clinker-record

- Crate name: `clinker-record`
- Path: `crates/clinker-record`
- Role: Library crate plus `record_ops` Criterion bench.
- Purpose: Defines Clinker's core in-memory data model: `Value`, `Record`, `Schema`, field strings, coercion, grouping keys, provenance, document context, storage traits, pipeline counters, and aggregate accumulator state.
- Important public modules: `accumulator`, `coercion`, `counters`, `document_context`, `field_str`, `group_key`, `minimal`, `provenance`, `record`, `record_view`, `resolver`, `schema`, `schema_def`, `storage`, `value`.
- Internal dependencies: none for normal build; dev-depends on `clinker-bench-support`.
- Architecturally important external dependencies: `serde`, `serde_json`, `chrono`, `ahash`, `indexmap`, `smol_str`; `postcard` and `criterion` for dev/bench.
- Known tests/examples/benches: unit tests across module files; `crates/clinker-record/src/accumulator/tests.rs`; `crates/clinker-record/benches/record_ops.rs`. Many downstream `clinker-exec` tests exercise records indirectly.
- Confidence: High.
- Evidence: `crates/clinker-record/Cargo.toml`; `crates/clinker-record/src/lib.rs` re-exports `Value`, `Record`, `Schema`, `RecordStorage`, `PipelineCounters`, `RetractionCounters`, and accumulator APIs.

### clinker-core-types

- Crate name: `clinker-core-types`
- Path: `crates/clinker-core-types`
- Role: Library crate.
- Purpose: Provides leaf vocabulary shared by planning, execution, diagnostics, and channels: source spans, structured diagnostics, name-keyed graph utilities, and DLQ categories/stage helpers.
- Important public modules: `diagnostic`, `dlq`, `graph`, `span`.
- Internal dependencies: none.
- Architecturally important external dependencies: `miette`, `petgraph`, `serde-saphyr`.
- Known tests/examples/benches: unit tests in `diagnostic.rs`, `dlq.rs`, `graph.rs`, and `span.rs`; no integration tests or benches listed by Cargo metadata.
- Confidence: High.
- Evidence: `crates/clinker-core-types/src/lib.rs` explicitly describes the crate as leaf vocabulary and re-exports `Diagnostic`, `NameGraph`, `Span`, and `DlqErrorCategory`; `crates/clinker-core-types/Cargo.toml`.

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
- Important public modules: `bom`, `counting`, `csv`, `doc_index`, `edifact`, `envelope`, `envelope_writer`, `error`, `fixed_width`, `hl7`, `json`, `multi_record`, `source`, `splitting`, `swift`, `traits`, `x12`, `xml`. `segment_tokenizer` is crate-private.
- Internal dependencies: `clinker-record`, `cxl`; dev-depends on `clinker-bench-support`.
- Architecturally important external dependencies: `csv`, `quick-xml`, `serde`, `serde_json`, `miette`, `tracing`, `indexmap`, `chrono`.
- Known tests/examples/benches: `crates/clinker-format/tests/streaming_doc_index_json.rs`; `crates/clinker-format/tests/streaming_doc_index_xml.rs`; `crates/clinker-format/src/splitting/tests.rs`; `crates/clinker-format/benches/io_throughput.rs`.
- Confidence: High.
- Evidence: `crates/clinker-format/Cargo.toml`; `crates/clinker-format/src/lib.rs` re-exports `FormatReader`, `FormatWriter`, `FormatError`, `DocArenaIndex`, `EnvelopeFramer`, and EDI/HL7 defaults.

### clinker-plan

- Crate name: `clinker-plan`
- Path: `crates/clinker-plan`
- Role: Library crate with in-crate plan/config tests.
- Purpose: Parses YAML pipeline/composition configuration, resolve schemas and source discovery, validate configs, compile CXL against row types, and produce typed execution DAGs consumed by `clinker-exec`.
- Important public modules: `config`, `error`, `plan`, `runtime_error`, `schema`, `security`, `span`, `validation`, `yaml`. `config` exposes source/output/format/route/aggregate/storage/composition modules; `plan` exposes `compiled`, `execution`, `properties`, `statistics`, `streaming_eligibility`, `deferred_region`, and `envelope_synthesis`.
- Internal dependencies: `clinker-core-types`, `clinker-format`, `clinker-record`, `cxl`.
- Architecturally important external dependencies: `serde`, `serde_json`, `serde-saphyr`, `toml`, `indexmap`, `miette`, `petgraph`, `regex`, `tracing`, `walkdir`, `glob`, `blake3`, `postcard`, `lz4_flex`, `tempfile`, platform `nix`/`windows-sys`.
- Known tests/examples/benches: in-crate rename gates in `src/lib.rs`; plan tests under `crates/clinker-plan/src/plan/tests/` for DAGs, CK lattice/aligned partitions, cull validation, deferred regions, route ports, watermark validation, doc paths, envelope synthesis, and explain output; config composition tests in `crates/clinker-plan/src/config/composition/tests.rs`.
- Confidence: High.
- Evidence: `crates/clinker-plan/src/lib.rs` states it sits below execution and produces `plan::execution::ExecutionPlanDag`; `crates/clinker-plan/src/config/mod.rs`; `crates/clinker-plan/src/plan/mod.rs`; `crates/clinker-plan/src/plan/execution/mod.rs` symbols `PlanNode`, `PlanEdge`, `ExecutionPlanDag`, and `PlanError`.

### clinker-exec

- Crate name: `clinker-exec`
- Path: `crates/clinker-exec`
- Role: Library crate with the largest integration-test and benchmark surface.
- Purpose: Executes compiled pipeline DAGs: source ingestion, dispatch for node kinds, transforms, aggregations, combines/joins, route/merge/reshape/cull/output dispatch, DLQ, metrics, memory arbitration, spill handling, record sources, progress, and runtime modules.
- Important public modules: `aggregation`, `dlq`, `executor`, `exit_codes`, `log_dispatch`, `log_rules`, `log_template`, `metrics`, `modules`, `output`, `partial`, `pipeline`, `progress`, `projection`, `sketch`, `source`. The `executor` module exposes `PipelineExecutor`, `PipelineRunParams`, `ExecutionReport`, `WriterRegistry`, `RecordSource`, `SourceInput`, and validation types. The `pipeline` module exposes sort, combine, grace hash, IEJoin, memory, spill, streaming merge, and window context helpers.
- Internal dependencies: `clinker-core-types`, `clinker-format`, `clinker-plan`, `clinker-record`, `cxl`; optional normal dependency on `clinker-bench-support`; dev-depends on `clinker-bench-support` and `clinker-channel`.
- Architecturally important external dependencies: `crossbeam-channel`, `rayon`, `arc-swap`, `hashbrown`, `lz4_flex`, `postcard`, `fs4`, `petgraph`, `miette`, `tracing`, `serde-saphyr`, `serde_json`, `csv`, `glob`, `uuid`, `ctrlc` on native targets, `windows-sys` on Windows, `criterion`, `insta`, `proptest`, `serial_test`.
- Known tests/examples/benches: many integration tests in `crates/clinker-exec/tests/`, covering aggregates, combine, composition, correlation/retraction, output, formats, storage, memory, streaming, docs, and fixtures under `crates/clinker-exec/tests/fixtures/`; white-box tests under `crates/clinker-exec/src/executor/tests/`; benches under `crates/clinker-exec/benches/` including `sort`, `arena`, `window`, `pipeline`, `parallel`, `provenance`, `composition`, `combine`, `combine_iejoin`, `combine_nary_3input`, `combine_grace_hash`, `deferred_buffer_pruning`, `arbitration_poll`, and `spill_compression`.
- Confidence: High.
- Evidence: `crates/clinker-exec/Cargo.toml`; `crates/clinker-exec/src/lib.rs`; `crates/clinker-exec/src/executor/mod.rs` symbols `PipelineExecutor`, `SourceReaders`, `single_file_reader`, `PipelineRunParams`; `crates/clinker-exec/src/source/mod.rs` symbols `RecordSource` and `SourceInput`.

### clinker-channel

- Crate name: `clinker-channel`
- Path: `crates/clinker-channel`
- Role: Library crate plus integration tests and `channel_merge` bench.
- Purpose: Manages channel files for multi-tenant pipeline/composition launches: binding channel targets, validating config override paths, applying overlays, and staging source copies with reuse/crash-safety logic.
- Important public modules: `binding`, `error`, `overlay`, `staging_copy`.
- Internal dependencies: `clinker-core-types`, `clinker-plan`, `clinker-record`.
- Architecturally important external dependencies: `serde-saphyr`, `serde`, `serde_json`, `blake3`, `indexmap`, `tracing`, `thiserror`, `walkdir`, `uuid`, `tempfile`, `fs4`, Unix `nix`.
- Known tests/examples/benches: `crates/clinker-channel/tests/overlay_resolution_test.rs`, `discovery_test.rs`, `channel_manifest_test.rs`, `group_parse_test.rs`, `source_patch_parse_test.rs`, `staging_reuse_concurrent.rs`; `crates/clinker-channel/benches/channel_merge.rs`; the multitenant overlay workspace under `examples/multitenant/`.
- Confidence: High.
- Evidence: `crates/clinker-channel/src/lib.rs` channel/group overlay guide; re-exports `resolve`, `OverlayResolution`, `resolve_channel_overlay`, `scan_channels`, `scan_groups`, `DottedPath`, `ChannelManifest`, `OverlayFile`, `Group`, and `SourceStager`.

### clinker-net

- Crate name: `clinker-net`
- Path: `crates/clinker-net`
- Role: Library crate plus REST integration tests.
- Purpose: Provides finite-pull network source readers, currently REST, that adapt paginated network data into executor `RecordSource` inputs.
- Important public modules: no public submodules; `rest` is private. Public API is `build_rest_source`.
- Internal dependencies: `clinker-exec`, `clinker-format`, `clinker-plan`, `clinker-record`; dev-depends on `clinker-bench-support` and `clinker-exec` with `test-utils`.
- Architecturally important external dependencies: `ureq` with rustls, `serde_json`, `indexmap`, `tracing`.
- Known tests/examples/benches: `crates/clinker-net/tests/rest_executor_e2e.rs`; `crates/clinker-net/tests/rest_pagination.rs`; executor transport coverage also appears in `crates/clinker-exec/tests/transport_validation.rs` and `record_source_transport.rs`.
- Confidence: High.
- Evidence: `crates/clinker-net/src/lib.rs` documents the finite-pull model and returns `Box<dyn clinker_exec::source::RecordSource>` from `build_rest_source`.

### clinker

- Crate name: `clinker`
- Path: `crates/clinker`
- Role: Binary crate for the main CLI.
- Purpose: Provides the user-facing ETL CLI that runs pipelines, performs dry-run/explain flows, applies channels, resolves memory/threads/output behavior, collects metrics, and explains diagnostic codes.
- Important public modules: none; all code is in `src/main.rs`. Main symbols include `Cli`, `Commands`, `RunArgs`, `MetricsCommands`, `CollectArgs`, and `ExplainArgs`.
- Internal dependencies: `clinker-channel`, `clinker-core-types`, `clinker-exec`, `clinker-format`, `clinker-net`, `clinker-plan`, `clinker-record`.
- Architecturally important external dependencies: `clap`, `miette`, `tracing`, `tracing-subscriber`, `serde-saphyr`, `serde_json`, `indexmap`, `chrono`, `num_cpus`, `uuid`, `tempfile`.
- Known tests/examples/benches: integration tests `crates/clinker/tests/atomic_output_test.rs`, `explain_provenance_test.rs`, `miette_rendering.rs`, `storage_config_cli.rs`; unit tests in `src/main.rs`; YAML examples under `examples/pipelines/` are primarily CLI-facing runnable examples.
- Confidence: High.
- Evidence: `crates/clinker/Cargo.toml`; `crates/clinker/src/main.rs` Clap command help for `run`, `metrics`, and `explain`; uses `clinker_exec::executor::PipelineExecutor`.

### clinker-schema

- Crate name: `clinker-schema`
- Path: `crates/clinker-schema`
- Role: Library crate.
- Purpose: Parses `.schema.yaml` files, discover schemas and pipelines in a workspace, build schema indexes, and validate pipeline source/schema references.
- Important public modules: `discovery`, `model`, `parse`, `validate`.
- Internal dependencies: `clinker-plan`.
- Architecturally important external dependencies: `serde`, `serde_json`, `serde-saphyr`, `ahash`; `tempfile` for dev tests.
- Known tests/examples/benches: unit tests in `parse.rs`, `discovery.rs`, and `validate.rs`; schema examples/fixtures under `examples/pipelines/retract-demo/*.schema.yaml`.
- Confidence: Medium. The crate is an edge/authoring support crate, and its long-term boundary with `clinker-plan` is tracked as an open question.
- Evidence: `crates/clinker-schema/src/lib.rs`; public symbols `build_workspace_schema_index`, `parse_schema`, `parse_schema_file`, `validate_pipeline`, `SourceSchema`, and `SchemaIndex`.

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
- Path: `reserve`
- Role: Separate non-workspace library package.
- Purpose: Reserves the published crate name while implementation continues in the main workspace.
- Important public modules: none.
- Internal dependencies: none.
- Architecturally important external dependencies: none.
- Known tests/examples/benches: none found.
- Confidence: High.
- Evidence: `reserve/Cargo.toml` has package metadata, `[lib] path = "src/lib.rs"`, and its own `[workspace]`; `reserve/src/lib.rs` says "Name reservation for the `clinker` crate" and "placeholder release."

## Examples, Tests, Benches, And CI

- YAML examples live under `examples/pipelines/` and include runnable pipeline configs, data, channels, compositions, baseline test configs, and README files for per-source CK and retract demos.
- Benchmark pipeline configs live under `benches/pipelines/` and are grouped by `combine`, `cxl_ops`, `execution_mode`, `features`, `format`, `realistic`, and `scale`; `future/` configs are explicitly skipped by `clinker-bench-support::discover_pipeline_configs`.
- CI runs `cargo fmt --all --check`, two Clippy passes (`cargo clippy --workspace -- -D warnings` and `cargo clippy --workspace --all-targets -- -D warnings`), `cargo test --workspace`, bench compile/smoke checks, native Windows/macOS tests, selected cross-target checks, and `cargo deny` (`.github/workflows/ci.yml`).
- Release workflow builds CLI tools `clinker` and `cxl-cli` (`.github/workflows/release.yml`).

## Unresolved Items

Open questions are centralized in
[docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md). Crate-map-specific
items currently tracked there include `clinker-schema` versus `clinker-plan`
ownership, the `clinker-format -> cxl` edge, the optional
`clinker-exec -> clinker-bench-support` `bench-alloc` edge, and stale
`clinker-core` references in older docs.
