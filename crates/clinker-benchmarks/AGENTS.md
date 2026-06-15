# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/clinker-benchmarks`.
Follow the root instructions first.

## Purpose

`clinker-benchmarks` is the end-to-end benchmark harness and reporting crate
for Clinker pipeline YAML configs. It intentionally lives outside
`clinker-exec` so benchmark runner code can depend on both `clinker-exec` and
`clinker-bench-support` without creating a cycle.

## Responsibilities

- Run discovered `benches/pipelines/**/*.yaml` benchmark configs through `PipelineExecutor`.
- Generate or reuse cached benchmark input data via `clinker-bench-support`.
- Map planner `InputFormat` values to benchmark `DataFormat` values.
- Produce Criterion benchmark groups for Small, Medium, Large, and feature-gated XLarge runs.
- Produce optional human summary tables and CI JSON under `target/bench-results/summary.json`.
- Keep benchmark-only path rewrites, cache handling, and report formatting out of runtime crates.

## Important public APIs

- `runner::BenchPipelineRunner::{new, run}`.
- `format_mapping::{input_format_to_data_format, FormatMappingError}`.
- `report::{format_summary_table, print_summary_table}`.
- `report::{BenchReport, BenchResult, StageResult}`.
- `report::{bench_result_from, write_ci_json, git_short_sha}`.

## Internal module map

- `src/lib.rs`: crate docs and public module exports.
- `src/runner.rs`: loads benchmark YAML, prepares generated source readers, compiles plans, runs executor, and rewrites split-output temp paths.
- `src/format_mapping.rs`: converts source `InputFormat` to generated-data `DataFormat` and rejects unsupported benchmark-generation formats.
- `src/report.rs`: summary table formatting, CI JSON structs, and conversion from `ExecutionReport`.
- `benches/e2e_matrix.rs`: Criterion matrix over discovered configs at Small, Medium, and Large scales, with a Small pre-flight run.
- `benches/e2e_xlarge.rs`: feature-gated XLarge Criterion benchmark target.

## Dependency rules

### Allowed dependencies

Existing dependencies are intentional evidence:

- Workspace/internal: `clinker-exec`, `clinker-plan`, `clinker-bench-support`, `cxl`.
- Benchmark/reporting/data support: `criterion`, `indexmap`, `serde`, `serde_json`, `chrono`, `tempfile`.
- Features: `bench-alloc` forwards to `clinker-bench-support/bench-alloc`; `bench-xlarge` gates the XLarge bench target.

### Forbidden or suspicious dependencies

- Do not move benchmark runner code into `clinker-exec`; `src/lib.rs` says this crate exists to avoid that circular dependency.
- Do not add benchmark helpers to default runtime paths.
- Do not add network, async runtime, native TLS/OpenSSL, C toolchain, or cargo-deny exceptions without approval.
- Be suspicious of broad dependencies not directly tied to Criterion, benchmark data setup, executor invocation, or reporting.

## Important invariants

- Benchmark runner code stays outside `clinker-exec`.
- XLarge setup stays behind `bench-xlarge` and the separate `e2e_xlarge` target.
- `benches/pipelines/future/` configs are not active benchmark coverage; discovery excludes them through `clinker-bench-support`.
- `e2e_matrix` pre-flights each config at `Scale::Small` before timed loops.
- Benchmark source data is derived from each source node schema and cached through `BenchDataCache`.
- Unsupported generated-data mappings are explicit errors for JSON object, EDIFACT, X12, HL7, and SWIFT.
- Split-output benchmarks need real temp filesystem paths; the runner rewrites placeholder paths under `target/bench-split-tmp`.
- `CLINKER_BENCH_SUMMARY=1` triggers summary output and CI JSON; `CLINKER_BENCH_VERBOSE=1` expands report columns.

## Common mistakes for AI agents to avoid

- Treating `cargo test --benches -p clinker-benchmarks` as a quick compile-only check.
- Running real `cargo bench` casually; Criterion runs are expensive and performance-sensitive.
- Adding benchmark configs under `future/` and assuming they run.
- Adding a new source format without updating `format_mapping.rs` and generated data support.
- Removing the pre-flight run in `e2e_matrix`.
- Treating the runner's in-memory config mutations as product behavior.
- Forgetting `bench-xlarge` when working on `e2e_xlarge`.
- Depending on benchmark allocator metrics as production throughput data.

## Local commands

- **Inferred:** `cargo test -p clinker-benchmarks --locked --offline`
- **Verified:** `cargo check --features bench-alloc -p clinker-benchmarks --locked --offline`
- **Verified:** `cargo test --benches -p clinker-benchmarks --locked --offline`
- **Verified:** `cargo check --benches --workspace --locked --offline`
- **Inferred:** `cargo bench -p clinker-benchmarks --bench e2e_matrix`
- **Inferred:** `cargo bench -p clinker-benchmarks --features bench-xlarge --bench e2e_xlarge`
- **Verified:** `git diff --check`

Real Criterion runs are expensive. Prefer check/test gates before running `cargo bench`.

## Documentation updates

Update relevant docs when benchmark commands, benchmark coverage, report output, feature gates, or crate boundaries change:

- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/40_COMMON_PATTERNS.md`
- `docs/ai/50_TESTING_AND_COMMANDS.md`
- `docs/ai/60_PERFORMANCE_NOTES.md`
- `docs/ai/80_OPEN_QUESTIONS.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
- Relevant YAML under `benches/pipelines/`

## Unclear / ask human

Track unresolved questions only in `docs/ai/80_OPEN_QUESTIONS.md`. Relevant entries include unsupported generated benchmark formats and benchmark report compatibility.

## Evidence

- `crates/clinker-benchmarks/Cargo.toml`
- `crates/clinker-benchmarks/src/lib.rs`
- `crates/clinker-benchmarks/src/runner.rs`
- `crates/clinker-benchmarks/src/format_mapping.rs`
- `crates/clinker-benchmarks/src/report.rs`
- `crates/clinker-benchmarks/benches/e2e_matrix.rs`
- `crates/clinker-benchmarks/benches/e2e_xlarge.rs`
- `crates/clinker-bench-support/src/lib.rs`
- `crates/clinker-bench-support/src/cache.rs`
- `benches/pipelines/`
- `docs/ai/50_TESTING_AND_COMMANDS.md`
- `docs/ai/80_OPEN_QUESTIONS.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
