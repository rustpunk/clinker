# AGENTS.md

Root `AGENTS.md` still applies. This file adds local guidance for the main `clinker` CLI crate.

## Purpose

`clinker` is the main user-facing CLI binary. It runs pipeline YAML configs, prints explain plans, collects metrics, looks up diagnostic docs, and wires edge concerns into lower crates.

## Responsibilities

- Define the `clinker` command surface: `run`, `metrics collect`, and `explain`.
- Load pipeline YAML through `clinker-plan`, resolve compile anchors, apply output path templates, and compile plans.
- Apply channel overlays and abort before execution on overlay diagnostics with error severity.
- Load workspace `clinker.toml` storage config and validate spill/staging setup before ingest.
- Build executor source inputs from file discovery or REST sources.
- Open output writers, preserve atomic final-output semantics, write DLQ files, and write output sidecars.
- Invoke `PipelineExecutor` with `CompiledPlan`, `SourceReaders`, `WriterRegistry`, and `PipelineRunParams`.
- Render run errors through miette and map `PipelineError` variants to process exit codes.
- Spool execution metrics and collect metrics spool files into NDJSON.
- Support diagnostic-code lookup and composition field provenance through `clinker explain`.

## Important public APIs

This is a binary crate; it has no library API.

User-facing commands:

- `clinker run <CONFIG>`
- `clinker run <CONFIG> --explain [text|json|dot]`
- `clinker metrics collect --spool-dir <DIR> --output-file <FILE>`
- `clinker explain --code <CODE>`
- `clinker explain <CONFIG> --field <node.param>`

Important in-crate symbols:

- CLI types: `Cli`, `Commands`, `RunArgs`, `MetricsCommands`, `CollectArgs`, `ExplainArgs`, `ExplainFormat`
- Runtime helpers: `run`, `run_metrics`, `run_explain`, `render_pipeline_error`
- Storage/explain helpers: `resolve_compile_anchor`, `cap_headroom_explain`, `staging_plan_explain`, `build_storage_summary_json`
- Output helpers: `PendingOutput`, `output_is_fan_out`, `upstream_source_for_output`, `fsync_dir`

## Internal module map

`crates/clinker/src/main.rs` is the only source file.

- CLI declarations and help text.
- `main`: tracing setup, signal handler install, command dispatch, exit-code mapping.
- Error rendering: miette wrapper with YAML `NamedSource`.
- `run`: config load, channel load, compile context, storage config, explain path, source discovery, staging, writer setup, executor invocation, output/DLQ promotion, metrics spool.
- `run_metrics`: metrics spool collection.
- `run_explain`: diagnostic-code lookup and field provenance.
- Unit tests in `main.rs`: CLI parsing, compile-anchor behavior, cap-headroom rendering, staging-plan rendering.
- Integration tests in `crates/clinker/tests`: atomic output, miette rendering, storage CLI behavior, explain provenance/code lookup.

## Dependency rules

### Allowed dependencies

Current dependencies are intentional unless a change is approved: workspace crates `clinker-exec`, `clinker-plan`, `clinker-core-types`, `clinker-net`, `clinker-channel`, `clinker-format`, and `clinker-record`; external crates `clap`, `indexmap`, `serde_json`, `serde-saphyr`, `chrono`, `num_cpus`, `tracing`, `tracing-subscriber`, `uuid` with `v7`, `miette`, and `tempfile`.

### Forbidden or suspicious dependencies

- Do not add async runtime assumptions or async network clients to the CLI run path casually.
- Do not add native TLS/OpenSSL/cmake/C toolchain requirements.
- Do not route production YAML parsing around `clinker_plan::yaml`.
- Do not add benchmark/test helper dependencies to default CLI runtime behavior.
- Do not move planner or executor/operator logic into this binary.
- Treat Kiln/Klinx/Dioxus references as stale or compatibility-specific unless a maintainer confirms otherwise.

## Important invariants

- Exit-code mapping is intentional: success `0`, completed-with-DLQ `2`, interrupted `130`, and distinct nonzero classes for config/compile, data/eval, and infrastructure failures.
- Pipeline diagnostics should render through miette with the config filename/source attached when possible.
- `--explain json` and `--explain dot` must not print human-readable preambles that make output unparsable.
- Storage/spill/staging validation happens before source ingest starts.
- `resolve_compile_anchor` must preserve `workspace_root.join(pipeline_dir) == config parent`.
- File and REST sources normalize into executor `SourceInput`.
- Outputs are promoted atomically on success; failed runs must not leave truncated final outputs.
- Runtime failure preserves partial temp output for inspection.
- DLQ output uses atomic temp-and-rename discipline.
- Staging cleanup depends on run outcome; interrupted or DLQ-producing runs keep staged copies unless policy says otherwise.
- CLI remains synchronous and calls the synchronous executor directly.

## Common mistakes for AI agents to avoid

- Treating `main.rs` as the place to implement planner or executor behavior.
- Changing exit codes, help text, or diagnostic-code ranges without updating docs and tests.
- Adding stdout text before JSON/DOT explain output.
- Bypassing `clinker_plan::yaml` or losing span-aware diagnostics.
- Weakening atomic output or DLQ promotion behavior.
- Moving source staging into lower crates without architectural review.
- Copying stale "inputs/outputs/transformations" wording from older help/docs without checking current unified `nodes:` config.
- Assuming every parsed CLI flag is fully wired; verify use in `run` before documenting behavior.

## Local commands

- Inferred: `cargo check -p clinker --locked --offline`
- Inferred: `cargo test -p clinker --locked --offline`
- Inferred: `cargo clippy -p clinker --all-targets --locked --offline -- -D warnings`
- Inferred: `cargo run --locked --offline -p clinker -- --help`
- Inferred: `cargo run --locked --offline -p clinker -- run examples/pipelines/customer_etl.yaml --explain`
- Inferred, targeted: `cargo test -p clinker --test atomic_output_test --locked --offline`
- Inferred, targeted: `cargo test -p clinker --test miette_rendering --locked --offline`
- Inferred, targeted: `cargo test -p clinker --test storage_config_cli --locked --offline`
- Inferred, targeted: `cargo test -p clinker --test explain_provenance_test --locked --offline`

## Documentation updates

Update these when changing related behavior:

- `docs/user/src/ops/cli-reference.md`
- `docs/user/src/ops/explain.md`
- `docs/user/src/ops/exit-codes.md`
- `docs/user/src/ops/metrics.md`
- `docs/user/src/ops/storage.md`
- `docs/user/src/ops/validation.md`
- `docs/user/src/pipelines/channels.md`
- Relevant `examples/pipelines/*.yaml` or channel examples
- `docs/ai/10_ARCHITECTURE.md`, `docs/ai/20_CRATE_MAP.md`, `docs/ai/30_DESIGN_RULES.md`, `docs/ai/50_TESTING_AND_COMMANDS.md`, `docs/ai/80_OPEN_QUESTIONS.md`

## Evidence

- `crates/clinker/Cargo.toml`
- `crates/clinker/src/main.rs`
- `crates/clinker/tests/atomic_output_test.rs`
- `crates/clinker/tests/miette_rendering.rs`
- `crates/clinker/tests/storage_config_cli.rs`
- `crates/clinker/tests/explain_provenance_test.rs`
- `docs/user/src/ops/cli-reference.md`
- `docs/user/src/ops/explain.md`
- `docs/user/src/ops/exit-codes.md`
- `docs/user/src/ops/metrics.md`
- `docs/user/src/ops/storage.md`
- `docs/ai/10_ARCHITECTURE.md`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/30_DESIGN_RULES.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
