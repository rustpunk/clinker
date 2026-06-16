# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/clinker-plan`.
Follow the root instructions first.

## Purpose

`clinker-plan` is the compile-time planning crate. It loads YAML pipeline and
composition config, preserves YAML spans, validates topology and paths, binds
schemas, typechecks CXL against row types, and produces `CompiledPlan` /
`ExecutionPlanDag` for `clinker-exec`.

## Responsibilities

- Own `PipelineConfig`, `PipelineNode`, node bodies, and source/output/format/storage/composition config.
- Route production YAML parsing through `clinker_plan::yaml`.
- Preserve `Spanned<PipelineNode>`, `CxlSource`, and input-reference spans for diagnostics.
- Validate names, cycles, undeclared inputs, dotted names, log directives, and config paths.
- Bind source schemas, scoped vars, compositions, CXL programs, `$doc` paths, and output rows.
- Lower validated config into `PlanNode`, `PlanEdge`, `ExecutionPlanDag`, `CompileArtifacts`, explain data, and planner-inserted nodes.
- Own shared pipeline error vocabulary; do not own runtime operator execution.

## Important public APIs

- `config::{parse_config, load_config, load_config_with_vars, interpolate_env_vars}`.
- `PipelineConfig::{compile_topology_only, compile, compile_with_diagnostics, source_configs, output_configs}`.
- `config::CompileContext`.
- `yaml::{from_str, to_string, Spanned, CxlSource, MAX_INPUT_BYTES}`.
- `config::PipelineNode` and node bodies such as `SourceBody`, `TransformBody`, `AggregateBody`, `RouteBody`, `MergeBody`, `CombineBody`, `OutputBody`, `ReshapeBody`, `CullBody`, and `EnvelopeBody`.
- `plan::{CompiledPlan, ChannelIdentity, BoundBody, CompositionBodyId}`.
- `plan::execution::{ExecutionPlanDag, PlanNode, PlanEdge, NodeExecutionReqs}`.
- `plan::bind_schema::{CompileArtifacts, MAX_COMPOSITION_DEPTH}`.
- `security::{ValidatedPath, validate_path, validate_all_config_paths, check_overwrite}`.
- `span::SourceDb`.

## Internal module map

- `config/`: YAML config model, env interpolation, topology compile entry, source/output/format/storage/path-template/composition config.
- `config/composition/`: `.comp.yaml` signatures, scanner, provenance, resource declarations, and raw parsing tests.
- `plan/`: schema binding, CXL artifacts, DAG lowering, combine strategy selection, composition bodies, deferred regions, explain/provenance, statistics, and streaming eligibility.
- `plan/execution/`: `PlanNode`, `PlanEdge`, `ExecutionPlanDag`, scheduling, enforcers, explain renderers, graph utilities, and streaming classification.
- `schema/`: schema loading and resolution.
- `security.rs`: `ValidatedPath` proof token and config path validation.
- `yaml.rs`: serde-saphyr chokepoint, parser budget, span re-exports, and `CxlSource`.
- `span.rs`: source database for diagnostics and validated source loading.
- `error.rs` / `runtime_error.rs`: shared pipeline and runtime error enums.

## Dependency rules

### Allowed dependencies

Existing normal dependencies are intentional evidence: `clinker-core-types`,
`clinker-record`, `cxl`, `clinker-format`, `serde`, `serde_json`,
`serde-saphyr`, `toml`, `indexmap`, `chrono`, `regex`, `blake3`, `miette`,
`petgraph`, `tracing`, `walkdir`, `fastrand`, `tempfile`, `postcard`,
`lz4_flex`, `glob`, plus target-gated `nix` and `windows-sys` for filesystem
type/device detection.

### Forbidden or suspicious dependencies

- Do not add `clinker-exec`, `clinker-net`, `clinker-channel`, `clinker`, benchmark crates, or CLI/runtime dependencies without architecture review.
- Be suspicious of async runtime/client dependencies, network clients, `rayon`/`crossbeam-channel`, native TLS/OpenSSL, `cmake`, or C build dependencies here.
- Do not add dependencies, native tool requirements, or cargo-deny exceptions without approval.

## Important invariants

- `clinker-plan` stays below execution and must not depend on runtime operators.
- Production YAML parsing goes through `clinker_plan::yaml::from_str`.
- Top-level pipeline shape is unified `nodes: Vec<Spanned<PipelineNode>>`; legacy top-level `inputs` / `outputs` / `transformations` should stay rejected.
- Preserve custom `PipelineNode` deserialization; avoid JSON intermediates that lose spans or `deny_unknown_fields` behavior.
- Keep `#[serde(deny_unknown_fields)]` where established.
- Use `ValidatedPath` for trusted paths; do not expose raw path bypasses.
- `CompileContext::default()` is test convenience. Production callers should pass explicit workspace root / pipeline directory.
- Retired `cxl_compile` module name and `E100` diagnostic code must stay absent.
- Schema/CXL binding should remain ahead of DAG execution so runtime code consumes compiled artifacts rather than re-typechecking user programs.
- Composition bodies should stay isolated from enclosing scope except through declared ports, config, resources, and scoped vars.

## Common mistakes for AI agents to avoid

- Moving validation or YAML parsing into `clinker-exec`.
- Adding planner outputs or downstream APIs that normalize raw YAML/config around compiled-plan boundaries.
- Replacing span-aware serde-saphyr paths with `serde_yaml`, `serde_yml`, or JSON-mediated parsing.
- Adding permissive `serde(default)` or compatibility shims for retired config shapes without explicit approval.
- Treating REST/network transport behavior or runtime memory arbitration as planner-owned.
- Making composition resources stringly typed or claiming non-file resource support beyond implementation evidence.
- Copying stale `clinker-core` wording from older docs without checking current source.

## Local commands

- **Verified:** `git diff --check`
- **Inferred:** `cargo check -p clinker-plan --locked --offline`
- **Inferred:** `cargo test -p clinker-plan --locked --offline`
- **Inferred:** `cargo test -p clinker-plan plan:: --locked --offline`
- **Verified:** `cargo fmt --all --check`

For Rust source changes, also run the workspace gates from the root
`AGENTS.md` unless the user explicitly scopes validation narrower.

## Documentation updates

Update relevant docs/examples when this crate changes user-visible config,
diagnostics, planning behavior, explain output, or architecture:

- `docs/user/src/pipelines/structure.md`
- `docs/user/src/nodes/` docs for touched node types
- `docs/user/src/pipelines/compositions.md`
- `docs/user/src/pipelines/channels.md`
- `docs/user/src/pipelines/variables.md`
- `docs/user/src/pipelines/correlation-keys.md`
- `docs/user/src/formats/source-network.md` and relevant format docs when source/format config changes
- `docs/engine/src/architecture.md`
- `docs/engine/src/execution-model.md`
- `docs/engine/src/memory-arbitration.md` when plan-time memory/explain behavior changes
- `docs/ai/*.md`, especially architecture, crate map, design rules, common patterns, testing, and open questions
- Relevant `examples/pipelines/*.yaml`, `examples/pipelines/compositions/*.comp.yaml`, and `benches/pipelines/**/*.yaml`

## Evidence

- `crates/clinker-plan/Cargo.toml`
- `crates/clinker-plan/src/lib.rs`
- `crates/clinker-plan/src/yaml.rs`
- `crates/clinker-plan/src/security.rs`
- `crates/clinker-plan/src/config/compile_context.rs`
- `crates/clinker-plan/src/config/pipeline.rs`
- `crates/clinker-plan/src/config/pipeline_node.rs`
- `crates/clinker-plan/src/plan/compiled.rs`
- `crates/clinker-plan/src/plan/bind_schema.rs`
- `crates/clinker-plan/src/plan/execution/mod.rs`
- `crates/clinker-plan/src/plan/tests/`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
