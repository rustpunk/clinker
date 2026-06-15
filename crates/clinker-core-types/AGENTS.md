# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/clinker-core-types`.
Follow the root instructions first.

## Purpose

Leaf vocabulary crate shared by higher Clinker crates. It holds source spans,
structured diagnostics, name-keyed graph utilities, and DLQ category/stage
vocabulary.

## Responsibilities

- Define small shared value types: `FileId` and `Span`.
- Define compile-time diagnostic structures and the diagnostic-code registry.
- Provide name-keyed DAG utilities for early topology validation.
- Define DLQ category names and stage-label helpers shared by executor and CLI-adjacent code.
- Preserve dependency direction so higher crates can depend on this crate without cycles.

## Important public APIs

- `span::{FileId, Span}` and `Span::{SYNTHETIC, point, line_only, synthetic_line_number, end, from_saphyr}`.
- `diagnostic::{Diagnostic, DiagnosticPayload, LabeledSpan, Severity}`.
- `Diagnostic::{error, warning, with_secondary, with_help, with_payload, from_serde_saphyr_error, input_ref_payload}`.
- `graph::NameGraph::{new, add_node, add_edge, index_of, graph, detect_cycle, topo_sort}`.
- `dlq::DlqErrorCategory`, `DlqErrorCategory::as_str`, `stage_aggregate`, `stage_time_window`, and `stage_reshape_mutation_conflict`.

## Internal module map

- `lib.rs`: crate boundary docs, module declarations, and public re-exports.
- `span.rs`: `FileId`, `Span`, synthetic spans, line-only spans, and `serde_saphyr::Span` conversion.
- `diagnostic.rs`: diagnostic structs, payload enum, diagnostic-code registry comments, and parse-error conversion.
- `graph.rs`: `NameGraph` wrapper over `petgraph::DiGraph`, Tarjan SCC cycle detection, and topological sort.
- `dlq.rs`: `DlqErrorCategory`, stable string names, and stage-label helpers.

## Dependency rules

### Allowed dependencies

Current normal dependencies are intentionally small:

- `serde-saphyr` for span/error interop.
- `petgraph` for graph storage and algorithms.
- `miette` is declared in `Cargo.toml`; Hypothesis: it is retained for diagnostic interoperability even though direct source use in this crate is currently weak.

No internal workspace crate dependencies are currently allowed.

### Forbidden or suspicious dependencies

- Do not add dependencies on `clinker-plan`, `clinker-exec`, `clinker-record`, `clinker-format`, `cxl`, `clinker-channel`, `clinker-net`, `clinker-schema`, CLI crates, or benchmark crates.
- Be suspicious of config, schema, executor, format, record, channel, network, storage, async/runtime, filesystem-walking, path-validation, native TLS/OpenSSL, or C-build dependencies.
- Do not move `SourceDb`, `ValidatedPath`, `PipelineError`, `CompiledPlan`, `ExecutionPlanDag`, `Record`, `Schema`, or `FormatReader` concepts into this crate.

## Important invariants

- This crate is leaf vocabulary and deliberately holds no executor, config, or schema types.
- `FileId` is 1-based and `NonZeroU32`-backed.
- `Span` is a 12-byte `Copy` value; source interning and line/column resolution live above this crate.
- `Span::SYNTHETIC` uses `u32::MAX` as the file id and must not be treated as a real file-backed span.
- `NameGraph` is for name-keyed topology validation before CXL parsing/lowering; it is not `PlanNode` or `ExecutionPlanDag`.
- Diagnostic code literals must be registered in `diagnostic.rs`; no orphan diagnostic codes.
- `DiagnosticPayload` variants are append-only once downstream tests or tooling destructure them.
- DLQ categories are enums, not display strings; call sites should construct the correct variant at the failure site.

## Common mistakes for AI agents to avoid

- Adding higher-layer concepts because several higher crates use these types.
- Treating `NameGraph` as the runtime execution DAG.
- Moving source loading or path-security logic into `span.rs`; that belongs in `clinker-plan`.
- Adding diagnostic codes at emission sites without updating the registry comments and tests.
- String-matching DLQ categories instead of using `DlqErrorCategory`.
- Changing `DlqErrorCategory::as_str` values casually; they appear in DLQ output.
- Making `FileId` zero-based or using raw integers where `FileId`/`Span` should carry meaning.
- Assuming synthetic spans can render file/column context without a higher-layer source database.

## Local commands

- **Inferred:** `cargo check -p clinker-core-types --locked --offline`
- **Inferred:** `cargo test -p clinker-core-types --locked --offline`

If public diagnostics or DLQ behavior changes, also run relevant downstream
planner/executor/channel tests that consume the changed API.

## Documentation updates

- `crates/clinker-core-types/src/lib.rs` for crate purpose or boundary changes.
- `crates/clinker-core-types/src/diagnostic.rs` for any diagnostic-code addition, removal, or meaning change.
- `docs/ai/20_CRATE_MAP.md` for dependency, role, or module-map changes.
- `docs/ai/30_DESIGN_RULES.md` for leaf-boundary, diagnostics, or dependency-rule changes.
- `docs/ai/80_OPEN_QUESTIONS.md` for unresolved architecture or dependency questions.
- User explain docs when diagnostic codes become user-facing.

## Unclear / ask human

- Keep this file short and boundary-focused because this crate is small leaf
  vocabulary.
- Direct source use of the declared `miette` dependency is weak in this crate; verify intent before documenting it as essential or removing it.

## Evidence

- `crates/clinker-core-types/Cargo.toml`
- `crates/clinker-core-types/src/lib.rs`
- `crates/clinker-core-types/src/span.rs`
- `crates/clinker-core-types/src/diagnostic.rs`
- `crates/clinker-core-types/src/graph.rs`
- `crates/clinker-core-types/src/dlq.rs`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/30_DESIGN_RULES.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
