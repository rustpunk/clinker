# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Pre-commit checks

Before any git commit, run the same checks as GitHub CI (`.github/workflows/ci.yml`):

1. `cargo fmt --all` (CI runs `--check`; locally fix first)
2. `cargo clippy --workspace -- -D warnings`
3. `cargo test --workspace`
4. `cargo deny check`

Fix any issues before committing. All four must pass — these are the exact checks CI enforces on every PR.

## Build & test commands

```bash
cargo build --workspace          # Build all crates
cargo test --workspace           # Run all tests (~1100 tests)
cargo test -p cxl                # Test a single crate
cargo test -p clinker-core -- node_taxonomy  # Run tests matching a pattern
cargo bench -p clinker-core      # Run benchmarks (arena, parallel, pipeline, sort, window)
cargo bench -p cxl               # Run CXL benchmarks (eval, parse)
cargo deny check                 # License/ban/advisory audit (pure Rust policy)
```

## Architecture

Clinker is a **streaming ETL engine** with a custom expression language (CXL), YAML pipeline orchestration, and a desktop IDE (Kiln).

### Crate dependency layers (bottom → top)

```
Applications:   clinker (CLI)  |  cxl-cli (REPL)  |  clinker-kiln (IDE)
                     ↓                ↓                    ↓
Orchestration:  clinker-core (DAG planner + executor)
                clinker-channel (workspace/channel mgmt)
                clinker-schema (source .schema.yaml validation)
                clinker-git (VCS abstraction)
                     ↓
Language/IO:    cxl (lexer → parser → typecheck → eval)
                clinker-format (CSV/JSON/XML/fixed-width readers/writers)
                     ↓
Foundation:     clinker-record (Value, Record, Schema, coercion)
```

### Key design decisions

- **CXL is NOT SQL.** It's a custom ETL DSL. Boolean operators are `and`/`or`/`not`, not `&&`/`||`. System namespaces use `$` prefix (`$pipeline.*`, `$window.*`, `$meta.*`).
- **Unified node taxonomy.** Pipelines use a single `nodes:` list with variants: Source, Transform, Aggregate, Route, Merge, Output, Composition. Each wrapped in `Spanned<PipelineNode>` for source location tracking.
- **Execution plan as petgraph DAG.** `ExecutionPlanDag` holds topologically-sorted nodes with per-node parallelism strategy and `NodeProperties` (ordering/partitioning provenance).
- **Compile-time CXL typechecking.** `TypedProgram` output from type inference; schema propagation across the DAG at plan time.
- **Memory-aware aggregation.** Hash aggregation with disk spill; streaming aggregation when sort order permits; RSS tracking with soft/hard limits.
- **Pure Rust policy.** `deny.toml` bans cmake; no C build dependencies in clinker crates (Dioxus/GTK transitive deps are exempted via skip).
- **Multi-tenant via channels.** One pipeline + multiple channels override variables/defaults; workspace discovery via `clinker.toml`.

### Diagnostics

All user-facing errors use `miette` for rich span-annotated diagnostics. CXL compilation errors, YAML parse errors, and runtime failures all carry source spans.

### Testing patterns

- Integration tests in `crates/clinker-core/tests/` use YAML fixture pipelines from `tests/fixtures/`.
- Snapshot tests use `insta` (in `pre_lift_baselines.rs`).
- `clinker-bench-support` crate provides deterministic `RecordFactory` and payload generators for benchmarks.
- Example pipelines live in `examples/pipelines/` with sample data in `examples/pipelines/data/`.

### Kiln IDE

Desktop app via Dioxus 0.7. Builds for both desktop (wry) and web targets from the same codebase with feature gates. Playwright tests target the web build only (cannot drive wry).

## Rust edition & toolchain

Edition 2024, Rust 1.91 (pinned in `rust-toolchain.toml`).
