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

## Running the binaries

The workspace ships three user-facing binaries:

```bash
# Pipeline engine — execute, dry-run, or explain a pipeline
cargo run -p clinker -- run pipeline.yaml
cargo run -p clinker -- run pipeline.yaml --explain          # plan-only, no I/O
cargo run -p clinker -- run pipeline.yaml --dry-run -n 10
cargo run -p clinker -- run pipeline.yaml --channel acme-corp
cargo run -p clinker -- explain pipeline.yaml --field enrich1.fuzzy_threshold
cargo run -p clinker -- explain --code E105                  # diagnostic code lookup
cargo run -p clinker -- metrics collect ...                  # spool sweep utilities

# CXL REPL
cargo run -p cxl-cli

# Kiln IDE (Dioxus 0.7) — needs the dx CLI installed (`cargo install dioxus-cli`)
dx serve --package clinker-kiln                              # web target (Playwright drives this)
dx serve --package clinker-kiln --platform desktop           # default per Dioxus.toml
```

Subcommands: `run`, `metrics`, `explain`. There is no `guess` subcommand yet — source schemas are required at runtime and authored by hand.

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

Bench plumbing: clinker-bench-support (deterministic RecordFactory + payload generators)
                clinker-benchmarks (cross-crate benchmark harness)
```

12 workspace crates total. The bench crates are siblings — not part of the runtime layer.

### Key design decisions

- **CXL is NOT SQL.** It's a custom ETL DSL. Boolean operators are `and`/`or`/`not`, not `&&`/`||`. System namespaces use `$` prefix (`$pipeline.*`, `$window.*`, `$meta.*`).
- **Unified node taxonomy.** Pipelines use a single `nodes:` list. Variants (defined in `crates/clinker-core/src/config/pipeline_node.rs`):
  - `Source` — input reader bound to a `.schema.yaml`
  - `Transform` — record-level CXL projection / filter / lookup (1×1 table)
  - `Aggregate` — grouped or windowed reduction
  - `Route` — predicate-based fan-out
  - `Merge` — streamwise concatenation of inputs
  - `Combine` — N-ary record combining with mixed predicates (equi + range + arbitrary CXL); distinct from Merge and Transform+lookup. Active work on `feat/combine-node` (see `docs/internal/plans/cxl-engine/phase-combine-node/`).
  - `Output` — sink writer
  - `Composition` — call-site node referencing a `.comp.yaml` reusable sub-pipeline; lowered in compile Stage 5 with body nodes stored in `CompileArtifacts.composition_bodies`.

  Every node is wrapped in `Spanned<PipelineNode>` so YAML positions flow into `miette` diagnostics alongside CXL spans.

- **Execution plan as petgraph DAG.** `ExecutionPlanDag` holds topologically-sorted nodes with per-node parallelism strategy and `NodeProperties` (ordering/partitioning provenance).
- **Compile-time CXL typechecking.** `TypedProgram` output from type inference; schema propagation across the DAG at plan time.
- **Memory-aware aggregation.** Hash aggregation with disk spill; streaming aggregation when sort order permits; RSS tracking with soft/hard limits.
- **Pure Rust policy.** `deny.toml` bans cmake; no C build dependencies in clinker crates (Dioxus/GTK transitive deps are exempted via skip).
- **Multi-tenant via channels.** One pipeline + multiple channels override variables/defaults. A `clinker.toml` at the workspace root anchors discovery for `clinker-channel` and `clinker-core`; channel files (`*.channel.yaml`) layer over the base pipeline.

### Locked decisions

Architectural rules are tracked in `docs/internal/plans/cxl-engine/LOCKED-DECISIONS.md` (gitignored). The load-bearing ones to know:

- **LD-001 — YAML via `serde-saphyr`.** All YAML deserialization goes through `serde-saphyr` with a budget. `serde_yaml` (archived 2024) and `serde_yml` are forbidden; `serde_yaml_bw` points at `serde-saphyr` as its successor.
- **LD-011 — Greenfield rip-and-replace.** Clinker has zero users. When two paths exist, pick the one that reaches the correct target architecture, even when it touches more files. Forbidden shapes: `Legacy*` / `Internal*` / `*Block` rename-instead-of-delete, `#[serde(default)]` on mandatory-post-rename fields, `#[ignore]` on tests verifying a cutover, parallel "new + old path" coexistence beyond a single commit, "pragmatic" / "incremental" / "deferred cleanup" / "lower-cascade" / "bottom-up migration" / "prep phase" / "file-by-file" justifications.

Deleted symbols are tracked append-only in `docs/internal/plans/cxl-engine/RIP-LOG.md`. Never resurrect a symbol listed there without amending the log.

### Refactoring policy

Prefer a full rip over a shim. Prefer a bold correct refactor over a minimal patch. Prefer breaking changes over compatibility layers. Delete dead code; deleted stays deleted.

When a change cascades to N files, touch all N. Library constraints (e.g. `serde-saphyr` tagged-enum + flatten span limitation) are documented as LD entries, not called shortcuts.

Apply this policy to every refactor in this repo, not just the one currently in flight.

### Dependency policy

No new crate dependency lands without maintenance verification. The verification skill at `.claude/skills/crate-vetting/SKILL.md` auto-loads on any `Cargo.toml` edit and runs the checklist (release recency, GitHub `archived` flag, open RustSec advisories, blessed-alternatives check). `cargo deny check` at pre-commit enforces `unmaintained` and `yanked` advisories mechanically.

Prefer crates with a release in the last 12 months, a non-archived repo, and zero open RustSec advisories. Workspace-blessed alternatives include `thiserror` / `anyhow` (not `failure` / `error-chain`), `std::sync::LazyLock` (not `lazy_static`), `tokio` (not legacy async runtimes).

Apply the verification workflow to every new crate, not just the first one in a batch.

### Diagnostics

All user-facing errors use `miette` for rich span-annotated diagnostics. CXL compilation errors, YAML parse errors, and runtime failures all carry source spans — `Spanned<PipelineNode>` covers the YAML side, `cxl::Span` covers the expression side, and they compose into a single report.

### Testing patterns

- Integration tests in `crates/clinker-core/tests/` use YAML fixture pipelines from `tests/fixtures/`.
- Snapshot tests use `insta` (in `pre_lift_baselines.rs` and elsewhere).
- `clinker-bench-support` crate provides deterministic `RecordFactory` and payload generators for benchmarks.
- Example pipelines live in `examples/pipelines/` with sample data in `examples/pipelines/data/`.

### Repository layout outside `crates/`

- `examples/pipelines/` — runnable sample pipelines + their input data
- `tests/` — workspace-level integration tests and shared fixtures
- `benches/` — workspace-level benchmark drivers (per-crate benches live under each crate)
- `bench-data/` — generated CSV inputs for benchmarks (with `.meta.json` sidecars describing schema/seed)
- `lancedb/` — scratch/working LanceDB store used by integration tests
- `docs/internal/plans/` — phase plans (e.g. `cxl-engine/phase-combine-node/`); gitignored, never `git add -f`

### Kiln IDE

`clinker-kiln` is the Dioxus 0.7.4 IDE for authoring Clinker YAML pipelines. One codebase, two targets:

- **Desktop** — `wry` webview, runs on Linux/macOS/Windows. Default per `Dioxus.toml`. Uses `dioxus = { features = ["desktop"] }` under `[target.'cfg(not(target_arch = "wasm32"))'.dependencies]`, plus `tokio` for async. Launch: `dx serve --package clinker-kiln --platform desktop`.
- **Web** — `wasm32` target. Launch: `dx serve --package clinker-kiln`. Playwright drives this build only; it cannot drive the desktop wry webview, so all UI integration tests live on the web side.

Dioxus version is pinned to `=0.7.4` to avoid silent breakage. The `dx` CLI is required — install via `cargo install dioxus-cli`.

## Rust edition & toolchain

Edition 2024, Rust 1.91 (pinned in `rust-toolchain.toml`).
