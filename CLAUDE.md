# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Pre-commit checks

Before any git commit, run the same checks as GitHub CI (`.github/workflows/ci.yml`):

1. `cargo fmt --all` (CI runs `--check`; locally fix first)
2. `cargo clippy --workspace -- -D warnings`
3. `cargo test --workspace`
4. `cargo check --benches --workspace` — `cargo test --workspace` does NOT compile benches; a changed crate API can leave bench call-sites broken and only surface in CI
5. `cargo check --features bench-alloc -p clinker-benchmarks`
6. `cargo test --benches -p clinker-benchmarks`
7. `cargo deny check`

Fix any issues before committing. All seven must pass — these are the exact checks CI enforces on every PR. (CI also runs `cargo check` against `x86_64-pc-windows-msvc` and `aarch64-apple-darwin` for `clinker-core`; cross-compile setup is optional locally.)

## Build & test commands

```bash
cargo build --workspace          # Build all crates
cargo test --workspace           # Run all tests
cargo test -p cxl                # Test a single crate
cargo test -p clinker-core -- node_taxonomy  # Run tests matching a pattern
cargo bench -p clinker-core      # arena, parallel, pipeline, sort, window, combine, combine_grace_hash, combine_iejoin, combine_nary_3input, composition, deferred_buffer_pruning, provenance
cargo bench -p cxl               # CXL benchmarks (eval, parse)
cargo bench -p clinker-benchmarks # Cross-crate harness (e2e_matrix, e2e_xlarge)
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
  - `Combine` — N-ary record combining with mixed predicates (equi + range + arbitrary CXL); distinct from Merge and Transform+lookup.
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
- **LD-011 — Greenfield rip-and-replace.** Clinker has zero users. When two paths exist, pick the one that reaches the correct target architecture, even when it touches more files. Forbidden shapes (measured at sprint close): `Legacy*` / `Internal*` / `*Block` rename-instead-of-delete, `#[serde(default)]` on mandatory-post-rename fields, `#[ignore]` on tests verifying a cutover, parallel "new + old path" coexistence surviving the sprint's closing commit, "pragmatic" / "incremental" / "deferred cleanup" / "lower-cascade" / "bottom-up migration" / "prep phase" / "file-by-file" justifications used to defer work past the sprint.

Deleted symbols are tracked append-only in `docs/internal/plans/cxl-engine/RIP-LOG.md`. Never resurrect a symbol listed there without amending the log.

### Refactoring policy

Prefer a full rip over a shim. Prefer a bold correct refactor over a minimal patch. Prefer breaking changes over compatibility layers. Delete dead code; deleted stays deleted.

When a change cascades to N files, touch all N. Library constraints (e.g. `serde-saphyr` tagged-enum + flatten span limitation) are documented as LD entries, not called shortcuts.

Apply this policy to every refactor in this repo, not just the one currently in flight.

Concrete shortcut signatures — the patterns this policy forbids and the architecturally correct alternative for each — live in `.claude/policies/architectural-rigor.md`. That file is the single source of truth read by the `audit-shortcuts` skill (sprint-close gate) and the `shortcut-auditor` subagent it invokes. There is no per-commit pre-commit hook anymore — the audit runs at sprint close.

**Sprint-boundary principle — atomicity matches architecture per sprint.**
A sprint is a contiguous series of commits implementing a single
architectural unit. Every `pub` or `pub(crate)` item the sprint
introduces must have a non-test intra-crate caller present in the
sprint's closing commit. `#[cfg(test)]` references do not count
because CI runs `cargo clippy --workspace -- -D warnings` without
`--all-targets`, so test-only references are invisible to the dead-code
lint at sprint close.

Intermediate commits within the sprint may legitimately split a type
from its consumer. CI-failing transitional states (compile errors,
dead-code warnings, fmt/clippy noise, transient `#[allow]` /
`#[ignore]`, unused imports, even temporary tombstone comments) are
acceptable as long as the sprint's closing commit eliminates every such
signature. The boundary is user-declared (typically
`<merge-base>..HEAD` for the current branch).

Applies to public enums, structs, traits, trait methods, free functions,
type aliases, and modules. The audit measures the sprint's final state,
not any intermediate snapshot. Run `/audit-shortcuts --range
<base>..HEAD` (or `--working`) to verify a sprint is sealed before
declaring the work done — the audit runs the full CI gauntlet plus the
architectural-rigor signature scan against the working-tree state at
HEAD. Final-commit invariants: no `#[allow(dead_code)]`, no `#[ignore]`,
no tombstone comments, no dead code, no speculative `pub` APIs without
intra-crate callers, no shims, no compat layers, no
`Legacy*`/`Internal*`/`*Block` rename-instead-of-delete patterns. The
architectural-rigor policy (`.claude/policies/architectural-rigor.md`)
enumerates the full signature list and is the single source of truth.

### Comment policy

Comments explain WHY the code is the way it is. A short WHAT is fine when
it adds precision the signature can't express (invariants, units, streaming
vs blocking, memory model) or orients a reader to a non-obvious idiom.
Detailed rules plus before/after worked examples live in
`.claude/skills/comment-style/SKILL.md`, which auto-loads on `.rs` edits.

**Every public item gets a `///` summary** in third-person present
indicative. Include `# Errors` / `# Panics` / `# Safety` when applicable.
Module/crate `//!` blocks earn their keep only when they frame a subsystem
— never when they restate the module path.

**Banned in both source comments AND commit messages** (these are public
on GitHub; the artifacts they point at are not):

- Phase / task / wave / drill-pass labels (`Phase 16b`, `Task 16c.4.3`, `Wave 3`, `D59`, `Q7=γ`).
- Locked-decision codes (`LD-16c-17`), RIP-LOG pointers, `hard-gate` / `drill pass` tags.
- Paths into gitignored planning artifacts: `docs/internal/research/*`, `docs/internal/plans/*`, `RIP-LOG.md`, `LOCKED-DECISIONS.md`.
- Deletion tombstones (global rule).

**Allowed references**: stable public artifacts (RFCs, CVEs, vendor-bug
URLs, published specs, GitHub issues on upstream crates). Cite the URL,
not an internal ticket.

**If you feel the need to cite a phase or LD code, distill the actual
reasoning from that phase item and write *that* into the comment or
commit message instead.** The phase label is a pointer to a real
rationale; replace the pointer with the rationale itself. A reader on
GitHub can then understand the change without needing access to any
internal doc.

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
- `notes/` — in-tree scratchpad for in-flight reasoning that belongs alongside the code it describes; durable across sessions, distinct from auto-memory

### Kiln IDE

`clinker-kiln` is the Dioxus 0.7.4 IDE for authoring Clinker YAML pipelines. One codebase, two targets:

- **Desktop** — `wry` webview, runs on Linux/macOS/Windows. Default per `Dioxus.toml`. Uses `dioxus = { features = ["desktop"] }` under `[target.'cfg(not(target_arch = "wasm32"))'.dependencies]`, plus `tokio` for async. Launch: `dx serve --package clinker-kiln --platform desktop`.
- **Web** — `wasm32` target. Launch: `dx serve --package clinker-kiln`. Playwright drives this build only; it cannot drive the desktop wry webview, so all UI integration tests live on the web side.

Dioxus version is pinned to `=0.7.4` to avoid silent breakage. The `dx` CLI is required — install via `cargo install dioxus-cli`.

## Rust edition & toolchain

Edition 2024, Rust 1.91 (pinned in `rust-toolchain.toml`).
