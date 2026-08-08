# AGENTS.md

Root `AGENTS.md` still applies. This file adds local guidance for `clinker-lineage`.

## Purpose

`clinker-lineage` serializes Clinker pipeline lineage as OpenLineage events —
the vendor-neutral open standard for dataset and column-level lineage. It maps
Source/Output nodes to dataset identities, walks a `CompiledPlan` DAG to
compute column-level lineage, and assembles run events for NDJSON output. It
is a plan-time, read-only consumer of `clinker-plan`: it never runs pipelines.

## Responsibilities

- Map each Source/Output node to its OpenLineage dataset identity (`dataset`).
- Compute DIRECT (value-derivation) per-column lineage and whole-dataset
  INDIRECT influence (filter / join / group-by / sort / conditional) from the
  compiled DAG, tracing through composition bodies to true source columns
  (`builder::column_lineage`).
- Trace envelope (`$doc`) reads: DIRECT lineage on the originating source for
  value-carrying reads, INDIRECT influence for reads in route / cull / combine
  predicates.
- Assemble run events (`emit`): a static `START`/`COMPLETE` pair for the
  plan-derived `run --lineage` export, or live run-lifecycle events via
  `emit::LiveRunEmitter` (a `START` at run begin, a terminal
  `COMPLETE`/`FAIL`/`ABORT` carrying real run stats).
- Own the OpenLineage wire model and NDJSON writer (`openlineage`), pinned to
  core spec `2-0-2` and `ColumnLineageDatasetFacet` `1-2-0`; structs are
  hand-rolled against the published JSON Schema because no general-purpose
  Rust OpenLineage client exists.

## Important public APIs

- `column_lineage(...)` -> `PlanColumnLineage` / `OutputColumnLineage`
- `dataset_identity(...)` / `DatasetId`
- `run_events(...)`, `start_event(...)`, `terminal_event(...)`, `RunStats`,
  `Terminal`, `LiveRunEmitter`
- `openlineage::write_ndjson` plus the event/facet model re-exports

## Internal module map

- `src/lib.rs`: crate docs, module wiring, curated re-exports.
- `src/builder.rs`: compiled-plan walk, DIRECT/INDIRECT column lineage,
  composition-body tracing.
- `src/dataset.rs`: dataset naming/namespace rules (`FILE_NAMESPACE`,
  `FALLBACK_NAMESPACE`).
- `src/emit.rs`: run-event assembly, live run-lifecycle emitter.
- `src/openlineage/`: `event.rs`, `facet.rs`, `ndjson.rs` — wire model and
  writer.
- `tests/composition_lineage.rs` plus `tests/fixtures/`: composition tracing
  coverage.

## Dependency rules

### Allowed dependencies

Current normal dependencies are intentional: `clinker-plan`, `clinker-record`,
`cxl`, `petgraph`, `serde`, `serde_json`. Dev dependency: `clinker-core-types`.

### Forbidden or suspicious dependencies

- No dependency on `clinker-exec`: the engine core has no lineage dependency,
  and lineage has no runtime dependency — the run lifecycle is orchestrated at
  the CLI edge. Route any proposal to invert this through architecture review.
- No HTTP/network transport dependencies: live emission over HTTP is a
  separate, deferred layer. The current transport is the NDJSON file writer.
- No randomness, and no clock in event assembly: the CLI supplies every
  timestamp that appears in an event. Do not add `chrono::Utc::now()`-style
  calls to the emit path.
- The bounded delivery worker is the one exception, and only for its own
  deadlines: it reads `Instant::now()` to enforce the configured flush timeout.
  That clock never reaches an event field.

## Important invariants

- Plan-time and read-only: input is a `CompiledPlan` (plus CLI-supplied run
  facts); the crate never executes nodes or reads pipeline data.
- Spec pins are load-bearing: OpenLineage core `2-0-2`,
  `ColumnLineageDatasetFacet` `1-2-0`. Bumping either is a contract change —
  update facet schema URLs, fixtures, and docs together.
- `run --lineage` (static export) must not process data; `run --lineage-events`
  runs the pipeline and cannot be combined with `--lineage`, `--explain`, or
  dry-run modes (Clap `conflicts_with_all` in `crates/clinker/src/main.rs`).
- Documented builder limitations (cases out of scope for `$doc` tracing) live
  in the module docs — extend the docs when extending coverage.

## Common mistakes for AI agents to avoid

- Adding an executor dependency or moving run-lifecycle orchestration into the
  engine core.
- Emitting events with locally generated timestamps instead of caller-supplied
  ones.
- Extending the wire model away from the pinned OpenLineage JSON Schemas.
- Treating INDIRECT influence as per-column when it is dataset-level.
- Documenting HTTP transport as implemented; it is deferred.

## Local commands

- Inferred: `cargo check -p clinker-lineage --locked --offline`
- Inferred: `cargo test -p clinker-lineage --locked --offline`
- Inferred, CLI surface: `cargo test -p clinker --locked --offline` when the
  `run --lineage` / `run --lineage-events` flags change.

## Documentation updates

Update these when changing related behavior:

- `docs/ai/10_ARCHITECTURE.md`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/70_GLOSSARY.md` (lineage / CLI terms)
- User CLI docs for the lineage flags under `docs/user/src/`

## Evidence

- `crates/clinker-lineage/Cargo.toml`
- `crates/clinker-lineage/src/lib.rs`
- `crates/clinker-lineage/src/builder.rs`
- `crates/clinker-lineage/src/emit.rs`
- `crates/clinker-lineage/tests/composition_lineage.rs`
- `crates/clinker/src/main.rs` (`RunArgs` lineage flags)
- `docs/ai/20_CRATE_MAP.md`
