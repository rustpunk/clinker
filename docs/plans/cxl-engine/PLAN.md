# Implementation Plan: CXL Streaming ETL Engine

**Spec:** `docs/cxl-engine-spec.md`
**Status:** In Progress
**Created:** 2026-03-28
**Last Updated:** 2026-03-30

## Summary

Build a memory-efficient, high-throughput CLI ETL tool in pure Rust. 6-crate workspace
processing CSV, XML, JSON, and fixed-width files with a custom DSL (CXL) for row-level
transformation, window functions, and cross-source lookups. Two-pass streaming pipeline
with spill-to-disk under a configurable memory budget.

## Assumptions

| # | Assumption | Risk if wrong |
|---|-----------|---------------|
| 1 | Rust edition 2024 (rustc >= 1.85) available on all build targets | Must downgrade to 2021; minor syntax differences |
| 2 | `serde-saphyr` API is stable enough for YAML config parsing | Fallback: manual YAML deserialization layer |
| 3 | `quick-xml` 0.37 API remains stable | Pin version; wrap in adapter trait |
| 4 | Arena fits in memory for typical workloads (≤100MB files) | Arena spill-to-disk is a v2 optimization |
| 5 | Single-level module imports (`use`) sufficient for v1 | Cross-module imports deferred to v2 |
| 6 | `WindowContext<'a, S: RecordStorage>` lifetime-parameterized trait (Phase 5 decision) | If Phase 6 rayon needs to send views across task boundaries, upgrade to `Arc<Arena>` (mechanical) |
| 7 | `RecordStorage` trait in `clinker-record` foundation crate (Phase 5 decision) | All crates depending on `clinker-record` gain access; `cxl::eval` becomes generic over `S` |
| 8 | Unified `PipelineExecutor` replaces `StreamingExecutor` (Phase 5 decision) | Phase 6+ modifies one executor, not two. Streaming mode is an internal branch. |
| 9 | `config::SortField` extended with optional `null_order` (Phase 5 decision) | Output sort ordering uses `null_order: None`; window sorting uses `Some(Last)` default |
| 10 | Explicit `rayon::ThreadPool` via `build()` + `pool.install()`, not `build_global()` (Phase 6 decision) | Tests can configure per-test thread counts without global state races. Polars + Apollo Rover precedent. Zero perf difference. |
| 11 | Inline `par_iter_mut` in executor loop, no ChunkProcessor struct (Phase 6 decision) | Ecosystem consensus: inline swap is the dominant pattern for row-oriented chunk parallelism. Tasks 6.3-6.5 bolt onto chunk boundary as loop-level checks. |
| 12 | `IngestionOutput` struct carries Phase 1 arenas + indices, not a god-object ExecutionContext (Phase 6 decision) | DataFusion precedent: focused data-product struct. Plan and thread pool passed separately. |
| 13 | Phase 6 spill scope is IO primitives only; spill triggers live in Phase 8 (Phase 6 decision) | Phase 2 streaming drops chunks after processing — nothing accumulates. Spec §9.1 spill is for blocking stages (sort, distinct) in Phase 8. |
| 14 | Error threshold denominator: Arena total in TwoPass, running ratio in Streaming (Phase 6 decision) | Spec says "total record count is known from Phase 1." Streaming has no Phase 1, so running ratio is the only option. |
| 15 | Exit codes: spec {0,1,2,3,4} + 130 for SIGINT/SIGTERM (Phase 6 decision) | Spec §10.2 is authoritative. 130 is Unix convention (128 + SIGINT=2). No collision. |
| 16 | `now` keyword is wall-clock, non-deterministic by design (Phase 6 validation) | Users need wall-clock for timestamps and time-based filtering. Determinism guarantee applies to pipelines not using `now`. `pipeline.start_time` is frozen once (deterministic). |
| 17 | `build_eval_context` must freeze `pipeline_start_time` once at pipeline start (Phase 6 validation) | Pre-existing bug: current code recreates `pipeline_start_time` per record via `Local::now()`. Fix in Phase 6 Task 6.1.0 prep. |
| 18 | `ctrlc::set_handler_with_signals` required for SIGTERM, not default `set_handler` (Phase 6 validation) | Default `set_handler()` only catches SIGINT. SIGTERM (kill, Docker stop, systemd) requires explicit opt-in. |
| 19 | Adjacently tagged serde enum for format config (`InputFormat`/`OutputFormat`) replaces flat `InputOptions`/`OutputOptions` (Phase 7 decision) | Research: `deny_unknown_fields` broken with internally tagged enums (serde #2123). Adjacently tagged preserves YAML shape. Empirically validated with serde-saphyr. `FormatKind` enum removed. |
| 20 | Streaming `DeserializeSeed` + `IgnoredAny` for JSON path navigation — O(1 record) memory (Phase 7 decision, revised) | Original plan used `Value::pointer()` (3-11x overhead). Replaced with serde's `DeserializeSeed` + `IgnoredAny` which navigates the tree via `visit_map` without buffering. ~10KB + 1 record. |
| 21 | Plain `quick_xml::Reader` with `QName::local_name()` for namespace stripping (Phase 7 decision) | `NsReader` resolves URIs (overkill). `local_name()` strips prefix. Config flag selects strip vs qualify. |
| 22 | `SortBuffer` shared sort-and-spill engine for both source-sort and output-sort (Phase 8 decision) | One implementation of external merge sort + loser tree; executor calls SortBuffer at two intercept points; window sort unchanged in sort.rs |
| 23 | `SortFieldSpec` untagged serde enum (`Short(String)` / `Full(SortField)`) for sort config (Phase 8 decision) | serde-saphyr supports untagged; YAML scalars vs mappings unambiguous; unified type for InputConfig and OutputConfig sort_order |
| 24 | `PipelineMeta.sort_output` removed; sort lives only on per-source `InputConfig.sort_order` and per-output `OutputConfig.sort_order` (Phase 8 decision) | Per-source and per-output sorts independently optional; global sort creates ambiguous precedence; stub was untyped serde_json::Value |
| 25 | `InputConfig.sort_order` means "ensure this order" (active sort), not pre-sorted declaration (Phase 8 decision) | Subsumes pre-sorted optimization via is_sorted() fast path; one field, one semantic; no silent correctness bugs from false declarations |
| 26 | Hybrid comparator: closure sort_by for in-memory, memcomparable byte encoding for loser tree merge (Phase 8 decision) | In-memory sorts don't need encoding overhead; loser tree needs Ord; Arrow-rs/DataFusion/Polars converge on this split |
| 27 | Hand-rolled loser tree (~80 lines), DataFusion-style flat array (Phase 8 decision) | No usable crate exists (all "tournament" crates are BinaryHeap wrappers); 50-72% faster than BinaryHeap (DataFusion PR #4301) |
| 28 | Self-tracking allocation counting for sort buffer memory budget (Phase 8 decision) | Industry consensus (DataFusion, Spark, SQLite); deterministic and testable; Value::heap_size() + running counter |
| 29 | Structured DlqErrorCategory enum (6 variants) passed from error site (Phase 8 decision) | Correct by construction; replaces fragile classify_error() string matching |
| 30 | DLQ always CSV per spec §10.4; JSON/XML sources get _cxl_source_record column (Phase 8 decision) | Industry consensus (Spark, Beam): fixed envelope + raw record as string; one DLQ file, cross-source queryable |
| 31 | `--explain` plain text output; structured JSON deferred for Kiln (Phase 8 decision) | ExecutionPlan struct is the right abstraction; adding Serialize for Kiln's JSON-RPC is mechanical |
| 32 | Callback-based ProgressReporter trait with phase-specific counters (Phase 8 decision) | Testable via VecReporter; NullReporter for --quiet; extensible for Kiln Tier 1 JSON Lines |
| 33 | `cxl eval`: -e for inline, positional for file, --field + --record (Phase 8 decision) | Follows sed/perl -e convention; --field auto-type-inferred; no auto-detection (anti-pattern per clig.dev) |
| 34 | `-n` partial processing deferred to Phase 10; --dry-run stays config-validation-only (Phase 8 decision) | Phase 8 scope is large enough; -n is closer to "sample mode" than dry run |

## Open Questions

- [ ] Benchmark target: specific records/sec threshold for "beat polars"?
- [ ] CI platform: GitHub Actions? Self-hosted runner for memory tests?

## Phase Overview

| Phase | Name | Status | Tasks | Passing | Blocked |
|-------|------|--------|-------|---------|---------|
| 1 | Foundation — Workspace + Data Model | ✅ Complete | 5 | 5 | — |
| 2 | CXL Lexer + Parser | ✅ Complete | 4 | 4 | — |
| 3 | CXL Resolver, Type Checker, Evaluator | ✅ Complete | 4 | 4 | — |
| 4 | CSV + Minimal End-to-End Pipeline | ✅ Complete | 5 | 5 | — |
| 5 | Two-Pass Pipeline — Arena, Indexing, Windows | ✅ Complete | 5 | 5 | — |
| 6 | Parallelism + Memory Management | ✅ Complete | 5 | 5 | Phase 5 |
| 7 | JSON + XML Readers/Writers | ✅ Complete | 5 | 5 | Phase 4 |
| 8 | Sort, DLQ Polish, CLI Completion | 🔲 Ready (drilled + validated) | 4 | 0 | Phase 6, 7 |
| 9 | Schema System, Fixed-Width, Multi-Record | 🔲 Not Started | 4 | 0 | Phase 7 |
| 10 | Modules, Logging, Validations, Polish | 🔲 Not Started | 6 | 0 | Phase 9 |

> Status key: 🔲 Not Started · 🔄 In Progress · ⛔ Blocked · ✅ Complete

## Dependency Map

```
Phase 1 (Foundation)
  │
  ├── Phase 2 (CXL Parser)
  │     │
  │     └── Phase 3 (CXL Evaluator)
  │           │
  │           ├── Phase 4 (CSV + Pipeline) ──────────────────┐
  │           │     │                                         │
  │           │     ├── Phase 5 (Two-Pass + Windows)          │
  │           │     │     │                                   │
  │           │     │     └── Phase 6 (Parallelism + Memory)  │
  │           │     │           │                             │
  │           │     └── Phase 7 (JSON + XML) ─────────────────┤
  │           │                 │                             │
  │           │                 └── Phase 9 (Schema + FW) ────┤
  │           │                       │                       │
  │           │                       └── Phase 10 (Modules+) │
  │           │                                               │
  │           └───────────────── Phase 8 (Sort + CLI) ────────┘
  │                              (needs 6 + 7)
```

Phase 5 requires the `ExecutionPlan` concept from Phase 4's executor.
Phase 6 layers parallelism onto the sequential pipeline from Phase 5. Spill IO primitives built in Phase 6; spill triggers in Phase 8.
Phase 7 can start after Phase 4 (format readers are independent of windows).
Phase 8 needs both Phase 6 (spill infrastructure) and Phase 7 (all formats).
Phase 9 needs Phase 7 (schema applies to all formats).
Phase 10 needs Phase 9 (validations reference schemas and modules).

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| CXL Pratt parser complexity exceeds estimate | Medium | High — blocks Phases 3-10 | Start with minimal grammar (no match, no window), add productions incrementally. Budget 2 weeks. |
| `serde-saphyr` API incompatibility | Low | Medium — blocks Phase 4 | Evaluate API surface during Phase 1 stub crate setup. Fallback: thin wrapper. |
| Deterministic output violated under parallelism | Medium | High — spec guarantee broken | Golden-file tests: run 1-thread vs 4-thread, diff output. Run 10x, verify identical. Add in Phase 6. |
| Arena exceeds memory budget on large files | Low | Medium — OOM on production workloads | Phase 6 adds RSS tracking + spill. Arena spill is v2; Phase 6 halts with diagnostic if Arena overflows. |
| `quick-xml` 0.37 breaking changes | Low | Low — isolated to Phase 7 | Pin version. Wrap reader behind `FormatReader` trait. |
| `WindowContext<'a, S>` generic propagation through eval | Low | Medium — 8 functions in `cxl::eval` need `S` param | Mechanical change. Monomorphized once for `Arena`. Sub-task 5.2.6 enumerates all sites. |
| Phase 5 `RecordView` lifetime vs Phase 6 rayon closures | Low | Medium — if views need to cross task boundaries | `&Arena` is `Send` (Arena is Sync). Views don't escape closures. Upgrade to Arc is mechanical fallback. |
| `ctrlc::set_handler` global state in tests | Low | Low — test flakiness | Phase 6 wraps in `Once`; tests manipulate `AtomicBool` directly, never install the real handler. |
| RSS measurement is process-wide, not pipeline-scoped | Low | Low — premature spill triggers | 60% threshold provides headroom. Acceptable for v1. v2 could use custom allocator wrapper. |
| Mixed parallelism classes force conservative sequential dispatch | Low | Medium — reduced parallelism | If any transform is Sequential, entire chunk is sequential. v2 could interleave parallel/sequential per-transform. |
| CXL `now` keyword makes output non-reproducible | Low | Low — by design | Document that `now` is wall-clock. Golden file tests must not use `now`. `pipeline.start_time` is deterministic. |
| Phase 8 Task 8.4 exit codes contradict spec §10.2 | ~~Medium~~ Resolved | ~~Medium~~ | Phase 8 drill corrected exit codes to match spec. Phase 6 owns constants; Phase 8 owns CLI flags + integration tests. |
| Memory ceiling best-effort until Phase 8 spill triggers | Low | Medium — RSS can exceed 512MB during Phase 2 | Arena hard cap covers dominant consumer. Phase 6 adds `tracing::warn` on RSS ceiling breach. Phase 8 adds self-tracking allocation counting in SortBuffer. |
| Memcomparable encoding correctness for Date/DateTime | Low | Medium — wrong sort order for temporal values | Must use days-since-epoch (Date) and micros-since-epoch (DateTime) with sign-flip. Chrono's public API is stable. Covered by sort key encoding tests. |
| DlqErrorCategory threading through evaluator | Low | Medium — touches cxl + clinker-core crates | Mechanical change: evaluator error types gain category field. Scope is bounded — 6 error sites, each constructs one variant. |
