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
| 5 | Two-Pass Pipeline — Arena, Indexing, Windows | 🔲 Ready (drilled + validated) | 5 | 0 | — |
| 6 | Parallelism + Memory Management | 🔲 Not Started | 5 | 0 | Phase 5 |
| 7 | JSON + XML Readers/Writers | 🔲 Not Started | 4 | 0 | Phase 4 |
| 8 | Sort, DLQ Polish, CLI Completion | 🔲 Not Started | 4 | 0 | Phase 6, 7 |
| 9 | Schema System, Fixed-Width, Multi-Record | 🔲 Not Started | 4 | 0 | Phase 7 |
| 10 | Modules, Logging, Validations, Polish | 🔲 Not Started | 5 | 0 | Phase 9 |

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
Phase 6 layers parallelism onto the sequential pipeline from Phase 5.
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
