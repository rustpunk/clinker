# Implementation Plan: CXL Streaming ETL Engine

**Spec:** `docs/cxl-engine-spec.md`
**Status:** In Progress
**Created:** 2026-03-28
**Last Updated:** 2026-03-31

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
| 23 | ~~`SortFieldSpec` untagged serde enum~~ → **Superseded by #40**: custom Deserialize impl for consistent error messages (Phase 9 decision) | Original: serde-saphyr supports untagged. Revised: untagged has bad error messages (serde#773); custom deser is ~20 lines with full control. |
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
| 34 | `-n` partial processing deferred to Phase 11 (Task 11.6); --dry-run stays config-validation-only (Phase 8 decision) | Phase 8 scope is large enough; -n is closer to "sample mode" than dry run |
| 35 | Override files use `.channel.yaml` extension + `_channel:` header (Phase 10 decision) | Ansible entity-name→filename pattern; "channel" is the domain concept; `_channel:` header consistent with `_composition:` convention |
| 36 | ~~`interpolate_env_vars` auto-wraps substituted values in YAML single-quotes~~ → **Superseded by #52**: Bare text substitution (Phase 10 drill decision, spec updated) | Original rationale invalidated by Docker Compose #8297. See #52. |
| 37 | `--channel-path <path>` (repeatable) for explicit `.channel.yaml` override injection (Phase 10 decision) | Helm/Terraform pattern; enables hotfix injection without touching workspace channel dirs; applied after derived-name file |
| 38 | Schema types in `clinker-record`, loading/resolution in `clinker-core` (Phase 9 decision) | Follows existing Schema/Arena split. No new dependency edges. FieldDef alongside Schema in foundation crate. |
| 39 | `FieldDef` flat struct with typed `FieldType`/`Justify` enums, `serde_json::Value` for default (Phase 9 decision) | Research: every execution engine uses typed enums (Arrow, Polars, Spark, Beam). serde-saphyr has no Value type; serde_json::Value proven to work. |
| 40 | `SchemaSource` + `SortFieldSpec` use custom `Deserialize` impl, not `#[serde(untagged)]` (Phase 9 decision) | Research: serde untagged has bad error messages (serde#773). Docker Compose uses normalize-then-deser. Custom deser is ~20 lines with full error control. |
| 41 | `ResolvedSchema` enum (SingleRecord/MultiRecord) distinct from `SchemaDefinition` (Phase 9 decision) | Research: 5-to-2 for distinct resolved types (DataFusion, Protobuf, rustc, TS compiler, serde vs Polars, Spark). Eliminates variant ambiguity. |
| 42 | Same `FieldDef` for definitions and overrides; `drop`/`record` fields validated as None in non-override contexts (Phase 9 decision) | Research: Docker Compose, Terraform use same type for base and override. Override semantics in loader, not type system. |
| 43 | Alias creates identity boundary (SQL model); mapping keys reference post-alias names (Phase 9 decision) | Research: unanimous across SQL, dbt, Singer, Informatica, NiFi, Beam. Aliases resolved in executor; writers see final names. |
| 44 | Byte-offset extraction for fixed-width; character mode is v2 toggle (Phase 9 decision) | Research: Informatica, Cobrix, Talend bytes mode. Spec says "byte position". go-fixedwidth toggleable pattern for v2. |
| 45 | Type-aware truncation: numeric→Error, string→Warn, per-field override via TruncationPolicy (Phase 9 decision) | Research: Informatica rejects numeric overflow. Silent numeric truncation caused real payment routing failures. |
| 46 | Constructor injection for FixedWidthReader schema; no FormatReader trait changes (Phase 9 decision) | Research: unanimous — DataFusion FileScanConfig, Spark ScanBuilder, Polars with_schema(). Schema at build time, read-only getter on trait. |
| 47 | ~~`QualifiedFieldRef.parts: Vec<Box<str>>`~~ → **`Box<[Box<str>]>`** replaces source+field (Phase 9 validation decision) | Research: sqlparser-rs CompoundIdentifier pattern. Boxed slice per SWC/oxc AST node size conventions (saves 8 bytes inline, no wasted capacity). |
| 48 | `MultiRecordDispatcher` in clinker-core between reader and arenas (Phase 9 decision) | Arena::build() consumes reader into one arena. Multi-record needs N arenas from one file read. Dispatcher routes by discriminator. |
| 49 | `SchemaDefinition.format: Option<String>` not `Option<FormatKind>` (Phase 9 validation decision) | Research: 7/8 systems keep format external to schema (Arrow, Polars, Spark, Beam, Protobuf, Avro, JSON Schema). String avoids circular dep between clinker-record and clinker-core. |
| 50 | `SchemaError` enum in `clinker-core/src/schema/mod.rs` with `PipelineError::Schema(SchemaError)` (Phase 9 validation decision) | DataFusion SchemaError pattern. Distinct from ConfigError (different lifecycle: schema resolution vs YAML parsing). |
| 51 | Path validation deferred to Phase 11 central layer; Phase 9 load_schema() trusts paths (Phase 9 validation decision) | Research: 15+ CVEs from per-subsystem path validation (Ansible, Docker Compose, Terraform, Helm, dbt). Central validation is the correct architecture. |
| 52 | Bare text substitution for `${VAR}` — no YAML auto-quoting (Phase 10 drill decision, revises W1) | Research: Docker Compose #8297 broke typed fields. dbt/Helm/envsubst bare insertion is industry standard. Ansible CVE-2021-3583 was re-evaluation, not quoting. |
| 53 | `TransformEntry` uses custom `Deserialize` impl, not `#[serde(untagged)]` (Phase 10 drill decision) | serde#773 (9 years, 3 rejected PRs). Consistent with Phase 9 SortFieldSpec/SchemaSource. Concourse/Cargo/Vector migrated away from untagged. |
| 54 | `PipelineConfig::transforms()` accessor for resolved transforms (Cargo `InheritableField` pattern) (Phase 10 drill decision) | Cargo uses identical pattern. Minimizes downstream churn from Vec type change. Panics on unresolved imports. |
| 55 | `when:` conditions use split-based parser, not regex (Phase 10 drill decision) | Concourse StepDetector pattern. 4 operators, no nesting. ~50-80 lines, zero dependencies. |
| 56 | `clinker.toml` is 100% CLI-owned config; `default_channel` is CLI fallback with INFO log (Phase 10 drill decision) | No Kiln-specific fields in CLI config. Visible logging prevents silent override application. |
| 57 | Override delta structs are dedicated hand-written types, not derived from config types (Phase 10 drill decision) | Cargo precedent. Different serde attributes (no deny_unknown_fields). 3 structs / 17 fields — too small for proc macro. |
| 58 | `use` path separator is `.` not `::` (Phase 11 drill decision, spec updated) | Research: every ETL/data tool uses `.` (dbt, Spark, Beam, SQL). `::` alien to data engineers. `.` consistent with qualified access. Parser change ~5 lines. |
| 59 | `pipeline.batch_id` built-in: user-supplied via `--batch-id`, auto UUID v7 default (Phase 11 drill decision, spec updated) | Research: Airflow 3.0 two-ID pattern; SSIS/Informatica precedent. execution_id (auto/system) + batch_id (user/domain). |
| 60 | Separate `parse_module()` → `Module` AST type (Phase 11 drill decision) | Research: HCL/CUE/syn/rustc all use separate entry points for different file kinds. tree-sitter #870 shows mode flags cause problems. |
| 61 | Topological sort for module-level `let` constants (Phase 11 drill decision) | Research: CUE/Nix/Jsonnet/Terraform/Rust const all order-independent. Kahn's algorithm with cycle detection. |
| 62 | `.debug()` allowed in fn bodies (Phase 11 drill decision, spec deviation) | Pragmatic DX; zero overhead when trace disabled; only emit/trace are banned statement-level side effects. |
| 63 | Native YAML 1.2 inference for pipeline.vars (Phase 11 drill decision) | serde-saphyr YAML 1.2 avoids Norway problem; dbt/Helm precedent. serde_json::Value deserialization. |
| 64 | Level 1 `when` = lifecycle hook per spec (Phase 11 drill decision) | Spec authoritative. before_transform/after_transform are executor events, not CXL expressions. |
| 65 | Validation schema: spec properties + optional `name` (Phase 11 drill decision) | Research: Soda Core/Pandera/Griffin pattern. Auto-derived from field+check if omitted. |
| 66 | serde-saphyr Budget API for YAML DoS limits (Phase 11 drill decision) | Research confirmed: max_depth, max_nodes, max_reader_input_bytes all available via budget! macro. |
| 67 | Custom harness=false benchmark binary (Phase 11 drill decision) | Research: DataFusion dfbench pattern. criterion/divan can't track RSS. VmHWM for peak RSS. |
| 68 | Generic TakeReader\<R\> for -n record limiting (Phase 11 drill decision) | One implementation for all formats. Iterator::take() pattern. ~15 lines. |

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
| 8 | Sort, DLQ Polish, CLI Completion | ✅ Complete | 4 | 4 | Phase 6, 7 |
| 9 | Schema System, Fixed-Width, Multi-Record | 🔲 Validated (READY) | 4 | 0 | Phase 7 |
| 10 | Channel Overrides + Composition System | 🔲 Validated (READY) | 7 | 0 | Phase 9 |
| 11 | Modules, Logging, Validations, Polish | 🔄 In Progress | 7 | 1 | Phase 10 |

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
  │           │                       └── Phase 10 (Channels) │
  │           │                             │                 │
  │           │                             └── Phase 11 (Modules+)
  │           │                                               │
  │           └───────────────── Phase 8 (Sort + CLI) ────────┘
  │                              (needs 6 + 7)
```

Phase 5 requires the `ExecutionPlan` concept from Phase 4's executor.
Phase 6 layers parallelism onto the sequential pipeline from Phase 5. Spill IO primitives built in Phase 6; spill triggers in Phase 8.
Phase 7 can start after Phase 4 (format readers are independent of windows).
Phase 8 needs both Phase 6 (spill infrastructure) and Phase 7 (all formats).
Phase 9 needs Phase 7 (schema applies to all formats).
Phase 10 needs Phase 9 (channel system needs all format readers + schema complete).
Phase 11 needs Phase 10 (validations reference schemas and modules; logging references pipeline vars from channel system).

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
