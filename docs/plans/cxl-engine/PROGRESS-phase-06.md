# Phase 6 Execution Progress: Parallelism + Memory Management

**Phase file:** docs/plans/cxl-engine/phase-06-parallelism-memory.md
**Started:** 2026-03-30
**Last updated:** 2026-03-30
**Status:** 🔄 In Progress

---

## Current state

**Active task:** [6.1] Rayon Thread Pool + Chunk-Level Parallelism
**Completed:** 0 of 5 tasks
**Blocked:** none (Phase 5 entry criteria assumed met)

---

## Task list

### 🔄 [6.1] Rayon Thread Pool + Chunk-Level Parallelism  ← ACTIVE
**Sub-tasks:**
- [ ] **6.1.0** (Prep) Fix `build_eval_context` bug: freeze `pipeline_start_time` once at pipeline start
- [ ] **6.1.1** Build explicit `rayon::ThreadPool` from `ParallelismProfile::worker_threads`
- [ ] **6.1.2** Restructure Phase 2 loop to batched chunks (`Vec<(u32, Record)>`)
- [ ] **6.1.3** Apply `pool.install(|| chunk.par_iter_mut().for_each(...))` for Stateless/IndexReading; sequential for Sequential
- [ ] **6.1.4** Wire `--threads` CLI flag through config
- [ ] **6.1.5** (Optional) Add `par_iter_mut` to Phase 1.5 partition sorting

**Gate tests that must pass before Task 6.2 unlocks:**
- `test_thread_pool_default_count` -- default thread count is `min(num_cpus - 2, 4)`
- `test_thread_pool_cli_override` -- `--threads 2` overrides to 2 workers
- `test_par_stateless_deterministic_output` -- 1000-record chunk identical with 1 vs 4 threads
- `test_par_index_reading_deterministic_output` -- window aggregate identical with 1 vs 4 threads
- `test_sequential_not_parallelized` -- sequential transform not dispatched to pool
- `test_golden_file_diff_1_vs_4_threads` -- full CSV→CXL→CSV byte-identical 1 vs 4 threads
- `test_arc_ast_shared_not_cloned` -- Arc::strong_count == 1 after parallel eval
- `test_empty_pipeline_zero_records` -- 0-record input exits cleanly

**Done when:** Explicit rayon pool built at startup, Phase 2 loop batched into chunks, par_iter_mut applied for Stateless/IndexReading, output byte-identical across thread counts
**Commit:** `feat(phase-6): implement rayon thread pool and chunk-level parallelism`
**Commit ID:** --

---

### ⛔ [6.2] Source-Level Parallelism (Phase 1)  ← BLOCKED on [6.1] gate tests
**Sub-tasks:**
- [ ] **6.2.1** Define `IngestionOutput` struct in `pipeline/ingestion.rs`
- [ ] **6.2.2** Implement `thread::scope` dispatch over `SourceTier` DAG
- [ ] **6.2.3** Single-source fast path (skip `thread::scope`)
- [ ] **6.2.4** Add periodic `shutdown_requested()` check in Arena::build loop

**Gate tests that must pass before Task 6.3 unlocks:**
- `test_independent_sources_concurrent` -- two sources, wall-clock < 2x single
- `test_dependent_sources_sequential` -- source B starts after A completes
- `test_source_thread_owns_arena` -- distinct arenas, no shared mutable state
- `test_arenas_moved_to_context` -- IngestionOutput contains all source arenas
- `test_source_error_propagation` -- malformed source → Err from scope
- `test_single_source_no_spawn` -- one source, no thread::scope
- `test_signal_during_phase1_ingestion` -- shutdown flag during Arena::build aborts cleanly

**Done when:** Independent sources ingested concurrently via `thread::scope`, DAG ordering respected, arenas collected into `IngestionOutput`
**Commit:** `feat(phase-6): implement source-level parallelism with thread::scope`
**Commit ID:** --

---

### ⛔ [6.3] Cross-Platform RSS Tracking  ← BLOCKED on [6.2] gate tests
**Sub-tasks:**
- [ ] **6.3.0** (Prep) Add `libc` and `num_cpus` to clinker-core/Cargo.toml
- [ ] **6.3.1** Implement `rss_bytes()` with `#[cfg(target_os)]` platform dispatch
- [ ] **6.3.2** Implement `MemoryBudget` struct with `should_spill()`
- [ ] **6.3.3** Wire `--memory-limit` through existing `PipelineMeta.memory_limit`

**Gate tests that must pass before Task 6.4 unlocks:**
- `test_rss_bytes_returns_some` -- on supported platform, Some(n) where n > 0
- `test_rss_bytes_increases_after_alloc` -- 10MB alloc → RSS increases ≥5MB
- `test_memory_budget_below_threshold` -- 512MB budget, low RSS → false
- `test_memory_budget_above_threshold` -- 1MB budget → true
- `test_memory_budget_default_values` -- 512MB limit, 60% threshold
- `test_memory_limit_cli_parse_suffixes` -- "512M" → 536870912, "2G" → 2147483648

**Done when:** `rss_bytes()` returns `Some` on Linux/macOS/Windows, `MemoryBudget::should_spill()` triggers correctly, `--memory-limit` parsed
**Commit:** `feat(phase-6): implement cross-platform RSS tracking and memory budget`
**Commit ID:** --

---

### ⛔ [6.4] Spill-to-Disk Infrastructure  ← BLOCKED on [6.3] gate tests
**Sub-tasks:**
- [ ] **6.4.1** Implement `SpillWriter` (LZ4-compressed NDJSON with schema header)
- [ ] **6.4.2** Implement `SpillReader` as `Iterator<Item=Result<Record, SpillError>>`
- [ ] **6.4.3** Wire `--spill-dir` CLI flag with early validation (canonicalize + exists + writable)

**Gate tests that must pass before Task 6.5 unlocks:**
- `test_spill_roundtrip_all_value_types` -- 100 records, 8 Value variants, field-level equality
- `test_spill_schema_preserved` -- column names/order identical after round-trip
- `test_spill_lz4_compression_ratio` -- spill file < 80% of raw NDJSON
- `test_spill_tempfile_cleanup` -- after drop, file gone
- `test_spill_empty_chunk` -- zero records → valid empty spill file
- `test_spill_overflow_fields_preserved` -- overflow IndexMap round-trips
- `test_spill_dir_override` -- `--spill-dir /tmp/custom` respected

**Done when:** SpillWriter/SpillReader round-trip all Value types with LZ4 compression, tempfile cleanup on drop, schema preserved
**Commit:** `feat(phase-6): implement spill-to-disk NDJSON+LZ4 infrastructure`
**Commit ID:** --

---

### ⛔ [6.5] Error Threshold + Signal Handling  ← BLOCKED on [6.4] gate tests
**Sub-tasks:**
- [ ] **6.5.1** Implement `ErrorThreshold` struct (TwoPass: Arena total denominator, Streaming: running ratio)
- [ ] **6.5.2** Implement signal handling with `ctrlc::set_handler_with_signals` (SIGINT + SIGTERM)
- [ ] **6.5.3** Define exit code constants, wire threshold + shutdown checks into executor chunk loop

**Gate tests that must pass to close Phase 6:**
- `test_error_threshold_below_limit` -- 5% errors, 10% threshold → continue
- `test_error_threshold_exceeded` -- 15% errors, 10% threshold → halt exit 3
- `test_error_threshold_zero_means_no_errors` -- 0.0 → any error halts
- `test_error_threshold_one_means_unlimited` -- 1.0 → never halts
- `test_signal_flag_sets_atomic` -- set AtomicBool → next check triggers
- `test_graceful_shutdown_flushes_output` -- set flag mid-pipeline → valid output
- `test_exit_codes_documented` -- all 6 exit codes reachable
- `test_shutdown_dlq_summary_to_stderr` -- shutdown with DLQ records → stderr summary

**Done when:** Error threshold checked at chunk boundaries with mode-aware denominator, SIGINT+SIGTERM handled gracefully, exit codes {0,1,2,3,4,130} defined and wired
**Commit:** `feat(phase-6): implement error threshold, signal handling, and exit codes`
**Commit ID:** --

---

## Gate test log

| Task | Test | Status | Run | Commit |
|------|------|--------|-----|--------|
| 6.1 | `test_thread_pool_default_count` | ⛔ Not run | -- | -- |
| 6.1 | `test_thread_pool_cli_override` | ⛔ Not run | -- | -- |
| 6.1 | `test_par_stateless_deterministic_output` | ⛔ Not run | -- | -- |
| 6.1 | `test_par_index_reading_deterministic_output` | ⛔ Not run | -- | -- |
| 6.1 | `test_sequential_not_parallelized` | ⛔ Not run | -- | -- |
| 6.1 | `test_golden_file_diff_1_vs_4_threads` | ⛔ Not run | -- | -- |
| 6.1 | `test_arc_ast_shared_not_cloned` | ⛔ Not run | -- | -- |
| 6.1 | `test_empty_pipeline_zero_records` | ⛔ Not run | -- | -- |
| 6.2 | `test_independent_sources_concurrent` | ⛔ Not run | -- | -- |
| 6.2 | `test_dependent_sources_sequential` | ⛔ Not run | -- | -- |
| 6.2 | `test_source_thread_owns_arena` | ⛔ Not run | -- | -- |
| 6.2 | `test_arenas_moved_to_context` | ⛔ Not run | -- | -- |
| 6.2 | `test_source_error_propagation` | ⛔ Not run | -- | -- |
| 6.2 | `test_single_source_no_spawn` | ⛔ Not run | -- | -- |
| 6.2 | `test_signal_during_phase1_ingestion` | ⛔ Not run | -- | -- |
| 6.3 | `test_rss_bytes_returns_some` | ⛔ Not run | -- | -- |
| 6.3 | `test_rss_bytes_increases_after_alloc` | ⛔ Not run | -- | -- |
| 6.3 | `test_memory_budget_below_threshold` | ⛔ Not run | -- | -- |
| 6.3 | `test_memory_budget_above_threshold` | ⛔ Not run | -- | -- |
| 6.3 | `test_memory_budget_default_values` | ⛔ Not run | -- | -- |
| 6.3 | `test_memory_limit_cli_parse_suffixes` | ⛔ Not run | -- | -- |
| 6.4 | `test_spill_roundtrip_all_value_types` | ⛔ Not run | -- | -- |
| 6.4 | `test_spill_schema_preserved` | ⛔ Not run | -- | -- |
| 6.4 | `test_spill_lz4_compression_ratio` | ⛔ Not run | -- | -- |
| 6.4 | `test_spill_tempfile_cleanup` | ⛔ Not run | -- | -- |
| 6.4 | `test_spill_empty_chunk` | ⛔ Not run | -- | -- |
| 6.4 | `test_spill_overflow_fields_preserved` | ⛔ Not run | -- | -- |
| 6.4 | `test_spill_dir_override` | ⛔ Not run | -- | -- |
| 6.5 | `test_error_threshold_below_limit` | ⛔ Not run | -- | -- |
| 6.5 | `test_error_threshold_exceeded` | ⛔ Not run | -- | -- |
| 6.5 | `test_error_threshold_zero_means_no_errors` | ⛔ Not run | -- | -- |
| 6.5 | `test_error_threshold_one_means_unlimited` | ⛔ Not run | -- | -- |
| 6.5 | `test_signal_flag_sets_atomic` | ⛔ Not run | -- | -- |
| 6.5 | `test_graceful_shutdown_flushes_output` | ⛔ Not run | -- | -- |
| 6.5 | `test_exit_codes_documented` | ⛔ Not run | -- | -- |
| 6.5 | `test_shutdown_dlq_summary_to_stderr` | ⛔ Not run | -- | -- |

---

## Completed tasks

| Task | Name | Commit message | Commit ID | Completed |
|------|------|---------------|-----------|-----------|
| (none yet) | | | | |

---

## Notes

- Phase 6 was drilled and validated on 2026-03-30. See VALIDATION-phase-06.md for full report.
- Key architectural decisions: explicit rayon pool (not build_global), inline par_iter_mut (no ChunkProcessor), IngestionOutput struct (not god-object ExecutionContext), spill IO primitives only (triggers in Phase 8).
- `now` keyword is wall-clock by design — determinism guarantee applies to pipelines not using `now`.
- `ctrlc::set_handler_with_signals` required for SIGTERM (not default `set_handler`).
- Phase 8 exit codes were corrected to match spec §10.2 during Phase 6 validation.
- Tasks 6.2 and 6.3 are technically parallelizable (no code dependency), but kept linear for simplicity.
