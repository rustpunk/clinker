# Phase 6: Parallelism + Memory Management

**Status:** 🔲 Not Started
**Depends on:** Phase 5 (Two-Pass Pipeline — Arena, Indexing, Windows)
**Entry criteria:** `cargo test -p clinker-core` passes all Phase 5 tests; two-pass executor handles window functions over grouped/sorted partitions
**Exit criteria:** Rayon chunk-level parallelism produces byte-identical output vs single-threaded; RSS tracking works on Linux/macOS/Windows; spill-to-disk triggers under memory pressure; error threshold + signal handling gracefully halt the pipeline; `cargo test -p clinker-core` passes 30+ new tests

---

## Tasks

### Task 6.1: Rayon Thread Pool + Chunk-Level Parallelism
**Status:** 🔲 Not Started
**Blocked by:** Phase 5 exit criteria

**Description:**
Create a rayon `ThreadPoolBuilder` at startup and apply `par_iter_mut` to chunk processing
for Stateless and IndexReading transforms. Output order must be deterministic regardless of
thread count.

**Implementation notes:**
- `ThreadPoolBuilder::new().num_threads(min(num_cpus::get().saturating_sub(2), 4))` at startup. Overridable via config key `concurrency.worker_threads` or `--threads` CLI flag.
- For transforms classified as `Stateless` or `IndexReading` in the `ExecutionPlan`: replace `iter_mut` with `rayon::par_iter_mut` on the chunk's `Vec<Record>`.
- `Arc<TypedAst>` shared immutably across workers — no per-thread cloning of the AST.
- Chunk barrier: `par_iter_mut` blocks until all records in the chunk are processed before the chunk is emitted. This guarantees deterministic output order (records within a chunk maintain their original positions).
- `Sequential` transforms remain single-threaded — they are not parallelized.
- Thread pool is built once and reused for all chunks — never rebuilt mid-pipeline.

**Acceptance criteria:**
- [ ] Thread pool respects `--threads` CLI override
- [ ] Thread pool defaults to `min(num_cpus - 2, 4)` when unset
- [ ] Stateless transforms execute via `par_iter_mut`
- [ ] IndexReading transforms execute via `par_iter_mut`
- [ ] Sequential transforms remain single-threaded
- [ ] Output order is deterministic regardless of thread count

**Required unit tests (must pass before Task 6.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_thread_pool_default_count` | Default thread count is `min(num_cpus - 2, 4)` | ⛔ Hard gate |
| `test_thread_pool_cli_override` | `--threads 2` overrides default to 2 workers | ⛔ Hard gate |
| `test_par_stateless_deterministic_output` | 1000-record chunk through stateless transform: output identical with 1 thread vs 4 threads | ⛔ Hard gate |
| `test_par_index_reading_deterministic_output` | Window aggregate over 500 records: output identical with 1 thread vs 4 threads | ⛔ Hard gate |
| `test_sequential_not_parallelized` | Sequential transform runs on calling thread only (verify via `rayon::current_num_threads` is not invoked) | ⛔ Hard gate |
| `test_golden_file_diff_1_vs_4_threads` | Full pipeline CSV→CXL→CSV: byte-identical output files with `--threads 1` vs `--threads 4` | ⛔ Hard gate |
| `test_arc_ast_shared_not_cloned` | `Arc::strong_count` on TypedAst equals 1 + worker count, not 1 per record | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 6.2 status remains `Blocked` until all tests above pass.

---

### Task 6.2: Source-Level Parallelism (Phase 1)
**Status:** ⛔ Blocked (waiting on Task 6.1)
**Blocked by:** Task 6.1 — rayon thread pool must be working

**Description:**
Use `std::thread::scope` to read independent sources concurrently during Phase 1 (data
ingestion). The source dependency DAG from Phase D determines which sources can run in
parallel.

**Implementation notes:**
- `std::thread::scope` (not rayon) for source-level parallelism — these are I/O-bound threads that should not compete with rayon's compute pool.
- Phase D's dependency analysis produces a DAG of source dependencies. Sources with no edges between them are independent and can be read concurrently.
- Each source thread builds its own `Arena` + `SecondaryIndex` locally. After `thread::scope` joins, all arenas and indices are moved into the shared `ExecutionContext`.
- If a source depends on another (e.g., reference data required before main data), the dependent source waits for its predecessor to complete.
- Error in any source thread propagates to the main thread via `Result` — fail-fast semantics.

**Acceptance criteria:**
- [ ] Independent sources read concurrently within `thread::scope`
- [ ] Dependent sources respect DAG ordering
- [ ] Each source thread owns its Arena + SecondaryIndex
- [ ] Arenas moved into shared ExecutionContext after join
- [ ] Source-thread error propagates to main thread

**Required unit tests (must pass before Task 6.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_independent_sources_concurrent` | Two independent sources: both threads execute (verify via timestamps or thread IDs, wall-clock < 2x single-source time) | ⛔ Hard gate |
| `test_dependent_sources_sequential` | Source B depends on Source A: B starts only after A completes | ⛔ Hard gate |
| `test_source_thread_owns_arena` | Each source thread produces a distinct Arena; no shared mutable state during read | ⛔ Hard gate |
| `test_arenas_moved_to_context` | After join, `ExecutionContext` contains arenas from all sources | ⛔ Hard gate |
| `test_source_error_propagation` | Malformed source file in one thread → entire scope returns Err | ⛔ Hard gate |
| `test_single_source_no_spawn` | Pipeline with one source: no extra thread spawned (fast path) | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 6.3 status remains `Blocked` until all tests above pass.

---

### Task 6.3: Cross-Platform RSS Tracking
**Status:** ⛔ Blocked (waiting on Task 6.2)
**Blocked by:** Task 6.2 — source parallelism must be working

**Description:**
Implement a `clinker-core::pipeline::memory` module with cross-platform RSS (Resident Set
Size) measurement and a `MemoryBudget` struct that governs spill decisions.

**Implementation notes:**
- `fn rss_bytes() -> Option<u64>` — returns current process RSS.
- Linux: read `/proc/self/statm`, multiply second field (resident pages) by page size. Cost ~1us.
- macOS: `mach_task_basic_info` via `mach2` crate (pure Rust FFI). Gated behind `#[cfg(target_os = "macos")]`.
- Windows: `K32GetProcessMemoryInfo` via `windows-sys` crate. Gated behind `#[cfg(target_os = "windows")]`.
- Unsupported platforms: return `None`. Caller falls back to allocation-counting heuristic.
- `MemoryBudget` struct: `limit: u64` (default 512MB), `spill_threshold_pct: f64` (default 0.60), `fn should_spill(&self) -> bool` returns `true` when `rss_bytes() > limit * spill_threshold_pct`.
- Polled once per chunk at the start of Phase 2 processing — not per-record (too expensive).
- `--memory-limit` CLI flag sets `MemoryBudget::limit` in bytes (accepts `512M`, `2G` suffixes).

**Acceptance criteria:**
- [ ] `rss_bytes()` returns `Some(n)` on Linux, macOS, and Windows
- [ ] `rss_bytes()` returns `None` on unsupported platforms
- [ ] `MemoryBudget::should_spill` returns true when RSS exceeds threshold
- [ ] `--memory-limit` CLI flag parsed and applied
- [ ] RSS measurement cost is < 10us per call

**Required unit tests (must pass before Task 6.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_rss_bytes_returns_some` | On supported platform, `rss_bytes()` returns `Some(n)` where `n > 0` | ⛔ Hard gate |
| `test_rss_bytes_increases_after_alloc` | Allocate 10MB vec, RSS increases by at least 5MB (accounting for page granularity) | ⛔ Hard gate |
| `test_memory_budget_below_threshold` | Budget 512MB, RSS well below 307MB → `should_spill()` returns false | ⛔ Hard gate |
| `test_memory_budget_above_threshold` | Budget set to artificially low value (1MB) → `should_spill()` returns true | ⛔ Hard gate |
| `test_memory_budget_default_values` | Default limit is 512MB, default spill threshold is 60% | ⛔ Hard gate |
| `test_memory_limit_cli_parse_suffixes` | `"512M"` → 536870912, `"2G"` → 2147483648, `"1024"` → 1024 | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 6.4 status remains `Blocked` until all tests above pass.

---

### Task 6.4: Spill-to-Disk Infrastructure
**Status:** ⛔ Blocked (waiting on Task 6.3)
**Blocked by:** Task 6.3 — RSS tracking and MemoryBudget must be working

**Description:**
Implement `SpillWriter` and `SpillReader` that serialize records to LZ4-compressed NDJSON
on disk when memory pressure exceeds the spill threshold.

**Implementation notes:**
- `SpillWriter`: accepts `Record` references, serializes each to JSON via `serde_json::to_string`, compresses the NDJSON stream with `lz4_flex` frame compression, writes to a `tempfile::NamedTempFile`.
- `SpillReader`: opens a spill file, decompresses via `lz4_flex::frame::FrameDecoder`, deserializes NDJSON lines back into `Record` instances.
- Spill is triggered when `MemoryBudget::should_spill()` returns true at a chunk boundary. The oldest in-memory chunk is spilled first (LRU eviction).
- `tempfile` crate handles automatic cleanup on drop — no manual deletion required.
- Spill files are written to the system temp directory by default; overridable via `--spill-dir` CLI flag.
- Schema is written as the first line of each spill file so the reader can reconstruct Records without external state.

**Acceptance criteria:**
- [ ] `SpillWriter` serializes records to LZ4-compressed NDJSON
- [ ] `SpillReader` deserializes back to identical records
- [ ] Round-trip: write N records, read N records, all field values match
- [ ] Spill files are automatically cleaned up on drop
- [ ] Schema preserved across spill/reload boundary
- [ ] LZ4 compression reduces spill file size vs raw NDJSON

**Required unit tests (must pass before Task 6.5 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_spill_roundtrip_all_value_types` | Write 100 records with all 8 Value variants, read back, assert field-level equality | ⛔ Hard gate |
| `test_spill_schema_preserved` | Schema column names and order identical after round-trip | ⛔ Hard gate |
| `test_spill_lz4_compression_ratio` | Spill file size < 80% of raw NDJSON size for typical tabular data | ⛔ Hard gate |
| `test_spill_tempfile_cleanup` | After SpillWriter is dropped, the temp file no longer exists on disk | ⛔ Hard gate |
| `test_spill_empty_chunk` | Writing zero records produces a valid spill file that reads back as empty iterator | ⛔ Hard gate |
| `test_spill_overflow_fields_preserved` | Records with overflow fields round-trip correctly | ⛔ Hard gate |
| `test_spill_dir_override` | `--spill-dir /tmp/custom` places spill files in the specified directory | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 6.5 status remains `Blocked` until all tests above pass.

---

### Task 6.5: Error Threshold + Signal Handling
**Status:** ⛔ Blocked (waiting on Task 6.4)
**Blocked by:** Task 6.4 — spill infrastructure must be working (pipeline must handle memory pressure before adding graceful shutdown)

**Description:**
Enforce the `type_error_threshold` at chunk boundaries and handle OS signals (SIGINT/SIGTERM)
for graceful pipeline shutdown.

**Implementation notes:**
- `type_error_threshold` (from config): a `f64` in `[0.0, 1.0]`. At each chunk boundary, compute `error_count / total_records_so_far`. If ratio exceeds threshold, halt pipeline with exit code 3.
- `ctrlc` crate registers a handler that sets an `AtomicBool` flag (`SHUTDOWN_REQUESTED`).
- Chunk boundary check: after processing each chunk, check the `AtomicBool`. If set: flush all output buffers, close file handles, write DLQ summary to stderr, exit with code 130 (standard SIGINT convention).
- DLQ summary format: `"DLQ: {dlq_count} records written to {dlq_path}"` on stderr.
- Exit codes: 0 = success, 1 = fatal error, 2 = config/parse error, 3 = error threshold exceeded, 130 = interrupted by signal.

**Acceptance criteria:**
- [ ] Error threshold checked at every chunk boundary
- [ ] Threshold exceeded → exit code 3 with diagnostic message
- [ ] SIGINT sets shutdown flag → graceful flush and exit 130
- [ ] Output buffers flushed before exit on signal
- [ ] DLQ summary written to stderr on shutdown
- [ ] Exit codes are consistent and documented

**Required unit tests (must pass to close Phase 6):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_error_threshold_below_limit` | 5% errors with 10% threshold → pipeline continues | ⛔ Hard gate |
| `test_error_threshold_exceeded` | 15% errors with 10% threshold → pipeline halts with exit code 3 | ⛔ Hard gate |
| `test_error_threshold_zero_means_no_errors` | Threshold 0.0 → any single error halts immediately | ⛔ Hard gate |
| `test_error_threshold_one_means_unlimited` | Threshold 1.0 → pipeline never halts on errors alone | ⛔ Hard gate |
| `test_signal_flag_sets_atomic` | Simulate setting the AtomicBool → next chunk boundary triggers shutdown | ⛔ Hard gate |
| `test_graceful_shutdown_flushes_output` | Set shutdown flag mid-pipeline → output file is valid (not truncated) | ⛔ Hard gate |
| `test_exit_codes_documented` | Exit codes 0, 1, 2, 3, 130 all reachable via corresponding conditions | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 6 exit criteria require all tests above to pass.
