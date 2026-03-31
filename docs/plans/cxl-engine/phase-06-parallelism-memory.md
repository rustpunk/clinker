# Phase 6: Parallelism + Memory Management

**Status:** 🔲 Ready (drilled + validated)
**Depends on:** Phase 5 (Two-Pass Pipeline — Arena, Indexing, Windows)
**Entry criteria:** `cargo test -p clinker-core` passes all Phase 5 tests; two-pass executor handles window functions over grouped/sorted partitions
**Exit criteria:** Rayon chunk-level parallelism produces byte-identical output vs single-threaded (for pipelines not using `now`); RSS tracking works on Linux/macOS/Windows; spill-to-disk IO primitives pass round-trip tests; error threshold + signal handling (SIGINT + SIGTERM) gracefully halt the pipeline; `cargo test -p clinker-core` passes 35+ new tests
**Validated:** 2026-03-30 — see `VALIDATION-phase-06.md`

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
| `test_empty_pipeline_zero_records` | 0-record CSV input → exit code 0, no crash, no threshold violation | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 6.2 status remains `Blocked` until all tests above pass.

#### Sub-tasks
<!-- 1-3 hours each. Split again if still too large. -->
- [ ] **6.1.0** (Prep) Fix pre-existing `build_eval_context` bug: freeze `pipeline_start_time` once at pipeline start, pass as parameter. Current code recreates it per record via `chrono::Local::now()`, causing drift.
- [ ] **6.1.1** Build explicit `rayon::ThreadPool` at pipeline startup from `ParallelismProfile::worker_threads`. Store as a local in `execute_two_pass` / `execute_streaming`, pass via `pool.install(|| ...)`.

> ✅ **[V-7-1] Resolved:** `now` keyword is wall-clock by design — users need it for
> timestamps and time-based filtering. Determinism guarantee applies to pipelines not
> using `now`. `pipeline.start_time` is frozen once (deterministic). Golden file tests
> must not use transforms referencing `now`.

> ✅ **[V-7-2] Resolved:** `build_eval_context` `pipeline_start_time` bug fixed in
> sub-task 6.1.0 — freeze once at pipeline start, pass to all eval contexts.
- [ ] **6.1.2** Restructure Phase 2 loop from single-record iteration (`for pos in 0..record_count`) to batched chunks: read `chunk_size` records into `Vec<(u32, Record)>`, process chunk, write chunk, drop chunk.
- [ ] **6.1.3** Apply `pool.install(|| chunk.par_iter_mut().for_each(...))` for Stateless/IndexReading transforms; fall back to sequential `for` loop for Sequential transforms. Use the most restrictive `ParallelismClass` across all transforms in the pipeline for the chunk dispatch decision.
- [ ] **6.1.4** Wire `--threads` CLI flag through `ConcurrencyConfig` → `ParallelismProfile::worker_threads` → `ThreadPoolBuilder::new().num_threads(n)`.
- [ ] **6.1.5** (Optional) Add `par_iter_mut` to Phase 1.5 partition sorting — spec §8.2 Level 2. Low priority: partition counts are small (tens to hundreds).

> 🟡 **[V-3-2] Accepted:** Spec §8.2 Level 2 partition-level parallelism not an explicit
> task. Added as optional 6.1.5. Optimization gap, not functional — partition counts
> are too small for meaningful speedup.

#### Code scaffolding
```rust
// In crates/clinker-core/src/executor.rs — execute_two_pass, Phase 2 chunk loop

use rayon::prelude::*;

let chunk_size = config.pipeline.concurrency
    .as_ref()
    .and_then(|c| c.chunk_size)
    .unwrap_or(1024) as u32;

let pool = rayon::ThreadPoolBuilder::new()
    .num_threads(plan.parallelism.worker_threads)
    .thread_name(|i| format!("cxl-worker-{i}"))
    .build()
    .map_err(|e| PipelineError::ThreadPool(e.to_string()))?;

// Determine dispatch mode: parallel if ALL transforms are Stateless or IndexReading
let use_parallel = plan.parallelism.per_transform.iter().all(|c| {
    matches!(c, ParallelismClass::Stateless | ParallelismClass::IndexReading)
});

for chunk_start in (0..record_count).step_by(chunk_size as usize) {
    let chunk_end = (chunk_start + chunk_size).min(record_count);
    let mut chunk: Vec<(u32, Record, Result<IndexMap<String, Value>, _>)> =
        (chunk_start..chunk_end)
            .map(|pos| (pos, build_record_from_arena(pos), Ok(IndexMap::new())))
            .collect();

    if use_parallel {
        pool.install(|| {
            chunk.par_iter_mut().for_each(|(pos, record, result)| {
                let ctx = build_eval_context(config, &input.path, *pos as u64 + 1);
                *result = evaluate_record_with_window(
                    record, transforms, &ctx, plan, &arena, &indices, *pos,
                );
            });
        });
    } else {
        for (pos, record, result) in chunk.iter_mut() {
            let ctx = build_eval_context(config, &input.path, *pos as u64 + 1);
            *result = evaluate_record_with_window(
                record, transforms, &ctx, plan, &arena, &indices, *pos,
            );
        }
    }

    // Write chunk results inline (single-threaded)
    for (pos, record, result) in &chunk {
        // project + write or handle error...
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| Chunk loop + pool | `crates/clinker-core/src/executor.rs` | Inline in `execute_two_pass` and `execute_streaming` |
| `PipelineError::ThreadPool` | `crates/clinker-core/src/error.rs` | New variant for pool build failure |

#### Intra-phase dependencies
- Requires: Phase 5 complete (Arena, SecondaryIndex, PartitionWindowContext working)
- Unblocks: Task 6.2

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Explicit pool respects the configured thread count default.
    #[test]
    fn test_thread_pool_default_count() {
        let expected = std::cmp::min(
            num_cpus::get().saturating_sub(2),
            4,
        );
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(expected)
            .build()
            .unwrap();
        assert_eq!(pool.current_num_threads(), expected);
    }

    /// CLI --threads override is respected by explicit pool.
    #[test]
    fn test_thread_pool_cli_override() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        assert_eq!(pool.current_num_threads(), 2);
    }

    /// Stateless transform produces identical output with 1 vs 4 threads.
    #[test]
    fn test_par_stateless_deterministic_output() {
        // Arrange: 1000-record chunk, stateless CXL transform
        // Act: run with pool(1), run with pool(4)
        // Assert: output vecs are identical element-by-element
    }

    /// IndexReading (window) transform produces identical output with 1 vs 4 threads.
    #[test]
    fn test_par_index_reading_deterministic_output() {
        // Arrange: 500-record chunk with window.sum(), Arena + SecondaryIndex
        // Act: run with pool(1), run with pool(4)
        // Assert: output vecs are identical element-by-element
    }

    /// Sequential transform does not dispatch to rayon pool.
    #[test]
    fn test_sequential_not_parallelized() {
        // Arrange: transform with window.lag(1) classified Sequential
        // Act: run pipeline
        // Assert: chunk processed via sequential for loop, not par_iter_mut
        // Verify by checking the use_parallel flag is false
    }

    /// Full CSV→CXL→CSV pipeline: byte-identical with --threads 1 vs --threads 4.
    #[test]
    fn test_golden_file_diff_1_vs_4_threads() {
        // Arrange: CSV input, stateless CXL
        // Act: run_with_readers_writers with pool(1), then pool(4)
        // Assert: output bytes are identical
    }

    /// Arc<TypedProgram> is shared, not cloned per record.
    #[test]
    fn test_arc_ast_shared_not_cloned() {
        // Arrange: compile CXL, wrap in Arc
        // Act: run parallel evaluation on 100 records
        // Assert: Arc::strong_count == 1 (no additional clones)
    }
}
```

#### Risk / gotcha
> **Mixed parallelism classes:** If transform A is `Stateless` and transform B is
> `Sequential`, the entire chunk must be processed sequentially (most restrictive wins).
> The dispatch decision uses `plan.parallelism.per_transform.iter().all(...)`, not
> per-transform branching. A future optimization could interleave parallel and sequential
> transforms within a chunk, but v1 uses the conservative approach.

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
- Each source thread builds its own `Arena` + `SecondaryIndex` locally. After `thread::scope` joins, all arenas and indices are moved into the shared `IngestionOutput`.
- If a source depends on another (e.g., reference data required before main data), the dependent source waits for its predecessor to complete.
- Error in any source thread propagates to the main thread via `Result` — fail-fast semantics.

**Acceptance criteria:**
- [ ] Independent sources read concurrently within `thread::scope`
- [ ] Dependent sources respect DAG ordering
- [ ] Each source thread owns its Arena + SecondaryIndex
- [ ] Arenas moved into shared IngestionOutput after join
- [ ] Source-thread error propagates to main thread

**Required unit tests (must pass before Task 6.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_independent_sources_concurrent` | Two independent sources: both threads execute (verify via timestamps or thread IDs, wall-clock < 2x single-source time) | ⛔ Hard gate |
| `test_dependent_sources_sequential` | Source B depends on Source A: B starts only after A completes | ⛔ Hard gate |
| `test_source_thread_owns_arena` | Each source thread produces a distinct Arena; no shared mutable state during read | ⛔ Hard gate |
| `test_arenas_moved_to_context` | After join, `IngestionOutput` contains arenas from all sources | ⛔ Hard gate |
| `test_source_error_propagation` | Malformed source file in one thread → entire scope returns Err | ⛔ Hard gate |
| `test_single_source_no_spawn` | Pipeline with one source: no extra thread spawned (fast path) | ⛔ Hard gate |
| `test_signal_during_phase1_ingestion` | Set `SHUTDOWN_REQUESTED` during Arena::build → ingestion aborts cleanly, no panic | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 6.3 status remains `Blocked` until all tests above pass.

#### Sub-tasks
<!-- 1-3 hours each. Split again if still too large. -->
- [ ] **6.2.1** Define `IngestionOutput` struct in `clinker-core::pipeline::ingestion` with `arenas: HashMap<String, Arena>` and `indices: HashMap<String, Vec<SecondaryIndex>>`.
- [ ] **6.2.2** Implement `ingest_sources()` — iterates `plan.source_dag` tiers in order, spawns `thread::scope` threads for all sources within each tier, joins between tiers, collects results into `IngestionOutput`.
- [ ] **6.2.3** Single-source fast path — when `config.inputs.len() == 1`, call `ingest_one_source()` directly without `thread::scope`.
- [ ] **6.2.4** Add periodic `shutdown_requested()` check in Arena::build loop (every N records) so SIGINT during Phase 1 ingestion is responsive.

> ⚠️ **[V-5-3] Edge case:** SIGINT during Phase 1 (Arena building) has no shutdown check
> point in the current tight loop. Sub-task 6.2.4 adds periodic sampling.

#### Code scaffolding
```rust
// Module: crates/clinker-core/src/pipeline/ingestion.rs

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::PipelineConfig;
use crate::error::PipelineError;
use crate::pipeline::arena::Arena;
use crate::pipeline::index::SecondaryIndex;
use crate::plan::execution::ExecutionPlan;

/// Data products of Phase 1 ingestion, per source.
/// Consumed immutably by Phase 1.5 (sorting) and Phase 2 (evaluation).
pub struct IngestionOutput {
    pub arenas: HashMap<String, Arena>,
    pub indices: HashMap<String, Vec<SecondaryIndex>>,
}

/// Run Phase 1 ingestion across all sources, respecting the SourceTier DAG.
///
/// Streaming: each source thread builds its own Arena + indices locally.
/// Blocking: tiers execute sequentially; sources within a tier run concurrently.
pub fn ingest_sources(
    plan: &ExecutionPlan,
    config: &PipelineConfig,
) -> Result<IngestionOutput, PipelineError> {
    if config.inputs.len() == 1 {
        return ingest_single_source(plan, config);
    }

    let mut output = IngestionOutput {
        arenas: HashMap::new(),
        indices: HashMap::new(),
    };

    for tier in &plan.source_dag {
        std::thread::scope(|s| {
            let handles: Vec<_> = tier.sources.iter().map(|source_name| {
                s.spawn(|| ingest_one_source(source_name, plan, config))
            }).collect();

            for handle in handles {
                let (name, arena, idxs) = handle.join().unwrap()?;
                output.arenas.insert(name.clone(), arena);
                output.indices.insert(name, idxs);
            }
            Ok::<(), PipelineError>(())
        })?;
    }

    Ok(output)
}

/// Ingest a single source: build Arena + SecondaryIndices.
fn ingest_one_source(
    source_name: &str,
    plan: &ExecutionPlan,
    config: &PipelineConfig,
) -> Result<(String, Arena, Vec<SecondaryIndex>), PipelineError> {
    todo!()
}

/// Fast path for single-source pipelines — no thread::scope overhead.
fn ingest_single_source(
    plan: &ExecutionPlan,
    config: &PipelineConfig,
) -> Result<IngestionOutput, PipelineError> {
    todo!()
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `IngestionOutput` | `crates/clinker-core/src/pipeline/ingestion.rs` | New module |
| `ingest_sources()` | `crates/clinker-core/src/pipeline/ingestion.rs` | Entry point for Phase 1 |
| `ingest_one_source()` | `crates/clinker-core/src/pipeline/ingestion.rs` | Per-source ingestion |

#### Intra-phase dependencies
- Requires: Task 6.1 (rayon pool working — though this task uses `thread::scope`, the pool must exist for Phase 1.5 partition sorting)
- Unblocks: Task 6.3

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Two independent sources are ingested concurrently (wall-clock evidence).
    #[test]
    fn test_independent_sources_concurrent() {
        // Arrange: two CSV sources, each with artificial 100ms read delay
        // Act: ingest_sources() with two-source config
        // Assert: total wall-clock < 2 * 100ms (concurrency evidence)
    }

    /// Dependent source waits for its predecessor tier to complete.
    #[test]
    fn test_dependent_sources_sequential() {
        // Arrange: source "ref" in tier 0, source "primary" in tier 1
        // Act: ingest_sources() — record completion timestamps
        // Assert: tier 0 completes before tier 1 starts
    }

    /// Each source thread produces a distinct Arena with independent data.
    #[test]
    fn test_source_thread_owns_arena() {
        // Arrange: two sources with different schemas
        // Act: ingest_sources()
        // Assert: output.arenas has two entries with different schemas
    }

    /// After join, IngestionOutput contains arenas from all sources.
    #[test]
    fn test_arenas_moved_to_context() {
        // Arrange: three sources
        // Act: ingest_sources()
        // Assert: output.arenas.len() == 3, all source names present
    }

    /// Error in one source thread propagates to the caller.
    #[test]
    fn test_source_error_propagation() {
        // Arrange: two sources, one with malformed CSV
        // Act: ingest_sources()
        // Assert: returns Err
    }

    /// Single-source pipeline skips thread::scope entirely.
    #[test]
    fn test_single_source_no_spawn() {
        // Arrange: one-source config
        // Act: ingest_sources()
        // Assert: succeeds, output.arenas.len() == 1
        // (Verify no thread::scope by checking thread::current().name() is main)
    }
}
```

#### Risk / gotcha
> **File handle exhaustion with many sources:** Each source thread opens its own
> CSV/XML/JSON reader file handle. With the spec's target of 1-5 inputs and at most
> 2 tiers, this is at most 5 concurrent file handles — well within OS limits. No
> mitigation needed for v1.

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
- [x] `rss_bytes()` returns `Some(n)` on Linux, macOS, and Windows
- [x] `rss_bytes()` returns `None` on unsupported platforms
- [x] `MemoryBudget::should_spill` returns true when RSS exceeds threshold
- [x] `--memory-limit` CLI flag parsed and applied
- [x] RSS measurement cost is < 10us per call

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

#### Sub-tasks
<!-- 1-3 hours each. Split again if still too large. -->
- [x] **6.3.0** (Prep) Add `libc` and `num_cpus` to `clinker-core/Cargo.toml` as direct dependencies.
- [x] **6.3.1** Implement `fn rss_bytes() -> Option<u64>` with `#[cfg(target_os)]` platform dispatch. Linux uses `/proc/self/statm` + `libc::sysconf(libc::_SC_PAGESIZE)`. macOS uses `mach2::mach_task_basic_info`. Windows uses `windows-sys K32GetProcessMemoryInfo`. Unsupported returns `None`.
- [x] **6.3.1a** (Errata 2026-03-31) macOS and Windows stubs replaced with real implementations. `mach2` 0.6 and `windows-sys` 0.61 added as platform-gated deps in `clinker-core/Cargo.toml`. Commit: `fda0640`.

> 🟡 **[V-4-1] Accepted:** `libc` and `num_cpus` not yet in Cargo.toml. Added prep
> sub-task 6.3.0.
- [ ] **6.3.2** Implement `MemoryBudget` struct with `new()`, `from_config()`, and `should_spill()`. `from_config()` reads `PipelineMeta.memory_limit` via the existing `parse_memory_limit()` helper in `executor.rs` (refactor to shared location).
- [ ] **6.3.3** Wire `--memory-limit` through existing `PipelineMeta.memory_limit` field → `MemoryBudget::from_config()` at executor startup. No new config fields needed — the plumbing already exists.

#### Code scaffolding
```rust
// Module: crates/clinker-core/src/pipeline/memory.rs

/// Cross-platform RSS measurement. Returns None on unsupported platforms.
pub fn rss_bytes() -> Option<u64> {
    rss_bytes_impl()
}

#[cfg(target_os = "linux")]
fn rss_bytes_impl() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
    Some(resident_pages * page_size)
}

#[cfg(target_os = "macos")]
fn rss_bytes_impl() -> Option<u64> {
    use mach2::task::task_info;
    use mach2::task_info::{MACH_TASK_BASIC_INFO, mach_task_basic_info_t};
    use mach2::traps::mach_task_self;
    // ... mach_task_basic_info FFI call ...
    todo!()
}

#[cfg(target_os = "windows")]
fn rss_bytes_impl() -> Option<u64> {
    use windows_sys::Win32::System::ProcessStatus::{
        K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    // ... K32GetProcessMemoryInfo FFI call ...
    todo!()
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn rss_bytes_impl() -> Option<u64> {
    None
}

/// Memory budget governing spill decisions.
///
/// Polled once per chunk at the start of Phase 2 processing.
/// Streaming: RSS is the only signal. Blocking: not applicable until Phase 8 sort.
pub struct MemoryBudget {
    /// Total memory limit in bytes. Default: 512MB.
    pub limit: u64,
    /// Fraction of limit at which spill triggers. Default: 0.60.
    pub spill_threshold_pct: f64,
}

impl MemoryBudget {
    pub fn new(limit: u64, spill_threshold_pct: f64) -> Self {
        Self { limit, spill_threshold_pct }
    }

    /// Build from PipelineMeta.memory_limit string ("512M", "2G", raw bytes).
    pub fn from_config(memory_limit: Option<&str>) -> Self {
        let limit = parse_memory_limit_bytes(memory_limit);
        Self::new(limit, 0.60)
    }

    /// Returns true when current RSS exceeds the spill threshold.
    /// Returns false if RSS cannot be measured (unsupported platform).
    pub fn should_spill(&self) -> bool {
        rss_bytes().map_or(false, |rss| {
            rss > (self.limit as f64 * self.spill_threshold_pct) as u64
        })
    }
}

/// Parse memory limit string to bytes. Supports "512M", "2G", raw integer.
/// Returns 512MB default if None or unparseable.
pub fn parse_memory_limit_bytes(s: Option<&str>) -> u64 {
    // Refactored from executor.rs parse_memory_limit()
    todo!()
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `rss_bytes()` | `crates/clinker-core/src/pipeline/memory.rs` | New module |
| `MemoryBudget` | `crates/clinker-core/src/pipeline/memory.rs` | Struct + impl |
| `parse_memory_limit_bytes()` | `crates/clinker-core/src/pipeline/memory.rs` | Refactored from `executor.rs::parse_memory_limit()` |

#### Intra-phase dependencies
- Requires: Task 6.2 (source parallelism — memory tracking is meaningless without multi-source ingestion working)
- Unblocks: Task 6.4

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// On Linux/macOS/Windows, rss_bytes() returns a positive value.
    #[test]
    fn test_rss_bytes_returns_some() {
        let rss = rss_bytes();
        assert!(rss.is_some(), "rss_bytes() should return Some on this platform");
        assert!(rss.unwrap() > 0, "RSS should be positive");
    }

    /// Allocating 10MB increases RSS by at least 5MB.
    #[test]
    fn test_rss_bytes_increases_after_alloc() {
        let before = rss_bytes().unwrap();
        let _big_vec: Vec<u8> = vec![0u8; 10 * 1024 * 1024];
        // Touch the memory to ensure it's resident
        std::hint::black_box(&_big_vec);
        let after = rss_bytes().unwrap();
        assert!(after >= before + 5 * 1024 * 1024,
            "RSS should increase by at least 5MB, got before={before} after={after}");
    }

    /// Budget 512MB with RSS well below threshold → should_spill returns false.
    #[test]
    fn test_memory_budget_below_threshold() {
        let budget = MemoryBudget::new(512 * 1024 * 1024, 0.60);
        // Current test process RSS should be well under 307MB
        assert!(!budget.should_spill());
    }

    /// Budget set to 1MB → should_spill returns true (test process always exceeds 1MB).
    #[test]
    fn test_memory_budget_above_threshold() {
        let budget = MemoryBudget::new(1024 * 1024, 0.60);
        assert!(budget.should_spill());
    }

    /// Default values: 512MB limit, 60% threshold.
    #[test]
    fn test_memory_budget_default_values() {
        let budget = MemoryBudget::from_config(None);
        assert_eq!(budget.limit, 512 * 1024 * 1024);
        assert!((budget.spill_threshold_pct - 0.60).abs() < f64::EPSILON);
    }

    /// Parse memory limit suffixes.
    #[test]
    fn test_memory_limit_cli_parse_suffixes() {
        assert_eq!(parse_memory_limit_bytes(Some("512M")), 536_870_912);
        assert_eq!(parse_memory_limit_bytes(Some("2G")), 2_147_483_648);
        assert_eq!(parse_memory_limit_bytes(Some("1024")), 1024);
        assert_eq!(parse_memory_limit_bytes(None), 512 * 1024 * 1024);
    }
}
```

#### Risk / gotcha
> **RSS is process-wide, not pipeline-scoped:** RSS includes all process memory —
> config parsing, CXL compilation, the test harness itself. The 60% threshold provides
> headroom for non-pipeline allocations. A pipeline that barely fits in budget could
> trigger spill unnecessarily. Acceptable for v1; a future v2 could track pipeline-only
> allocations via a custom allocator wrapper.

---

### Task 6.4: Spill-to-Disk Infrastructure
**Status:** ⛔ Blocked (waiting on Task 6.3)
**Blocked by:** Task 6.3 — RSS tracking and MemoryBudget must be working

**Description:**
Implement `SpillWriter` and `SpillReader` that serialize records to LZ4-compressed NDJSON
on disk. These are reusable IO primitives — the actual spill triggers (sort buffer overflow,
LRU eviction for blocking stages) are implemented in Phase 8 (Sort).

**Implementation notes:**
- `SpillWriter`: accepts `Record` references, manually serializes each to a JSON object using `schema.columns()` + `record.values` + `record.overflow`, compresses the NDJSON stream with `lz4_flex` frame compression, writes to a `tempfile::NamedTempFile`.
- `SpillReader`: opens a spill file, decompresses via `lz4_flex::frame::FrameDecoder`, reads schema from the first line, deserializes NDJSON lines back into `Record` instances.
- `tempfile` crate handles automatic cleanup on drop — no manual deletion required.
- Spill files are written to the system temp directory by default; overridable via `--spill-dir` CLI flag.
- Schema is written as the first line of each spill file (JSON array of column names) so the reader can reconstruct `Arc<Schema>` without external state.
- No `Serialize`/`Deserialize` derives on `Record` or `Schema` — `SpillWriter` manually builds JSON objects from schema + values to avoid coupling the data model to serde.

**Scope clarification:** Phase 6 builds `SpillWriter`/`SpillReader` as reusable primitives
and wires `MemoryBudget::should_spill()` into the Phase 2 chunk loop as a check point.
The actual spill *triggers* for blocking stages (external merge sort, distinct) are
implemented in Phase 8 where those stages accumulate data. The Phase 2 streaming pipeline
drops chunks after processing — there are no in-memory chunks to evict.

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

#### Sub-tasks
<!-- 1-3 hours each. Split again if still too large. -->
- [ ] **6.4.1** Implement `SpillWriter` — constructor takes `Arc<Schema>` and optional `spill_dir: &Path`. Writes schema line (JSON array of column names), then accepts records via `write_record()`. Each record is manually serialized to a JSON object (schema fields + overflow fields). Stream is LZ4 frame-compressed. `finish()` consumes the writer and returns a `SpillFile` handle.
- [ ] **6.4.2** Implement `SpillReader` as `Iterator<Item = Result<Record, SpillError>>` — opens a `SpillFile`, reads schema from first line, reconstructs `Arc<Schema>`, deserializes subsequent NDJSON lines back to `Record` using the schema for positional mapping.
- [ ] **6.4.3** Wire `--spill-dir` CLI flag through `PipelineMeta` (new optional field) → `SpillWriter::new()`. Default: `std::env::temp_dir()`. Early validation: `canonicalize()` the path, verify it exists, is a directory, and is writable — fail fast at startup before Phase 1 ingestion.

> 🟠 **[V-8-2] Resolved:** `--spill-dir` now validated at startup (canonicalize + exists
> + is_dir + writable). Prevents late failures after ingestion on typos or bad paths.

#### Code scaffolding
```rust
// Module: crates/clinker-core/src/pipeline/spill.rs

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use tempfile::NamedTempFile;

use clinker_record::{Record, Schema, Value};

/// Error type for spill operations.
#[derive(Debug)]
pub enum SpillError {
    Io(std::io::Error),
    Json(serde_json::Error),
    InvalidSchema(String),
}

/// Writes records to an LZ4-compressed NDJSON spill file.
///
/// Format:
///   Line 0: JSON array of column names (schema)
///   Line 1..N: JSON objects with column names as keys (records)
///
/// All lines are LZ4 frame-compressed as a single stream.
pub struct SpillWriter {
    encoder: FrameEncoder<BufWriter<NamedTempFile>>,
    schema: Arc<Schema>,
}

impl SpillWriter {
    /// Create a new SpillWriter. Writes to system temp dir or `spill_dir` if provided.
    pub fn new(schema: Arc<Schema>, spill_dir: Option<&Path>) -> Result<Self, SpillError> {
        todo!()
    }

    /// Serialize one record as an NDJSON line.
    /// Manually builds JSON from schema columns + values + overflow.
    pub fn write_record(&mut self, record: &Record) -> Result<(), SpillError> {
        todo!()
    }

    /// Flush, finalize LZ4 frame, return handle to the completed spill file.
    pub fn finish(self) -> Result<SpillFile, SpillError> {
        todo!()
    }
}

/// Handle to a completed spill file. Auto-deletes on drop via TempPath.
pub struct SpillFile {
    path: tempfile::TempPath,
    schema: Arc<Schema>,
}

impl SpillFile {
    /// Open a reader over this spill file.
    pub fn reader(&self) -> Result<SpillReader, SpillError> {
        todo!()
    }

    /// Schema stored in this spill file.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

/// Reads records from an LZ4-compressed NDJSON spill file.
pub struct SpillReader {
    lines: std::io::Lines<BufReader<FrameDecoder<std::fs::File>>>,
    schema: Arc<Schema>,
}

impl Iterator for SpillReader {
    type Item = Result<Record, SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `SpillWriter` | `crates/clinker-core/src/pipeline/spill.rs` | New module |
| `SpillReader` | `crates/clinker-core/src/pipeline/spill.rs` | Iterator-based reader |
| `SpillFile` | `crates/clinker-core/src/pipeline/spill.rs` | Handle with auto-cleanup |
| `SpillError` | `crates/clinker-core/src/pipeline/spill.rs` | Spill-specific error type |

#### Intra-phase dependencies
- Requires: Task 6.3 (`MemoryBudget` — spill check point wired into chunk loop)
- Unblocks: Task 6.5

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;

    /// Round-trip 100 records with all 8 Value variants.
    #[test]
    fn test_spill_roundtrip_all_value_types() {
        // Arrange: schema with 8 columns, 100 records cycling all Value types
        // (Null, Bool, Integer, Float, String, Date, DateTime, Array)
        let schema = Arc::new(Schema::new(vec![
            "null_col".into(), "bool_col".into(), "int_col".into(),
            "float_col".into(), "str_col".into(), "date_col".into(),
            "dt_col".into(), "arr_col".into(),
        ]));

        // Act: write via SpillWriter, read back via SpillReader
        // Assert: each record matches field-by-field
    }

    /// Schema column names and order survive the round-trip.
    #[test]
    fn test_spill_schema_preserved() {
        // Arrange: schema ["z_col", "a_col", "m_col"] (non-alphabetical)
        // Act: write + read
        // Assert: reader schema columns == ["z_col", "a_col", "m_col"]
    }

    /// LZ4 compression achieves < 80% of raw NDJSON size.
    #[test]
    fn test_spill_lz4_compression_ratio() {
        // Arrange: 1000 records of typical tabular data (repetitive strings)
        // Act: write via SpillWriter, measure file size
        // Also: serialize same records as raw NDJSON, measure size
        // Assert: spill_size < 0.80 * raw_size
    }

    /// Spill file is deleted when SpillFile handle is dropped.
    #[test]
    fn test_spill_tempfile_cleanup() {
        // Arrange: write records, finish() to get SpillFile
        let path = spill_file.path.to_path_buf();
        assert!(path.exists());
        // Act: drop the SpillFile
        drop(spill_file);
        // Assert: file no longer exists
        assert!(!path.exists());
    }

    /// Zero records produces a valid spill file (schema-only).
    #[test]
    fn test_spill_empty_chunk() {
        // Arrange: schema, no records written
        // Act: finish() immediately, then reader()
        // Assert: iterator yields None on first call
    }

    /// Records with overflow fields survive the round-trip.
    #[test]
    fn test_spill_overflow_fields_preserved() {
        // Arrange: record with schema fields + overflow IndexMap
        // Act: write + read
        // Assert: overflow fields present and values match
    }

    /// --spill-dir places files in the specified directory.
    #[test]
    fn test_spill_dir_override() {
        // Arrange: create a temp dir
        // Act: SpillWriter::new(schema, Some(&custom_dir))
        // Assert: spill file path starts with custom_dir
    }
}
```

#### Risk / gotcha
> **Deserialize performance for large spill files:** `serde_json::from_str` per NDJSON
> line is not the fastest JSON parser. For v1 with ≤100MB inputs and typical 2-4 spill
> files, this is acceptable (~50-100MB/s parse throughput). If profiling shows it's a
> bottleneck in Phase 8's merge sort, switch to `serde_json::from_slice` on a reused
> byte buffer to avoid per-line String allocation.

---

### Task 6.5: Error Threshold + Signal Handling
**Status:** ⛔ Blocked (waiting on Task 6.4)
**Blocked by:** Task 6.4 — spill infrastructure must be working (pipeline must handle memory pressure before adding graceful shutdown)

**Description:**
Enforce the `type_error_threshold` at chunk boundaries and handle OS signals (SIGINT/SIGTERM)
for graceful pipeline shutdown.

**Implementation notes:**
- `type_error_threshold` (from config): a `f64` in `[0.0, 1.0]`. At each chunk boundary, compute error rate. Denominator depends on execution mode:
  - **TwoPass:** `error_count / total_records` where `total_records` is known from Phase 1 Arena size.
  - **Streaming:** `error_count / records_processed_so_far` (running ratio — Arena not available).
  - If ratio exceeds threshold, halt pipeline with exit code 3.
- `ctrlc` crate registers a handler that sets an `AtomicBool` flag (`SHUTDOWN_REQUESTED`).
- Chunk boundary check: after processing each chunk, check the `AtomicBool`. If set: flush all output buffers, close file handles, write DLQ summary to stderr, exit with code 130 (standard SIGINT convention).
- DLQ summary format: `"DLQ: {dlq_count} records written to {dlq_path}"` on stderr.
- Exit codes (per spec §10.2 + Unix signal convention):
  - 0 = success, zero errors
  - 1 = config or CXL compile error (no data processed)
  - 2 = partial success — some rows routed to DLQ
  - 3 = fatal data error — fail_fast triggered, error rate threshold exceeded, or NaN in group_by key
  - 4 = I/O or system error
  - 130 = interrupted by SIGINT/SIGTERM

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
| `test_exit_codes_documented` | Exit codes 0, 1, 2, 3, 4, 130 all reachable via corresponding conditions | ⛔ Hard gate |
| `test_shutdown_dlq_summary_to_stderr` | Set shutdown flag, pipeline with DLQ records → stderr contains DLQ summary line | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 6 exit criteria require all tests above to pass.

> 🟡 **[V-5-1] Resolved:** Added `test_shutdown_dlq_summary_to_stderr` — acceptance
> criterion "DLQ summary written to stderr on shutdown" previously had no test.

#### Sub-tasks
<!-- 1-3 hours each. Split again if still too large. -->
- [ ] **6.5.1** Implement `ErrorThreshold` struct in `clinker-core::pipeline::threshold` with `new(limit, total_records: Option<u64>)` and `exceeded(error_count, records_processed) -> bool`. TwoPass passes `Some(arena.record_count())`, Streaming passes `None`.
- [ ] **6.5.2** Implement signal handling in `clinker-core::pipeline::shutdown`: static `AtomicBool SHUTDOWN_REQUESTED`, `install_signal_handler()` using `ctrlc::set_handler_with_signals(&[Signal::Int, Signal::Term], ...)` to catch both SIGINT and SIGTERM. Wrap in `std::sync::Once` so repeated calls in tests are safe.

> 🟡 **[V-7-3] Resolved:** Updated to use `set_handler_with_signals` with explicit
> SIGTERM. Default `set_handler()` only catches SIGINT — SIGTERM (sent by `kill`,
> Docker stop, systemd) would be silently ignored.
- [ ] **6.5.3** Define exit code constants in `clinker-core::exit_codes` module: `EXIT_SUCCESS=0`, `EXIT_CONFIG_ERROR=1`, `EXIT_PARTIAL_DLQ=2`, `EXIT_FATAL_DATA=3`, `EXIT_IO_ERROR=4`, `EXIT_INTERRUPTED=130`. Wire into executor chunk loop: check `ErrorThreshold::exceeded()` and `shutdown_requested()` after each chunk.

#### Code scaffolding
```rust
// Module: crates/clinker-core/src/pipeline/threshold.rs

/// Error threshold checker. Denominator strategy depends on execution mode.
///
/// - TwoPass: denominator is total_records from Phase 1 Arena (known upfront).
/// - Streaming: denominator is records_processed_so_far (running ratio).
pub struct ErrorThreshold {
    /// Error rate limit in [0.0, 1.0].
    limit: f64,
    /// Some(n) for TwoPass (Arena count), None for Streaming.
    total_records: Option<u64>,
}

impl ErrorThreshold {
    pub fn new(limit: f64, total_records: Option<u64>) -> Self {
        Self { limit, total_records }
    }

    /// Returns true if error rate strictly exceeds the threshold.
    /// Called at chunk boundaries.
    pub fn exceeded(&self, error_count: u64, records_processed: u64) -> bool {
        if self.limit >= 1.0 { return false; }
        if self.limit <= 0.0 && error_count > 0 { return true; }
        let denominator = self.total_records.unwrap_or(records_processed);
        if denominator == 0 { return false; }
        let rate = error_count as f64 / denominator as f64;
        rate > self.limit
    }
}

// Module: crates/clinker-core/src/pipeline/shutdown.rs

use std::sync::atomic::{AtomicBool, Ordering};

/// Process-wide shutdown flag, set by SIGINT/SIGTERM handler.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Install the signal handler for SIGINT + SIGTERM. Safe to call multiple times
/// (uses Once internally). Must use `set_handler_with_signals` — default
/// `set_handler` only catches SIGINT, missing SIGTERM (kill, Docker stop, systemd).
pub fn install_signal_handler() -> Result<(), String> {
    use std::sync::Once;
    static INIT: Once = Once::new();
    let mut result = Ok(());
    INIT.call_once(|| {
        if let Err(e) = ctrlc::set_handler(|| {
            // Note: implementation must use ctrlc::set_handler_with_signals
            // with [Signal::Int, Signal::Term] to catch both SIGINT and SIGTERM.
            SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        }) {
            result = Err(e.to_string());
        }
    });
    result
}

/// Check if shutdown has been requested. Called at chunk boundaries.
pub fn shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

/// Reset the shutdown flag (for testing only).
#[cfg(test)]
pub fn reset_shutdown_flag() {
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
}

// Module: crates/clinker-core/src/exit_codes.rs

/// Success — zero errors, all records processed.
pub const EXIT_SUCCESS: i32 = 0;

/// Config parse error or CXL compile error — no data processed.
pub const EXIT_CONFIG_ERROR: i32 = 1;

/// Partial success — some rows routed to DLQ, pipeline completed.
pub const EXIT_PARTIAL_DLQ: i32 = 2;

/// Fatal data error — fail_fast triggered, error threshold exceeded,
/// or NaN in group_by key.
pub const EXIT_FATAL_DATA: i32 = 3;

/// I/O or system error — file not found, permission denied, disk full.
pub const EXIT_IO_ERROR: i32 = 4;

/// Interrupted by SIGINT (Ctrl-C) or SIGTERM. Unix convention: 128 + signal.
pub const EXIT_INTERRUPTED: i32 = 130;
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `ErrorThreshold` | `crates/clinker-core/src/pipeline/threshold.rs` | New module |
| `SHUTDOWN_REQUESTED` | `crates/clinker-core/src/pipeline/shutdown.rs` | New module |
| `install_signal_handler()` | `crates/clinker-core/src/pipeline/shutdown.rs` | Once-guarded |
| Exit code constants | `crates/clinker-core/src/exit_codes.rs` | New module |

#### Intra-phase dependencies
- Requires: Task 6.4 (spill infrastructure — pipeline must handle memory pressure before graceful shutdown)
- Unblocks: Phase 6 exit criteria (phase complete)

#### Expanded test stubs
```rust
#[cfg(test)]
mod threshold_tests {
    use super::*;

    /// 5% errors with 10% threshold → pipeline continues.
    #[test]
    fn test_error_threshold_below_limit() {
        let t = ErrorThreshold::new(0.10, Some(1000));
        assert!(!t.exceeded(50, 1000)); // 5% < 10%
    }

    /// 15% errors with 10% threshold → halt.
    #[test]
    fn test_error_threshold_exceeded() {
        let t = ErrorThreshold::new(0.10, Some(1000));
        assert!(t.exceeded(150, 1000)); // 15% > 10%
    }

    /// Threshold 0.0 → any single error halts immediately.
    #[test]
    fn test_error_threshold_zero_means_no_errors() {
        let t = ErrorThreshold::new(0.0, Some(1000));
        assert!(t.exceeded(1, 1));
    }

    /// Threshold 1.0 → pipeline never halts on errors alone.
    #[test]
    fn test_error_threshold_one_means_unlimited() {
        let t = ErrorThreshold::new(1.0, Some(100));
        assert!(!t.exceeded(100, 100)); // 100% is not > 1.0
    }

    /// Streaming mode: denominator is records_processed_so_far.
    #[test]
    fn test_error_threshold_streaming_running_ratio() {
        let t = ErrorThreshold::new(0.10, None); // Streaming — no Arena total
        assert!(t.exceeded(2, 10));   // 20% > 10%
        assert!(!t.exceeded(1, 100)); // 1% < 10%
    }

    /// TwoPass mode: denominator is Arena total, not records processed so far.
    #[test]
    fn test_error_threshold_twopass_uses_total() {
        let t = ErrorThreshold::new(0.10, Some(1000));
        // 10 errors after processing only 10 records: 10/1000 = 1%, not 10/10 = 100%
        assert!(!t.exceeded(10, 10));
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    /// Manually setting the AtomicBool triggers shutdown check.
    #[test]
    fn test_signal_flag_sets_atomic() {
        reset_shutdown_flag();
        assert!(!shutdown_requested());
        SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(shutdown_requested());
        reset_shutdown_flag();
    }

    /// Set shutdown flag mid-pipeline → output file is valid (not truncated).
    #[test]
    fn test_graceful_shutdown_flushes_output() {
        // Arrange: 1000-record pipeline, set SHUTDOWN_REQUESTED after chunk 1
        // Act: run pipeline
        // Assert: output contains chunk 1's records, properly terminated
        // Assert: exit code would be EXIT_INTERRUPTED
    }

    /// All exit codes are reachable.
    #[test]
    fn test_exit_codes_documented() {
        assert_eq!(EXIT_SUCCESS, 0);
        assert_eq!(EXIT_CONFIG_ERROR, 1);
        assert_eq!(EXIT_PARTIAL_DLQ, 2);
        assert_eq!(EXIT_FATAL_DATA, 3);
        assert_eq!(EXIT_IO_ERROR, 4);
        assert_eq!(EXIT_INTERRUPTED, 130);
    }
}
```

#### Risk / gotcha
> **`ctrlc::set_handler` can only be called once per process:** Same global-state
> issue as rayon's `build_global()`. The `Once` guard in `install_signal_handler()`
> prevents panics from double-registration. Tests should manipulate the `AtomicBool`
> directly via `reset_shutdown_flag()` rather than installing the actual signal handler,
> avoiding test-ordering dependencies.

---

## Intra-Phase Dependency Graph

```
Task 6.1 (Rayon Pool + Chunks)
    │
    └──> Task 6.2 (Source-Level Parallelism)
              │
              └──> Task 6.3 (RSS Tracking)
                        │
                        └──> Task 6.4 (Spill Infrastructure)
                                  │
                                  └──> Task 6.5 (Error Threshold + Signals)
```

Critical path: 6.1 → 6.2 → 6.3 → 6.4 → 6.5
Parallelizable: None — strict linear dependency chain. Each task builds on the previous.

---

## Decisions Log (drill-phase — 2026-03-30)
| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| 1 | Inline `par_iter_mut` in executor loop (no ChunkProcessor struct) | Ecosystem consensus: inline swap is the dominant pattern for row-oriented chunk parallelism. ChunkProcessor has no precedent and adds abstraction with no consumer. | Task 6.1 |
| 2 | Explicit `ThreadPool` via `build()` + `pool.install()`, not `build_global()` | `build_global()` is set-once per process — tests asserting thread count become race conditions under `cargo test`. Explicit pool is per-test configurable. Polars + Apollo Rover both use explicit pools. Zero performance difference (rayon maintainer confirmed). | Task 6.1 |
| 3 | Defer crossbeam-channel writer thread — keep inline writing at chunk granularity | I/O-compute overlap is negligible for ≤100MB outputs (kernel page cache already provides it). `FormatWriter` trait is already `Send` — a future `ChannelWriter` adapter slots in with zero executor changes when multicast arrives in Phase 8+. | Task 6.1 |
| 4 | Focused `IngestionOutput` struct (arenas + indices only), not a god-object `ExecutionContext` | DataFusion explicitly refactored away from monolithic ExecutionContext. Plan and thread pool are orthogonal to Phase 1 data products. Arenas + indices travel together through Phase 1.5 and Phase 2 — justifies grouping. | Task 6.2 |
| 5 | `SourceTier` DAG as-is drives `thread::scope` dispatch — no extension needed | `build_source_dag()` already produces topologically sorted tiers. Sources within a tier are independent. Two-tier structure (reference → primary) covers all spec use cases. | Task 6.2 |
| 6 | `libc::sysconf(_SC_PAGESIZE)` for Linux page size | `libc` is already a transitive dependency (pure Rust FFI bindings). Same class as `mach2` and `windows-sys`. Single-line call, ~1us cost. | Task 6.3 |
| 7 | `--memory-limit` flows through existing `PipelineMeta.memory_limit` | Config field and `parse_memory_limit()` helper already exist in executor.rs. Refactor to shared location in `pipeline::memory`. No new plumbing needed. | Task 6.3 |
| 8 | Phase 6 builds SpillWriter/SpillReader as IO primitives; spill triggers live in Phase 8 | Phase 2 streaming drops chunks after processing — no in-memory chunks to evict. Spec §9.1 spill is for blocking stages (sort, distinct) which are Phase 8 scope. Plan's "LRU chunk eviction" language was inaccurate for the streaming model. | Task 6.4 |
| 9 | Manual JSON serialization in SpillWriter, no Serialize/Deserialize on Record or Schema | Avoids coupling the data model to a specific serialization format. SpillWriter builds JSON from schema columns + values + overflow directly. | Task 6.4 |
| 10 | Spec exit codes {0,1,2,3,4} + 130 for SIGINT/SIGTERM | Spec §10.2 is authoritative for pipeline outcomes. 130 is Unix convention (128 + SIGINT=2). No collision with spec codes 0-4. | Task 6.5 |
| 11 | Error threshold denominator: Phase 1 total in TwoPass, running ratio in Streaming | Spec says "total record count is known from Phase 1." Streaming has no Phase 1, so running ratio is the only option. Both behaviors are deterministic per execution mode. | Task 6.5 |

## Decisions Log (validate-phase — 2026-03-30)
| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| 12 | `now` keyword stays wall-clock; non-deterministic by design | Users need wall-clock for timestamps and time filtering. Determinism guarantee applies to non-`now` pipelines. | Task 6.1 golden tests, spec §8.5 |
| 13 | Fix `build_eval_context` to freeze `pipeline_start_time` once | Pre-existing bug: `Local::now()` called per record causes drift. Fix as prep sub-task 6.1.0. | Task 6.1 |
| 14 | Use `ctrlc::set_handler_with_signals` for SIGINT + SIGTERM | Default `set_handler()` misses SIGTERM (kill, Docker stop, systemd). | Task 6.5 |
| 15 | Phase 8 exit codes are the bug, not Phase 6 | Phase 6 matches spec §10.2 and Phase 4 implementation. Phase 8 Task 8.4 needs correction. | Phase 8 |
| 16 | Level 2 partition parallelism is optional optimization | Partition counts are small (tens to hundreds). Added as optional sub-task 6.1.5. | Task 6.1 |
| 17 | Add early `--spill-dir` validation at startup | Canonicalize, verify dir exists + writable. Prevents late failures. | Task 6.4 |

## Assumptions Log (drill-phase — 2026-03-30)
| # | Assumption | Basis | Risk if wrong |
|---|-----------|-------|---------------|
| 1 | CXL transforms have roughly uniform per-record cost | Spec targets field mapping, coercion, string ops — no unbounded computation per record | If highly variable, rayon's binary-tree splitting may leave cores idle. Mitigate with `with_max_len()` tuning. |
| 2 | `Arena` is `Send + Sync` and safe to share across rayon workers | Arena is `Vec<MinimalRecord>` + `Arc<Schema>`, both are Send + Sync. Verified in Phase 5 code. | If not, compiler will reject `par_iter_mut` closures capturing `&Arena`. Mechanical fix: wrap in `Arc`. |
| 3 | Single output per pipeline in Phase 6 | Spec targets 1-5 inputs but current config supports one output. Multicast is Phase 8+. | If multiple outputs are needed sooner, writer thread + channel becomes necessary. |
| 4 | `ctrlc` crate is pure Rust and handles both SIGINT and SIGTERM | Spec §2 crate table confirms. `ctrlc` 3.x registers for both signals on Unix. | If it only handles SIGINT, add `signal-hook` for SIGTERM (also pure Rust). |
| 5 | RSS ≤100MB typical for Arena with ≤500K records from ≤100MB inputs | Spec §6.1 estimate: ~100-200 bytes/record × 500K = 50-100MB | If Arena is larger than expected, the 60% spill threshold on a 512MB budget triggers prematurely. Phase 8 sort spill will handle this. |
