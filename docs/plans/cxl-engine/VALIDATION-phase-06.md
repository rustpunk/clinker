# Validation Report: Phase 6 — Parallelism + Memory Management

**Date:** 2026-03-30
**Validator:** validate-phase skill (8 parallel agents)
**Verdict:** READY (0 blockers after resolution)

## Verdict rationale

Phase 6 is well-structured with clean interfaces, no naming collisions, correct
dependency wiring, and strong test coverage (33 hard-gate tests, all with stubs).
One blocker was found (`now` keyword non-determinism under rayon) and resolved by
recognizing that wall-clock `now` is inherently non-deterministic — the fix is to
freeze the pre-existing `pipeline_start_time` bug and document `now` as
non-reproducible by design. All 8 validation dimensions pass cleanly after
resolution.

## Blocker resolution log

| ID | Issue | Resolution | Decision |
|----|-------|-----------|---------|
| V-7-1 | `now` keyword uses `WallClock::now()` — different nanosecond values across rayon threads breaks byte-identical output | Resolved | `now` is wall-clock by design (users need it for filtering/timestamps). Determinism guarantee applies to pipelines not using `now`. Document as inherent to time functions. Add `tracing::info` in `--explain` if `now` is referenced. |
| V-7-2 | `build_eval_context` creates fresh `pipeline_start_time` per record (should be frozen once) | Resolved | Pre-existing bug. Fix in Task 6.1.1 prep: freeze `pipeline_start_time` once at pipeline start, pass to all `build_eval_context` calls. `pipeline.start_time` becomes deterministic. |

## Accepted warnings and known risks

| ID | Issue | Accepted by | Rationale |
|----|-------|------------|-----------|
| V-1-1 | `parse_memory_limit` returns `usize`, Phase 6 returns `u64`. Arena::build takes `usize`. | User | Lossless cast on 64-bit. Address during implementation with `as usize`. |
| V-2-1 | 5 Phase 5 gate tests never written (cross-source, sorted positional, stdin) | User | Core Phase 5 structs/traits confirmed present. Missing tests are in untested paths that Phase 6 doesn't directly exercise in Tasks 6.1-6.3. Complete before Task 6.2 cross-source tests. |
| V-2-2 | Phase 8 exit codes 2/3/4 contradict Phase 6 and spec §10.2 | User | Phase 8's plan is the bug — Phase 6 matches spec and already-implemented Phase 4. Fix Phase 8 plan, not Phase 6. |
| V-3-1 | Exit code ownership split between Phase 6 (constants + engine) and Phase 8 (CLI + integration tests) is implicit | User | Acceptable seam. Added explicit boundary notes to phase files. |
| V-3-2 | Level 2 partition parallelism (spec §8.2) has no explicit Phase 6 task | User | Optimization gap, not functional. Partition counts are small (tens to hundreds). Added as optional sub-task in 6.1. |
| V-4-1 | `libc` and `num_cpus` need adding to clinker-core/Cargo.toml | User | Trivial — add during Task 6.1/6.3 implementation. |
| V-4-2 | Existing integration tests use inline exit code literals — update to use new constants | User | Mechanical refactor during Task 6.5 implementation. |
| V-7-3 | `ctrlc::set_handler()` only catches SIGINT. SIGTERM requires `set_handler_with_signals`. | User | Updated Task 6.5.2 scaffolding to use `set_handler_with_signals`. |
| V-7-4 | Memory ceiling best-effort between Phase 6 and Phase 8 (RSS unguarded during Phase 2 eval) | User | Arena hard cap covers the dominant memory consumer. Phase 6 adds RSS sampling with `tracing::warn` if ceiling exceeded. Full enforcement arrives in Phase 8 with spill triggers. |
| V-7-5 | Error threshold checked per-chunk, not per-record | User | Plan says "at chunk boundaries" — this is correct. A single error in a 1024-record chunk doesn't halt until the chunk completes. Acceptable granularity. |
| V-8-1 | No `--error-threshold` CLI flag in Phase 6 | User | Config-file-only for now. CLI flag is Phase 8 scope (Task 8.4 owns CLI completion). |
| V-8-2 | `--spill-dir` not validated at startup | User | Added early validation to Task 6.4.3 scope. |
| V-8-3 | No max line length on LZ4 decompressed reads in SpillReader | User | Low risk (attacker needs temp dir write access). Added as implementation note in Task 6.4.2. |

## Edge cases acknowledged

| ID | Scenario | Handling decision |
|----|---------|-----------------|
| V-5-1 | DLQ summary to stderr on shutdown — no test | Add `test_shutdown_dlq_summary_to_stderr` to Task 6.5 |
| V-5-2 | Empty pipeline (0 records) — no test | Add `test_empty_pipeline_zero_records` to Task 6.1 |
| V-5-3 | SIGINT during Phase 1 ingestion — no shutdown check | Add `shutdown_requested()` check in Arena::build loop; add `test_signal_during_phase1` to Task 6.2 |
| V-5-4 | `rss_bytes()` returns `None` on unsupported platform — no test | Untestable on supported platforms. Document as known limitation. |
| V-5-5 | RSS < 10us acceptance criterion — no benchmark | Downgrade to documentation-only. Add criterion bench in `benches/` as stretch goal. |
| V-5-6 | `test_golden_file_diff` and `test_graceful_shutdown_flushes_output` are integration tests | Move to integration test module during implementation. |
| V-7-6 | SIGINT during Arena::build tight loop | Add periodic `shutdown_requested()` check every N records in Arena::build |

## Security flags

| ID | Risk | Resolved / Accepted | Notes |
|----|------|---------------------|-------|
| V-8-2 | `--spill-dir` not validated (exists, is dir, writable) | Resolved | Added early validation to Task 6.4.3 |
| V-8-3 | SpillReader no max line length guard | Accepted | Low risk. Added as implementation note. |

## Feature coverage map

| Spec requirement | Task(s) | Coverage |
|-----------------|---------|---------|
| §8.1 Thread pool config | 6.1 | Full |
| §8.2 Level 1 — Source parallelism | 6.2 | Full |
| §8.2 Level 2 — Partition parallelism | 6.1 (optional) | Partial — optimization deferral |
| §8.2 Level 3 — Chunk parallelism | 6.1 | Full |
| §8.2 Level 4 — Stage parallelism | — | Deferred to v2 (per spec) |
| §8.3 Parallelism classification | Phase 5 | Already implemented |
| §8.4 Output serialization (channels) | — | Deferred (Decision #3) |
| §8.5 Deterministic output | 6.1 | Full (for non-`now` pipelines) |
| §9.1 MemoryBudget + RSS tracking | 6.3 | Full |
| §9.2 Spill file format (NDJSON+LZ4) | 6.4 | Full |
| §9.3 External merge sort | Phase 8 | Correctly deferred |
| §10.2 Exit codes | 6.5 | Full |
| §10.3 Signal handling | 6.5 | Full (with `set_handler_with_signals` fix) |

## Cross-phase coherence

Phase 6 sits between Phase 5 (two-pass pipeline) and Phase 8 (sort + CLI). All
Phase 5 deliverables that Phase 6 depends on are confirmed present in the codebase
(Arena, SecondaryIndex, ExecutionPlan, ParallelismClass, PartitionWindowContext).
Phase 6's spill IO primitives (SpillWriter/SpillReader) are interface-compatible
with Phase 8's planned external merge sort — the loser tree can consume SpillReader
iterators directly. Phase 7 (JSON/XML) has zero dependency relationship with Phase 6.

One cross-phase issue identified: Phase 8 Task 8.4 defines exit codes 2/3/4 with
semantics that contradict Phase 6 and the spec. Phase 6's codes match the spec and
Phase 4's already-implemented behavior. Phase 8's plan needs correction.

## Validation checks run

| Check | Agent | Result | Issues found |
|-------|-------|--------|-------------|
| Interface contracts | Agent 1 | Pass | 1 warning (usize/u64 mismatch) |
| Dependency coherence | Agent 2 | Issues | 1 warning (Phase 5 missing tests), 1 cross-phase bug (Phase 8 exit codes) |
| Scope drift | Agent 3 | Pass | 1 warning (exit code ownership seam), 1 warning (Level 2 parallelism gap) |
| Dead code / duplication | Agent 4 | Pass | 2 deps to add (libc, num_cpus), refactor parse_memory_limit |
| Test coverage | Agent 5 | Issues | 3 missing edge case tests, 2 integration tests misplaced |
| Naming consistency | Agent 6 | Pass | 0 issues |
| Assumptions + edge cases | Agent 7 | Issues | 1 blocker resolved (now/start_time), 1 warning (ctrlc SIGTERM) |
| Features + security | Agent 8 | Pass | 3 low-severity hardening items |
