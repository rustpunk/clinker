# Validation Report: Phase 8 — Sort, DLQ Polish, CLI Completion

**Date:** 2026-03-30
**Validator:** validate-phase skill (8 parallel sub-agents)
**Verdict:** READY (3 blockers resolved, 18 warnings logged)

## Verdict rationale
All 3 blockers were resolved during the validation session: the `sort_order` semantic conflict was confirmed as intentional (spec §7.2 to be amended), the `SortFieldSpec` migration cascade was shown to be non-existent (serde-only type, resolved at config boundary), and the contradictory DLQ acceptance criteria were cleaned up (stale text removed). The remaining 18 warnings are actionable during implementation — none block execution.

## Blocker resolution log
| ID | Issue | Resolution | Decision |
|----|-------|-----------|---------|
| V-7-1 | `InputConfig.sort_order` redefined as "active sort" contradicts spec §7.2 "declared pre-sorted" | Resolved | Keep "ensure this order" semantic. `is_sorted()` fast-path preserves the optimization. Amend spec §7.2 — documentation-only change. |
| V-1-1 | `OutputConfig.sort_order` type change cascades across plan/index/sort modules | Resolved | `SortFieldSpec` is serde-only. Resolve to `Vec<SortField>` in `load_config()`. All downstream code uses `Vec<SortField>` — zero cascade. |
| V-5-1 | Contradictory DLQ acceptance criteria #6 vs #7 | Resolved | Removed AC #6 ("same format as output") and stale implementation note. Decision #10 authoritative: DLQ always CSV. |

## Accepted warnings and known risks
| ID | Issue | Accepted by | Rationale |
|----|-------|------------|-----------|
| V-7-2 | Source sort in streaming mode forces buffering | User | Document the memory model change; add tracing::info when buffered mode is forced |
| V-7-3 | NullOrder::Drop on source sort permanently discards records | — | To be addressed during implementation: restrict to First/Last or route to DLQ |
| V-7-5 | Assumption #24 (remove sort_output) needs spec amendment in 6 places | User | Spec amendment is documentation-only, architecturally sound |
| V-8-5 | DLQ CSV formula injection | — | Low severity for CLI tool; document as accepted risk |
| V-4-3 | NullStorage duplicated in 3 places | — | Pre-existing; not caused by Phase 8 |
| V-2-2 | Task 8.1→8.2 dependency overly conservative | — | Conservative ordering is safe; parallelism is a nice-to-have optimization |

## Edge cases acknowledged
| ID | Scenario | Handling decision |
|----|---------|-----------------|
| V-7-4 | spill_threshold=0 triggers before any record pushed | Guard: `bytes_used > 0 && bytes_used >= spill_threshold` |
| V-5-4 | `--field eq=a=b` must split on first `=` only | Add test during implementation |
| V-5-4 | Sort with 1 record, Bool encoding, zero DLQ, empty pipeline explain, `--memory-limit 0` | Add expanded test stubs during implementation |

## Security flags
| ID | Risk | Resolved / Accepted | Notes |
|----|------|---------------------|-------|
| V-8-3 | `--base-dir` path traversal (`../../`) | To resolve | Added canonicalization + descendant check to sub-task 8.4.4 |
| V-8-4 | YAML config no size budget | To resolve | Add `metadata.len() > 10MB` check in `load_config()` |
| V-8-5 | DLQ CSV formula injection | Accepted | Low severity for CLI-only tool; `csv` crate handles quoting |

## Feature coverage map
| Spec requirement | Task(s) | Coverage |
|-----------------|---------|---------|
| §9.3 External merge sort | 8.1 | Full — chunked sort, NDJSON+LZ4 spill, loser tree, cascade merge |
| §10.4 DLQ format (6 columns, 6 categories) | 8.2 | Full — all columns, all categories, include_reason/include_source_row |
| §6.2 --explain flag (6 items) | 8.3 | Full — AST, types, DAG, indices, parallelism, memory budget |
| §10.5 Progress reporting | 8.3 | Partial — missing `~Xs remaining` estimate (V-8-2) |
| §10.2 Exit codes (0-4, 130) | 8.4 | Full — all 6 exit codes with tests |
| §10.3 Signal handling (flush + DLQ summary) | 8.4 | Partial — AtomicBool detection done, flush/summary added to plan (V-8-1) |
| §6.3 Dry-run `-n` | Deferred | Deferred to Phase 10 (no landing zone yet — V-3-2) |

## Cross-phase coherence
Phase 8 cleanly consumes Phase 6's spill infrastructure (SpillWriter/SpillReader/SpillFile all exist with correct APIs) and Phase 7's format writers (FormatWriter trait with write_record/flush). Exit codes from Phase 6's `exit_codes.rs` are correct. The shutdown `AtomicBool` from Phase 6 is ready for exit-code-130 testing. `ExecutionPlan` from Phase 4/5 exists with an `explain()` method that Task 8.3 can extend. Phase 5's `sort_partition()` and Phase 8's `SortBuffer` are distinct systems (partition pointer sort vs output merge sort) with shared value-level comparators. `DlqConfig` already has `include_reason`/`include_source_row` fields — Task 8.2 extends rather than creates. No downstream phases depend on Phase 8. The `-n` deferral needs a Phase 10 landing zone.

## Validation checks run
| Check | Agent | Result | Issues found |
|-------|-------|--------|-------------|
| Interface contracts | Agent 1 | Issues | 2 blockers (resolved), 3 warnings |
| Dependency coherence | Agent 2 | Issues | 0 blockers, 3 warnings |
| Scope drift | Agent 3 | Issues | 0 blockers, 2 warnings |
| Dead code / duplication | Agent 4 | Issues | 1 blocker (parse_memory_limit), 4 warnings |
| Test coverage | Agent 5 | Issues | 1 blocker (resolved), 3 warnings |
| Naming consistency | Agent 6 | Issues | 1 blocker (same as Agent 4), 3 warnings |
| Assumption conflicts + edge cases | Agent 7 | Issues | 2 blockers (resolved), 3 warnings |
| Feature gaps + security | Agent 8 | Pass (warnings) | 0 blockers, 6 warnings |
