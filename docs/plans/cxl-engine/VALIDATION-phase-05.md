# Validation Report: Phase 5 — Two-Pass Pipeline: Arena, Indexing, Windows

**Date:** 2026-03-30
**Validator:** validate-phase skill (8 parallel agents)
**Verdict:** READY (all blockers resolved)

## Verdict rationale

Phase 5 is architecturally sound with 16 drill decisions backed by production engine research. All 4 blockers have been resolved: eval signature blast radius documented, object safety assertion updated, SortField collision eliminated, and collect/distinct added. 14 warnings addressed via plan amendments — missing test stubs added, LocalWindowConfig typed struct specified, InputConfig.sort_order field noted.

## Blocker resolution log

| ID | Issue | Resolution | Decision |
|----|-------|-----------|---------|
| V-1-1 | Object safety assertion breaks — can't use `Arena` in `clinker-record` | Resolved | Replace with test-local dummy `RecordStorage` impl. Added to Task 5.2.5. |
| V-1-2 | `eval_program` generic propagation — ~8 functions need `S: RecordStorage` param | Resolved | Added Task 5.2.6 enumerating all eval functions. Mechanical change, monomorphized once. |
| V-6-1 | `SortField` name collision — config vs plan versions | Resolved | Extend existing `config::SortField` with optional `null_order`. Rename `direction`→`order`. One struct. Updated Task 5.4.1. |
| V-8-1 | `window.collect()` and `window.distinct()` missing from spec | Resolved | Added to `WindowContext` trait, `PartitionWindowContext` impl, and test stubs in Task 5.4. |

## Accepted warnings and known risks

| ID | Issue | Accepted by | Rationale |
|----|-------|------------|-----------|
| V-1-3 | Re-export sites need updating | Accepted | Mechanical — covered by Task 5.2.6 eval migration |
| V-3-1 | `--explain` overlaps Phase 8 | Accepted | Phase 5 is foundational; Phase 8 extends it |
| V-3-2 | Error threshold overlaps Phase 6 | Accepted | Phase 6 inherits Phase 5 logic, adds thread-safe counters |
| V-4-1 | `CompiledTransform` not explicitly eliminated | Accepted | Superseded by `TransformPlan` during Task 5.5 executor rework |
| V-5-1 | No explicit hard-gate test tables | Accepted | Intra-phase dependency graph documents gates; stubs are comprehensive |
| V-6-2 | New nested modules in flat crate | Accepted | Natural evolution — `pipeline/` and `plan/` group related functionality |
| V-7-1 | WindowContext lifetime fragile for future Phase 6 | Accepted | Valid for current design; mechanical upgrade to Arc if needed |

## Edge cases acknowledged

| ID | Scenario | Handling decision |
|----|---------|-----------------|
| V-5-3a | 1-record partition | Test added: `test_window_single_record_partition` |
| V-5-3b | Composite sort (multi-field) | Test added: `test_sort_partition_composite` |
| V-5-3c | Empty cross-source partition | Covered by `PartitionLookup` returning `None` — window functions return Null |
| V-5-3d | Integer overflow in sum() | Accepted risk — f64 accumulator handles large sums; overflow → infinity |
| V-5-3e | No sort_by in local_window | Valid config — unsorted partition; positional fns use insertion order |
| V-7-3 | Cross-source ref to non-existent source | Test added: `test_plan_cross_source_missing_reference` |
| V-7-4 | Arena::build mid-stream reader error | FormatError propagated naturally from `reader.next_record()` |

## Security flags

| ID | Risk | Resolved / Accepted | Notes |
|----|------|---------------------|-------|
| V-8-2 | `records.len() as u32` unchecked cast | Resolved | Changed to `u32::try_from().expect()` in scaffolding |
| V-8-4 | Large GroupByKey strings | Accepted | Bounded by Arena memory budget (2x at worst) |

## Feature coverage map

| Spec requirement | Task(s) | Coverage |
|-----------------|---------|---------|
| window.first/last/lag/lead | 5.4 | Full |
| window.count/sum/avg/min/max | 5.4 | Full |
| window.any/all | 5.4 (evaluator-driven) | Full |
| window.collect/distinct | 5.4 | Full (added during validation) |
| Cross-source windows | 5.1, 5.3, 5.5 | Full |
| NaN rejection (exit code 3) | 5.3, 5.5 | Full |
| Null group_by exclusion | 5.3 | Full |
| Pre-sorted optimization | 5.4 | Full (compile-time + runtime) |
| ExecutionPlan compile-only display | 5.1 (`--explain`) | Full |
| Two-pass orchestration | 5.5 | Full |
| Stateless fallback to single-pass | 5.5 | Full |
| Memory budget on Arena | 5.2 | Halt only (spill deferred to Phase 6) |

## Cross-phase coherence

Phase 5 depends on Phase 4 (complete: `StreamingExecutor`, `PipelineConfig`, CSV readers/writers). Phase 6 depends on Phase 5 outputs (`ExecutionPlan`, `Arena`, `ParallelismClass`, `PipelineExecutor`). All contracts are clean. Phase 5 introduces `RecordStorage` trait in `clinker-record` — this is a foundation-layer addition that Phase 6 inherits transparently. The `--explain` flag introduced in Phase 5 Task 5.1.7 is extended (not reimplemented) by Phase 8 Task 8.3. Error threshold logic in Phase 5 Task 5.5.6 is inherited by Phase 6 Task 6.5, which adds thread-safe counter aggregation. PLAN.md status table is stale (shows Phase 4 as "Not Started" when it's complete) — should be updated.

## Validation checks run

| Check | Agent | Result | Issues found |
|-------|-------|--------|-------------|
| Interface contracts | Agent 1 | Issues | 2 blockers (resolved), 2 warnings |
| Dependency coherence | Agent 2 | Pass | 0 issues |
| Scope drift | Agent 3 | Issues | 2 warnings (accepted) |
| Dead code / duplication | Agent 4 | Issues | 1 warning (accepted) |
| Test coverage | Agent 5 | Issues | 5 warnings (3 resolved, 2 accepted) |
| Naming consistency | Agent 6 | Issues | 1 blocker (resolved), 1 warning (accepted) |
| Assumption conflicts + edge cases | Agent 7 | Issues | 3 warnings (2 resolved, 1 accepted) |
| Feature gaps + security | Agent 8 | Issues | 1 blocker (resolved), 2 warnings (1 resolved, 1 accepted) |
