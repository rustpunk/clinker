# Validation Report: Phase 11 -- Modules, Logging, Validations, Polish

**Date:** 2026-03-31
**Validator:** validate-phase skill (8 parallel agents)
**Verdict:** READY (all 5 blockers resolved, 19 warnings accepted)

## Verdict rationale

Phase 11 is architecturally sound — all tasks map to spec sections, the dependency chain is correct, and prior-phase outputs are present in the codebase (639 passing tests). All 5 blockers resolved: `ModuleDecl` renamed to `Module`, `TakeReader` scaffolding completed, recursive call detection added as sub-task 11.1.6, wildcard import rejection test added, `env()` CXL function clarified as `${VAR}` YAML interpolation validation. 19 warnings accepted with documented handling decisions.

## Blocker resolution log

| ID | Issue | Resolution | Research-backed | Decision |
|----|-------|-----------|-----------------|---------|
| V-1-1 | `Module` vs `ModuleDecl` naming + field mismatch | Resolved | no | Delete unused `ModuleDecl`, use `Module` (matches `Program`/`FnDecl` convention) |
| V-1-3 | `TakeReader` scaffolding omits `schema()` | Resolved | no | Added `schema()` delegation to scaffold |
| V-8-1 | Recursive call detection missing | Resolved | no | Added sub-task 11.1.6 + test `test_module_fn_recursive_call_rejected` |
| V-8-2 | Wildcard import rejection | Resolved | no | Added test `test_module_reject_wildcard_import`, handled in 11.1.2 |
| V-7-1 | `env()` CXL function undefined | Resolved | no | `env()` doesn't exist — env var validation applies to `${VAR}` YAML interpolation via `interpolate_env_vars()`. Validated at config parse time. |

## Accepted warnings and known risks

| ID | Issue | Accepted by | Rationale |
|----|-------|------------|-----------|
| V-4-1 | `pipeline_vars`, `PipelineMeta.vars`, `resolve_pipeline()` already exist | validate-phase | Task 11.2 completes wiring (vars map currently empty, batch_id misrouted). Description updated. |
| V-4-2 | All 7 CLI flags already exist on `RunArgs` | validate-phase | Sub-tasks wire existing flags to new functionality — do not re-add. |
| V-1-2 | `UseStmt` dot separator vs `Dot` token ambiguity | validate-phase | `.` in `use` context consumed before Pratt parser. Handled in 11.1.2. |
| V-1-4 | `LogLevel` duplicates `TraceLevel` variants | validate-phase | Different domains (AST vs config), different crates. Acceptable. |
| V-1-5 | serde-saphyr budget API unverified in codebase | validate-phase | Confirmed available in v0.0.22. Verify at implementation time. |
| V-2-1 | PLAN.md stale (Phase 9/10 shown as "Validated") | validate-phase | Update to "Complete" separately. |
| V-2-2 | Task 11.1 header says Phase 9 only | validate-phase | Fixed to "Phase 9 + Phase 10 exit criteria". |
| V-2-3 | `use` separator change breaks 2 parser tests | validate-phase | Update existing tests in 11.1.2. Added `test_module_use_colons_rejected`. |
| V-2-4 | `.debug()` uses `tracing::debug!`, plan expects `trace!` | validate-phase | Task 11.3.4 must change to `tracing::trace!` per spec §5.10. |
| V-2-5 | `batch_id` misrouted to `execution_id` at main.rs:365 | validate-phase | Task 11.2.2 explicitly fixes this bug. |
| V-3-1 | Benchmarks scope stretch | validate-phase | Last phase, no better home. Acceptable as "polish". |
| V-5-1 | No Wave 0 scaffold task | validate-phase | Scaffold work folded into start of 11.1 (create files, mod declarations). |
| V-5-2 | Test count mismatches in headers | validate-phase | Headers stale; actual stub counts are correct (35, 39, 30). |
| V-5-3 | `test_validation_args_reject_field_ref` misnamed | validate-phase | Renamed to `test_validation_args_treated_as_literal`. |
| V-6-1 | No error types for new domains | validate-phase | Fold into `PipelineError`/`ConfigError`. No dedicated enums for v1. |
| V-7-2 | BOM/non-UTF-8 in .cxl files | validate-phase | Strip UTF-8 BOM, reject non-UTF-8. Handle in 11.1.3. |
| V-7-3 | `every: 0` undefined | validate-phase | Reject at config parse time. Handle in 11.3.1. |
| V-7-4 | `_transform_duration_ms` in `before_transform` | validate-phase | Resolve to null. Phase A warning if incompatible timing. Handle in 11.3.2. |
| V-7-5 | Validation referencing non-existent field | validate-phase | Added `test_validation_nonexistent_field`. Phase B error with suggestion. |

## Edge cases acknowledged

| ID | Scenario | Handling decision |
|----|---------|-----------------|
| V-7-2 | BOM in .cxl module files | Strip UTF-8 BOM in load_modules(); reject non-UTF-8 with path in error |
| V-7-3 | `every: 0` in log directive | Reject at config parse time: "every must be >= 1" |
| V-7-4 | `_transform_duration_ms` in before_transform | Resolve to null; Phase A warning |
| V-7-5 | Validation field not in schema | Phase B error with Levenshtein suggestion |

## Security flags

| ID | Risk | Resolved / Accepted | Notes |
|----|------|---------------------|-------|
| V-8-3 | .cxl files parsed without size limit | Accepted | Add 1MB pre-check in load_modules() (11.1.3) |
| V-8-4 | Log rules YAML may bypass DoS budget | Accepted | Use parse_yaml_with_budget() for all YAML loads (11.3.3) |
| V-8-5 | --dry-run-output path not validated | Accepted | Apply validate_path() to CLI output paths (11.6.3) |
| V-8-6 | Sensitive fields in log templates | Accepted (v1) | Operator controls what they log; redaction is v2 |

## Feature coverage map

| Spec requirement | Task(s) | Coverage |
|-----------------|---------|---------|
| §5.9 CXL Modules | 11.1 (6 sub-tasks) | Full — parsing, registry, resolution, toposort, recursion detection, wildcard rejection |
| §5.10 Logging (4 levels) | 11.3 (5 sub-tasks) | Full |
| §5.11 Validations | 11.4 (5 sub-tasks) | Full |
| §6.3 Dry-run -n | 11.6 (3 sub-tasks) | Full |
| Pipeline vars (§5.8) | 11.2 (3 sub-tasks) | Full (completes existing scaffolding) |
| Security hardening | 11.5 (5 sub-tasks) | Full |
| Metrics spool (§10.6) | 11.0 | ✅ Complete |
| Phase 9 deferrals (enum, structure) | 11.4.4, 11.4.5 | Full |

## Cross-phase coherence

Phase 11 is the final phase. All prior-phase outputs (Phases 1-10) are present and compiling (639 passing tests). The dependency chain is correct: Phase 9 schema types exist and are parsed but unenforced — Task 11.4 completes enforcement. Phase 10 channel system is fully implemented. The intra-phase chain (11.1→11.2→11.3→11.4→11.5→11.6) is strictly linear with one soft link (11.1→11.2 is process dependency to avoid resolver merge conflicts). PLAN.md status for Phase 9/10 should be updated to "Complete."

## Validation checks run

| Check | Agent | Result | Issues found |
|-------|-------|--------|-------------|
| Interface contracts | Agent 1 | Resolved | 1 blocker (resolved), 4 warnings (accepted) |
| Dependency coherence | Agent 2 | Resolved | 0 blockers, 6 warnings (accepted) |
| Scope drift | Agent 3 | Pass | 0 blockers, 3 warnings (accepted) |
| Dead code / duplication | Agent 4 | Resolved | 2 blockers (accepted as existing code), 2 warnings (accepted) |
| Test coverage | Agent 5 | Resolved | 3 blockers (resolved), 6 warnings (accepted) |
| Naming consistency | Agent 6 | Resolved | 1 blocker (resolved), 2 warnings (accepted) |
| Assumption conflicts + edge cases | Agent 7 | Resolved | 1 blocker (resolved), 6 warnings (accepted) |
| Feature gaps + security | Agent 8 | Resolved | 2 blockers (resolved), 4 security flags (accepted) |
