# Validation Report: Phase 3 — CXL Resolver, Type Checker, Evaluator

**Date:** 2026-03-29
**Validator:** validate-phase skill (8 parallel agents)
**Verdict:** READY (all blockers resolved)

## Verdict rationale
Phase 3 had 6 blockers found during validation. All were resolved inline: spec updated with missing `pipeline.*` members, `Expr::Now` variant scoped into Task 3.2.1, 4 missing gate tests added to Task 3.3, `HashMap` → `IndexMap` for deterministic field_types, and string output size guard added for `.repeat(n)`. The phase is now ready for execution.

## Blocker resolution log
| ID | Issue | Resolution | Decision |
|----|-------|-----------|---------|
| V-1-1 | WindowContext trait vs Phase 5 struct signatures conflict | Resolved | Phase 3 defines the authoritative trait; Phase 5 must conform. Phase 5's informal description is shorthand, not a different design. |
| V-2-1 | `Expr::Now` variant missing from AST | Resolved | Added to Task 3.2.1 scope — `Expr::Now { node_id, span }` + parser nud handler for `Token::Now` |
| V-5-1 | 4 Task 3.3 acceptance criteria have zero gate tests | Resolved | Added 4 gate tests: `test_typecheck_field_type_conflict_both_spans`, `test_typecheck_schema_override_conflict`, `test_typecheck_zero_emit_warning`, `test_typecheck_field_type_map_produced` |
| V-7-1 | `TypedProgram.field_types: HashMap` non-deterministic | Resolved | Changed to `IndexMap<String, Type>` for deterministic iteration |
| V-8-1 | Resolver scope lists wrong `pipeline.*` members | Resolved | Updated spec SS5.8 to include `total_count`, `ok_count`, `dlq_count`, `source_file`, `source_row`. Updated Phase 3 resolver scope to match all 8 members + user vars. |
| V-8-2 | `.repeat(n)` / `.pad_left(n)` OOM with large n | Resolved | Added string output size guard (10MB default) to Task 3.4 implementation notes + `StringTooLarge` error variant |

## Accepted warnings and known risks
| ID | Issue | Accepted by | Rationale |
|----|-------|------------|-----------|
| V-1-2 | TypeTag vs Type parallel hierarchies | Accepted | Different purposes (signatures vs inference). TypeTag→Type conversion is trivial and will be added during implementation. |
| V-2-2 | NodeId breaks Phase 2 tests | Accepted | Scoped into Task 3.2.1 explicitly — "update all Phase 2 test AST constructions to include NodeId" |
| V-2-3 | `Diagnostic` type undefined | Accepted | Will be defined during Task 3.2 implementation — likely a custom struct with span + miette fields |
| V-3-1 | `cxl fmt`/`cxl check` overlap with Phase 8 | Accepted | Phase 3 owns core implementation; Phase 8 extends CLI flags. Note added. |
| V-3-2 | Phase 10 extends resolver with module-qualified names | Accepted | Phase 3 resolver designed with extensibility point (`Binding::Function` variant). Documented. |
| V-4-1 | Evaluator should delegate to clinker_record::coercion | Accepted | Implementation detail — will delegate, not reimplement |
| V-6-1 | `Binding` enum too generic | Accepted | Rename to `ResolvedBinding` during implementation |
| V-7-2 | Duplicate let binding semantics | Accepted | Will decide during implementation — likely allow with Phase B warning |
| V-7-3 | Duplicate emit name semantics | Accepted | Will add Phase C warning during implementation |
| V-7-4 | Integer overflow wrapping | Resolved | Added `checked_*` arithmetic mandate + `IntegerOverflow` error variant |
| V-8-3 | Three-valued AND/OR logic | Resolved | Added explicit implementation note to Task 3.4 |

## Edge cases acknowledged
| ID | Scenario | Handling decision |
|----|---------|-----------------|
| V-7-5 | `.substring()` negative start/len | Will clamp to 0 — documented as implementation detail |
| V-8-4 | String-to-Numeric implicit coercion | Added to Task 3.4 implementation notes — delegates to clinker_record::coercion |
| V-5-2 | No explicit `now` keyword test | Will add `test_eval_now_with_fixed_clock` during implementation |
| V-5-3 | Division by zero untested | Will add during implementation — variant already in EvalErrorKind |

## Security flags
| ID | Risk | Resolved / Accepted | Notes |
|----|------|---------------------|-------|
| V-8-2 | `.repeat(n)` OOM | Resolved | String output size guard (10MB default) + `StringTooLarge` error |
| V-8-5 | `cxl eval --record` deep JSON | Accepted | ARG_MAX caps input; serde_json has internal depth limits; low severity dev tool |

## Feature coverage map
| Spec requirement | Task(s) | Coverage |
|-----------------|---------|---------|
| SS6.1 Phase B — name resolution | Task 3.2 | Full |
| SS6.1 Phase C — type checking | Task 3.3 | Full (after adding 4 gate tests) |
| SS5.4 Type system — null propagation | Task 3.3, 3.4 | Full |
| SS5.4 Type system — implicit coercion | Task 3.4 | Full (after adding impl note) |
| SS5.4 Three-valued AND/OR | Task 3.4 | Full (after adding impl note) |
| SS5.5 Built-in methods (~60) | Task 3.4 | Full |
| SS5.8 Pipeline metadata (8 members + vars) | Task 3.2, 3.4 | Full (after spec update) |
| SS6.2 `cxl check` | Task 3.4 | Full |
| SS6.3 `cxl eval` | Task 3.4 | Full |
| `cxl fmt` pretty-printer | Task 3.4 | Full |

## Cross-phase coherence
Phase 3 sits between Phase 2 (parser) and Phases 4/5 (pipeline). The FieldResolver and WindowContext traits defined in Task 3.1 are the authoritative interface contracts — Phases 4, 5, and beyond implement these traits on concrete types (Record, ArenaRecordView, etc.). The TypedProgram struct produced by Task 3.3 is the primary output consumed by the pipeline evaluator in Phase 4. The `cxl-cli` binary in Task 3.4 is extended by Phase 8 with additional CLI flags and `--explain` mode. Phase 10 extends the Phase B resolver with module-qualified name resolution — Phase 3's `Binding::Function` variant and `FunctionRegistry` provide the extensibility point. No unresolved cross-phase conflicts remain.

## Validation checks run
| Check | Agent | Result | Issues found |
|-------|-------|--------|-------------|
| Interface contracts | Agent 1 | Issues | 2 blockers (WindowContext mismatch, Expr::Now), 2 warnings |
| Dependency coherence | Agent 2 | Issues | 1 blocker (Expr::Now), 2 warnings (NodeId breakage, Diagnostic type) |
| Scope drift | Agent 3 | Issues | 3 warnings (cxl fmt overlap, cxl check overlap, Phase 10 resolver extension) |
| Dead code / duplication | Agent 4 | Issues | 2 warnings (coercion delegation, Value::type_name delegation) |
| Test coverage | Agent 5 | Issues | 1 blocker (4 missing gate tests), 2 warnings, 6 edge cases |
| Naming consistency | Agent 6 | Issues | 2 warnings (Type name, Binding name) |
| Assumption conflicts + edge cases | Agent 7 | Issues | 1 blocker (HashMap determinism), 4 warnings |
| Feature gaps + security | Agent 8 | Issues | 2 blockers (pipeline members, .repeat OOM), 4 warnings |
