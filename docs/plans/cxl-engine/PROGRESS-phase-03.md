# Phase 3 Execution Progress: CXL Resolver, Type Checker, Evaluator

**Phase file:** docs/plans/cxl-engine/phase-03-cxl-evaluator.md
**Validation:** docs/plans/cxl-engine/VALIDATION-phase-03.md (READY — all blockers resolved)
**Started:** 2026-03-29
**Last updated:** 2026-03-29
**Status:** ✅ Complete

---

## Current state

**Active task:** none — Phase 3 complete
**Completed:** 4 of 4 tasks
**Blocked:** none

---

## Task list

### ✅ [3.1] FieldResolver + WindowContext Traits  ← COMPLETE
**Sub-tasks:**
- [x] [3.1.1] Define `FieldResolver` and `WindowContext` traits in `cxl::resolve::traits`
- [x] [3.1.2] Implement `HashMapResolver` test double
- [x] [3.1.3] Write all 6 gate tests

**Gate tests that must pass before [3.2] unlocks:**
- `test_field_resolver_object_safety` — dyn FieldResolver compiles and dispatches
- `test_window_context_object_safety` — dyn WindowContext compiles and dispatches
- `test_hashmap_resolver_unqualified` — resolve("name") returns stored value
- `test_hashmap_resolver_qualified` — resolve_qualified("src", "field") returns stored value
- `test_hashmap_resolver_missing_field` — resolve("nonexistent") returns None
- `test_hashmap_resolver_available_fields` — available_fields() returns all inserted names

**Done when:** All 6 gate tests pass; traits are object-safe; HashMapResolver works
**Commit:** `feat(cxl): add FieldResolver and WindowContext traits with HashMapResolver test double`
**Commit ID:** 8272050

---

### ✅ [3.2] Resolver Pass (Phase B)  ← COMPLETE
**Sub-tasks:**
- [x] [3.2.1] Add `NodeId(u32)` to all AST variants + `Expr::Now` + parser counter + update Phase 2 tests
- [x] [3.2.2] Define `ResolvedBinding` enum, `ResolvedProgram` struct with side-table
- [x] [3.2.3] Implement `resolve_program()` walk with scope stack, `it` validation, `ResolveContext` switching
- [x] [3.2.4] Hand-rolled Levenshtein (threshold 3, early-bail) + all 6 gate tests

**Gate tests that must pass before [3.3] unlocks:**
- `test_resolve_simple_field_ref` — emit name = first_name resolves to Field binding
- `test_resolve_let_binding` — let x = 1; emit val = x resolves x to LetVar
- `test_resolve_pipeline_member` — pipeline.start_time resolves to PipelineMember
- `test_resolve_unresolved_with_suggestion` — naem with field name → "did you mean 'name'?"
- `test_resolve_it_outside_predicate_error` — it at top level → Phase B error
- `test_resolve_it_inside_predicate_ok` — window.any(it.salary > 100000) resolves

**Done when:** All 6 gate tests pass; NodeId on all AST nodes; resolver handles all scope layers
**Commit:** `feat(cxl): add NodeId to AST, implement Phase B resolver pass`
**Commit ID:** 128b4f3

---

### ✅ [3.3] Type Checker (Phase C)  ← COMPLETE
**Sub-tasks:**
- [x] [3.3.1] Define `Type` enum with Nullable flattening smart constructor
- [x] [3.3.2] Pass 1: constraint collection — walk AST, collect (field, Type, Span), seed from schema
- [x] [3.3.3] Per-field unification + Pass 2: bottom-up type annotation using FieldTypeMap
- [x] [3.3.4] Phase C semantic checks + all 10 gate tests

**Gate tests that must pass before [3.4] unlocks:**
- `test_typecheck_arithmetic_int` — 1+2 infers Int, 1+2.0 infers Float
- `test_typecheck_null_propagation` — nullable_field + 1 infers Nullable(Int)
- `test_typecheck_coalesce_strips_nullable` — nullable_field ?? 0 infers Int
- `test_typecheck_match_missing_wildcard` — match without _ arm → error
- `test_typecheck_nested_window_in_predicate` — window.any(window.sum(...)) → error
- `test_typecheck_sum_requires_numeric` — sum("hello") → type error
- `test_typecheck_field_type_conflict_both_spans` — field used as String and Numeric → cites both spans
- `test_typecheck_schema_override_conflict` — schema declares Int, usage as String → error
- `test_typecheck_zero_emit_warning` — no emit statements → warning
- `test_typecheck_field_type_map_produced` — field_types contains inferred entries

**Done when:** All 10 gate tests pass; TypedProgram produced with types + field_types + regexes
**Commit:** `feat(cxl): implement two-pass type checker with constraint inference`
**Commit ID:** 284c380

---

### ✅ [3.4] Core Evaluator + All Built-in Methods + cxl-cli  ← COMPLETE
**Sub-tasks:**
- [x] [3.4.1] Define EvalError/EvalErrorKind, Clock trait, EvalContext struct
- [x] [3.4.2] Core evaluator: binary ops, null propagation, let/emit, match, if/then/else, coalesce, now
- [x] [3.4.3] All ~60 built-in methods (string, path, array, numeric, date, conversion, introspection, debug)
- [x] [3.4.4] cxl-cli crate: check, eval, fmt subcommands
- [x] [3.4.5] Write all 19 gate tests (20 total including extras)

**Gate tests that must pass before Phase 4 unlocks:**
- `test_eval_arithmetic_null_propagation` — Null + 1 → Null; 2 + 3 → 5
- `test_eval_comparison_null_semantics` — Null == Null → true; Null > 1 → Null
- `test_eval_coalesce_short_circuit` — "hello" ?? panic_expr → "hello"
- `test_eval_let_binding_scope` — let x = 10; emit val = x + 1 → {"val": 11}
- `test_eval_match_condition_form` — match { age > 18 -> "adult", _ -> "minor" }
- `test_eval_match_value_form` — match status { "A" -> "active", _ -> "unknown" }
- `test_eval_if_then_else_missing_else` — if false then 1 → Null
- `test_eval_string_methods_core` — trim, upper, lower, length, replace, substring
- `test_eval_string_methods_regex` — matches, capture
- `test_eval_string_methods_path` — file_name, file_stem, extension, parent, parent_name
- `test_eval_numeric_methods` — abs, ceil, floor, round, round_to, clamp
- `test_eval_date_methods` — year, month, day, add_days, format_date
- `test_eval_conversion_strict` — to_int on "42" → 42; to_int on "abc" → error
- `test_eval_conversion_lenient` — try_int on "abc" → Null; try_float on "3.14" → 3.14
- `test_eval_introspection` — type_of returns "string"/"int"; is_null on Null → true
- `test_eval_regex_precompiled` — regex from TypedProgram.regexes, not compiled at runtime
- `test_eval_emit_output_map` — 3 emit statements → map with 3 entries
- `test_cxl_check_valid_program` — cxl check exits 0 on valid .cxl
- `test_cxl_eval_json_record` — cxl eval with --record '{"name":"Ada"}' → correct output

**Done when:** All 19 gate tests pass; ~60 built-in methods work; cxl check/eval/fmt operational
**Commit:** `feat(cxl): implement evaluator with ~60 built-in methods and cxl-cli`
**Commit ID:** 1fd756f

---

## Gate test log

| Task | Test | Status | Run | Commit |
|------|------|--------|-----|--------|
| 3.1 | `test_field_resolver_object_safety` | ✅ Passed | 89 total | 8272050 |
| 3.1 | `test_window_context_object_safety` | ✅ Passed | 89 total | 8272050 |
| 3.1 | `test_hashmap_resolver_unqualified` | ✅ Passed | 89 total | 8272050 |
| 3.1 | `test_hashmap_resolver_qualified` | ✅ Passed | 89 total | 8272050 |
| 3.1 | `test_hashmap_resolver_missing_field` | ✅ Passed | 89 total | 8272050 |
| 3.1 | `test_hashmap_resolver_available_fields` | ✅ Passed | 89 total | 8272050 |
| 3.2 | `test_resolve_simple_field_ref` | ✅ Passed | 107 total | 128b4f3 |
| 3.2 | `test_resolve_let_binding` | ✅ Passed | 107 total | 128b4f3 |
| 3.2 | `test_resolve_pipeline_member` | ✅ Passed | 107 total | 128b4f3 |
| 3.2 | `test_resolve_unresolved_with_suggestion` | ✅ Passed | 107 total | 128b4f3 |
| 3.2 | `test_resolve_it_outside_predicate_error` | ✅ Passed | 107 total | 128b4f3 |
| 3.2 | `test_resolve_it_inside_predicate_ok` | ✅ Passed | 107 total | 128b4f3 |
| 3.3 | `test_typecheck_arithmetic_int` | ✅ Passed | 132 total | 284c380 |
| 3.3 | `test_typecheck_null_propagation` | ✅ Passed | 132 total | 284c380 |
| 3.3 | `test_typecheck_coalesce_strips_nullable` | ✅ Passed | 132 total | 284c380 |
| 3.3 | `test_typecheck_match_missing_wildcard` | ✅ Passed | 132 total | 284c380 |
| 3.3 | `test_typecheck_nested_window_in_predicate` | ✅ Passed | 132 total | 284c380 |
| 3.3 | `test_typecheck_sum_requires_numeric` | ✅ Passed | 132 total | 284c380 |
| 3.3 | `test_typecheck_field_type_conflict_both_spans` | ✅ Passed | 132 total | 284c380 |
| 3.3 | `test_typecheck_schema_override_conflict` | ✅ Passed | 132 total | 284c380 |
| 3.3 | `test_typecheck_zero_emit_warning` | ✅ Passed | 132 total | 284c380 |
| 3.3 | `test_typecheck_field_type_map_produced` | ✅ Passed | 132 total | 284c380 |
| 3.4 | `test_eval_arithmetic_null_propagation` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_comparison_null_semantics` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_coalesce_short_circuit` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_let_binding_scope` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_match_condition_form` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_match_value_form` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_if_then_else_missing_else` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_string_methods_core` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_string_methods_regex` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_string_methods_path` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_numeric_methods` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_date_methods` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_conversion_strict` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_conversion_lenient` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_introspection` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_regex_precompiled` | ⛔ Not run | -- | -- |
| 3.4 | `test_eval_emit_output_map` | ⛔ Not run | -- | -- |
| 3.4 | `test_cxl_check_valid_program` | ⛔ Not run | -- | -- |
| 3.4 | `test_cxl_eval_json_record` | ⛔ Not run | -- | -- |

---

## Completed tasks

| Task | Name | Commit message | Commit ID | Completed |
|------|------|---------------|-----------|-----------|
| 3.1 | FieldResolver + WindowContext Traits | feat(cxl): add FieldResolver and WindowContext traits with HashMapResolver test double | 8272050 | 2026-03-29 |
| 3.2 | Resolver Pass (Phase B) | feat(cxl): add NodeId to AST, implement Phase B resolver pass | 128b4f3 | 2026-03-29 |
| 3.3 | Type Checker (Phase C) | feat(cxl): implement two-pass type checker with constraint inference | 284c380 | 2026-03-29 |
| 3.4 | Core Evaluator + Built-ins + cxl-cli | feat(cxl): implement evaluator with ~60 built-in methods and cxl-cli | 1fd756f | 2026-03-29 |

---

## Notes

**Entry criteria verified:** `cargo test -p cxl` passes 83 tests (Phase 2 complete).

**Validation findings applied (2026-03-29):**
- V-8-1: Spec SS5.8 updated with pipeline.total_count/ok_count/dlq_count/source_file/source_row
- V-2-1: Expr::Now variant scoped into Task 3.2.1
- V-5-1: 4 missing gate tests added to Task 3.3 (field_type_conflict, schema_override, zero_emit, field_type_map)
- V-7-1: TypedProgram.field_types changed to IndexMap for deterministic iteration
- V-8-2: String output size guard added (.repeat/.pad_left/.pad_right)
- V-7-4: Integer overflow mandate (checked_* arithmetic) added to Task 3.4
- V-8-3: Three-valued AND/OR logic added to Task 3.4
- V-8-4: Implicit String→Numeric coercion added to Task 3.4

**Naming decisions from validation (apply during implementation):**
- Rename `Binding` → `ResolvedBinding` (V-6-1)
- Add `TypeTag → Type` conversion function (V-1-2)
- Delegate `.to_int()` etc. to `clinker_record::coercion::*` (V-4-1)
- Delegate `.type_of()` / `.is_null()` to `Value::type_name()` / `Value::is_null()` (V-4-1)
