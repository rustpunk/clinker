# Phase 11 Execution Progress: Modules, Logging, Validations, Polish

**Phase file:** docs/plans/cxl-engine/phase-11-modules-logging-validations.md
**Started:** 2026-03-31
**Last updated:** 2026-03-31
**Status:** 🔄 In Progress

---

## Current state

**Active task:** [11.1] CXL Modules (.cxl Files)
**Completed:** 1 of 7 tasks (11.0 completed prior to this tracking)
**Blocked:** 11.2, 11.3, 11.4, 11.5, 11.6

---

## Task list

### ✅ [11.0] Execution Metrics Spool
**Gate tests:** `cargo test metrics` — 10 tests in `clinker-core::metrics::tests` + 3 in `clinker::tests`
**Done when:** Spool file written atomically, collect command works, peak RSS tracked
**Commit:** completed prior to phase tracking
**Commit ID:** (pre-existing)

---

### 🔄 [11.1] CXL Modules (.cxl Files)  ← ACTIVE
**Gate tests that must pass first:**
- `test_module_parse_fn_declaration` -- fn parsed with name, params, body
- `test_module_parse_let_constant` -- let constant parsed
- `test_module_reject_emit_in_fn` -- emit in fn body rejected
- `test_module_reject_trace_in_fn` -- trace in fn body rejected
- `test_module_use_path_resolution` -- use statement resolves via dot separator
- `test_module_use_nested_path` -- nested dot path resolves to directory
- `test_module_registry_dedup` -- two transforms share single Arc
- `test_module_qualified_fn_call` -- qualified fn call resolves and evaluates
- `test_module_qualified_constant` -- qualified constant access resolves
- `test_module_reject_cross_import` -- module containing use → parse error
- `test_module_rules_path_cli_override` -- --rules-path overrides default
- `test_module_missing_file_error` -- missing module file → error
- `test_module_const_forward_reference` -- forward reference works (toposort)
- `test_module_const_cycle_detection` -- cyclic constants → error
- `test_module_const_duplicate_name` -- duplicate constant names rejected
- `test_module_fn_duplicate_name` -- duplicate function names rejected
- `test_module_const_reject_field_reference` -- constant referencing field → error
- `test_module_use_alias` -- use with alias resolves via alias
- `test_module_empty_file` -- empty module file is valid
- `test_module_constants_only` -- module with only constants
- `test_module_functions_only` -- module with only functions
- `test_module_fn_no_params` -- function with zero parameters
- `test_module_fn_method_chain_body` -- method chain in fn body
- `test_module_fn_conditional_body` -- if/then/else in fn body
- `test_module_fn_match_body` -- match expression in fn body
- `test_module_with_comments` -- comments in module file
- `test_module_qualified_nonexistent_constant` -- nonexistent constant → error with suggestion
- `test_module_fn_name_shadows_builtin` -- qualified access uses module fn
- `test_module_deep_nested_path` -- 3+ level nested path
- `test_module_use_after_let_error` -- use after let → parse error

**Sub-tasks:**
- [ ] **11.1.1** Add `Module` AST type + `parse_module()` entry point
- [ ] **11.1.2** Switch `use` path separator from `::` to `.` in lexer/parser
- [ ] **11.1.3** Module registry (`HashMap<String, Arc<Module>>`) + filesystem loading
- [ ] **11.1.4** Phase B qualified name resolution (module functions + constants + aliases)
- [ ] **11.1.5** Topological sort for module-level `let` constants (Kahn's algorithm + cycle detection)
- [ ] **11.1.6** Phase C recursive call detection — reject self-referencing fn calls

**Done when:** All 30 module tests pass, `parse_module()` rejects emit/trace/use, qualified function calls and constant access work, toposort with cycle detection works
**Commit:** `feat(phase-11): CXL module parsing, registry, and qualified resolution`
**Commit ID:** --

---

### ⛔ [11.2] Pipeline Variables + Metadata  ← BLOCKED on [11.1] gate tests
**Gate tests that must pass first:**
- `test_vars_int` -- integer inference
- `test_vars_float` -- float inference
- `test_vars_bool` -- bool inference
- `test_vars_string` -- string inference
- `test_vars_collision_start_time` -- reserved name collision
- `test_vars_collision_execution_id` -- reserved name collision
- `test_pipeline_start_time_consistent` -- same value across records
- `test_pipeline_execution_id_uuid_v7` -- valid UUID v7
- `test_pipeline_batch_id_cli` -- --batch-id flag
- `test_pipeline_batch_id_default` -- auto UUID v7
- `test_vars_unknown_reference` -- pipeline.nonexistent → error
- `test_vars_nested_object_error` -- nested object rejected
- `test_vars_array_error` -- array rejected
- `test_vars_null_value` -- null rejected
- `test_vars_empty_section` -- empty vars Ok
- `test_vars_yaml12_bool_inference` -- YAML 1.2 bool rules
- `test_vars_quoted_number_is_string` -- quoted numbers are strings
- `test_pipeline_name_from_config` -- pipeline.name matches
- `test_pipeline_execution_id_format` -- UUID v7 format
- `test_pipeline_batch_id_auto_default` -- no flag → UUID v7
- `test_pipeline_batch_id_arbitrary_string` -- arbitrary string accepted
- `test_vars_collision_all_reserved` -- all 9 reserved names
- `test_pipeline_source_provenance_per_record` -- source_file/row differ

**Sub-tasks:**
- [ ] **11.2.1** Parse `pipeline.vars` + validate no collision with reserved names
- [ ] **11.2.2** Add `batch_id` to `EvalContext` + wire `--batch-id` CLI flag
- [ ] **11.2.3** Phase B resolution for `pipeline.*` — extend `PIPELINE_MEMBERS` + user vars

**Done when:** All 23 pipeline variable tests pass
**Commit:** `feat(phase-11): pipeline variables and batch_id metadata`
**Commit ID:** --

---

### ⛔ [11.3] Logging System (4 Levels)  ← BLOCKED on [11.2] gate tests
**Gate tests that must pass first:**
- 34 tests covering Level 1-4 logging, lifecycle hooks, edge cases, template resolution

**Sub-tasks:**
- [ ] **11.3.1** Define `LogDirective` config struct with lifecycle hook enum
- [ ] **11.3.2** Level 1: Wire lifecycle hooks into executor
- [ ] **11.3.3** Level 2: Load external log rules YAML, merge with overrides
- [ ] **11.3.4** Level 3: `.debug()` passthrough verification
- [ ] **11.3.5** Level 4: `trace` statement evaluation

**Done when:** All 34 logging tests pass
**Commit:** `feat(phase-11): 4-level logging system with lifecycle hooks`
**Commit ID:** --

---

### ⛔ [11.4] Declarative Validations + Schema Structure Validation  ← BLOCKED on [11.3] gate tests
**Gate tests that must pass first:**
- 26 tests covering inline CXL, module fn, DLQ, short-circuit, Phase C, enum, structure

**Sub-tasks:**
- [ ] **11.4.1** Define `ValidationEntry` config struct
- [ ] **11.4.2** Validation execution in Phase 2: after let, before emit
- [ ] **11.4.3** Phase C verification: fn exists, arg count, return Bool
- [ ] **11.4.4** Enum (allowed_values) enforcement
- [ ] **11.4.5** Structure ordering validation

**Done when:** All 26 validation tests pass
**Commit:** `feat(phase-11): declarative validations with DLQ routing`
**Commit ID:** --

---

### ⛔ [11.5] Security Hardening + Benchmarks  ← BLOCKED on [11.4] gate tests
**Gate tests that must pass first:**
- 37 tests covering path validation, overwrite protection, YAML DoS, env var, benchmarks, security edge cases

**Sub-tasks:**
- [ ] **11.5.1** Path security: `validate_path()` — reject `..`, absolute, `--base-dir`
- [ ] **11.5.2** Output overwrite protection
- [ ] **11.5.3** serde-saphyr DoS budgets
- [ ] **11.5.4** `cargo deny` setup
- [ ] **11.5.5** Benchmark suite

**Done when:** All 37 security/benchmark tests pass
**Commit:** `feat(phase-11): security hardening and benchmark suite`
**Commit ID:** --

---

### ⛔ [11.6] --dry-run -n Partial Processing  ← BLOCKED on [11.5] gate tests
**Gate tests that must pass first:**
- 26 tests covering TakeReader, CLI wiring, format-specific, pipeline interaction

**Sub-tasks:**
- [ ] **11.6.1** `TakeReader<R>` wrapper
- [ ] **11.6.2** CLI wiring: `-n` flag validation, `--dry-run-output`
- [ ] **11.6.3** Output routing: stdout default, file with flag

**Done when:** All 26 dry-run tests pass
**Commit:** `feat(phase-11): --dry-run -n partial processing`
**Commit ID:** --

---

## Gate test log

| Task | Test | Status | Run | Commit |
|------|------|--------|-----|--------|
| 11.0 | `cargo test metrics` (13 tests) | ✅ Passed | pre-existing | pre-existing |
| 11.1 | 33 module tests | ⛔ Not run | -- | -- |
| 11.2 | 23 pipeline variable tests | ⛔ Not run | -- | -- |
| 11.3 | 35 logging tests | ⛔ Not run | -- | -- |
| 11.4 | 28 validation tests | ⛔ Not run | -- | -- |
| 11.5 | 39 security/benchmark tests | ⛔ Not run | -- | -- |
| 11.6 | 30 dry-run tests | ⛔ Not run | -- | -- |

---

## Completed tasks

| Task | Name | Commit message | Commit ID | Completed |
|------|------|---------------|-----------|-----------|
| 11.0 | Execution Metrics Spool | (completed prior to tracking) | (pre-existing) | pre-phase-11 |

---

## Notes
- Task 11.0 was completed ahead of Phase 10; no dependency on channel system.
- Critical path is strictly linear: 11.1 → 11.2 → 11.3 → 11.4 → 11.5 → 11.6
- No parallelizable tasks after 11.0.
- 19 drill-phase decisions logged in phase file (2026-03-31).
- Validation complete (2026-03-31): READY. 5 blockers resolved, 19 warnings accepted. See VALIDATION-phase-11.md.
- Task 11.1 has 6 sub-tasks (11.1.6 added for recursive call detection).
- Task 11.1 test count updated to 33 (3 tests added: wildcard rejection, recursive call, colon rejection).
- Task 11.4 test count updated to 28 (2 tests added: nonexistent field, args literal treatment).
- env() references in Task 11.5 corrected to ${VAR} YAML interpolation validation.
- Existing code note: `pipeline_vars`, CLI flags, `resolve_pipeline()` fallback already exist — Task 11.2 completes wiring.
