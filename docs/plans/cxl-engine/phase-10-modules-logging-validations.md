# Phase 10: Modules, Logging, Validations, Polish

**Status:** 🔲 Not Started
**Depends on:** Phase 9 (schema system, all format readers complete)
**Entry criteria:** Schema loading, overrides, fixed-width, and multi-record all passing Phase 9 tests; CXL evaluator supports function calls (Phase 3)
**Exit criteria:** CXL modules loadable from `.cxl` files with qualified function calls; pipeline variables resolvable; 4-level logging system operational; declarative validations route failures to DLQ; security hardening passes; benchmark suite tracks records/sec + peak RSS; `cargo test --workspace` passes 80+ tests

---

## Tasks

### Task 10.1: CXL Modules (.cxl Files)
**Status:** 🔲 Not Started
**Blocked by:** Phase 9 exit criteria

**Description:**
Implement CXL module file parsing, module registry, and qualified function/constant
resolution. Modules are `.cxl` files containing `fn` declarations (pure functions) and
module-level `let` bindings (constants). Transforms reference modules via `use` statements.

**Implementation notes:**
- Module file syntax: `fn name(param: Type, ...) -> ReturnType { body }` — pure functions only. No `emit`, `trace`, or `.log()` allowed inside `fn` bodies. Validate and reject at parse time.
- Module-level `let` bindings: `let MAX_SALARY: Int = 500000;` — constants only (literal values, no field references, no function calls).
- `use` statement in transform CXL: `use validators;` imports the module. Maps to filesystem path `{rules_path}/validators.cxl`. Nested paths: `use reporting.fiscal;` maps to `{rules_path}/reporting/fiscal.cxl`.
- Module registry: `HashMap<String, Arc<ModuleAst>>` keyed by module path. Deduplicated — if two transforms both `use validators;`, only one parse. Registry populated during compiler Phase A.
- Qualified function calls: `validators.is_valid_email(Email)` — resolved during Phase B (name resolution). Look up module in registry, find function by name, bind arguments.
- Qualified constant access: `validators.MAX_SALARY` — resolved during Phase B as a read-only `Value`.
- No cross-file imports in v1: a module cannot `use` another module. Validate and reject.
- `pipeline.rules_path` config key (default `./rules/`). CLI override: `--rules-path <DIR>`.

**Acceptance criteria:**
- [ ] Module `.cxl` files parsed with `fn` declarations and `let` constants
- [ ] `emit`/`trace`/`.log()` in `fn` bodies rejected at parse time
- [ ] `use` statement maps module path to filesystem path under rules_path
- [ ] Module registry deduplicates across transforms
- [ ] Qualified function calls resolved during Phase B
- [ ] Qualified constant access resolved during Phase B
- [ ] Cross-file imports rejected with clear error
- [ ] `--rules-path` CLI flag overrides `pipeline.rules_path`

**Required unit tests (must pass before Task 10.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_module_parse_fn_declaration` | `fn is_valid(x: String) -> Bool { ... }` parsed with name, params, return type, body | ⛔ Hard gate |
| `test_module_parse_let_constant` | `let MAX: Int = 100;` parsed as constant with name, type, value | ⛔ Hard gate |
| `test_module_reject_emit_in_fn` | `fn bad() -> Bool { emit { ... } }` produces parse error | ⛔ Hard gate |
| `test_module_reject_trace_in_fn` | `fn bad() -> Bool { trace "msg" }` produces parse error | ⛔ Hard gate |
| `test_module_reject_log_in_fn` | `fn bad() -> String { Name.log("debug") }` produces parse error | ⛔ Hard gate |
| `test_module_use_path_resolution` | `use validators;` resolves to `{rules_path}/validators.cxl` | ⛔ Hard gate |
| `test_module_use_nested_path` | `use reporting.fiscal;` resolves to `{rules_path}/reporting/fiscal.cxl` | ⛔ Hard gate |
| `test_module_registry_dedup` | Two transforms using same module share single `Arc<ModuleAst>` (ptr equality) | ⛔ Hard gate |
| `test_module_qualified_fn_call` | `validators.is_valid_email(Email)` resolves and evaluates correctly | ⛔ Hard gate |
| `test_module_qualified_constant` | `validators.MAX_SALARY` resolves to constant value 500000 | ⛔ Hard gate |
| `test_module_reject_cross_import` | Module file containing `use other;` produces parse error | ⛔ Hard gate |
| `test_module_rules_path_cli_override` | `--rules-path /custom/` overrides default `./rules/` | ⛔ Hard gate |
| `test_module_missing_file_error` | `use nonexistent;` produces error naming the expected file path | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 10.2 status remains `Blocked` until all tests above pass.

---

### Task 10.2: Pipeline Variables + Metadata
**Status:** ⛔ Blocked (waiting on Task 10.1)
**Blocked by:** Task 10.1 — module registry must be working (variables share the Phase B resolver)

**Description:**
Implement `pipeline.vars` YAML section for user-defined typed constants and built-in
pipeline metadata fields accessible in CXL expressions.

**Implementation notes:**
- `pipeline.vars` YAML section: map of name → typed value. Supported types inferred from YAML scalar: integer, float, boolean, string. Example: `vars: { threshold: 0.95, region: "US", debug: false }`.
- Available in CXL as `pipeline.VARNAME` (e.g., `pipeline.threshold`, `pipeline.region`).
- Built-in pipeline metadata (always available, no config needed):
  - `pipeline.start_time`: `NaiveDateTime` set once at pipeline startup (not per-record).
  - `pipeline.execution_id`: UUID v7 generated once at pipeline startup.
  - `pipeline.name`: from `pipeline.name` YAML key.
  - `pipeline.batch_id`: from `--batch-id` CLI flag or generated UUID v7.
- Collision check: if a user var name matches a built-in (`start_time`, `name`, `execution_id`, `batch_id`), emit config error at load time.
- Resolution: `pipeline.*` references resolved during Phase B alongside field and module resolution.

**Acceptance criteria:**
- [ ] `pipeline.vars` parsed with correct type inference
- [ ] User variables accessible as `pipeline.VARNAME` in CXL
- [ ] `pipeline.start_time` set at startup, consistent across all records
- [ ] `pipeline.execution_id` is UUID v7, consistent across all records
- [ ] `pipeline.name` and `pipeline.batch_id` accessible in CXL
- [ ] Collision with built-in names produces config error

**Required unit tests (must pass before Task 10.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_vars_int` | `vars: { count: 42 }` → `pipeline.count` evaluates to `Integer(42)` | ⛔ Hard gate |
| `test_vars_float` | `vars: { rate: 0.05 }` → `pipeline.rate` evaluates to `Float(0.05)` | ⛔ Hard gate |
| `test_vars_bool` | `vars: { active: true }` → `pipeline.active` evaluates to `Bool(true)` | ⛔ Hard gate |
| `test_vars_string` | `vars: { region: "US" }` → `pipeline.region` evaluates to `String("US")` | ⛔ Hard gate |
| `test_vars_collision_start_time` | `vars: { start_time: 1 }` → config error naming the collision | ⛔ Hard gate |
| `test_vars_collision_execution_id` | `vars: { execution_id: "x" }` → config error | ⛔ Hard gate |
| `test_pipeline_start_time_consistent` | `pipeline.start_time` returns same value for record 1 and record 1000 | ⛔ Hard gate |
| `test_pipeline_execution_id_uuid_v7` | `pipeline.execution_id` is valid UUID v7 | ⛔ Hard gate |
| `test_pipeline_batch_id_cli` | `--batch-id RUN-001` → `pipeline.batch_id` evaluates to `"RUN-001"` | ⛔ Hard gate |
| `test_pipeline_batch_id_default` | No `--batch-id` → `pipeline.batch_id` is generated UUID v7 | ⛔ Hard gate |
| `test_vars_unknown_reference` | CXL `pipeline.nonexistent` → Phase B resolution error | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 10.3 status remains `Blocked` until all tests above pass.

---

### Task 10.3: Logging System (4 Levels)
**Status:** ⛔ Blocked (waiting on Task 10.2)
**Blocked by:** Task 10.2 — pipeline variables must be working (log templates reference pipeline.* vars)

**Description:**
Implement the 4-level logging system: YAML log directives, external log rules files,
`.log()` passthrough method, and `trace` statements. Set up `tracing-subscriber` with
env-filter in the CLI binary.

**Implementation notes:**
- **Level 1 — YAML log directives:** per-transform `log:` section with `level` (info/warn/error/debug/trace), `when` (CXL bool expression — optional, default always), `message` (template string with `{field_name}` interpolation), `condition` (alias for `when`), `fields` (list of field names to include as structured key-value pairs), `every` (sampling — log every Nth matching record).
- **Level 2 — External log rules:** `pipeline.log_rules` config key referencing a YAML file. Each rule has same schema as Level 1. Transforms reference rules via `log_rule: rule_name`. Override merge: transform-level `log:` keys override matched rule keys.
- **Level 3 — `.log(prefix)` passthrough:** `FieldName.log("debug_prefix")` in CXL returns the field value unchanged while emitting a `tracing::trace!` event with the prefix and value. Zero overhead when trace level disabled. NOT allowed inside `fn` bodies (validated at parse time, already covered by Task 10.1).
- **Level 4 — `trace` statement:** standalone statement in transform CXL: `trace "message {field}" when condition level warn`. `when` guard is optional (default: always). `level` override is optional (default: trace). `{field_name}` interpolation in message string. NOT allowed inside `fn` bodies.
- Template variables available in log messages: provenance fields (`_source_file`, `_source_row`), pipeline counters (`_ok_count`, `_dlq_count`, `_total_count`), record fields, `_transform_name`, `_transform_duration_ms`.
- `tracing-subscriber` setup: in `clinker/src/main.rs`, initialize with `EnvFilter` from `--log-level` flag. Format: compact, timestamps, target module.

**Acceptance criteria:**
- [ ] Level 1 YAML log directives emit at configured level with interpolated messages
- [ ] Level 1 `when` condition filters log emission per record
- [ ] Level 1 `every` sampling logs every Nth match
- [ ] Level 2 external log rules loaded and merged with transform overrides
- [ ] Level 3 `.log()` passthrough returns value unchanged, emits trace event
- [ ] Level 4 `trace` statement emits with guard, level override, and interpolation
- [ ] Template variables resolved in log messages
- [ ] `tracing-subscriber` configured with env-filter from `--log-level`

**Required unit tests (must pass before Task 10.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_log_level1_basic_emit` | YAML `log: { level: info, message: "processed {name}" }` emits info event | ⛔ Hard gate |
| `test_log_level1_when_condition` | `when: "Amount > 1000"` — log emitted only for matching records | ⛔ Hard gate |
| `test_log_level1_every_sampling` | `every: 100` — log emitted for records 100, 200, 300 only | ⛔ Hard gate |
| `test_log_level1_fields_structured` | `fields: [name, amount]` — structured key-value pairs in log event | ⛔ Hard gate |
| `test_log_level2_external_rules_load` | External log rules YAML file loaded and rules accessible by name | ⛔ Hard gate |
| `test_log_level2_rule_reference` | Transform with `log_rule: audit_trail` inherits rule's log config | ⛔ Hard gate |
| `test_log_level2_override_merge` | Transform-level `level: warn` overrides rule's `level: info` | ⛔ Hard gate |
| `test_log_level3_passthrough_value` | `Name.log("dbg")` returns same value as `Name` alone | ⛔ Hard gate |
| `test_log_level3_emits_trace` | `.log("prefix")` emits tracing::trace event with prefix and value | ⛔ Hard gate |
| `test_log_level4_trace_basic` | `trace "saw {Name}"` emits trace-level event with interpolated field | ⛔ Hard gate |
| `test_log_level4_trace_when_guard` | `trace "big" when Amount > 1000` — only emits when condition true | ⛔ Hard gate |
| `test_log_level4_trace_level_override` | `trace "alert" level warn` emits at warn level instead of trace | ⛔ Hard gate |
| `test_log_template_provenance_vars` | `{_source_file}` and `{_source_row}` resolve in log message template | ⛔ Hard gate |
| `test_log_template_pipeline_counters` | `{_ok_count}` and `{_dlq_count}` resolve in log message template | ⛔ Hard gate |
| `test_log_template_transform_meta` | `{_transform_name}` and `{_transform_duration_ms}` resolve in template | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 10.4 status remains `Blocked` until all tests above pass.

---

### Task 10.4: Declarative Validations
**Status:** ⛔ Blocked (waiting on Task 10.3)
**Blocked by:** Task 10.3 — logging must be working (warn-severity validations log and continue)

**Description:**
Implement the declarative `validations:` section on transforms. Each validation is a
named check with a CXL bool expression or module function reference, arguments, and
severity routing (error → DLQ, warn → log + continue).

**Implementation notes:**
- `validations:` YAML section on each transform: list of validation entries.
- Each entry: `name` (human-readable label), `check` (module function reference like `validators.is_valid_email` OR inline CXL bool expression), `args` (named parameters for function checks — YAML literals or `pipeline.*` references, NOT field references), `severity` (`error` or `warn`, default `error`).
- Severity `error`: record routed to DLQ with `_cxl_dlq_error_category: validation_failure` and `_cxl_dlq_error_detail` containing the validation name. Short-circuit: first error-severity failure stops remaining checks for that record.
- Severity `warn`: log warning with validation name and field values, continue processing. No short-circuit — all remaining checks still run.
- Execution order: Phase 2, after all `let` bindings evaluated, before `emit`. Validations run in declaration order.
- Phase C verification (compile time): function reference exists in module registry, argument count matches function signature, function return type is `Bool`. Inline CXL expression must type-check to `Bool`.
- `args:` map: keys are parameter names matching the function signature. Values are YAML literals (`threshold: 0.95`) or pipeline variable references (`max: pipeline.MAX_SALARY`). Field references are NOT allowed in args (fields are passed as the function's first positional parameter implicitly).

**Acceptance criteria:**
- [ ] Inline CXL bool expression evaluated as validation check
- [ ] Module function reference resolved and called with args
- [ ] Error severity routes record to DLQ with `validation_failure` category
- [ ] Warn severity logs warning and continues processing
- [ ] Short-circuit: first error stops remaining checks for that record
- [ ] Warn checks not short-circuited (all run even after warn)
- [ ] Validations execute after let bindings, before emit
- [ ] Phase C verifies function exists, arg count, return type Bool
- [ ] Field references in args rejected at config parse time

**Required unit tests (must pass before Task 10.5 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_validation_inline_cxl_pass` | `check: "Amount > 0"` with Amount=100 → validation passes, record emitted | ⛔ Hard gate |
| `test_validation_inline_cxl_fail_error` | `check: "Amount > 0"` with Amount=-5, severity error → record sent to DLQ | ⛔ Hard gate |
| `test_validation_inline_cxl_fail_warn` | `check: "Amount > 0"` with Amount=-5, severity warn → warning logged, record emitted | ⛔ Hard gate |
| `test_validation_module_fn_call` | `check: validators.is_positive` with `args: {}` calls module function | ⛔ Hard gate |
| `test_validation_module_fn_with_args` | `check: validators.in_range` with `args: { min: 0, max: pipeline.MAX }` passes args | ⛔ Hard gate |
| `test_validation_dlq_category` | DLQ record from validation failure has `_cxl_dlq_error_category: validation_failure` | ⛔ Hard gate |
| `test_validation_dlq_detail` | DLQ record contains validation name in `_cxl_dlq_error_detail` | ⛔ Hard gate |
| `test_validation_short_circuit_error` | Two error validations: first fails → second never evaluated | ⛔ Hard gate |
| `test_validation_no_short_circuit_warn` | Two warn validations: first fails → second still evaluated | ⛔ Hard gate |
| `test_validation_order_after_let` | Validation references a `let` binding — binding value available | ⛔ Hard gate |
| `test_validation_order_before_emit` | Record failing validation not present in output (never emitted) | ⛔ Hard gate |
| `test_validation_phase_c_fn_missing` | `check: validators.nonexistent` → compile error at Phase C | ⛔ Hard gate |
| `test_validation_phase_c_arg_count` | Function expects 2 args, 1 provided → compile error at Phase C | ⛔ Hard gate |
| `test_validation_phase_c_return_type` | Function returns String (not Bool) → compile error at Phase C | ⛔ Hard gate |
| `test_validation_args_reject_field_ref` | `args: { val: Amount }` (field reference) → config error | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 10.5 status remains `Blocked` until all tests above pass.

---

### Task 10.5: Security Hardening + Benchmarks
**Status:** ⛔ Blocked (waiting on Task 10.4)
**Blocked by:** Task 10.4 — validations must be working (benchmark suite exercises full pipeline including validations)

**Description:**
Implement path security validation, output overwrite protection, YAML parser DoS budgets,
environment variable name validation, and a benchmark suite tracking throughput and memory.
Verify zero C dependencies in the dependency tree.

**Implementation notes:**
- Path validation: reject any path containing `..` (directory traversal). Reject absolute paths unless `--allow-absolute-paths` flag is set. Apply to all paths in YAML config: input files, output files, schema files, rules_path, log_rules.
- `--base-dir <DIR>` resolution: all relative paths in YAML resolved relative to `base_dir`. Default: directory containing the YAML pipeline file itself.
- Output overwrite protection: if output file already exists, refuse to run unless `--force` flag is set or `overwrite: true` in output config. Check performed before any data processing begins.
- `serde-saphyr` DoS budgets: set `recursion_depth_limit: 64`, `sequence_length_limit: 10_000`, `document_size_limit: 10_485_760` (10MB). Applied at YAML parse time.
- Environment variable name validation: names referenced in `env()` CXL function must match `[A-Z_][A-Z0-9_]*`. Reject at parse time with descriptive error.
- Benchmark suite (criterion or custom harness): 50MB XML input, 20MB CSV input, 10MB JSON input. Track `records/sec` throughput and `peak RSS` (via `/proc/self/status` VmHWM on Linux). Benchmarks in `benches/` directory, not gated on CI but tracked.
- `cargo deny check`: verify zero C dependencies in the entire dependency tree. Run as part of CI. Any crate pulling in a `-sys` crate or `cc` build dependency fails the check.

**Acceptance criteria:**
- [ ] Paths with `..` rejected with descriptive error
- [ ] Absolute paths rejected unless `--allow-absolute-paths`
- [ ] `--base-dir` resolves all relative paths correctly
- [ ] Output overwrite refused without `--force` or `overwrite: true`
- [ ] Overwrite check runs before any data processing
- [ ] YAML DoS budgets enforced (recursion, sequence length, document size)
- [ ] Environment variable names validated against `[A-Z_][A-Z0-9_]*`
- [ ] Benchmark suite runs and reports records/sec + peak RSS
- [ ] `cargo deny check` confirms zero C dependencies

**Required unit tests (must pass before Phase 10 exit):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_path_reject_dotdot` | Path `../etc/passwd` rejected with error mentioning `..` | ⛔ Hard gate |
| `test_path_reject_absolute` | Path `/tmp/output.csv` rejected without `--allow-absolute-paths` | ⛔ Hard gate |
| `test_path_allow_absolute_flag` | Path `/tmp/output.csv` accepted with `--allow-absolute-paths` | ⛔ Hard gate |
| `test_path_base_dir_resolution` | Relative path `data/input.csv` with `--base-dir /opt/` resolves to `/opt/data/input.csv` | ⛔ Hard gate |
| `test_path_base_dir_default` | No `--base-dir` → relative paths resolved from YAML file's parent directory | ⛔ Hard gate |
| `test_overwrite_refuse_existing` | Output file exists, no `--force` → error before any processing | ⛔ Hard gate |
| `test_overwrite_force_flag` | Output file exists, `--force` set → processing proceeds | ⛔ Hard gate |
| `test_overwrite_config_flag` | Output file exists, `overwrite: true` in config → processing proceeds | ⛔ Hard gate |
| `test_overwrite_check_before_processing` | Overwrite error raised before any input file is opened | ⛔ Hard gate |
| `test_yaml_dos_recursion_depth` | YAML with 100 levels of nesting rejected (limit 64) | ⛔ Hard gate |
| `test_yaml_dos_sequence_length` | YAML with 20,000-element array rejected (limit 10,000) | ⛔ Hard gate |
| `test_yaml_dos_document_size` | YAML file >10MB rejected (limit 10,485,760 bytes) | ⛔ Hard gate |
| `test_env_var_name_valid` | `env("DATABASE_URL")` accepted | ⛔ Hard gate |
| `test_env_var_name_invalid_lowercase` | `env("database_url")` rejected with descriptive error | ⛔ Hard gate |
| `test_env_var_name_invalid_leading_digit` | `env("1BAD")` rejected with descriptive error | ⛔ Hard gate |
| `test_cargo_deny_zero_c_deps` | `cargo deny check` exits 0 (no C dependencies in tree) | ⛔ Hard gate |
| `test_benchmark_csv_20mb_runs` | 20MB CSV benchmark completes and reports records/sec | ⛔ Hard gate |
| `test_benchmark_json_10mb_runs` | 10MB JSON benchmark completes and reports records/sec | ⛔ Hard gate |
| `test_benchmark_xml_50mb_runs` | 50MB XML benchmark completes and reports records/sec | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 10.6 status remains `Blocked` until all tests above pass.

---

### Task 10.6: --dry-run -n Partial Processing
**Status:** ⛔ Blocked (waiting on Task 10.5)
**Blocked by:** Task 10.5 — security hardening must be in place before partial processing

**Description:**
Implement `clinker --dry-run -n <N> pipeline.yaml` which runs the full pipeline on the
first N records per input. Validates types, CXL evaluation, output format. Writes output
to stdout (or a temp file with `--dry-run-output`). Deferred from Phase 8 (decision #34).

**Implementation notes:**
- `-n <N>` flag on `Cli` struct: `Option<u64>`, only valid with `--dry-run`.
- When `-n` is set, each `FormatReader` wraps in a `Take` adapter that yields at most N records.
- Pipeline runs normally through all phases (ingestion, transform, projection, output).
- Output defaults to stdout; `--dry-run-output <PATH>` writes to file instead.
- Exit codes: same as normal run (0 for success, 2 for partial DLQ, etc.).
- Without `-n`, `--dry-run` remains config-validation-only (Phase 8 behavior).

**Acceptance criteria:**
- [ ] `-n 100` processes only the first 100 records per input
- [ ] `-n` without `--dry-run` produces an error
- [ ] `--dry-run` without `-n` validates config only (existing behavior preserved)
- [ ] Output written to stdout by default
- [ ] `--dry-run-output <PATH>` redirects to file
- [ ] Exit codes match normal pipeline run

**Required unit tests (must pass before Phase 10 exit):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_dry_run_n_limits_records` | `-n 10` with 1000-record input produces 10 output records | ⛔ Hard gate |
| `test_dry_run_n_without_dry_run_error` | `-n 100` without `--dry-run` produces CLI error | ⛔ Hard gate |
| `test_dry_run_without_n_config_only` | `--dry-run` alone validates config, reads no data | ⛔ Hard gate |
| `test_dry_run_output_to_file` | `--dry-run-output out.csv` writes to file instead of stdout | ⛔ Hard gate |
| `test_dry_run_n_exit_code_2_dlq` | Partial DLQ during dry-run produces exit code 2 | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 10 exit criteria not met until all tests above pass.
