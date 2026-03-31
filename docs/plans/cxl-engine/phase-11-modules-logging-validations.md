# Phase 11: Modules, Logging, Validations, Polish

**Status:** 🔄 In Progress (Task 11.0 complete; Tasks 11.1–11.6 blocked on Phase 10)
**Depends on:** Phase 10 (channel overrides + composition system)
**Entry criteria:** Schema loading, overrides, fixed-width, and multi-record all passing Phase 9 tests; channel overrides and composition system passing Phase 10 tests; CXL evaluator supports function calls (Phase 3)
**Exit criteria:** CXL modules loadable from `.cxl` files with qualified function calls; pipeline variables resolvable; 4-level logging system operational; declarative validations route failures to DLQ; security hardening passes; benchmark suite tracks records/sec + peak RSS; execution metrics spool operational; `cargo test --workspace` passes 80+ tests

---

## Tasks

### Task 11.0: Execution Metrics Spool
**Status:** ✅ Complete
**Blocked by:** None (implemented ahead of Phase 10; no dependency on channel system)

**Description:**
Each pipeline execution atomically writes one JSON spool file to a configurable directory.
A `clinker metrics collect` subcommand sweeps the spool and appends records to an NDJSON
archive. Spool files are uniquely named by execution UUID v7, so 30–40 concurrent servers
never contend on the same file.

**Design:** Write-then-rename pattern (`<id>.json.tmp` → `<id>.json`). If the spool write
fails (NFS unavailable, disk full), all metric fields are emitted via `tracing::warn!` so
the scheduler's captured stderr preserves the data. No new crate dependencies.

**Config precedence (highest → lowest):**
1. `--metrics-spool-dir <path>` CLI flag
2. `CLINKER_METRICS_SPOOL_DIR` environment variable
3. `pipeline.metrics.spool_dir` in the YAML config
4. Disabled (no spool file written)

**YAML example:**
```yaml
pipeline:
  name: daily_orders
  metrics:
    spool_dir: /var/spool/clinker
```

**Collect command:**
```bash
clinker metrics collect \
  --spool-dir /var/spool/clinker \
  --output-file /data/metrics/archive.ndjson \
  --delete-after-collect
```

**Key files:**
- `crates/clinker-core/src/metrics.rs` — `ExecutionMetrics`, `write_spool()`, `collect_spool()`, `append_ndjson()`, `resolve_spool_dir()`
- `crates/clinker-core/src/executor.rs` — `ExecutionReport` struct (replaces `(PipelineCounters, Vec<DlqEntry>)` return tuple); captures timing + execution mode + peak RSS
- `crates/clinker-core/src/pipeline/memory.rs` — `MemoryBudget::observe()` + `peak_rss` field
- `crates/clinker-core/src/config.rs` — `MetricsConfig` struct + `pipeline.metrics` YAML key
- `crates/clinker/src/main.rs` — `clinker run` / `clinker metrics collect` subcommand structure

**Breaking change:** CLI restructured from `clinker pipeline.yaml` → `clinker run pipeline.yaml`.

**Acceptance criteria:**
- [x] Spool file written atomically (write-then-rename, no partial files visible to collector)
- [x] `.tmp` files skipped by `collect_spool()`
- [x] Config precedence: CLI > env var > YAML > disabled
- [x] Spool write failure emits `tracing::warn!` with all metric fields inline (never silent loss)
- [x] `clinker metrics collect` sweeps spool, appends NDJSON, optionally deletes spool files
- [x] `--dry-run` on collect prints what would be collected without writing
- [x] `peak_rss_bytes` tracked via `MemoryBudget::observe()` at chunk boundaries
- [x] All 10 unit tests passing

**Tests:** `cargo test metrics` — 10 tests in `clinker-core::metrics::tests` + 3 in `clinker::tests`

---

### Task 11.1: CXL Modules (.cxl Files)
**Status:** 🔲 Not Started
**Blocked by:** Phase 9 + Phase 10 exit criteria

**Description:**
Implement CXL module file parsing, module registry, and qualified function/constant
resolution. Modules are `.cxl` files containing `fn` declarations (pure functions) and
module-level `let` bindings (constants). Transforms reference modules via `use` statements.

**Implementation notes:**
- Module file syntax: `fn name(params) = expr` — pure functions only, untyped parameters, single expression body. No `emit` or `trace` allowed inside `fn` bodies. `.debug()` IS allowed inside `fn` bodies (pragmatic DX exception to purity; zero overhead when trace disabled). Validate `emit`/`trace` rejection at parse time via dedicated `parse_module()` entry point.
- Module-level `let` bindings: `let MAX_SALARY = 500000` — constants only (literal values or expressions over other module-level `let` bindings). No field references, no `pipeline.*`, no `window.*`. Evaluated via **topological sort** (order-independent, Kahn's algorithm with cycle detection).
- `use` statement in transform CXL: `use validators` imports the module. Maps to filesystem path `{rules_path}/validators.cxl`. Nested paths use `.` separator: `use reporting.fiscal` maps to `{rules_path}/reporting/fiscal.cxl`. Alias supported: `use shared.date_helpers as dates`.
- Module parse entry point: **Separate `parse_module()` returning dedicated `ModuleAst`** (HCL/CUE/syn/rustc pattern). Rejects `emit`/`trace`/`use` at the token level. Reuses existing `parse_fn_decl()` and `parse_let()` internals.
- Module registry: `HashMap<String, Arc<Module>>` keyed by module path. Deduplicated — if two transforms both `use validators`, only one parse. Registry populated during compiler Phase A.
- Qualified function calls: `validators.is_valid_email(Email)` — resolved during Phase B (name resolution). Look up module in registry, find function by name, bind arguments.
- Qualified constant access: `validators.MAX_SALARY` — resolved during Phase B as a read-only `Value`.
- No cross-file imports in v1: a module cannot `use` another module. Validate and reject.
- `pipeline.rules_path` config key (default `./rules/`). CLI override: `--rules-path <DIR>`.

#### Sub-tasks
- [ ] **11.1.1** Add `Module` AST type + `parse_module()` entry point (reuses `parse_fn_decl` + `parse_let`)
- [ ] **11.1.2** Switch `use` path separator from `::` to `.` in lexer/parser (~5-line change)
- [ ] **11.1.3** Module registry (`HashMap<String, Arc<Module>>`) + filesystem loading from `rules_path`
- [ ] **11.1.4** Phase B qualified name resolution (module functions + constants + aliases)
- [ ] **11.1.5** Topological sort for module-level `let` constants (Kahn's algorithm + cycle detection)
- [ ] **11.1.6** Phase C recursive call detection — walk fn body for self-referencing calls; reject with "recursive calls are not supported"

> ✅ **[V-1-1] Resolved:** Delete unused `ModuleDecl` from `ast.rs`, use `Module` as the type name (matches `Program`/`FnDecl` convention). Plan prose reference to "ModuleAst" normalized to `Module`.

> ✅ **[V-8-1] Resolved:** Added sub-task 11.1.6 for Phase C recursive call detection. Single-expression bodies + no cross-module imports = only direct self-recursion possible. Walk fn body AST for FunctionCall nodes matching the function's own name.

> ✅ **[V-8-2] Resolved:** Wildcard import rejection (`use mod.*`) handled in 11.1.2 — parser produces "wildcard imports not supported" error when `*` follows dot in use path. Test `test_module_reject_wildcard_import` added below.

#### Code scaffolding
```rust
// cxl/src/ast.rs

/// A parsed CXL module file (only fn declarations and let constants).
pub struct Module {
    pub functions: Vec<FnDecl>,
    pub constants: Vec<ModuleConst>,
    pub span: Span,
}

/// A module-level constant binding.
pub struct ModuleConst {
    pub node_id: NodeId,
    pub name: Box<str>,
    pub expr: Expr,
    pub span: Span,
}
```

```rust
// cxl/src/parser.rs

pub struct ModuleParseResult {
    pub module: Module,
    pub errors: Vec<ParseError>,
    pub node_count: u32,
}

impl Parser {
    /// Parse a .cxl module file. Only `fn` and `let` are accepted.
    /// `emit`, `trace`, and `use` produce immediate parse errors.
    pub fn parse_module(source: &str) -> ModuleParseResult {
        todo!()
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Module`, `ModuleConst` | `cxl/src/ast.rs` | AST types alongside existing `FnDecl` |
| `parse_module()`, `ModuleParseResult` | `cxl/src/parser.rs` | New entry point, reuses internals |
| Module registry + loader | `clinker-core/src/modules.rs` | New file — `load_modules()`, `ModuleRegistry` |
| Phase B module resolution | `cxl/src/resolve/pass.rs` | Extend `UseStmt` handling + qualified name resolution |
| Toposort for constants | `cxl/src/module_eval.rs` | New file — `evaluate_module_constants()` |

#### Intra-phase dependencies
- Requires: Phase 9 + 10 complete (schema loading, channel system)
- Unblocks: Task 11.2 (pipeline variables share the Phase B resolver)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    // ── Module parsing ──────────────────────────────────────────

    /// fn declaration parsed with name, untyped params, expression body
    #[test]
    fn test_module_parse_fn_declaration() {
        // Arrange: "fn is_valid(x) = x > 0"
        // Assert: name="is_valid", params=["x"], body=Binary(Gt)
    }

    /// let constant parsed with name and literal value
    #[test]
    fn test_module_parse_let_constant() {
        // Arrange: "let MAX = 100"
        // Assert: name="MAX", expr=Literal(Int(100))
    }

    /// emit inside fn body rejected at parse time
    #[test]
    fn test_module_reject_emit_in_fn() {
        // Arrange: "fn bad() = emit { x = 1 }"
        // Assert: parse error
    }

    /// trace inside fn body rejected at parse time
    #[test]
    fn test_module_reject_trace_in_fn() {
        // Arrange: "fn bad() = trace \"msg\""
        // Assert: parse error (trace is a statement, not expression)
    }

    /// use statement resolves module path via dot separator
    #[test]
    fn test_module_use_path_resolution() {
        // Arrange: `use validators` with rules_path="/rules/"
        // Assert: resolved to "/rules/validators.cxl"
    }

    /// Nested dot path resolves to nested directory
    #[test]
    fn test_module_use_nested_path() {
        // Arrange: `use reporting.fiscal` with rules_path="/rules/"
        // Assert: resolved to "/rules/reporting/fiscal.cxl"
    }

    /// Two transforms sharing a module get single Arc (dedup)
    #[test]
    fn test_module_registry_dedup() {
        // Assert: Arc::ptr_eq for same module from registry
    }

    /// Qualified function call resolves and evaluates
    #[test]
    fn test_module_qualified_fn_call() {
        // validators.is_valid_email(Email) with Email="test@example.com"
        // Assert: Bool(true)
    }

    /// Qualified constant access resolves to value
    #[test]
    fn test_module_qualified_constant() {
        // validators.MAX_SALARY → Int(500000)
    }

    /// Module containing `use` → parse error
    #[test]
    fn test_module_reject_cross_import() {
        // "use other" → "modules cannot import other modules"
    }

    /// --rules-path CLI flag overrides default
    #[test]
    fn test_module_rules_path_cli_override() {
        // --rules-path /custom/ → looks for /custom/validators.cxl
    }

    /// Missing module file → error naming expected path
    #[test]
    fn test_module_missing_file_error() {
        // `use nonexistent` → error contains "/rules/nonexistent.cxl"
    }

    // ── Topological sort for constants ──────────────────────────

    /// Forward reference works (order-independent)
    #[test]
    fn test_module_const_forward_reference() {
        // "let B = A + 1\nlet A = 5" → A=5, B=6
    }

    /// Cyclic constants → error
    #[test]
    fn test_module_const_cycle_detection() {
        // "let A = B + 1\nlet B = A + 1" → cycle error
    }

    /// Duplicate constant names rejected
    #[test]
    fn test_module_const_duplicate_name() {
        // "let X = 1\nlet X = 2" → duplicate error
    }

    /// Duplicate function names rejected
    #[test]
    fn test_module_fn_duplicate_name() {
        // "fn f(x) = x\nfn f(y) = y + 1" → duplicate error
    }

    /// Constant referencing field → error
    #[test]
    fn test_module_const_reject_field_reference() {
        // "let BAD = Amount" → "module constants cannot reference fields"
    }

    /// use with alias resolves via alias
    #[test]
    fn test_module_use_alias() {
        // `use shared.date_helpers as dates` then `dates.parse_date(raw)`
        // Assert: resolves to shared/date_helpers.cxl function
    }

    // ── Module file edge cases ──────────────────────────────────

    /// Empty module file is valid
    #[test]
    fn test_module_empty_file() {
        // "" → Ok, 0 functions, 0 constants
    }

    /// Module with only constants
    #[test]
    fn test_module_constants_only() {
        // "let A = 1\nlet B = 2" → functions empty, constants=[A,B]
    }

    /// Module with only functions
    #[test]
    fn test_module_functions_only() {
        // "fn add(a, b) = a + b" → constants empty, functions=[add]
    }

    /// Function with zero parameters
    #[test]
    fn test_module_fn_no_params() {
        // "fn pi() = 3.14159" → params=[], body=Literal(Float)
    }

    /// Function body with method chain
    #[test]
    fn test_module_fn_method_chain_body() {
        // "fn clean(val) = val.trim().upper()" → MethodCall chain
    }

    /// Function body with if/then/else
    #[test]
    fn test_module_fn_conditional_body() {
        // "fn clamp(val, lo, hi) = if val < lo then lo else if val > hi then hi else val"
    }

    /// Function body with match
    #[test]
    fn test_module_fn_match_body() {
        // fn tier(score) = match { score >= 90 => "A", ... }
    }

    /// Module with comments
    #[test]
    fn test_module_with_comments() {
        // "# Utility functions\nfn add(a, b) = a + b\n# Constants\nlet MAX = 100"
    }

    /// Qualified access to non-existent constant → Phase B error
    #[test]
    fn test_module_qualified_nonexistent_constant() {
        // validators.MIN (module has MAX but not MIN) → error suggesting "MAX"
    }

    /// Module fn name doesn't shadow built-in methods (qualified access)
    #[test]
    fn test_module_fn_name_shadows_builtin() {
        // module fn upper(val) = ...; CXL: validators.upper(Name) → module fn
    }

    /// Deep nested path (3+ levels)
    #[test]
    fn test_module_deep_nested_path() {
        // `use shared.utils.string_helpers` → shared/utils/string_helpers.cxl
    }

    /// use after let/emit → parse error
    #[test]
    fn test_module_use_after_let_error() {
        // "let x = 1\nuse validators" → "use must appear before let/emit/trace"
    }

    /// Wildcard import rejected with targeted error
    #[test]
    fn test_module_reject_wildcard_import() {
        // "use validators.*" → "wildcard imports not supported in v1"
    }

    /// Recursive function call rejected at Phase C
    #[test]
    fn test_module_fn_recursive_call_rejected() {
        // "fn f(x) = if x > 0 then f(x - 1) else x" → Phase C error
    }

    /// :: separator rejected after migration to .
    #[test]
    fn test_module_use_colons_rejected() {
        // "use validators::helpers" → parse error suggesting "validators.helpers"
    }
}
```

> ✅ **[V-2-2] Resolved:** Task 11.1 header fixed to "Phase 9 + Phase 10 exit criteria".

> ✅ **[V-2-3] Accepted:** `use` separator change from `::` to `.` (11.1.2) will break 2 existing parser tests at `parser.rs:1124-1144`. Update them as part of 11.1.2. Test `test_module_use_colons_rejected` added above.

> ✅ **[V-7-2] Accepted:** BOM/non-UTF-8 in .cxl files — strip UTF-8 BOM in `load_modules()`, reject non-UTF-8 with clear error naming file path. Handle in 11.1.3 (no separate sub-task needed).

> ✅ **[V-8-3] Accepted:** Add 1MB pre-check in `load_modules()` for .cxl file size. Handle in 11.1.3.

#### Risk / gotcha
> **Parser entry point drift:** Two parse entry points (`parse()` and `parse_module()`)
> can diverge when new statement types are added. Mitigated by exhaustive token match in
> `parse_module()` (wildcard with descriptive error) and hard-gate rejection tests.

> ⛔ **Hard gate:** Task 11.2 status remains `Blocked` until all tests above pass.

---

### Task 11.2: Pipeline Variables + Metadata
**Status:** ⛔ Blocked (waiting on Task 11.1)
**Blocked by:** Task 11.1 — module registry must be working (variables share the Phase B resolver)

**Description:**
Complete the `pipeline.vars` wiring (scaffolding already exists: `PipelineMeta.vars`,
`EvalContext.pipeline_vars`, `resolve_pipeline()` fallback). Add `batch_id` to
`EvalContext` and `PIPELINE_MEMBERS`. Fix `batch_id` misrouting at `main.rs:365`
(currently assigned to `execution_id`).

**Implementation notes:**
- `pipeline.vars` YAML section: map of name → typed value. Types inferred from YAML 1.2 Core Schema via native serde-saphyr deserialization into `serde_json::Value`: unquoted `42` → Int, `0.05` → Float, `true` → Bool, quoted `"US"` → String. YAML 1.2 avoids the Norway problem (`yes`/`no`/`on`/`off` stay strings). Nested objects and arrays in vars → config error (scalars only).
- Available in CXL as `pipeline.VARNAME` (e.g., `pipeline.threshold`, `pipeline.region`).
- Built-in pipeline metadata (always available, no config needed):
  - `pipeline.start_time`: `NaiveDateTime` set once at pipeline startup (not per-record).
  - `pipeline.execution_id`: UUID v7 generated once at pipeline startup.
  - `pipeline.name`: from `pipeline.name` YAML key.
  - `pipeline.batch_id`: from `--batch-id` CLI flag or generated UUID v7. User-supplied for orchestrator correlation (Airflow/Dagster pattern). Defaults to auto UUID v7 for ad-hoc runs.
- Reserved names (collision check at config parse time): `start_time`, `name`, `execution_id`, `batch_id`, `total_count`, `ok_count`, `dlq_count`, `source_file`, `source_row` (9 names).
- Resolution: `pipeline.*` references resolved during Phase B alongside field and module resolution.

#### Sub-tasks
- [ ] **11.2.1** Parse `pipeline.vars` into `IndexMap<String, Value>` via native serde; validate no collision with 9 reserved names; reject nested objects/arrays
- [ ] **11.2.2** Add `batch_id` to `EvalContext` pipeline members; wire `--batch-id` CLI flag to `EvalContext`
- [ ] **11.2.3** Phase B resolution for `pipeline.*` — extend `PIPELINE_MEMBERS` const with `batch_id`; resolve user vars from `pipeline_vars` map

#### Code scaffolding
```rust
// clinker-core/src/config.rs — PipelineMeta already exists
// Add: vars: Option<IndexMap<String, serde_json::Value>>

// cxl/src/eval/context.rs — EvalContext
// Add: pipeline_batch_id: Arc<str>
// Extend resolve_pipeline() match with "batch_id" arm

// cxl/src/resolve/pass.rs
// Add "batch_id" to PIPELINE_MEMBERS const
// For unrecognized pipeline.X, check pipeline_vars keys

const RESERVED_PIPELINE_NAMES: &[&str] = &[
    "start_time", "name", "execution_id", "batch_id",
    "total_count", "ok_count", "dlq_count", "source_file", "source_row",
];
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `vars` field on `PipelineMeta` | `clinker-core/src/config.rs` | `Option<IndexMap<String, serde_json::Value>>` |
| `pipeline_batch_id` | `cxl/src/eval/context.rs` | `Arc<str>` on `EvalContext` |
| `PIPELINE_MEMBERS` | `cxl/src/resolve/pass.rs` | Add `batch_id` to existing const |
| Reserved name check | `clinker-core/src/config.rs` | During config loading |

#### Intra-phase dependencies
- Requires: Task 11.1 (module registry for Phase B resolver integration)
- Unblocks: Task 11.3 (log templates reference `pipeline.*` vars)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    // ── Type inference ──────────────────────────────────────────

    #[test] fn test_vars_int()    { /* vars: { count: 42 } → Integer(42) */ }
    #[test] fn test_vars_float()  { /* vars: { rate: 0.05 } → Float(0.05) */ }
    #[test] fn test_vars_bool()   { /* vars: { active: true } → Bool(true) */ }
    #[test] fn test_vars_string() { /* vars: { region: "US" } → String("US") */ }

    // ── Collision detection ─────────────────────────────────────

    #[test] fn test_vars_collision_start_time()   { /* config error */ }
    #[test] fn test_vars_collision_execution_id()  { /* config error */ }

    // ── Built-in metadata ───────────────────────────────────────

    #[test] fn test_pipeline_start_time_consistent() { /* same value for record 1 and 1000 */ }
    #[test] fn test_pipeline_execution_id_uuid_v7()  { /* valid UUID v7 */ }
    #[test] fn test_pipeline_batch_id_cli()          { /* --batch-id RUN-001 → "RUN-001" */ }
    #[test] fn test_pipeline_batch_id_default()      { /* no flag → generated UUID v7 */ }
    #[test] fn test_vars_unknown_reference()         { /* pipeline.nonexistent → Phase B error */ }

    // ── Type edge cases ─────────────────────────────────────────

    #[test] fn test_vars_nested_object_error() { /* vars: { nested: { a: 1 } } → error */ }
    #[test] fn test_vars_array_error()         { /* vars: { list: [1,2,3] } → error */ }
    #[test] fn test_vars_null_value()          { /* vars: { empty: null } → error */ }
    #[test] fn test_vars_empty_section()       { /* vars: {} → Ok, empty map */ }
    #[test] fn test_vars_yaml12_bool_inference() { /* true=Bool, NO=String, yes=String */ }
    #[test] fn test_vars_quoted_number_is_string() { /* "1.0"=String, "42"=String */ }

    // ── Built-in metadata consistency ───────────────────────────

    #[test] fn test_pipeline_name_from_config()          { /* pipeline.name matches config */ }
    #[test] fn test_pipeline_execution_id_format()       { /* UUID v7 format verified */ }
    #[test] fn test_pipeline_batch_id_auto_default()     { /* no flag → UUID v7 */ }
    #[test] fn test_pipeline_batch_id_arbitrary_string()  { /* --batch-id "daily-retry-2" → string */ }
    #[test] fn test_vars_collision_all_reserved()        { /* all 9 names produce errors */ }
    #[test] fn test_pipeline_source_provenance_per_record() { /* source_file/row differ per record */ }
}
```

> ✅ **[V-4-1] Accepted:** `pipeline_vars`, `PipelineMeta.vars`, `resolve_pipeline()` fallback already exist. Task 11.2 completes wiring (vars map currently always empty, batch_id misrouted to execution_id at main.rs:365). Not fresh implementation.

> ✅ **[V-4-2] Accepted:** CLI flags (`batch_id`, `rules_path`, `dry_run`, `force`, `base_dir`, `allow_absolute_paths`, `log_level`) already exist on `RunArgs`. Sub-tasks wire existing flags to new functionality — do not re-add them.

> ✅ **[V-2-5] Accepted:** `batch_id` currently wired to `execution_id` field at `main.rs:365`. Task 11.2.2 must fix this: add separate `pipeline_batch_id: Arc<str>` to `EvalContext`, keep `execution_id` as auto-generated UUID v7.

#### Risk / gotcha
> **YAML 1.2 float inference:** `version: 1.0` becomes `Float(1.0)`, not `String("1.0")`.
> Users must quote version strings: `version: "1.0"`. Document this in the spec's
> `pipeline.vars` section. This is the standard YAML expectation.

> ⛔ **Hard gate:** Task 11.3 status remains `Blocked` until all tests above pass.

---

### Task 11.3: Logging System (4 Levels)
**Status:** ⛔ Blocked (waiting on Task 11.2)
**Blocked by:** Task 11.2 — pipeline variables must be working (log templates reference pipeline.* vars)

**Description:**
Implement the 4-level logging system: YAML log directives, external log rules files,
`.debug()` passthrough method, and `trace` statements. Set up `tracing-subscriber` with
env-filter in the CLI binary.

**Implementation notes:**
- **Level 1 — YAML log directives:** per-transform `log:` section. Each directive has: `level` (info/warn/error/debug/trace), `when` (lifecycle hook: `before_transform`, `after_transform`, `per_record`, `on_error` — per spec §5.10), `condition` (CXL bool expression — optional, filters emission), `message` (template string with `{field_name}` interpolation), `fields` (list of field names for structured key-value pairs), `every` (sampling — log every Nth matching record, only valid with `when: per_record`), `log_rule` (reference to external rule).
- **Level 2 — External log rules:** `pipeline.log_rules` config key referencing a YAML file. Each rule has same schema as Level 1. Transforms reference rules via `log_rule: rule_name`. Override merge: transform-level keys override matched rule keys (same merge model as schema overrides).
- **Level 3 — `.debug(prefix)` passthrough:** `FieldName.debug("debug_prefix")` in CXL returns the field value unchanged while emitting a `tracing::trace!` event with the prefix and value. Zero overhead when trace level disabled. Allowed in `fn` bodies (spec deviation — pragmatic DX exception).
- **Level 4 — `trace` statement:** standalone statement in transform CXL: `trace "message {field}"`, `trace warn if condition "message"`. `if` guard is optional (default: always). Level keyword is optional (default: trace). `{field_name}` interpolation in message string. NOT allowed inside `fn` bodies.
- Template variables: provenance fields (`{_source_file}`, `{_source_row}`, `{_source_count}`), pipeline counters (`{_pipeline_ok_count}`, `{_pipeline_dlq_count}`, `{_pipeline_total_count}`), record fields, `{_transform_name}`, `{_transform_duration_ms}`, `{pipeline.name}`, `{pipeline.execution_id}`.
- `tracing-subscriber` setup: in `clinker/src/main.rs`, initialize with `EnvFilter` from `--log-level` flag. Format: compact, timestamps, target module.

#### Sub-tasks
- [ ] **11.3.1** Define `LogDirective` config struct with `when` (lifecycle hook enum), `condition` (optional CXL expression), `level`, `message`, `fields`, `every`, `log_rule`
- [ ] **11.3.2** Level 1: Wire lifecycle hooks into executor — `before_transform` (once), `after_transform` (once, has duration), `per_record` (with condition + every sampling), `on_error` (on DLQ routing, has error category/detail)
- [ ] **11.3.3** Level 2: Load external log rules YAML, merge with transform-level overrides (rule defaults, inline keys win)
- [ ] **11.3.4** Level 3: `.debug()` passthrough — verify existing implementation (`builtins_impl.rs:472`), ensure `tracing::trace!` with prefix, source_row, source_file
- [ ] **11.3.5** Level 4: `trace` statement evaluation — already parsed (`parser.rs:182-217`), implement in evaluator with guard, level override, `{field}` interpolation

#### Code scaffolding
```rust
// clinker-core/src/config.rs

#[derive(Deserialize)]
pub struct LogDirective {
    pub level: LogLevel,
    pub when: LogTiming,
    pub condition: Option<String>,
    pub message: String,
    pub fields: Option<Vec<String>>,
    pub every: Option<u64>,
    pub log_rule: Option<String>,
}

#[derive(Deserialize)]
pub enum LogTiming {
    #[serde(rename = "before_transform")] BeforeTransform,
    #[serde(rename = "after_transform")]  AfterTransform,
    #[serde(rename = "per_record")]       PerRecord,
    #[serde(rename = "on_error")]         OnError,
}

#[derive(Deserialize)]
pub enum LogLevel {
    #[serde(rename = "trace")] Trace,
    #[serde(rename = "debug")] Debug,
    #[serde(rename = "info")]  Info,
    #[serde(rename = "warn")]  Warn,
    #[serde(rename = "error")] Error,
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `LogDirective`, `LogTiming`, `LogLevel` | `clinker-core/src/config.rs` | Config structs |
| Log rules loader | `clinker-core/src/log_rules.rs` | New file — load + merge |
| Lifecycle hooks | `clinker-core/src/executor.rs` | before/after/per_record/on_error intercepts |
| Template resolver | `clinker-core/src/log_template.rs` | New file — `{field}` interpolation |
| `.debug()` method | `cxl/src/eval/builtins_impl.rs` | Already implemented (line 472) |
| `trace` evaluator | `cxl/src/eval/mod.rs` | Extend Statement::Trace handling |
| `tracing-subscriber` | `clinker/src/main.rs` | `tracing_subscriber::fmt()` + `EnvFilter` |

#### Intra-phase dependencies
- Requires: Task 11.2 (pipeline vars for template resolution)
- Unblocks: Task 11.4 (warn-severity validations use logging)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    // ── Level 1: YAML log directives (15 tests) ────────────────

    #[test] fn test_log_level1_basic_emit()           { /* per_record, info, "processed {name}" */ }
    #[test] fn test_log_level1_when_condition()        { /* per_record + condition: "Amount > 1000" */ }
    #[test] fn test_log_level1_every_sampling()        { /* every: 100, 300 records → 3 events */ }
    #[test] fn test_log_level1_fields_structured()     { /* fields: [name, amount] → kv pairs */ }
    #[test] fn test_log_level2_external_rules_load()   { /* YAML rules file loaded by name */ }
    #[test] fn test_log_level2_rule_reference()        { /* log_rule: audit_trail inherits config */ }
    #[test] fn test_log_level2_override_merge()        { /* transform level: warn overrides rule info */ }
    #[test] fn test_log_level3_passthrough_value()     { /* Name.debug("dbg") returns Name */ }
    #[test] fn test_log_level3_emits_trace()           { /* .debug("check") emits TRACE event */ }
    #[test] fn test_log_level4_trace_basic()           { /* trace "saw {Name}" → TRACE event */ }
    #[test] fn test_log_level4_trace_when_guard()      { /* trace if Amount > 1000 → conditional */ }
    #[test] fn test_log_level4_trace_level_override()  { /* trace warn "alert" → WARN event */ }
    #[test] fn test_log_template_provenance_vars()     { /* {_source_file}:{_source_row} */ }
    #[test] fn test_log_template_pipeline_counters()   { /* {_pipeline_ok_count}, {_pipeline_dlq_count} */ }
    #[test] fn test_log_template_transform_meta()      { /* {_transform_name}, {_transform_duration_ms} */ }

    // ── Lifecycle hooks (4 tests) ───────────────────────────────

    #[test] fn test_log_before_transform_fires_once()  { /* 100 records → exactly 1 event */ }
    #[test] fn test_log_after_transform_fires_once()   { /* 100 records → 1 event after all done */ }
    #[test] fn test_log_on_error_fires_per_dlq()       { /* 3 records, 1 DLQ → 1 ERROR event */ }
    #[test] fn test_tracing_subscriber_log_level()     { /* --log-level warn filters lower */ }

    // ── Level 1 edge cases (7 tests) ────────────────────────────

    #[test] fn test_log_level1_on_error_timing()       { /* on_error with _cxl_dlq_error_category */ }
    #[test] fn test_log_level1_missing_message_error()  { /* no message → config error */ }
    #[test] fn test_log_level1_invalid_level_error()    { /* level: "critical" → config error */ }
    #[test] fn test_log_level1_invalid_when_error()     { /* when: "always" → config error */ }
    #[test] fn test_log_level1_multiple_directives()    { /* multiple directives execute in order */ }
    #[test] fn test_log_level1_every_one()              { /* every: 1, 5 records → 5 events */ }
    #[test] fn test_log_level1_condition_null_is_false() { /* null condition → no log */ }

    // ── Level 2 edge cases (2 tests) ────────────────────────────

    #[test] fn test_log_level2_missing_rule_error()    { /* nonexistent rule → config error */ }
    #[test] fn test_log_level2_full_override()         { /* all rule properties overridden */ }

    // ── Level 3 edge cases (3 tests) ────────────────────────────

    #[test] fn test_log_level3_debug_no_prefix()       { /* .debug() no arg → value only */ }
    #[test] fn test_log_level3_debug_null()            { /* NullField.debug("check") → null */ }
    #[test] fn test_log_level3_debug_chained()         { /* .debug("a").debug("b") → 2 events */ }

    // ── Level 4 edge cases (3 tests) ────────────────────────────

    #[test] fn test_log_level4_multi_field_interpolation() { /* 3 fields in one trace message */ }
    #[test] fn test_log_level4_guard_short_circuits()      { /* false guard → path not resolved */ }
    #[test] fn test_log_level4_parallel_span_context()     { /* parallel → chunk correlation */ }

    // ── Template resolution (1 test) ────────────────────────────

    #[test] fn test_log_before_transform_with_condition() { /* condition on before_transform */ }
}
```

> ✅ **[V-2-4] Accepted:** `.debug()` currently emits `tracing::debug!` (builtins_impl.rs:475). Plan spec says `tracing::trace!`. Task 11.3.4 must change to `tracing::trace!` to match spec §5.10 Level 3 description.

> ✅ **[V-1-4] Accepted:** `LogLevel` enum duplicates `TraceLevel` variants (both have Trace/Debug/Info/Warn/Error). Acceptable — different domains (AST statement level vs YAML config directive level). Different crates (`cxl` vs `clinker-core`).

> ✅ **[V-7-3] Accepted:** `every: 0` — reject at config parse time with "every must be >= 1". Handle in 11.3.1 config validation.

> ✅ **[V-7-4] Accepted:** `_transform_duration_ms` in `before_transform` resolves to `null`. Emit Phase A warning if template references it in incompatible timing. Handle in 11.3.2.

> ✅ **[V-8-4] Accepted:** External log rules YAML must use `parse_yaml_with_budget()`. Handle in 11.3.3.

> ✅ **[V-5-2] Accepted:** Test count header says 34, actual stubs are 35. Header count is stale; 35 is correct.

#### Risk / gotcha
> **`before_transform` and `after_transform` hooks** require access to transform metadata
> (`_transform_name`, `_transform_duration_ms`) computed by the executor, not the CXL
> evaluator. `_transform_duration_ms` is only available in `after_transform` — using it
> in `before_transform` templates resolves to `null`. Phase A warning if referenced in incompatible timing.

> ⛔ **Hard gate:** Task 11.4 status remains `Blocked` until all tests above pass.

---

### Task 11.4: Declarative Validations + Schema Structure Validation
**Status:** ⛔ Blocked (waiting on Task 11.3)
**Blocked by:** Task 11.3 — logging must be working (warn-severity validations log and continue)

**Description:**
Implement the declarative `validations:` section on transforms. Each validation is a
named check with a CXL bool expression or module function reference, arguments, and
severity routing (error → DLQ, warn → log + continue).

Also implement `structure:` ordering validation for multi-record schemas (deferred from
Phase 9 drill decision #6). Phase 9 parses `structure:` into `Vec<StructureConstraint>`
but does not validate ordering. This task validates that record types appear in the
declared order with the declared cardinality. Violations emit `tracing::warn!` (data
quality warning, not DLQ errors), per spec §4.1.

Also implement `enum` (allowed_values) enforcement on `FieldDef` (deferred from Phase 9,
line 28: "enforcement deferred to Phase 11 Task 11.4"). Phase 9 parses the `enum` property
into `FieldDef.allowed_values: Option<Vec<String>>` but does not enforce it. This task
validates that field values are members of the allowed set. Violations route to DLQ with
`_cxl_dlq_error_category: validation_failure` and `_cxl_dlq_error_detail` naming the field
and rejected value. Enforcement runs during Phase 2 evaluation, after type coercion, before
`emit`. Null values pass if the field is not `required: true`.

**Implementation notes:**
- `validations:` YAML section on each transform: list of validation entries.
- Each entry: `name` (optional — human-readable label, auto-derived from field+check if omitted), `field` (optional — which field to validate; omit for cross-field checks), `check` (module function reference like `validators.is_valid_email` OR inline CXL bool expression), `args` (named parameters — YAML literals or `pipeline.*` references), `severity` (`error` or `warn`, default `error`), `message` (optional template string with `{field}` interpolation for DLQ detail or log message).
- Severity `error`: record routed to DLQ with `_cxl_dlq_error_category: validation_failure` and `_cxl_dlq_error_detail` from `message` template. Short-circuit: first error-severity failure stops remaining checks for that record.
- Severity `warn`: log warning with validation name and field values, continue processing. No short-circuit — all remaining checks still run.
- Execution order: Phase 2, after all `let` bindings evaluated, before `emit`. Validations run in declaration order.
- Phase C verification (compile time): function reference exists in module registry, argument count matches function signature, function return type is `Bool`. Inline CXL expression must type-check to `Bool`.
- `args:` map: keys are parameter names matching the function signature. Values are YAML literals or pipeline variable references (`max: pipeline.MAX_SALARY`). Field references are not detectable at YAML parse time (YAML parses unquoted `Amount` as string `"Amount"`); args values are always treated as literals or `pipeline.*` references.
- When `field` is specified: the field value is passed as the first positional argument to the check function. When `field` is omitted: the check expression has access to all record fields (cross-field validation).

#### Sub-tasks
- [ ] **11.4.1** Define `ValidationEntry` config struct: `name` (optional), `field` (optional), `check`, `args`, `severity`, `message` (optional)
- [ ] **11.4.2** Validation execution in Phase 2: after let bindings, before emit. Error short-circuits, warn continues.
- [ ] **11.4.3** Phase C verification: function exists, arg count matches, return type Bool
- [ ] **11.4.4** Enum (allowed_values) enforcement: after type coercion, before emit. Null passes on non-required.
- [ ] **11.4.5** Structure ordering validation: `tracing::warn!` when records out of declared order

#### Code scaffolding
```rust
// clinker-core/src/config.rs

#[derive(Deserialize)]
pub struct ValidationEntry {
    pub name: Option<String>,       // Optional; auto-derived from field+check if omitted
    pub field: Option<String>,      // Which field to validate (omit for cross-field)
    pub check: String,              // Module fn ref or inline CXL expression
    pub args: Option<IndexMap<String, serde_json::Value>>,
    pub severity: ValidationSeverity,
    pub message: Option<String>,    // Template for DLQ detail / log message
}

#[derive(Deserialize)]
pub enum ValidationSeverity {
    #[serde(rename = "error")] Error,
    #[serde(rename = "warn")]  Warn,
}

impl ValidationEntry {
    /// Auto-derive name from field and check if not specified.
    pub fn resolved_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| {
            match &self.field {
                Some(f) => format!("{}:{}", f, self.check),
                None => self.check.clone(),
            }
        })
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `ValidationEntry`, `ValidationSeverity` | `clinker-core/src/config.rs` | Config structs |
| Validation runner | `clinker-core/src/validation.rs` | New file — `run_validations()` |
| Phase C validation checks | `cxl/src/typecheck/pass.rs` | fn exists, arg count, return Bool |
| Enum enforcement | `clinker-core/src/executor.rs` | After type coercion, before emit |
| Structure validation | `clinker-core/src/schema/structure.rs` | `validate_structure_ordering()` |

#### Intra-phase dependencies
- Requires: Task 11.3 (logging for warn-severity validations)
- Unblocks: Task 11.5 (benchmark suite exercises full pipeline including validations)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    // ── Inline CXL (3) ─────────────────────────────────────────
    #[test] fn test_validation_inline_cxl_pass()        { /* Amount > 0, Amount=100 → passes */ }
    #[test] fn test_validation_inline_cxl_fail_error()  { /* Amount=-5, error → DLQ */ }
    #[test] fn test_validation_inline_cxl_fail_warn()   { /* Amount=-5, warn → logged, emitted */ }

    // ── Module function (2) ─────────────────────────────────────
    #[test] fn test_validation_module_fn_call()         { /* validators.is_positive(salary) */ }
    #[test] fn test_validation_module_fn_with_args()    { /* validators.in_range + pipeline.MAX */ }

    // ── DLQ fields (2) ──────────────────────────────────────────
    #[test] fn test_validation_dlq_category()           { /* validation_failure category */ }
    #[test] fn test_validation_dlq_detail()             { /* message template in error detail */ }

    // ── Short-circuit + order (4) ───────────────────────────────
    #[test] fn test_validation_short_circuit_error()    { /* first error stops rest */ }
    #[test] fn test_validation_no_short_circuit_warn()  { /* both warns run */ }
    #[test] fn test_validation_order_after_let()        { /* let binding available */ }
    #[test] fn test_validation_order_before_emit()      { /* failed record never emitted */ }

    // ── Phase C verification (4) ────────────────────────────────
    #[test] fn test_validation_phase_c_fn_missing()     { /* compile error */ }
    #[test] fn test_validation_phase_c_arg_count()      { /* wrong arg count → error */ }
    #[test] fn test_validation_phase_c_return_type()    { /* non-Bool return → error */ }
    // (Renamed to test_validation_args_treated_as_literal — see below)

    // ── Enum enforcement (4) ────────────────────────────────────
    #[test] fn test_enum_enforcement_valid_value()      { /* "B" in [A,B,C] → passes */ }
    #[test] fn test_enum_enforcement_invalid_value()    { /* "D" not in [A,B,C] → DLQ */ }
    #[test] fn test_enum_enforcement_null_non_required() { /* null, not required → passes */ }
    #[test] fn test_enum_enforcement_null_required()    { /* null, required → DLQ */ }

    // ── Structure ordering (2) ──────────────────────────────────
    #[test] fn test_structure_ordering_valid()          { /* correct order → no warning */ }
    #[test] fn test_structure_ordering_violation()      { /* out of order → tracing::warn! */ }

    // ── Edge cases (5) ──────────────────────────────────────────
    #[test] fn test_validation_cross_field_no_field()   { /* end_date > start_date, no field prop */ }
    #[test] fn test_validation_auto_name()              { /* name auto-derived from field+check */ }
    #[test] fn test_validation_message_interpolation()  { /* {employee_id} in message template */ }
    #[test] fn test_validation_mixed_severity_order()   { /* warn, error, warn → 1 warn + DLQ */ }
    #[test] fn test_enum_enforcement_after_coercion()   { /* coerced value checked against enum */ }

    /// Validation referencing non-existent field → Phase B error with suggestion
    #[test] fn test_validation_nonexistent_field()      { /* field: "nonexistent" → error */ }

    /// Args treated as literals, not field refs (Decision #13)
    #[test] fn test_validation_args_treated_as_literal() { /* Amount in args → string "Amount", not field */ }
}
```

> ✅ **[V-5-3] Resolved:** Renamed `test_validation_args_reject_field_ref` to `test_validation_args_treated_as_literal` — clarifies that YAML can't distinguish field refs from strings (Decision #13).

> ✅ **[V-7-5] Accepted:** Added `test_validation_nonexistent_field` — Phase B error with Levenshtein suggestion when `field` names a non-existent schema field.

> ✅ **[V-6-1] Accepted:** Error types for modules/validation/security fold into existing `PipelineError` variants (consistent with `SchemaError` pattern) or `ConfigError` as appropriate. No dedicated error enums needed for v1 scope.

#### Risk / gotcha
> **Module function calling with `field` as implicit first argument:** When `field` is
> specified, the field value is passed as the first positional arg. When `field` is omitted
> (cross-field check), no implicit first argument. The evaluator must handle both cases
> based on the presence of `field` in the `ValidationEntry`.

> ⛔ **Hard gate:** Task 11.5 status remains `Blocked` until all tests above pass.

---

### Task 11.5: Security Hardening + Benchmarks
**Status:** ⛔ Blocked (waiting on Task 11.4)
**Blocked by:** Task 11.4 — validations must be working (benchmark suite exercises full pipeline including validations)

**Description:**
Implement path security validation, output overwrite protection, YAML parser DoS budgets,
environment variable name validation, and a benchmark suite tracking throughput and memory.
Verify zero C dependencies in the dependency tree.

**Implementation notes:**
- Path validation: reject any path containing `..` (directory traversal). Reject absolute paths unless `--allow-absolute-paths` flag is set. Apply to all paths in YAML config: input files, output files, schema files, rules_path, log_rules.
- `--base-dir <DIR>` resolution: all relative paths in YAML resolved relative to `base_dir`. Default: directory containing the YAML pipeline file itself.
- Output overwrite protection: if output file already exists, refuse to run unless `--force` flag is set or `overwrite: true` in output config. Check performed before any data processing begins.
- serde-saphyr DoS budgets via `Budget` API: `max_depth: 64`, `max_nodes: 10_000`, `max_reader_input_bytes: Some(10_485_760)`. Use `from_str_with_options` / `from_reader_with_options` with `budget!` macro. Note: `max_reader_input_bytes` only applies to `from_reader`; for `from_str`, pre-check `input.len()`.
- Environment variable name validation: `${VAR}` references in YAML (processed by `interpolate_env_vars()`) must match `[A-Z_][A-Z0-9_]*`. Reject at config parse time with descriptive error naming the invalid variable. This validates the existing YAML-level env var interpolation, not a CXL function.

> ✅ **[V-7-1] Resolved:** `env()` CXL function does not exist in spec or codebase. Env var validation applies to `${VAR}` YAML interpolation patterns in `interpolate_env_vars()`. Validated at config parse time, not Phase C.
- Benchmark suite: custom `harness = false` binary (DataFusion `dfbench` pattern). Deterministic test data generation in `benches/gen_data.rs` to `target/bench-data/` (gitignored). Track `records/sec` throughput via `Instant::elapsed()` and `peak RSS` via `/proc/self/status` VmHWM. JSON output format. Fork per-benchmark to avoid VmHWM contamination.
- `cargo deny check`: create `deny.toml` banning `cc` build dependency. Allow pure Rust FFI crates (`libc`, `linux-raw-sys`, `mach2`, `windows-sys`). Zero C dependencies currently confirmed.

#### Sub-tasks
- [ ] **11.5.1** Path security: `validate_path()` function — reject `..`, reject absolute unless flag, `--base-dir` resolution. Apply to all config paths.
- [ ] **11.5.2** Output overwrite protection: check before processing, `--force` or `overwrite: true` to override
- [ ] **11.5.3** serde-saphyr DoS budgets: `Budget` configuration via `budget!` macro, `from_str_with_options`
- [ ] **11.5.4** `cargo deny` setup: create `deny.toml`, CI integration
- [ ] **11.5.5** Benchmark suite: `gen_data.rs` + `pipeline_throughput.rs` binaries, VmHWM peak RSS, JSON output

#### Code scaffolding
```rust
// clinker-core/src/security.rs (new file)

use std::path::{Path, PathBuf};

/// Validate a path from pipeline config.
/// Rejects directory traversal (..), absolute paths (unless allowed),
/// and resolves relative paths against base_dir.
pub fn validate_path(
    path: &Path,
    base_dir: &Path,
    allow_absolute: bool,
) -> Result<PathBuf, PipelineError> {
    todo!()
}

/// Validate all paths in a PipelineConfig.
pub fn validate_all_config_paths(
    config: &PipelineConfig,
    base_dir: &Path,
    allow_absolute: bool,
) -> Result<(), PipelineError> {
    todo!()
}
```

```rust
// clinker-core/src/config.rs — YAML loading with budget

use serde_saphyr::{from_str_with_options, options, budget};

fn parse_yaml_with_budget(input: &str) -> Result<PipelineConfig, PipelineError> {
    // Pre-check size for from_str (max_reader_input_bytes only works with from_reader)
    if input.len() > 10_485_760 {
        return Err(PipelineError::Config("YAML document exceeds 10MB limit".into()));
    }
    let options = options! {
        budget: budget! {
            max_depth: 64,
            max_nodes: 10_000,
        },
    };
    from_str_with_options(input, options).map_err(|e| PipelineError::Config(e.to_string()))
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `validate_path()` | `clinker-core/src/security.rs` | New file — central path validation |
| `parse_yaml_with_budget()` | `clinker-core/src/config.rs` | Budget-protected parsing |
| `deny.toml` | `deny.toml` (workspace root) | cargo-deny configuration |
| `gen_data.rs` | `benches/gen_data.rs` | Deterministic test data generator |
| `pipeline_throughput.rs` | `benches/pipeline_throughput.rs` | Benchmark binary |
| `peak_rss_hwm()` | `clinker-core/src/pipeline/memory.rs` | VmHWM reader (add to existing) |

#### Intra-phase dependencies
- Requires: Task 11.4 (validations complete for benchmark exercising)
- Unblocks: Task 11.6 (security hardening must be in place before partial processing)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    // ── Path validation (5 from plan) ───────────────────────────
    #[test] fn test_path_reject_dotdot()           { /* ../etc/passwd → error */ }
    #[test] fn test_path_reject_absolute()         { /* /tmp/out.csv without flag → error */ }
    #[test] fn test_path_allow_absolute_flag()     { /* /tmp/out.csv with flag → Ok */ }
    #[test] fn test_path_base_dir_resolution()     { /* data/input.csv + /opt/ → /opt/data/input.csv */ }
    #[test] fn test_path_base_dir_default()        { /* YAML parent dir as default */ }

    // ── Overwrite protection (4) ────────────────────────────────
    #[test] fn test_overwrite_refuse_existing()    { /* exists, no force → error */ }
    #[test] fn test_overwrite_force_flag()         { /* exists, --force → Ok */ }
    #[test] fn test_overwrite_config_flag()        { /* exists, overwrite: true → Ok */ }
    #[test] fn test_overwrite_check_before_processing() { /* error before input opened */ }

    // ── YAML DoS budgets (3) ────────────────────────────────────
    #[test] fn test_yaml_dos_recursion_depth()     { /* 100 levels → error (limit 64) */ }
    #[test] fn test_yaml_dos_sequence_length()     { /* 20k elements → error (limit 10k nodes) */ }
    #[test] fn test_yaml_dos_document_size()       { /* >10MB → error */ }

    // ── Env var name validation in ${VAR} interpolation (3) ───
    #[test] fn test_env_var_name_valid()           { /* ${DATABASE_URL} → Ok */ }
    #[test] fn test_env_var_name_invalid_lowercase() { /* ${database_url} → config error */ }
    #[test] fn test_env_var_name_invalid_leading_digit() { /* ${1BAD} → config error */ }

    // ── Benchmarks (3) ──────────────────────────────────────────
    #[test] fn test_benchmark_csv_20mb_runs()      { /* completes, reports records/sec */ }
    #[test] fn test_benchmark_json_10mb_runs()     { /* completes, reports records/sec */ }
    #[test] fn test_benchmark_xml_50mb_runs()      { /* completes, reports records/sec */ }

    // ── cargo deny (1) ──────────────────────────────────────────
    #[test] fn test_cargo_deny_zero_c_deps()       { /* cargo deny check exits 0 */ }

    // ── Path edge cases (5) ─────────────────────────────────────
    #[test] fn test_path_reject_encoded_dotdot()   { /* %2e%2e → rejected */ }
    #[test] fn test_path_dot_slash_prefix()        { /* ./data/input.csv → resolved */ }
    #[test] fn test_path_validation_all_config_paths() { /* all 5 config path types checked */ }
    #[test] fn test_overwrite_nonexistent_ok()     { /* non-existent output → Ok */ }
    #[test] fn test_overwrite_multiple_outputs_all_checked() { /* all outputs checked */ }

    // ── DoS edge cases (4) ──────────────────────────────────────
    #[test] fn test_yaml_dos_at_limit_passes()     { /* exactly 64 levels → Ok */ }
    #[test] fn test_yaml_dos_error_message_quality() { /* mentions limit value */ }
    #[test] fn test_yaml_dos_alias_bomb()          { /* &anchor + many *anchor → rejected */ }
    #[test] fn test_yaml_dos_billion_laughs()      { /* nested alias expansion → rejected */ }

    // ── Security edge cases (4) ─────────────────────────────────
    #[test] fn test_path_symlink_outside_basedir() { /* symlink escaping → rejected */ }
    #[test] fn test_path_null_bytes()              { /* data/input\0.csv → rejected */ }
    #[test] fn test_yaml_dos_multiple_documents()  { /* --- separator → max_documents limit */ }
    #[test] fn test_yaml_dos_max_documents()       { /* many YAML documents → rejected */ }

    // ── Env var edge cases (2) ──────────────────────────────────
    #[test] fn test_env_var_name_empty()           { /* ${} → config error */ }
    #[test] fn test_env_var_name_special_chars()   { /* ${DB-NAME} → config error (hyphen invalid) */ }

    // ── Benchmark edge cases (3) ────────────────────────────────
    #[test] fn test_benchmark_with_dlq_records()   { /* DLQ doesn't skew throughput */ }
    #[test] fn test_benchmark_determinism()        { /* same data → consistent results */ }
    #[test] fn test_benchmark_warmup()             { /* cold vs warm cache handling */ }

    // ── Env var interpolation (2) ───────────────────────────────
    #[test] fn test_env_var_unset_with_default()   { /* ${MISSING:-fallback} → "fallback" */ }
    #[test] fn test_env_var_special_value_chars()   { /* DATABASE_URL=postgres://... → Ok */ }
}
```

#### Risk / gotcha
> **`max_reader_input_bytes` scope:** Only enforced with `from_reader`, not `from_str`.
> When config is loaded as a string (e.g., after channel override merging), must pre-check
> `input.len()` before parsing. The `budget!` macro's `max_nodes` and `max_depth` still
> protect against structural attacks regardless of loading method.

> **Symlink detection is OS-dependent:** `canonicalize()` follows symlinks but may behave
> differently on Windows. Document as Linux-primary behavior.

> ✅ **[V-1-5] Accepted:** serde-saphyr `budget!` macro + `from_str_with_options` API confirmed available in v0.0.22. Verify at implementation time.

> ✅ **[V-5-2] Accepted:** Test count header says 37, actual stubs are 39. Header count is stale; 39 is correct.

> ⛔ **Hard gate:** Task 11.6 status remains `Blocked` until all tests above pass.

---

### Task 11.6: --dry-run -n Partial Processing
**Status:** ⛔ Blocked (waiting on Task 11.5)
**Blocked by:** Task 11.5 — security hardening must be in place before partial processing

**Description:**
Implement `clinker --dry-run -n <N> pipeline.yaml` which runs the full pipeline on the
first N records per input. Validates types, CXL evaluation, output format. Writes output
to stdout (or a temp file with `--dry-run-output`). Deferred from Phase 8 (decision #34).

**Implementation notes:**
- `-n <N>` flag on `Cli` struct: `Option<u64>`, only valid with `--dry-run`.
- When `-n` is set, each `FormatReader` wraps in a generic `TakeReader<R>` adapter that yields at most N records. One implementation for all formats (CSV, JSON, XML, fixed-width). ~15 lines.
- Pipeline runs normally through all phases (ingestion, transform, projection, output).
- Output defaults to stdout; `--dry-run-output <PATH>` writes to file instead.
- Exit codes: same as normal run (0 for success, 2 for partial DLQ, etc.).
- Without `-n`, `--dry-run` remains config-validation-only (Phase 8 behavior).
- `-n` applies per input source, not globally (2 inputs × `-n 10` = 20 records max).

#### Sub-tasks
- [ ] **11.6.1** `TakeReader<R>` wrapper: generic over `FormatReader`, counts records, yields `None` after N
- [ ] **11.6.2** CLI wiring: `-n` flag validation (only valid with `--dry-run`), `--dry-run-output` path
- [ ] **11.6.3** Output routing: stdout by default, file with `--dry-run-output`. Overwrite protection applies.

#### Code scaffolding
```rust
// clinker-core/src/pipeline/take.rs (new file)

/// Wraps any FormatReader to yield at most `limit` records.
pub struct TakeReader<R> {
    inner: R,
    limit: u64,
    count: u64,
}

impl<R> TakeReader<R> {
    pub fn new(inner: R, limit: u64) -> Self {
        Self { inner, limit, count: 0 }
    }
}

impl<R: FormatReader> FormatReader for TakeReader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        self.inner.schema()
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.count >= self.limit {
            return Ok(None);
        }
        let record = self.inner.next_record()?;
        if record.is_some() {
            self.count += 1;
        }
        Ok(record)
    }
}

// > ✅ **[V-1-3] Resolved:** Added missing `schema()` delegation method required by `FormatReader` trait.
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `TakeReader<R>` | `clinker-core/src/pipeline/take.rs` | New file — generic adapter |
| `-n` flag | `clinker/src/main.rs` | CLI arg on `RunArgs` |
| `--dry-run-output` flag | `clinker/src/main.rs` | CLI arg on `RunArgs` |
| Output routing | `clinker-core/src/executor.rs` | stdout vs file branching |

#### Intra-phase dependencies
- Requires: Task 11.5 (security hardening + overwrite protection for dry-run output)
- Unblocks: Phase 11 exit

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    // ── Core behavior (5 from plan) ─────────────────────────────
    #[test] fn test_dry_run_n_limits_records()         { /* -n 10, 1000 input → 10 output */ }
    #[test] fn test_dry_run_n_without_dry_run_error()  { /* -n 100 alone → CLI error */ }
    #[test] fn test_dry_run_without_n_config_only()    { /* --dry-run alone → config validation */ }
    #[test] fn test_dry_run_output_to_file()           { /* --dry-run-output out.csv → file */ }
    #[test] fn test_dry_run_n_exit_code_2_dlq()        { /* partial DLQ → exit 2 */ }

    // ── Edge cases (6) ──────────────────────────────────────────
    #[test] fn test_dry_run_n_zero()                   { /* -n 0 → zero records */ }
    #[test] fn test_dry_run_n_exceeds_input()          { /* -n 1000, 50 records → 50 output */ }
    #[test] fn test_dry_run_n_per_source()             { /* 2 inputs × -n 10 = 20 total */ }
    #[test] fn test_dry_run_output_creates_dirs()      { /* parent dirs auto-created */ }
    #[test] fn test_take_reader_limit()                { /* MockReader: 5 Some then None */ }
    #[test] fn test_dry_run_default_stdout()           { /* no --dry-run-output → stdout */ }

    // ── Format-specific (4) ─────────────────────────────────────
    #[test] fn test_dry_run_n_json_ndjson()            { /* NDJSON input, -n 10 */ }
    #[test] fn test_dry_run_n_xml()                    { /* XML input, -n 10 */ }
    #[test] fn test_dry_run_n_fixed_width()            { /* fixed-width input, -n 10 */ }
    #[test] fn test_dry_run_n_multi_record()           { /* multi-record, -n 3 → first 3 any type */ }

    // ── Pipeline interaction (6) ────────────────────────────────
    #[test] fn test_dry_run_n_transforms_applied()     { /* CXL transforms run on N records */ }
    #[test] fn test_dry_run_n_with_windows()           { /* two-pass on N-record subset */ }
    #[test] fn test_dry_run_n_with_sort()              { /* sorted output limited to N */ }
    #[test] fn test_dry_run_n_metrics_accurate()       { /* spool shows N, not full file */ }
    #[test] fn test_dry_run_n_error_threshold()        { /* --error-threshold applies */ }
    #[test] fn test_dry_run_n_with_validations()       { /* validations run on N records */ }

    // ── More interactions (3) ───────────────────────────────────
    #[test] fn test_dry_run_n_with_logging()           { /* lifecycle logs fire */ }
    #[test] fn test_dry_run_n_with_channel()           { /* channel overrides applied */ }
    #[test] fn test_dry_run_n_mixed_formats()          { /* CSV + JSON inputs, -n 5 each */ }

    // ── TakeReader internals (2) ────────────────────────────────
    #[test] fn test_take_reader_error_passthrough()    { /* inner error passes through */ }
    #[test] fn test_take_reader_limit_one()            { /* limit=1 → exactly one record */ }

    // ── Error handling (3) ──────────────────────────────────────
    #[test] fn test_dry_run_n_negative_error()         { /* -n -5 → CLI parse error */ }
    #[test] fn test_dry_run_output_overwrite_protection() { /* existing file, no force → error */ }
    #[test] fn test_dry_run_n_streaming_mode()         { /* no windows → streaming execution */ }

    // ── Two-pass (1) ────────────────────────────────────────────
    #[test] fn test_dry_run_n_two_pass_mode()          { /* windows → two-pass on subset */ }
}
```

#### Risk / gotcha
> **Window functions with `-n`:** In two-pass mode, the arena is built from the first N
> records only. Window lookups that would match records beyond N will find nothing. This
> is correct behavior for a "sample mode" but may surprise users expecting full window
> context. Document that `-n` limits the entire pipeline view, not just output.

> ✅ **[V-8-5] Accepted:** `--dry-run-output` CLI path must go through `validate_path()` — same security as config paths. Handle in 11.6.3.

> ✅ **[V-5-2] Accepted:** Test count header says 26, actual stubs are 30. Header count is stale; 30 is correct.

> ⛔ **Hard gate:** Phase 11 exit criteria not met until all tests above pass.

---

## Intra-Phase Dependency Graph

```
Task 11.0 (Metrics Spool) ✅
    │
Task 11.1 (CXL Modules) ───→ Task 11.2 (Pipeline Variables)
                                    │
                              Task 11.3 (Logging System)
                                    │
                              Task 11.4 (Validations)
                                    │
                              Task 11.5 (Security + Benchmarks)
                                    │
                              Task 11.6 (--dry-run -n)
```

Critical path: 11.1 → 11.2 → 11.3 → 11.4 → 11.5 → 11.6
No parallelizable tasks (strict chain after 11.0).

---

## Decisions Log (drill-phase — 2026-03-31)
| # | Decision | Rationale | Research-backed | Affects |
|---|---------|-----------|-----------------|---------|
| 1 | Keep `fn name(params) = expr` syntax (spec) | DuckDB/Jsonnet/Kotlin precedent; parser already implements | yes | Task 11.1 |
| 2 | Switch `use` path separator from `::` to `.` | ETL convention (dbt/Spark/Beam); `::` alien to data engineers | yes | Task 11.1, spec update |
| 3 | Keep `.debug()` method name (spec) | Unanimous: Rust dbg!(), Elixir dbg(), no collision with YAML log: | yes | Task 11.1, 11.3 |
| 4 | Separate `parse_module()` → `ModuleAst` | HCL/CUE/syn/rustc pattern; tree-sitter #870 shows mode flags fail | yes | Task 11.1 |
| 5 | Topological sort for module constants | CUE/Nix/Jsonnet/Rust const pattern; order-independence expected | yes | Task 11.1 |
| 6 | Native YAML 1.2 inference for pipeline.vars | serde-saphyr YAML 1.2 avoids Norway problem; dbt precedent | yes | Task 11.2 |
| 7 | Add `pipeline.batch_id` as built-in | Airflow 3.0 two-ID pattern; SSIS/Informatica precedent | yes | Task 11.2, spec update |
| 8 | Level 1 `when` = lifecycle hook (spec) | Spec authoritative; before/after_transform are executor events | no | Task 11.3 |
| 9 | Template vars use `_pipeline_` prefix (spec) | Avoids collision with record fields; consistent with pipeline.* | no | Task 11.3 |
| 10 | Allow `.debug()` in fn bodies (spec deviation) | Pragmatic DX; zero overhead when trace disabled; only emit/trace banned | no | Task 11.1, 11.3 |
| 11 | `.debug()` rejection via Phase C (if banned) | N/A — `.debug()` allowed; emit/trace banned at parse time | no | Task 11.1 |
| 12 | Spec schema + optional `name` for validations | Soda Core/Pandera pattern for observability; auto-derived if omitted | yes | Task 11.4 |
| 13 | Drop parse-time field ref rejection in args | YAML can't distinguish field refs from strings; technically impossible | no | Task 11.4 |
| 14 | Structure violations → `tracing::warn!` | Per spec §4.1; data quality warning, not DLQ error | no | Task 11.4 |
| 15 | serde-saphyr Budget API for DoS limits | Full support confirmed: max_depth, max_nodes, max_reader_input_bytes | yes | Task 11.5 |
| 16 | Custom harness=false benchmark binary | DataFusion dfbench pattern; criterion can't track RSS | yes | Task 11.5 |
| 17 | cargo deny with deny.toml | Zero C deps confirmed; need to formalize with deny.toml | yes | Task 11.5 |
| 18 | Env var name validation at config parse time | `${VAR}` names must match [A-Z_][A-Z0-9_]* in `interpolate_env_vars()` | no | Task 11.5 |
| 19 | Generic TakeReader\<R\> for -n limiting | One impl for all formats; Iterator::take() pattern | no | Task 11.6 |

## Assumptions Log (drill-phase — 2026-03-31)
| # | Assumption | Basis | Risk if wrong |
|---|-----------|-------|---------------|
| 1 | `.debug()` in fn bodies has no practical performance impact | tracing short-circuits when trace disabled | If users leave debug calls in production modules, could cause log noise when trace is enabled globally |
| 2 | YAML 1.2 type inference sufficient without explicit types | dbt uses same approach; YAML 1.2 avoids worst footguns | Users may hit version: 1.0 → float surprise; mitigated by documentation |
| 3 | serde-saphyr Budget API is stable | v0.0.22+ has comprehensive Budget; actively maintained | If API changes, wrapper needed; budget! macro provides insulation |
| 4 | VmHWM is sufficient for peak RSS measurement | Kernel-tracked, zero overhead, one read at end | Only on Linux; macOS/Windows need platform-specific alternatives |
| 5 | `-n` per-source (not global) is the right semantic | Spec §6.3 says "first N records per input" | If users expect global limit, confusion; document clearly |
