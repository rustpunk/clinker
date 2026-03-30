# Phase 3: CXL Resolver, Type Checker, Evaluator

**Status:** 🔲 Not Started
**Depends on:** Phase 2 exit criteria (parser produces typed AST, all Phase 2 tests pass)
**Entry criteria:** `cargo test -p cxl` passes all Phase 2 parser tests; `Expr` AST nodes are constructible and pattern-matchable
**Exit criteria:** `cxl check` validates a `.cxl` file through resolve + type-check; `cxl eval` evaluates a CXL program against a JSON record and prints output fields; `cxl fmt` pretty-prints a parsed program; all ~60 built-in methods pass unit tests; `cargo test -p cxl -p cxl-cli` passes 80+ tests

---

## Tasks

### Task 3.1: FieldResolver + WindowContext Traits
**Status:** ⛔ Blocked
**Blocked by:** Phase 2 exit criteria — parser must produce a complete AST

**Description:**
Define the `FieldResolver` and `WindowContext` traits in the `cxl` crate. These are the
runtime context interfaces that the evaluator calls to resolve field references and window
aggregations. Provide a `HashMapResolver` test double for unit testing.

**Implementation notes:**
- `FieldResolver` trait in `cxl::resolve::traits`:
  - `resolve(&self, name: &str) -> Option<Value>` — unqualified field lookup.
  - `resolve_qualified(&self, source: &str, field: &str) -> Option<Value>` — `source.field` lookup.
  - `available_fields(&self) -> Vec<&str>` — for fuzzy-match diagnostics in Phase B. Lifetime tied to `&self`.
- `WindowContext` trait in `cxl::resolve::traits` (spec SS11.4 signatures):
  - Navigation: `first() -> Option<Box<dyn FieldResolver>>`, `last() -> Option<Box<dyn FieldResolver>>`, `lag(offset: usize) -> Option<Box<dyn FieldResolver>>`, `lead(offset: usize) -> Option<Box<dyn FieldResolver>>`.
  - Aggregation: `count() -> i64`, `sum(field: &str) -> Value`, `avg(field: &str) -> Value`, `min(field: &str) -> Value`, `max(field: &str) -> Value`.
  - Predicate: `any(predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool`, `all(predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool`.
  - Positional functions return `Option<Box<dyn FieldResolver>>` — the `Box` heap allocation is O(1) per expression evaluation, not per record. Aggregation functions take `&str` field names — the trait is a data accessor, the evaluator handles expression evaluation before calling the trait.
- `HashMapResolver`: wraps `HashMap<String, Value>` for `resolve`, `HashMap<(String, String), Value>` for `resolve_qualified`. Constructor takes a flat map; qualified entries keyed as `("source", "field")` tuples.
- `it` binding: a sentinel identifier that is only valid inside `predicate_expr` nodes (arguments to `window.any` / `window.all`). The resolver does **not** handle `it` — that is enforced by Phase B (Task 3.2). The trait contract simply states that `resolve("it")` returns `None` in the primary context.
- All trait methods take `&self` — resolvers are shared references during evaluation.

**Acceptance criteria:**
- [ ] `FieldResolver` trait is object-safe (`dyn FieldResolver` compiles)
- [ ] `WindowContext` trait is object-safe (`dyn WindowContext` compiles)
- [ ] `HashMapResolver` implements `FieldResolver` and passes all lookup tests
- [ ] `resolve("unknown")` returns `None`
- [ ] `resolve_qualified("source", "unknown")` returns `None`
- [ ] `available_fields()` returns all registered field names

**Required unit tests (must pass before Task 3.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_field_resolver_object_safety` | `dyn FieldResolver` compiles and dispatches correctly | ⛔ Hard gate |
| `test_window_context_object_safety` | `dyn WindowContext` compiles and dispatches correctly | ⛔ Hard gate |
| `test_hashmap_resolver_unqualified` | `resolve("name")` returns the stored value | ⛔ Hard gate |
| `test_hashmap_resolver_qualified` | `resolve_qualified("src", "field")` returns the stored value | ⛔ Hard gate |
| `test_hashmap_resolver_missing_field` | `resolve("nonexistent")` returns `None` | ⛔ Hard gate |
| `test_hashmap_resolver_available_fields` | `available_fields()` returns all inserted names | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 3.2 remains Blocked until all tests pass.

#### Sub-tasks
- [ ] **[3.1.1]** Define `FieldResolver` and `WindowContext` traits in `cxl::resolve::traits` using spec SS11.4 signatures
- [ ] **[3.1.2]** Implement `HashMapResolver` test double with both unqualified and qualified lookup
- [ ] **[3.1.3]** Write all 6 gate tests (object safety, lookups, missing fields, available_fields)

#### Code scaffolding
```rust
// Module: cxl/src/resolve/traits.rs

use clinker_record::Value;

/// Resolve a field name to a value from the current record.
/// Object-safe: usable as `dyn FieldResolver`.
pub trait FieldResolver {
    /// Unqualified field lookup: `field_name` → Value.
    fn resolve(&self, name: &str) -> Option<Value>;

    /// Qualified field lookup: `source.field` → Value.
    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value>;

    /// All available field names. Used for fuzzy-match diagnostics.
    /// Lifetime tied to `&self` — zero-copy borrows from internal storage.
    fn available_fields(&self) -> Vec<&str>;
}

/// Access window partition data (Arena + Secondary Index).
/// Object-safe: usable as `dyn WindowContext`.
pub trait WindowContext {
    /// Positional: return a resolver for the first/last/lag/lead record.
    /// Returns None if the partition is empty or offset is out of bounds.
    /// Box allocation is O(1) per expression evaluation, not per record.
    fn first(&self) -> Option<Box<dyn FieldResolver>>;
    fn last(&self) -> Option<Box<dyn FieldResolver>>;
    fn lag(&self, offset: usize) -> Option<Box<dyn FieldResolver>>;
    fn lead(&self, offset: usize) -> Option<Box<dyn FieldResolver>>;

    /// Aggregation: operate on all records in the partition.
    /// Field name resolved by the caller (evaluator), not by the trait.
    fn count(&self) -> i64;
    fn sum(&self, field: &str) -> Value;
    fn avg(&self, field: &str) -> Value;
    fn min(&self, field: &str) -> Value;
    fn max(&self, field: &str) -> Value;

    /// Predicate: test all records in the partition against a closure.
    /// The `&dyn FieldResolver` argument IS the `it` binding.
    fn any(&self, predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool;
    fn all(&self, predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool;
}
```

```rust
// Module: cxl/src/resolve/test_double.rs

use std::collections::HashMap;
use clinker_record::Value;
use super::traits::FieldResolver;

/// Test double for unit testing. Wraps HashMaps for field lookup.
pub struct HashMapResolver {
    fields: HashMap<String, Value>,
    qualified: HashMap<(String, String), Value>,
}

impl HashMapResolver {
    pub fn new(fields: HashMap<String, Value>) -> Self {
        Self { fields, qualified: HashMap::new() }
    }

    pub fn with_qualified(mut self, source: &str, field: &str, value: Value) -> Self {
        self.qualified.insert((source.into(), field.into()), value);
        self
    }
}

impl FieldResolver for HashMapResolver {
    fn resolve(&self, name: &str) -> Option<Value> {
        self.fields.get(name).cloned()
    }

    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value> {
        self.qualified.get(&(source.into(), field.into())).cloned()
    }

    fn available_fields(&self) -> Vec<&str> {
        self.fields.keys().map(|s| s.as_str()).collect()
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `FieldResolver` | `cxl/src/resolve/traits.rs` | Object-safe trait, exported from `cxl::resolve` |
| `WindowContext` | `cxl/src/resolve/traits.rs` | Object-safe trait, same module |
| `HashMapResolver` | `cxl/src/resolve/test_double.rs` | Test double, `#[cfg(test)]` or `pub` for integration tests |
| `mod resolve` | `cxl/src/resolve/mod.rs` | Re-exports traits + test_double |

#### Intra-phase dependencies
- Requires: Phase 2 AST (`Program`, `Expr`, `Span`)
- Unblocks: Task 3.2 (resolver pass needs traits + test double)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;

    /// FieldResolver must be object-safe for dynamic dispatch in the evaluator.
    #[test]
    fn test_field_resolver_object_safety() {
        let resolver = HashMapResolver::new(HashMap::from([
            ("name".into(), Value::String("Ada".into())),
        ]));
        let dyn_ref: &dyn FieldResolver = &resolver;
        assert_eq!(dyn_ref.resolve("name"), Some(Value::String("Ada".into())));
    }

    /// WindowContext must be object-safe for dynamic dispatch in the evaluator.
    #[test]
    fn test_window_context_object_safety() {
        // Compile-time check: dyn WindowContext is constructible
        fn _accepts_dyn(_: &dyn WindowContext) {}
    }

    /// Unqualified lookup returns stored value.
    #[test]
    fn test_hashmap_resolver_unqualified() {
        let resolver = HashMapResolver::new(HashMap::from([
            ("age".into(), Value::Integer(30)),
        ]));
        assert_eq!(resolver.resolve("age"), Some(Value::Integer(30)));
    }

    /// Qualified lookup returns stored value.
    #[test]
    fn test_hashmap_resolver_qualified() {
        let resolver = HashMapResolver::new(HashMap::new())
            .with_qualified("src", "field", Value::Bool(true));
        assert_eq!(resolver.resolve_qualified("src", "field"), Some(Value::Bool(true)));
    }

    /// Missing field returns None, not an error.
    #[test]
    fn test_hashmap_resolver_missing_field() {
        let resolver = HashMapResolver::new(HashMap::new());
        assert_eq!(resolver.resolve("nonexistent"), None);
        assert_eq!(resolver.resolve_qualified("src", "nope"), None);
    }

    /// available_fields returns all registered field names.
    #[test]
    fn test_hashmap_resolver_available_fields() {
        let resolver = HashMapResolver::new(HashMap::from([
            ("a".into(), Value::Null),
            ("b".into(), Value::Null),
        ]));
        let mut fields = resolver.available_fields();
        fields.sort();
        assert_eq!(fields, vec!["a", "b"]);
    }
}
```

#### Risk / gotcha
> **No significant risk.** Trait definitions are straightforward. The only subtlety is ensuring
> `WindowContext` is object-safe — all methods use `&self`, return owned types or `Box<dyn>`,
> and no method is generic. Verified by the object-safety test stub.

---

### Task 3.2: Resolver Pass (Phase B)
**Status:** ⛔ Blocked
**Blocked by:** Task 3.1 — traits must be defined and test double must work

**Description:**
Implement the resolver pass that walks the parsed AST and binds every `Ident` node to its
definition site. Unresolved identifiers produce a `miette` diagnostic with fuzzy-match
suggestions. The `it` binding is validated to only appear inside `predicate_expr` contexts.

**Implementation notes:**
- **NodeId(u32)** added to every AST node variant. Parser assigns monotonic IDs via a counter at each of 32 construction sites. `NodeId` fits in existing alignment padding — `size_of::<Expr>()` stays at ≤64 bytes. Side-tables are `Vec<Option<T>>` indexed by `NodeId`, O(1) lookup, cache-friendly, no hashing.
- New module `cxl::resolve::pass` with `fn resolve_program(program: Program, fields: &[&str]) -> Result<ResolvedProgram, Vec<Diagnostic>>`.
- Walk the AST top-down. Maintain a scope stack:
  1. **let-bound variables** — pushed when entering a `let` binding, popped at end of program (CXL `let` is program-scoped, not block-scoped).
  2. **field references** — resolved via the `available_fields` list passed in.
  3. **`pipeline.*` members** — `pipeline.start_time`, `pipeline.name`, `pipeline.execution_id`, `pipeline.total_count`, `pipeline.ok_count`, `pipeline.dlq_count`, `pipeline.source_file`, `pipeline.source_row`, plus user-defined `pipeline.vars.*` from YAML config (see spec SS5.8).
  4. **Module functions/constants** — looked up in a `FunctionRegistry` (from Phase 2 built-in table).
- Unresolved identifier → `miette` diagnostic with label pointing at the identifier span. Compute Levenshtein distance (hand-rolled, ~15 lines, early-bail at threshold > 3) against all available names; if best distance <= 3, include `help: "did you mean '{suggestion}'?"`.
- `it` outside a `predicate_expr` context (i.e., not inside `window.any(...)` or `window.all(...)` argument position) → Phase B error: `"'it' is only valid inside window.any() or window.all() predicates"`.
- Schema context switching: inside `predicate_expr`, unqualified references resolve against the window's source record (`it.*`), not the primary record. The resolver tracks a `context: ResolveContext` enum (`Primary | PredicateExpr`) to switch resolution targets.
- Output: `ResolvedProgram` wraps the original AST with a `Vec<Option<Binding>>` side-table mapping each `NodeId` → `Binding` enum (`Field(usize)`, `LetVar(usize)`, `PipelineMember`, `Function`, `IteratorBinding`). Distinct type from `Program` — compiler enforces that unresolved ASTs cannot be passed to the type checker.

**Acceptance criteria:**
- [ ] All identifiers in a valid program resolve without errors
- [ ] Unresolved identifiers produce a diagnostic with source span
- [ ] Fuzzy-match suggestions appear when Levenshtein distance <= 3
- [ ] `it` outside predicate_expr produces a clear error
- [ ] `it.field` inside predicate_expr resolves against window source
- [ ] `let` bindings shadow field names and are accessible after their declaration

**Required unit tests (must pass before Task 3.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_resolve_simple_field_ref` | `emit name = first_name` resolves `first_name` to a field binding | ⛔ Hard gate |
| `test_resolve_let_binding` | `let x = 1; emit val = x` resolves `x` to a let-bound variable | ⛔ Hard gate |
| `test_resolve_pipeline_member` | `pipeline.total_count` resolves to `PipelineMember` binding | ⛔ Hard gate |
| `test_resolve_unresolved_with_suggestion` | `emit val = naem` with field `name` → diagnostic with `"did you mean 'name'?"` | ⛔ Hard gate |
| `test_resolve_it_outside_predicate_error` | `emit val = it` at top level → Phase B error mentioning `predicate_expr` | ⛔ Hard gate |
| `test_resolve_it_inside_predicate_ok` | `window.any(it.salary > 100000)` resolves `it.salary` without error | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 3.3 remains Blocked until all tests pass.

#### Sub-tasks
- [ ] **[3.2.1]** Add `NodeId(u32)` to all AST variants (12 Expr, 5 Statement, MatchArm, FnDecl) + `Expr::Now { node_id, span }` variant + parser counter at 33 construction sites + parser nud handler for `Token::Now` + update all Phase 2 test AST constructions to include NodeId
- [ ] **[3.2.2]** Define `Binding` enum, `ResolvedProgram` struct with `Vec<Option<Binding>>` side-table
- [ ] **[3.2.3]** Implement `resolve_program()` walk with scope stack, `it` validation, `ResolveContext` switching
- [ ] **[3.2.4]** Implement hand-rolled Levenshtein (threshold 3, early-bail) for fuzzy-match diagnostics + all 6 gate tests

#### Code scaffolding
```rust
// ast.rs addition

/// Unique identifier for an AST node. Assigned monotonically by the parser.
/// Used as index into side-tables (bindings, types, regexes).
/// Fits in alignment padding — size_of::<Expr>() stays at 64 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);
```

```rust
// Module: cxl/src/resolve/pass.rs

use crate::ast::{Program, Expr, NodeId};

/// What a resolved identifier binds to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Binding {
    Field(usize),           // index into the schema's field list
    LetVar(usize),          // index into the let-binding table
    PipelineMember,         // pipeline.total_count, etc.
    Function,               // built-in or module function
    IteratorBinding,        // `it` inside predicate_expr
}

/// Tracks whether we are resolving inside a predicate_expr (window.any/all argument).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolveContext {
    Primary,
    PredicateExpr,
}

/// Output of the resolver pass. Distinct type from Program — compiler enforces
/// that unresolved ASTs cannot be passed to the type checker.
pub struct ResolvedProgram {
    pub program: Program,
    pub bindings: Vec<Option<Binding>>,  // indexed by NodeId
}

/// Run Phase B: resolve all identifiers in the program.
/// Returns either a ResolvedProgram or a list of diagnostics.
pub fn resolve_program(
    program: Program,
    fields: &[&str],
) -> Result<ResolvedProgram, Vec<Diagnostic>> {
    todo!()
}
```

```rust
// Module: cxl/src/resolve/levenshtein.rs

/// Compute Levenshtein distance between two strings.
/// Returns None (bails early) if distance exceeds `threshold`.
pub fn levenshtein_bounded(a: &str, b: &str, threshold: usize) -> Option<usize> {
    todo!()
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `NodeId` | `cxl/src/ast.rs` | Added to every Expr/Statement/MatchArm/FnDecl variant |
| `Binding` | `cxl/src/resolve/pass.rs` | Enum describing what an identifier binds to |
| `ResolvedProgram` | `cxl/src/resolve/pass.rs` | Wrapper: Program + bindings side-table |
| `resolve_program` | `cxl/src/resolve/pass.rs` | Entry point for Phase B |
| `levenshtein_bounded` | `cxl/src/resolve/levenshtein.rs` | Hand-rolled, threshold 3, early-bail |
| Parser ID generation | `cxl/src/parser.rs` | `next_id: u32` field on Parser, `self.next_id()` at 32 sites |

#### Intra-phase dependencies
- Requires: Task 3.1 (`FieldResolver` trait for `available_fields`), Phase 2 AST
- Unblocks: Task 3.3 (type checker consumes `ResolvedProgram`)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Simple field reference resolves to a Field binding.
    #[test]
    fn test_resolve_simple_field_ref() {
        // Arrange: program = `emit name = first_name`, fields = ["first_name"]
        // Act: resolve_program(program, &["first_name"])
        // Assert: binding for `first_name` node is Binding::Field(0)
        todo!()
    }

    /// Let-bound variable resolves to a LetVar binding.
    #[test]
    fn test_resolve_let_binding() {
        // Arrange: program = `let x = 1; emit val = x`
        // Act: resolve_program(program, &[])
        // Assert: binding for second `x` is Binding::LetVar(0)
        todo!()
    }

    /// pipeline.total_count resolves to PipelineMember.
    #[test]
    fn test_resolve_pipeline_member() {
        // Arrange: program = `emit count = pipeline.total_count`
        // Act: resolve_program(program, &[])
        // Assert: binding for pipeline.total_count is Binding::PipelineMember
        todo!()
    }

    /// Unresolved identifier produces diagnostic with fuzzy suggestion.
    #[test]
    fn test_resolve_unresolved_with_suggestion() {
        // Arrange: program = `emit val = naem`, fields = ["name"]
        // Act: resolve_program fails with diagnostics
        // Assert: diagnostic contains "did you mean 'name'?"
        todo!()
    }

    /// `it` at top level is a Phase B error.
    #[test]
    fn test_resolve_it_outside_predicate_error() {
        // Arrange: program = `emit val = it`
        // Act: resolve_program fails
        // Assert: diagnostic mentions "only valid inside window.any() or window.all()"
        todo!()
    }

    /// `it.salary` inside window.any() resolves without error.
    #[test]
    fn test_resolve_it_inside_predicate_ok() {
        // Arrange: program = `emit has_high = window.any(it.salary > 100000)`
        // fields = ["salary"]
        // Act: resolve_program succeeds
        // Assert: `it` binding is Binding::IteratorBinding, `salary` resolves
        todo!()
    }
}
```

#### Risk / gotcha
> **`it` context switching:** The resolver must correctly track `ResolveContext::PredicateExpr`
> when entering `window.any()` / `window.all()` argument positions and revert to `Primary`
> when exiting. If the context stack push/pop is off-by-one, `it` could be allowed in wrong
> contexts or disallowed in correct ones. Test both positive (it inside predicate) and negative
> (it outside predicate) cases to catch this.

---

### Task 3.3: Type Checker (Phase C)
**Status:** ⛔ Blocked
**Blocked by:** Task 3.2 — resolver pass must produce `ResolvedProgram`

**Description:**
Implement two-pass type inference and checking over the resolved AST. Pass 1 collects
per-field type constraints from all usage sites (seeded by explicit schema types). Pass 2
walks bottom-up to annotate every expression node with a concrete type. Produces `TypedProgram`
or diagnostics. Enforces null-propagation rules and semantic constraints from the spec.

**Implementation notes:**
- Type enum in `cxl::typecheck::types`: `Null`, `Bool`, `Int`, `Float`, `String`, `Date`, `DateTime`, `Array`, `Numeric` (union of `Int | Float`), `Any`, `Nullable(Box<Type>)`.
- `Nullable` is idempotent: `Type::nullable(Nullable(T))` returns `Nullable(T)`, never `Nullable(Nullable(T))`. Enforced by a smart constructor.
- `Numeric` is a constraint type: `sum(x)` requires `x: Numeric`, meaning either `Int` or `Float`. During unification, `Int` unifies with `Numeric` → `Int`; `Float` unifies with `Numeric` → `Float`; `String` unifies with `Numeric` → type error.
- **Two-pass type checker:**
  - **Pass 1 — Constraint collection:** Walk entire AST. For each field usage in a typed context, record `(field_name, inferred_type, span)`. Seed the constraint map with explicit types from YAML schema overrides or schema files — these are `Declared` and override inference. After the walk, unify all constraints per field: all usage sites must agree, and inferred types must agree with declared types. Conflicts produce diagnostics citing both spans (e.g., "field 'status' used as String on line 3 but as Numeric on line 7"). Output: `FieldTypeMap` mapping each field to its final inferred/declared type.
  - **Pass 2 — Bottom-up annotation:** Walk AST post-order. Each node's type is computed directly from its children's types, the operator/method signature, and the `FieldTypeMap`. No type variables, no substitution. `emit out = some_field` where `some_field` has an inferred type from Pass 1 gets the inferred type. Fields used only in type-agnostic contexts stay `Any`.
- The `FieldTypeMap` is consumed by the runtime for DLQ decisions: if we inferred `amount: Numeric` and a row contains `amount = "N/A"`, that's a runtime type violation → DLQ or halt.
- Null propagation rules:
  - Arithmetic (`+`, `-`, `*`, `/`, `%`) on `Nullable(T)` → `Nullable(T)`.
  - Comparison (`>`, `<`, `>=`, `<=`) on `Nullable(T)` → `Nullable(Bool)`.
  - `??` (coalesce): `Nullable(T) ?? T` → `T` (strips nullability).
  - `==` / `!=` → always `Bool` (never nullable, null == null is true per spec).
- Phase C semantic checks:
  1. **Match exhaustiveness:** every `match` expression must have a `_` (wildcard) arm. Missing `_` → error.
  2. **Window inside predicate_expr:** `window.sum(...)` inside `window.any(window.sum(...) > 0)` is illegal — window calls cannot nest inside predicate_expr.
  3. **No-output-fields guard:** program must contain at least one `emit` statement. Zero emits → warning diagnostic.
  4. **Numeric argument guard:** `sum(expr)` and `avg(expr)` require `expr: Numeric`. String/Bool/Date → type error.
  5. **Pipeline read-only:** `pipeline.total_count = 5` or any assignment to `pipeline.*` → error.
- Method call type checking: look up return type from function registry (Phase 2). E.g., `.trim()` on `String` → `String`; `.trim()` on `Int` → type error.
- Output: `TypedProgram` — a flat struct that takes ownership of `ResolvedProgram`'s fields. Distinct type enforcing pass ordering at compile time: `Program` → `ResolvedProgram` → `TypedProgram`. Compiler rejects passing an unresolved AST to the type checker.

**Acceptance criteria:**
- [ ] Type inference assigns concrete types to all expression nodes in a valid program
- [ ] Cross-expression field type conflicts produce diagnostics citing both usage spans
- [ ] Schema-declared types override inference; conflicts with usage produce diagnostics
- [ ] Null propagation produces `Nullable(T)` through arithmetic chains
- [ ] `??` strips nullability from its left operand's type
- [ ] Match without `_` arm produces a diagnostic
- [ ] Nested window calls inside predicate_expr produce a diagnostic
- [ ] `sum("hello")` produces a type error mentioning `Numeric`
- [ ] Zero `emit` statements produce a warning
- [ ] `FieldTypeMap` is produced for runtime DLQ validation

**Required unit tests (must pass before Task 3.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_typecheck_arithmetic_int` | `1 + 2` infers `Int`, `1 + 2.0` infers `Float` | ⛔ Hard gate |
| `test_typecheck_null_propagation` | `nullable_field + 1` infers `Nullable(Int)` | ⛔ Hard gate |
| `test_typecheck_coalesce_strips_nullable` | `nullable_field ?? 0` infers `Int` (not `Nullable(Int)`) | ⛔ Hard gate |
| `test_typecheck_match_missing_wildcard` | Match without `_` arm → Phase C error | ⛔ Hard gate |
| `test_typecheck_nested_window_in_predicate` | `window.any(window.sum(it.x) > 0)` → Phase C error | ⛔ Hard gate |
| `test_typecheck_sum_requires_numeric` | `sum("hello")` → type error diagnostic | ⛔ Hard gate |
| `test_typecheck_field_type_conflict_both_spans` | field used as String on line N and Numeric on line M → diagnostic cites both spans | ⛔ Hard gate |
| `test_typecheck_schema_override_conflict` | schema declares field as Int, CXL uses as String → diagnostic produced | ⛔ Hard gate |
| `test_typecheck_zero_emit_warning` | program with no `emit` statements produces a warning diagnostic | ⛔ Hard gate |
| `test_typecheck_field_type_map_produced` | after type-check, `TypedProgram.field_types` contains entries for all inferred fields | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 3.4 remains Blocked until all tests pass.

#### Sub-tasks
- [ ] **[3.3.1]** Define `Type` enum with `Nullable` flattening smart constructor
- [ ] **[3.3.2]** Implement Pass 1: constraint collection — walk AST, collect `(field, Type, Span)` per usage site, seed from schema overrides
- [ ] **[3.3.3]** Implement per-field unification + Pass 2: bottom-up type annotation using `FieldTypeMap`
- [ ] **[3.3.4]** Implement Phase C semantic checks (match exhaustiveness, nested window, no-emit guard, numeric guard, pipeline read-only) + all 6 gate tests

#### Code scaffolding
```rust
// Module: cxl/src/typecheck/types.rs

/// CXL type system. 7 concrete types + Numeric (union) + Any (unknown) + Nullable wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    Null,
    Bool,
    Int,
    Float,
    String,
    Date,
    DateTime,
    Array,
    /// Union of Int | Float. Resolved during unification.
    Numeric,
    /// Unknown type — field used only in type-agnostic contexts.
    Any,
    /// Nullable wrapper. Idempotent: Nullable(Nullable(T)) is flattened to Nullable(T).
    Nullable(Box<Type>),
}

impl Type {
    /// Smart constructor: flattens nested Nullable.
    pub fn nullable(inner: Type) -> Type {
        match inner {
            Type::Nullable(_) => inner,
            Type::Null => Type::Null,
            other => Type::Nullable(Box::new(other)),
        }
    }

    /// True if this type is or wraps Nullable.
    pub fn is_nullable(&self) -> bool {
        matches!(self, Type::Nullable(_) | Type::Null)
    }

    /// Strip Nullable wrapper if present.
    pub fn unwrap_nullable(&self) -> &Type {
        match self {
            Type::Nullable(inner) => inner,
            other => other,
        }
    }
}
```

```rust
// Module: cxl/src/typecheck/pass.rs

use std::collections::HashMap;
use crate::ast::{Program, NodeId};
use crate::resolve::pass::{Binding, ResolvedProgram};
use super::types::Type;
use regex::Regex;

/// A constraint from a single usage site of a field.
#[derive(Debug, Clone)]
struct FieldConstraint {
    field: String,
    inferred_type: Type,
    span: Span,
}

/// Output of the type checker. Flat struct — takes ownership of ResolvedProgram fields.
/// Distinct type: compiler enforces Program → ResolvedProgram → TypedProgram ordering.
/// Must be Send + Sync for Arc sharing across rayon workers.
pub struct TypedProgram {
    pub program: Program,
    pub bindings: Vec<Option<Binding>>,
    pub types: Vec<Option<Type>>,           // per-node type, indexed by NodeId
    pub field_types: IndexMap<String, Type>, // inferred FieldTypeMap for runtime DLQ (deterministic iteration)
    pub regexes: Vec<Option<Regex>>,        // pre-compiled regex literals, indexed by NodeId
}

/// Run Phase C: type-check a resolved program.
/// `schema` contains explicitly declared field types from YAML — these override inference.
pub fn type_check(
    resolved: ResolvedProgram,
    schema: &HashMap<String, Type>,
) -> Result<TypedProgram, Vec<Diagnostic>> {
    todo!()
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Type` | `cxl/src/typecheck/types.rs` | Type enum with nullable smart constructor |
| `TypedProgram` | `cxl/src/typecheck/pass.rs` | Final output: program + bindings + types + field_types + regexes |
| `type_check` | `cxl/src/typecheck/pass.rs` | Entry point for Phase C |
| `mod typecheck` | `cxl/src/typecheck/mod.rs` | Re-exports types + pass |

#### Intra-phase dependencies
- Requires: Task 3.2 (`ResolvedProgram` with bindings side-table)
- Unblocks: Task 3.4 (evaluator consumes `TypedProgram`)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Integer arithmetic produces Int; mixed int/float promotes to Float.
    #[test]
    fn test_typecheck_arithmetic_int() {
        // Arrange: program = `emit a = 1 + 2; emit b = 1 + 2.0`
        // Act: type_check(resolved, &{})
        // Assert: type of `1 + 2` is Type::Int; type of `1 + 2.0` is Type::Float
        todo!()
    }

    /// Nullable field in arithmetic propagates Nullable.
    #[test]
    fn test_typecheck_null_propagation() {
        // Arrange: program = `emit val = nullable_field + 1`
        //   schema declares nullable_field as Nullable(Int)
        // Act: type_check
        // Assert: type of `nullable_field + 1` is Type::Nullable(Int)
        todo!()
    }

    /// Coalesce strips Nullable from left operand.
    #[test]
    fn test_typecheck_coalesce_strips_nullable() {
        // Arrange: program = `emit val = nullable_field ?? 0`
        //   schema declares nullable_field as Nullable(Int)
        // Act: type_check
        // Assert: type of `nullable_field ?? 0` is Type::Int (not Nullable(Int))
        todo!()
    }

    /// Match without wildcard arm produces Phase C error.
    #[test]
    fn test_typecheck_match_missing_wildcard() {
        // Arrange: program = `emit val = match status { "A" -> 1, "B" -> 2 }`
        // Act: type_check fails
        // Assert: diagnostic mentions missing `_` catch-all arm
        todo!()
    }

    /// Nested window call inside predicate_expr is illegal.
    #[test]
    fn test_typecheck_nested_window_in_predicate() {
        // Arrange: program = `emit val = window.any(window.sum(it.x) > 0)`
        // Act: type_check fails
        // Assert: diagnostic mentions nested window calls
        todo!()
    }

    /// sum() requires Numeric argument.
    #[test]
    fn test_typecheck_sum_requires_numeric() {
        // Arrange: program with window.sum applied to a String field
        // Act: type_check fails
        // Assert: diagnostic mentions Numeric requirement
        todo!()
    }
}
```

#### Risk / gotcha
> **Cross-expression constraint conflict reporting:** When field `status` is used as String on
> line 3 and as Numeric on line 7, the diagnostic must cite **both** spans — not just the second
> usage. The constraint collector must store all `(Type, Span)` pairs per field, and the
> unifier must report the first conflicting pair. If only the latest usage is stored, the
> diagnostic will point at line 7 but give no clue about line 3, making debugging difficult.

---

### Task 3.4: Core Evaluator + All Built-in Methods + cxl-cli
**Status:** ⛔ Blocked
**Blocked by:** Task 3.3 — type checker must pass all checks before evaluation

**Description:**
Implement the tree-walking evaluator that executes a type-checked CXL program against a
record. Implement all ~60 built-in methods from spec SS5.5. Build the `cxl-cli` binary with
`cxl check`, `cxl eval`, and `cxl fmt` subcommands.

**Implementation notes:**
- Entry point: `fn eval(expr: &Expr, typed: &TypedProgram, ctx: &EvalContext, resolver: &dyn FieldResolver, window: Option<&dyn WindowContext>) -> Result<Value, EvalError>`.
- **Error type:** `EvalError` struct with `EvalErrorKind` enum + shared `Span` metadata (Boa/Starlark convention). `#[non_exhaustive]` on `EvalErrorKind` for future growth. Pipeline error handler matches on `kind` for DLQ routing. Implements `std::error::Error + Send + Sync + Display`.
- **Clock trait for `now`:** `EvalContext` holds `Box<dyn Clock>` where `Clock: Send + Sync` has `fn now(&self) -> NaiveDateTime`. Production: `WallClock` (reads system time per call — fresh per record, matching spec "at the point of row evaluation"). Tests: `FixedClock(NaiveDateTime)` for deterministic assertions. `now` is a bare keyword in the lexer (like `null`, `true`, `false`), parsed as `Expr::Now { span }`. Methods chain naturally: `now.year`, `now.add_days(-7)`, `created_at.diff_days(now)`.
- **`pipeline.start_time`:** Separate `NaiveDateTime` field on `EvalContext`, frozen at pipeline start. Distinct from `now` — deterministic within a run.
- Binary operators: implement all arithmetic (`+`, `-`, `*`, `/`, `%`), comparison (`==`, `!=`, `>`, `<`, `>=`, `<=`), logical (`&&`, `||`, `!`), string concatenation (`++`). Null propagation per SS5.4: any arithmetic/comparison with `Null` operand returns `Null` (except `==`/`!=` which return `Bool`).
- `let` bindings: maintain a `HashMap<&str, Value>` local environment. Each `let` evaluates its RHS and inserts into the env. Subsequent expressions see the binding.
- `emit` bindings: collect into an output `HashMap<String, Value>`. The evaluator returns this map as the record's output fields.
- `match` expression: evaluate the scrutinee, test each arm's pattern in order, return first matching arm's body. Two forms: condition-match (`match { cond -> val, ... }`) and value-match (`match expr { val -> result, ... }`).
- `if/then/else`: evaluate condition; if truthy → evaluate then-branch; else → evaluate else-branch. Missing else → `Value::Null`.
- `??` coalesce: evaluate LHS; if `Null` → evaluate RHS; otherwise return LHS. Short-circuit: RHS is not evaluated if LHS is non-null.
- **Three-valued AND/OR logic** per spec SS5.4: `false && null → false` (not null); `true || null → true` (not null). Standard null propagation only applies when the short-circuit operand cannot determine the result.
- **Integer arithmetic** uses `i64::checked_*` methods. Overflow returns `EvalError { kind: IntegerOverflow, span }`. Float arithmetic uses standard IEEE 754 — NaN/Inf propagates through intermediates and is caught at emit output boundary (Phase 4 pipeline).
- **Implicit coercion at eval time** per spec SS5.4: String in comparison with Int/Float → attempt parse via `clinker_record::coercion`. Parse failure → null propagates. `"3.14" < 4` parses `"3.14"` as Float → `3.14 < 4.0 → true`.
- **String output size guard:** `.repeat(n)`, `.pad_left(n)`, `.pad_right(n)` check `receiver.len() * n <= MAX_STRING_OUTPUT` (default 10MB) before allocating. Exceeding the limit returns `EvalError { kind: StringTooLarge, span }`.
- **Regex:** All regex patterns are string literals, pre-compiled during Phase C (type-check pass) and stored in `TypedProgram.regexes` indexed by `NodeId`. The evaluator reads `typed.regexes[node_id]` — immutable, zero runtime overhead, no caching needed. Compile errors surface as Phase A diagnostics. This follows the Ripgrep convention: compile once at build time, own the `Regex` in the IR.
- **String methods (24 + 5 path = 29):** `.concat()`, `.trim()`, `.trim_start()`, `.trim_end()`, `.upper()`, `.lower()`, `.replace(old, new)`, `.substring(start, len)`, `.left(n)`, `.right(n)`, `.length()`, `.starts_with(s)`, `.ends_with(s)`, `.contains(s)`, `.split(sep)`, `.join(delim)`, `.find(pattern)`, `.matches(regex)`, `.capture(regex, group?)`, `.format(template)`, `.pad_left(n, ch)`, `.pad_right(n, ch)`, `.repeat(n)`, `.reverse()`, `.file_name()`, `.file_stem()`, `.extension()`, `.parent()`, `.parent_name()`.
- **Array methods (2):** `.join(delim)`, `.length()`.
- **Numeric methods (8):** `.abs()`, `.ceil()`, `.floor()`, `.round(n?)`, `.round_to(n)`, `.clamp(min, max)`, `.min(b)`, `.max(b)`.
- **Date methods (13):** `.year()`, `.month()`, `.day()`, `.hour()`, `.minute()`, `.second()`, `.add_days(n)`, `.add_months(n)`, `.add_years(n)`, `.diff_days(d)`, `.diff_months(d)`, `.diff_years(d)`, `.format_date(pattern)`.
- **Conversion strict (6):** `.to_int()`, `.to_float()`, `.to_string()`, `.to_bool()`, `.to_date(fmt)`, `.to_datetime(fmt)`.
- **Conversion lenient (5):** `.try_int()`, `.try_float()`, `.try_bool()`, `.try_date(fmt)`, `.try_datetime(fmt)`. Return `Null` on failure instead of error.
- **Introspection (4):** `.type_of()`, `.is_null()`, `.is_empty()`, `.catch(default)`.
- **Debug (1):** `.debug(prefix)` — emits to `tracing::debug!` and returns the value unchanged (pass-through).
- `cxl-cli` binary (`crates/cxl-cli/src/main.rs`) — new crate in the workspace, keeps `cxl` dependency-light (no `clap`):
  - `cxl check <file.cxl>` — parse + resolve + type-check, print diagnostics, exit 0 or 1.
  - `cxl eval <file.cxl> --record '<json>'` — parse + resolve + type-check + evaluate against a JSON record, print output fields as JSON.
  - `cxl fmt <file.cxl>` — parse and pretty-print the program (canonical formatting).
  - Uses `clap` for argument parsing, `miette` for diagnostic rendering.

**Acceptance criteria:**
- [ ] `eval` produces correct output for all binary operators with both non-null and null operands
- [ ] All ~60 built-in methods return correct values for representative inputs
- [ ] `let` bindings are visible to subsequent expressions
- [ ] `emit` bindings are collected into the output map
- [ ] `match` evaluates both condition-form and value-form correctly
- [ ] `if/then/else` returns `Null` when else-branch is missing and condition is false
- [ ] `??` short-circuits (does not evaluate RHS when LHS is non-null)
- [ ] Pre-compiled regexes are read from `TypedProgram`, not compiled at runtime
- [ ] `now` returns wall-clock time via `Clock` trait; testable with `FixedClock`
- [ ] `EvalError` struct with `EvalErrorKind` enum, all variants carry span
- [ ] `cxl check` exits 0 on valid input, 1 on errors
- [ ] `cxl eval` prints correct JSON output for a sample program + record
- [ ] `cxl fmt` round-trips a program through parse + pretty-print

**Required unit tests (must pass before Phase 4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_eval_arithmetic_null_propagation` | `Null + 1` returns `Null`; `2 + 3` returns `5` | ⛔ Hard gate |
| `test_eval_comparison_null_semantics` | `Null == Null` is `true`; `Null != 1` is `true`; `Null > 1` is `Null` | ⛔ Hard gate |
| `test_eval_coalesce_short_circuit` | `"hello" ?? panic_expr` returns `"hello"` without evaluating RHS | ⛔ Hard gate |
| `test_eval_let_binding_scope` | `let x = 10; emit val = x + 1` produces `{"val": 11}` | ⛔ Hard gate |
| `test_eval_match_condition_form` | `match { age > 18 -> "adult", _ -> "minor" }` selects correct arm | ⛔ Hard gate |
| `test_eval_match_value_form` | `match status { "A" -> "active", "I" -> "inactive", _ -> "unknown" }` | ⛔ Hard gate |
| `test_eval_if_then_else_missing_else` | `if false then 1` returns `Null` | ⛔ Hard gate |
| `test_eval_string_methods_core` | `.trim()`, `.upper()`, `.lower()`, `.length()`, `.replace()`, `.substring()` | ⛔ Hard gate |
| `test_eval_string_methods_regex` | `.matches("\\d+")`, `.capture("(\\d+)")` | ⛔ Hard gate |
| `test_eval_string_methods_path` | `.file_name()`, `.file_stem()`, `.extension()`, `.parent()`, `.parent_name()` | ⛔ Hard gate |
| `test_eval_numeric_methods` | `.abs()`, `.ceil()`, `.floor()`, `.round()`, `.round_to(2)`, `.clamp(0, 100)` | ⛔ Hard gate |
| `test_eval_date_methods` | `.year()`, `.month()`, `.day()`, `.add_days(7)`, `.format_date("%Y-%m-%d")` | ⛔ Hard gate |
| `test_eval_conversion_strict` | `.to_int()` on `"42"` → `42`; `.to_int()` on `"abc"` → error | ⛔ Hard gate |
| `test_eval_conversion_lenient` | `.try_int()` on `"abc"` → `Null`; `.try_float()` on `"3.14"` → `3.14` | ⛔ Hard gate |
| `test_eval_introspection` | `.type_of()` returns `"string"`/`"int"`/etc.; `.is_null()` on `Null` → `true` | ⛔ Hard gate |
| `test_eval_regex_precompiled` | Regex read from `TypedProgram.regexes`, not compiled at runtime | ⛔ Hard gate |
| `test_eval_emit_output_map` | Program with 3 `emit` statements produces map with 3 entries | ⛔ Hard gate |
| `test_cxl_check_valid_program` | `cxl check` on a valid `.cxl` file exits 0 | ⛔ Hard gate |
| `test_cxl_eval_json_record` | `cxl eval` with `--record '{"name":"Ada"}'` prints correct output | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 4 remains Blocked until all tests pass.

#### Sub-tasks
- [ ] **[3.4.1]** Define `EvalError` struct + `EvalErrorKind` enum (`#[non_exhaustive]`), `Clock` trait with `WallClock`/`FixedClock`, `EvalContext` struct
- [ ] **[3.4.2]** Implement core evaluator: binary ops with null propagation, let/emit, match (both forms), if/then/else, coalesce (short-circuit), `now` keyword
- [ ] **[3.4.3]** Implement all ~60 built-in methods (string, path, array, numeric, date, conversion strict/lenient, introspection, debug)
- [ ] **[3.4.4]** Implement `cxl-cli` crate with `check`, `eval`, `fmt` subcommands (clap derive, miette rendering)
- [ ] **[3.4.5]** Write all 19 gate tests

#### Code scaffolding
```rust
// Module: cxl/src/eval/error.rs

use crate::lexer::Span;

/// Runtime evaluation error with diagnostic metadata.
/// Follows the Boa/Starlark convention: struct with kind enum + shared span.
#[derive(Debug, Clone)]
pub struct EvalError {
    pub kind: EvalErrorKind,
    pub span: Span,
}

/// Error categories. #[non_exhaustive] allows future growth.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum EvalErrorKind {
    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: &'static str, got: &'static str },

    #[error("division by zero")]
    DivisionByZero,

    #[error("conversion failed: cannot convert {value} to {target}")]
    ConversionFailed { value: String, target: &'static str },

    #[error("invalid regex pattern: {error}")]
    RegexCompile { pattern: String, error: String },

    #[error("index out of bounds: {index} (length {length})")]
    IndexOutOfBounds { index: i64, length: usize },

    #[error("integer overflow in {op} operation")]
    IntegerOverflow { op: &'static str },

    #[error("string output exceeds maximum size ({size} bytes, limit {limit})")]
    StringTooLarge { size: usize, limit: usize },

    #[error("argument count mismatch for '{name}': expected {expected}, got {got}")]
    ArityMismatch { name: String, expected: usize, got: usize },
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl std::error::Error for EvalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.kind)
    }
}
```

```rust
// Module: cxl/src/eval/context.rs

use chrono::{NaiveDateTime, Utc};

/// Injectable clock for testability. Production uses WallClock; tests use FixedClock.
pub trait Clock: Send + Sync {
    fn now(&self) -> NaiveDateTime;
}

/// Production clock: reads system wall-clock. Fresh value per call (per-record).
pub struct WallClock;
impl Clock for WallClock {
    fn now(&self) -> NaiveDateTime {
        Utc::now().naive_utc()
    }
}

/// Test clock: returns a fixed time. Deterministic assertions.
pub struct FixedClock(pub NaiveDateTime);
impl Clock for FixedClock {
    fn now(&self) -> NaiveDateTime {
        self.0
    }
}

/// Evaluation context — injected per pipeline run or per test.
/// Holds clock, pipeline metadata, counters, provenance, and user vars.
/// All `pipeline.*` members in CXL resolve against this struct.
pub struct EvalContext {
    /// Clock for `now` keyword. WallClock in production, FixedClock in tests.
    pub clock: Box<dyn Clock>,
    /// pipeline.start_time — frozen at pipeline start, deterministic within a run.
    pub pipeline_start_time: NaiveDateTime,
    /// pipeline.name — from YAML config.
    pub pipeline_name: String,
    /// pipeline.execution_id — UUID v7, unique per run.
    pub pipeline_execution_id: String,
    /// pipeline.total_count / ok_count / dlq_count — cumulative, snapshotted at chunk boundaries.
    pub pipeline_counters: PipelineCounters,
    /// pipeline.source_file / source_row — from RecordProvenance, set per record.
    pub source_file: Arc<str>,
    pub source_row: u64,
    /// pipeline.vars.* — user-defined constants from YAML config.
    pub pipeline_vars: IndexMap<String, Value>,
}
```

```rust
// Module: cxl/src/eval/mod.rs

use clinker_record::Value;
use crate::ast::Expr;
use crate::typecheck::pass::TypedProgram;
use crate::resolve::traits::{FieldResolver, WindowContext};
use self::context::EvalContext;
use self::error::EvalError;

/// Evaluate a single expression against a record.
/// The TypedProgram provides per-node types, bindings, and pre-compiled regexes.
/// The EvalContext provides clock, pipeline metadata, and counters.
pub fn eval(
    expr: &Expr,
    typed: &TypedProgram,
    ctx: &EvalContext,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext>,
) -> Result<Value, EvalError> {
    todo!()
}

/// Evaluate a full CXL program against a record. Returns the output field map.
pub fn eval_program(
    typed: &TypedProgram,
    ctx: &EvalContext,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext>,
) -> Result<HashMap<String, Value>, EvalError> {
    todo!()
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `EvalError` / `EvalErrorKind` | `cxl/src/eval/error.rs` | Struct + kind enum, `#[non_exhaustive]` |
| `Clock` / `WallClock` / `FixedClock` | `cxl/src/eval/context.rs` | Injectable clock trait |
| `EvalContext` | `cxl/src/eval/context.rs` | Holds clock + pipeline metadata |
| `eval` / `eval_program` | `cxl/src/eval/mod.rs` | Core evaluator entry points |
| `cxl-cli` binary | `crates/cxl-cli/src/main.rs` | New workspace crate: check, eval, fmt |
| `cxl-cli` Cargo.toml | `crates/cxl-cli/Cargo.toml` | Depends on `cxl`, `clap`, `miette`, `serde_json` |

#### Intra-phase dependencies
- Requires: Task 3.3 (`TypedProgram` with types + regexes + field_types)
- Unblocks: Phase 4 (CSV pipeline consumes the evaluator)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    /// Null propagation: Null + 1 → Null; 2 + 3 → 5.
    #[test]
    fn test_eval_arithmetic_null_propagation() {
        // Arrange: two expressions — `null + 1` and `2 + 3`
        // Act: eval both
        // Assert: first is Value::Null, second is Value::Integer(5)
        todo!()
    }

    /// Null comparison semantics per spec SS5.4.
    #[test]
    fn test_eval_comparison_null_semantics() {
        // Assert: Null == Null → true, Null != 1 → true, Null > 1 → Null
        todo!()
    }

    /// Coalesce short-circuits: RHS not evaluated when LHS is non-null.
    #[test]
    fn test_eval_coalesce_short_circuit() {
        // Arrange: `"hello" ?? <expr that would error if evaluated>`
        // Act: eval
        // Assert: returns "hello", no error from RHS
        todo!()
    }

    /// Let bindings visible to subsequent expressions.
    #[test]
    fn test_eval_let_binding_scope() {
        // Arrange: `let x = 10; emit val = x + 1`
        // Act: eval_program
        // Assert: output map contains {"val": 11}
        todo!()
    }

    /// Condition-form match selects correct arm.
    #[test]
    fn test_eval_match_condition_form() {
        // Arrange: `match { age > 18 -> "adult", _ -> "minor" }` with age = 25
        // Assert: "adult"
        todo!()
    }

    /// Value-form match selects correct arm.
    #[test]
    fn test_eval_match_value_form() {
        // Arrange: `match status { "A" -> "active", _ -> "unknown" }` with status = "A"
        // Assert: "active"
        todo!()
    }

    /// Missing else branch returns Null.
    #[test]
    fn test_eval_if_then_else_missing_else() {
        // Arrange: `if false then 1`
        // Assert: Value::Null
        todo!()
    }

    /// Core string methods.
    #[test]
    fn test_eval_string_methods_core() {
        // Assert: " hello ".trim() → "hello", "hi".upper() → "HI", etc.
        todo!()
    }

    /// Regex methods use pre-compiled regexes from TypedProgram.
    #[test]
    fn test_eval_string_methods_regex() {
        // Assert: "abc123".matches("\\d+") → false (full match), "abc123".find("\\d+") → true
        todo!()
    }

    /// Path decomposition methods.
    #[test]
    fn test_eval_string_methods_path() {
        // Assert: "data/orders.csv".file_name() → "orders.csv", etc.
        todo!()
    }

    /// Numeric methods.
    #[test]
    fn test_eval_numeric_methods() {
        // Assert: (-5).abs() → 5, (3.7).ceil() → 4, (3.2).floor() → 3, etc.
        todo!()
    }

    /// Date methods.
    #[test]
    fn test_eval_date_methods() {
        // Assert: date.year() → 2026, date.add_days(7) → correct date, etc.
        todo!()
    }

    /// Strict conversion: success and failure.
    #[test]
    fn test_eval_conversion_strict() {
        // Assert: "42".to_int() → 42; "abc".to_int() → EvalError
        todo!()
    }

    /// Lenient conversion: failure returns Null.
    #[test]
    fn test_eval_conversion_lenient() {
        // Assert: "abc".try_int() → Null; "3.14".try_float() → 3.14
        todo!()
    }

    /// Introspection methods.
    #[test]
    fn test_eval_introspection() {
        // Assert: "hello".type_of() → "string"; Null.is_null() → true
        todo!()
    }

    /// Regex is read from TypedProgram.regexes, not compiled at runtime.
    #[test]
    fn test_eval_regex_precompiled() {
        // Arrange: TypedProgram with pre-compiled regex at a specific NodeId
        // Act: eval a .matches() call
        // Assert: regex comes from typed.regexes[node_id], no Regex::new at runtime
        todo!()
    }

    /// Multiple emit statements produce output map.
    #[test]
    fn test_eval_emit_output_map() {
        // Arrange: program with `emit a = 1; emit b = 2; emit c = 3`
        // Assert: output map has 3 entries
        todo!()
    }

    /// cxl check exits 0 on valid program.
    #[test]
    fn test_cxl_check_valid_program() {
        // Integration test: invoke cxl-cli check on a valid .cxl file
        todo!()
    }

    /// cxl eval produces correct JSON output.
    #[test]
    fn test_cxl_eval_json_record() {
        // Integration test: invoke cxl-cli eval with --record '{"name":"Ada"}'
        todo!()
    }
}
```

#### Risk / gotcha
> **Built-in method volume:** ~60 methods is substantial. Each needs correct null propagation
> (nullable receiver → null result for most methods), type checking against `BuiltinRegistry`
> signatures, and edge case handling (empty strings, zero-length substrings, negative indices,
> dates at month boundaries). Risk of inconsistency across methods. Mitigate by implementing
> a shared `dispatch_method` function that handles null propagation once, then delegates to
> per-category implementations (string_methods.rs, numeric_methods.rs, date_methods.rs, etc.).

---

## Intra-Phase Dependency Graph

```
Task 3.1 (traits) ---> Task 3.2 (resolver) ---> Task 3.3 (type checker) ---> Task 3.4 (evaluator + cli)
```

Critical path: 3.1 → 3.2 → 3.3 → 3.4 (strictly sequential — each task's output type is the next task's input)
Parallelizable: None within Phase 3 (linear dependency chain)

---

## Decisions Log (drill-phase — 2026-03-29)
| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| 1 | WindowContext uses spec SS11.4 signatures (FieldResolver-returning positionals, &str aggregations, closure predicates) | Spec design supports `window.first().name` and `it` binding; plan's simpler version can't express these semantics | Task 3.1, 3.4 |
| 2 | `available_fields(&self) -> Vec<&str>` with lifetime tied to `&self` | Zero-copy, diagnostic-path only, idiomatic Rust | Task 3.1, 3.2 |
| 3 | Explicit `NodeId(u32)` on every AST variant, not arena-indexed AST | Fits in alignment padding (0 memory cost), Vec side-tables O(1), clean API for companion editor (`cxl` crate stays simple), arena complexity not justified at CXL scale (200 lines, 200μs analysis). Researched rustc, rust-analyzer, oxc, ruff, swc. | Task 3.2, 3.3, 3.4, all downstream phases |
| 4 | Wrapper-per-pass types: Program → ResolvedProgram → TypedProgram | Compile-time enforcement of pass ordering; TypedProgram flattens all fields for Arc sharing | Task 3.2, 3.3, 3.4 |
| 5 | Hand-rolled Levenshtein with early-bail at distance > 3 | 15 lines, fixed threshold, no external crate needed for something this small | Task 3.2 |
| 6 | Two-pass type checker: constraint collection then bottom-up annotation | Cross-expression field type conflicts (status used as String and Numeric) require collecting constraints from all usage sites. Schema-declared types seed and override inference. FieldTypeMap enables runtime DLQ validation. | Task 3.3, Phase 4+ runtime |
| 7 | Bottom-up annotation (not full HM unification) for Pass 2 | CXL has no generics/polymorphism — every operator has fixed type rules. Constraint collection (Pass 1) handles the cross-expression problem; Pass 2 just reads the resolved FieldTypeMap. | Task 3.3 |
| 8 | `Nullable` flattened on construction via smart constructor | `Nullable(Nullable(T))` → `Nullable(T)`. Nullability is boolean, not a depth. 3-line constructor. | Task 3.3 |
| 9 | `EvalError` struct + `EvalErrorKind` enum with shared span (Boa/Starlark convention) | Span in one place, `#[non_exhaustive]` kind enum, miette Diagnostic impl once on struct. Convention from mature Rust evaluators. | Task 3.4 |
| 10 | Pre-compile regex literals during type-check, store in TypedProgram | All CXL regex patterns are string literals (spec). Ripgrep convention: compile once, own in IR. Zero runtime overhead, no caching/mutability. Compile errors surface at Phase A. | Task 3.3, 3.4 |
| 11 | `cxl-cli` as separate workspace crate | Keeps `cxl` library dependency-light (no clap). Spec workspace layout already lists it. Companion editor depends on `cxl` without pulling CLI deps. | Task 3.4 |
| 12 | `Clock` trait with `WallClock`/`FixedClock` on `EvalContext`; `now` as bare keyword | Boa convention (dedicated Clock trait on eval context). `now` is fresh per-record (spec SS5.4). `pipeline.start_time` is separate, frozen. Bare `now` keyword matches jq-style DSL convention; chains naturally: `now.year`, `now.add_days(-7)`. | Task 3.4 |

## Assumptions Log (drill-phase — 2026-03-29)
| # | Assumption | Basis | Risk if wrong |
|---|-----------|-------|---------------|
| 1 | CXL programs are ≤200 lines, analysis completes in <1ms | Spec target workload (50-200 lines), benchmarks from Gleam/Ruff/Nushell | If programs grow to thousands of lines, may need incremental analysis (salsa). Unlikely for a pipeline DSL. |
| 2 | `NodeId(u32)` fits in alignment padding, `size_of::<Expr>()` stays ≤64 bytes | Alignment analysis of current Expr layout (MethodCall is 56 bytes payload, 7 bytes padding available) | If Expr grows a new large variant, NodeId may push over 64 bytes. Monitor with the existing size assertion test. |
| 3 | Span uniqueness holds for CXL v1 (no macros, no synthetic nodes) | CXL is hand-written source only; parser error recovery uses dummy spans at distinct positions | If CXL v2 adds macros or code generation, spans may collide. NodeId solves this since it's independent of source position. |
| 4 | Companion editor does not need incremental analysis | Full re-analysis at 50-200μs is imperceptible at 150ms debounce | If the editor analyzes cross-file dependencies (multi-module CXL), may need module-level caching (Gleam pattern). |
