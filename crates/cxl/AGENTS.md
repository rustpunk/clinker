# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/cxl`.
Follow the root instructions first.

## Purpose

`cxl` owns Clinker's expression-language implementation: AST, lexing/parsing,
module evaluation, resolution, typechecking, static analysis, aggregate
extraction, and runtime evaluation over `clinker-record` values.

## Responsibilities

- Parse CXL source and `.cxl` module files into span-bearing ASTs with stable `NodeId`s.
- Resolve fields, modules, builtins, scoped variables, and system namespaces.
- Typecheck programs against `Row` types and aggregate-mode rules.
- Analyze typed programs for window use, `$doc` paths, accessed fields, and parallelism hints.
- Extract aggregate programs into `CompiledAggregate` IR.
- Evaluate typed programs per record through `ProgramEvaluator` and compiled scalar closures.
- Keep language semantics independent of `clinker-plan` and runtime execution crates.

## Important public APIs

- `parser::Parser::{parse, parse_module}`, `ParseResult`, and `ModuleParseResult`.
- `lexer::{Lexer, Span, Token, MAX_SOURCE_LEN}`.
- `ast::{Program, Statement, Expr, NodeId, EmitTarget, contains_emit, for_each_field_emit}`.
- `resolve::{resolve_program, resolve_program_with_modules, resolve_program_with_modules_and_vars, ResolvedProgram, ResolvedBinding, ScopedVarsRegistry}`.
- `typecheck::{type_check, type_check_with_mode, AggregateMode, TypedProgram, Type, Row, QualifiedField, RowTail, TailVarId}`.
- `analyzer::{analyze_transform, analyze_all, ParallelismHint}` and `analyzer::doc_paths::collect_doc_paths`.
- `plan::{extract_aggregates, CompiledAggregate, BindingArg, AggregateBinding, CompiledEmit}`.
- `eval::{ProgramEvaluator, EvalResult, EvalContext, StableEvalContext, EvalError, compile_scalar, CompiledScalar}`.
- `module_eval::{toposort_constants, check_recursive_calls}`.
- `builtins::BuiltinRegistry`.

## Internal module map

- `lexer.rs`: tokenization, spans, and source length limits.
- `parser.rs`: hand-rolled Pratt parser, module parsing, AST `node_count`, and recursion limits.
- `ast.rs`: CXL AST, `NodeId`, and recursive emit walkers.
- `resolve/`: name resolution, scoped vars, typo suggestions, and resolver test doubles.
- `typecheck/`: type lattice, row model, aggregate-mode checks, and typed side tables.
- `analyzer/`: typed-AST visitor, window analysis, and `$doc` path analysis.
- `plan/`: aggregate extraction IR and AST rewrite pass.
- `eval/`: eval context, errors, builtins, compile-once evaluator, and compiled scalar path.
- `module_eval.rs`: module constant dependency ordering and recursion checks.
- `builtins.rs`: builtin method/window registry.

## Dependency rules

### Allowed dependencies

Existing normal dependencies are intentional evidence: `clinker-record`,
`miette`, `chrono`, `regex`, `indexmap`, `ahash`, `tracing`,
`static_assertions`, and `serde`.

Existing dev/bench dependencies are also expected: `criterion`,
`clinker-bench-support`, `proptest`, and `tracing-subscriber`.

### Forbidden or suspicious dependencies

- Do not add `clinker-plan`, `clinker-exec`, `clinker-format`, `clinker`, `clinker-channel`, `clinker-net`, or `clinker-schema` dependencies without architecture review.
- Do not add async runtimes, network clients, native TLS/OpenSSL, C build requirements, or benchmark helpers to normal dependencies without approval.
- Be suspicious of deriving serde for `Expr` or aggregate plan artifacts; `plan/mod.rs` says `CompiledAggregate` is not serde-derived and spill stores runtime state, not plan AST.
- Do not move YAML, pipeline config, or runtime operator concepts into this crate.

## Important invariants

- Preserve the phase order: parse -> resolve -> typecheck -> analysis/aggregate extraction -> evaluation.
- `NodeId` and `node_count` must stay coherent because resolver/typechecker/evaluator side tables index by `NodeId`.
- Parser limits such as `MAX_SOURCE_LEN`, expression depth, and `emit each` nesting depth are stack-safety and input-budget guards.
- `ResolvedProgram` and `TypedProgram` are distinct boundary types; unresolved ASTs should not reach eval paths.
- `Row.declared` is private; use row lookup/accessor methods.
- Do not compare `TailVarId`s across different bind-schema runs.
- Scoped variable types mirror config-side definitions locally so `cxl` does not depend on `clinker-plan`.
- Aggregate mode is different from row-transform mode; keep `AggCall`, group-by field, `distinct`, and fan-out restrictions aligned across typecheck and extraction.
- `ProgramEvaluator` is compile-once and stateful per evaluator instance; distinct state, partition reset, and `max_expansion` live outside immutable compiled programs.
- Hypothesis: new AST variants should be reviewed across parser, resolver, typechecker, analyzer visitor, aggregate extractor, evaluator, docs, and tests in one change.

## Common mistakes for AI agents to avoid

- Editing syntax in the parser without updating resolver, typechecker, evaluator, and tests.
- Adding `Expr` or `Statement` variants without updating `analyzer::visitor`.
- Breaking recursive emit walkers; `emit each` bodies must stay visible to output-field discovery.
- Treating CXL as SQL or adding SQL-style syntax casually.
- Making module files impure; current module parsing accepts only `fn` and `let` declarations.
- Adding plan/runtime config concepts directly to `cxl`.
- Recompiling regexes or re-walking ASTs in hot eval paths when side tables or compiled closures already exist.
- Letting `clinker-bench-support` leak into normal dependencies.

## Local commands

- **Inferred:** `cargo check -p cxl --locked --offline`
- **Inferred:** `cargo test -p cxl --locked --offline`
- **Inferred:** `cargo clippy -p cxl --all-targets --locked --offline -- -D warnings`
- **Inferred:** `cargo check --benches -p cxl --locked --offline`
- **Inferred, performance-sensitive only:** `cargo bench -p cxl --bench parse`
- **Inferred, performance-sensitive only:** `cargo bench -p cxl --bench eval`

For Rust source changes, also run the workspace gates from the root
`AGENTS.md` unless the user explicitly scopes validation narrower.

## Documentation updates

Update relevant docs/examples when this crate changes language syntax,
semantics, diagnostics, CXL CLI behavior, aggregate extraction, or evaluator
behavior:

- `docs/user/src/cxl/*.md`
- `docs/user/src/nodes/transform.md`
- `docs/user/src/nodes/aggregate.md`
- `docs/engine/src/cxl-internals.md`
- `docs/user/src/cxl/cxl-cli.md`
- Relevant `examples/pipelines/*.yaml` and `benches/pipelines/cxl_ops/*.yaml`
- `docs/ai/*.md`, especially crate map, design rules, common patterns, testing, and open questions

## Unclear / ask human

- `docs/ai/80_OPEN_QUESTIONS.md` still asks for the intended expansion of "CXL"; do not make acronym wording canonical from secondary docs alone.
- Hypothesis: `resolve::HashMapResolver` remains public mainly for tests and compatibility after moving the implementation to `clinker-record`.
- Hypothesis: `cxl` should continue to own `typecheck::Row` because `typecheck/row.rs` says this preserves the `clinker-plan -> cxl` dependency direction.

## Evidence

- `crates/cxl/Cargo.toml`
- `crates/cxl/src/lib.rs`
- `crates/cxl/src/lexer.rs`
- `crates/cxl/src/parser.rs`
- `crates/cxl/src/ast.rs`
- `crates/cxl/src/resolve/mod.rs`
- `crates/cxl/src/resolve/scoped_vars.rs`
- `crates/cxl/src/typecheck/mod.rs`
- `crates/cxl/src/typecheck/row.rs`
- `crates/cxl/src/analyzer/mod.rs`
- `crates/cxl/src/plan/mod.rs`
- `crates/cxl/src/plan/extract_aggregates.rs`
- `crates/cxl/src/eval/mod.rs`
- `crates/cxl/src/eval/compiled.rs`
- `crates/cxl/src/eval/tests.rs`
- `crates/cxl/benches/parse.rs`
- `crates/cxl/benches/eval.rs`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
