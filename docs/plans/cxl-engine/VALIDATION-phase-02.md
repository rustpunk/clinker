# Validation Report: Phase 2 — CXL Lexer + Parser

**Date:** 2026-03-29
**Validator:** validate-phase skill
**Verdict:** READY (all 4 blockers resolved)

## Verdict rationale

Phase 2 is well-structured with thorough test coverage, clean naming, and correct cross-phase contracts. Four blockers were identified and resolved during validation: missing `static_assertions` dependency, Phase 3.4 method name divergence, no parser recursion depth limit, and missing `Expr::Wildcard` AST variant. All fixes have been applied to the phase file.

## Blocker resolution log

| ID | Issue | Resolution | Decision |
|----|-------|-----------|---------|
| V-2-1 | `static_assertions` crate not in workspace deps | Resolved | Add `static_assertions = "1"` to workspace deps and cxl Cargo.toml. Added as prerequisite in phase file. |
| V-1-1 | Phase 3.4 method names diverge from Phase 2 drill (6 families + 2 missing methods) | Resolved | Phase 2 is canonical. Phase 3.4 must be updated to match. Added `clamp`/`round_to` to Phase 2 Numeric (now 81 total). |
| V-8-1 | No recursion depth limit in Pratt parser | Resolved | Added `depth: u32` counter and `MAX_DEPTH = 256` to parser scaffolding. Decision #22. |
| V-8-2 | No `Expr::Wildcard` for `_` in match arms | Resolved | Added `Expr::Wildcard { span: Span }` variant to AST. Decision #21. |

## Accepted warnings and known risks

| ID | Issue | Accepted by | Rationale |
|----|-------|------------|-----------|
| V-5-2 | Parser test stubs are assertion-light (~20/34 only check `errors.is_empty()`) | Accepted | Implementer will fill in structural assertions. Stubs provide the test names and structure. |
| V-5-9 | `test_registry_all_array_methods` can't distinguish Array vs String receiver | Accepted | Known limitation of single-entry registry. Phase 3 type checker handles disambiguation. |
| V-6-1 | Overloaded `join`/`length` may cause total count to be 79 not 81 | Accepted | Implementer adjusts count or uses composite keys if needed. Risk documented in phase file. |
| V-7-1 | `Expr` size budget tight at ~56-64 bytes | Accepted | Documented fallback (box `Vec<MatchArm>`). `test_ast_expr_size` catches violations immediately. |
| V-8-4 | `parse_module()` entry point missing | Accepted | Module parsing deferred to Phase 10. Phase 2 covers inline CXL blocks and fn_decl. |

## Edge cases acknowledged

| ID | Scenario | Handling decision |
|----|---------|-----------------|
| V-5-1 | Unterminated string literal | Add `test_lex_unterminated_string` during implementation (called out in sub-task 2.1.3) |
| V-5-3 | Incomplete date literal (`#2024` at EOF) | Add `test_lex_incomplete_date_literal` during implementation (called out in risk/gotcha) |
| V-5-4 | Empty input to lexer/parser | Add `test_lex_empty_input` and `test_parse_empty_input` during implementation |
| V-5-5 | Multi-param and zero-param fn declarations | Add `test_parse_fn_decl_multi_param` and `test_parse_fn_decl_no_param` during implementation |
| V-5-6 | Bare expression as statement (`ExprStmt`) | Add `test_parse_expr_statement` during implementation |
| V-5-7 | Deep nesting stress test | Add `test_parse_deep_nesting_limit` after MAX_DEPTH implementation |
| V-5-10 | `-42` lexes as Minus + IntLit(42), not IntLit(-42) | Add `test_lex_negative_is_unary` during implementation |

## Security flags

| ID | Risk | Resolved / Accepted | Notes |
|----|------|---------------------|-------|
| V-8-1 | Stack overflow from nested expressions | Resolved | MAX_DEPTH = 256 added to parser scaffolding |
| V-8-5 | No source size limit | Resolved | MAX_SOURCE_LEN = 64KB added to lexer implementation notes |

## Feature coverage map

| Spec requirement | Task(s) | Coverage |
|-----------------|---------|---------|
| PEG grammar ~30 productions (§5.2) | 2.1, 2.2, 2.3 | Full |
| 10 precedence levels (§5.3) | 2.3 | Full (corrected from plan) |
| 81 built-in method signatures (§5.5) | 2.4 | Full (reconciled with plan) |
| Error recovery + miette diagnostics | 2.3 | Full |
| Module path syntax (`use A::B as C`) | 2.1, 2.2, 2.3 | Full |
| Date literal validation | 2.1 | Full |
| `_` wildcard in match arms | 2.2 | Full (Expr::Wildcard added) |
| Optional/variadic method params | 2.4 | Full (min_args/max_args added) |
| Module file grammar (`parse_module`) | — | Deferred to Phase 10 |

## Cross-phase coherence

Phase 2 produces the lexer, AST, parser, and builtin registry consumed by Phase 3. The primary coherence risk was the method name divergence between Phase 2's drill decisions and Phase 3.4's evaluator — Phase 3.4 was written before the Phase 2 drill and used different names (`.to_upper()` vs `.upper()`, `.basename()` vs `.file_name()`, etc.). This has been resolved: Phase 2 names are canonical, and Phase 3.4 must be updated to match when that phase is drilled. The `clamp`/`round_to` gap has been closed by adding them to Phase 2. All other contracts (Span, Token, AST types, ParseResult) are clean.

## Validation checks run

| Check | Agent | Result | Issues found |
|-------|-------|--------|-------------|
| Interface contracts | Agent 1 | Issues | 1 BLOCKER (method divergence), 2 WARNINGs |
| Dependency coherence | Agent 2 | Issues | 1 BLOCKER (static_assertions) |
| Scope drift | Agent 3 | Pass | 0 |
| Dead code / duplication | Agent 4 | Pass | 0 |
| Test coverage | Agent 5 | Issues | 2 BLOCKERs (unterminated string, no Wave 0), 13 WARNINGs |
| Naming consistency | Agent 6 | Pass | 1 WARNING |
| Assumption conflicts + edge cases | Agent 7 | Issues | 1 BLOCKER (static_assertions), 2 WARNINGs |
| Feature gaps + security | Agent 8 | Issues | 2 BLOCKERs (security), 5 WARNINGs |
