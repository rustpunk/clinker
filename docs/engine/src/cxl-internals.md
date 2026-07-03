# Compiler Phases & Type Unification

*User-facing view: the User Guide's "CXL Overview" and "Types & Literals" pages.*

This page is the engine-internals reference for how a CXL program is compiled before any record flows: the four-phase pipeline that turns source text into a typed, evaluable program, and the formal type-unification algorithm the typechecker runs when two types meet in an expression. CXL is a per-record ETL expression language — not SQL — so every program operates on one record at a time, and every type error is caught at compile time rather than surfacing mid-run. The depth here is the phase boundaries (Parse → Resolve → Typecheck → Eval), what each phase consumes and produces, the `miette` diagnostic surface, and the multi-point unification rules over CXL's 9 value types. The user-facing pages document the surface syntax and the literal grammar; this page documents the compiler that stands behind them.

## The four-phase compilation pipeline

CXL catches type errors before data processing begins. The compilation pipeline runs four ordered phases, each consuming the previous phase's output and producing the input to the next. A failure at any phase produces a rich diagnostic with source locations and fix suggestions via `miette`, and short-circuits the remaining phases — a parse error never reaches the typechecker, a type error never reaches eval.

1. **Parse** — tokenize and build an AST from CXL source text. The lexer turns raw source into a token stream; the parser assembles those tokens into an abstract syntax tree of statements (`emit`, `let`, `filter`, `distinct`) and the expressions inside them. This is the phase that rejects the symbolic boolean operators: `&&`, `||`, and `!` are syntax errors in CXL — the language uses the `and` / `or` / `not` keywords — and that rejection happens here, at parse time, before any name or type is known.

2. **Resolve** — bind field references, validate method names, and check arity. With the AST in hand, the resolver binds each field reference to a column in the input schema, confirms every method call names a real method, and checks that each call site supplies the right number of arguments. Name and arity errors are structural — they do not depend on types — so they are settled here, ahead of type inference, which lets the typechecker assume every reference resolves and every call is well-formed.

3. **Typecheck** — infer types, validate operator compatibility, and check method receiver types. The typechecker walks the resolved tree, infers a type for every expression, and applies the [unification rules](#type-unification-rules) below at each point two types meet (a binary operator, a method receiver, a conditional's branches). It rejects incompatible combinations — applying `+` to a `String` and an `Int`, for instance — and emits a span-annotated diagnostic that names both operand types and suggests a coercion. The output of this phase is a `TypedProgram`: the AST annotated with the inferred type of every node, ready to evaluate without further inference.

4. **Eval** — execute the typed program against each record. With types resolved, evaluation is a straightforward per-record walk of the `TypedProgram`. Statements execute top to bottom against the current record; later statements can reference fields produced by earlier `emit` or `let` statements, and a `filter` whose condition is false short-circuits the remaining statements and excludes the record from output. No type inference happens at this phase — every dispatch decision was settled in Typecheck — so eval is the hot per-record path and carries none of the compile-time machinery.

The phase split is what makes CXL's compile-time guarantee meaningful: a `cxl check transform.cxl` runs Parse → Resolve → Typecheck and reports any error with a span before a single record is read, e.g.

```text
error[typecheck]: cannot apply '+' to String and Int (at transform.cxl:12)
  help: convert one operand — use .to_int() or .to_string()
```

Because the typecheck phase produces a fully-typed program, an error here is a guarantee the corresponding runtime failure cannot occur — the class of error is eliminated before Eval, not merely detected earlier.

## The type lattice

CXL has 10 value types, and unification operates over them plus two compile-time-only constructs (`Numeric` and `Any`) and the `Nullable(T)` wrapper. The concrete value types and their Rust backings:

| Type | Rust backing | Description |
|------|-------------|-------------|
| Null | `Value::Null` | Missing or absent value |
| Bool | `bool` | `true` or `false` |
| Integer | `i64` | 64-bit signed integer |
| Float | `f64` | 64-bit double-precision float |
| Decimal | `rust_decimal::Decimal` | Exact base-10 fixed-point number (16 bytes) for monetary/financial data |
| String | `Box<str>` | UTF-8 text |
| Date | `NaiveDate` | Calendar date without timezone |
| DateTime | `NaiveDateTime` | Date and time without timezone |
| Array | `Vec<Value>` | Ordered collection of values |
| Map | `IndexMap<Box<str>, Value>` | Key-value pairs |

Two further type-level constructs appear only at compile time, never as a runtime `Value`:

- **`Numeric`** — a union accepting either `Int` or `Float`. It is the declared schema type for a column that may carry either; unification resolves it to a concrete numeric type when it meets one.
- **`Any`** — an unconstrained type with no type constraints, the declared type for a column whose type is unknown. It unifies away to whatever it meets.

And the `Nullable(T)` wrapper marks a type whose value may be `null`. Nullability is tracked through unification rather than discarded, so a nullable operand propagates its nullability into the result.

## Type unification rules

When two types meet in an expression — the two operands of a binary operator, the receiver and a method's expected type, the branches of a conditional — the typechecker unifies them to a single result type. The algorithm is a small, ordered set of rules; each is tried against the pair of types until one applies:

1. **Identity.** Same types unify to themselves: `Int + Int` produces `Int`. This is the base case — when both sides already agree, the result is that shared type.

2. **`Any` absorbs.** `Any` unifies with anything: `Any + T` produces `T`. An `Any` operand imposes no constraint, so the result takes the *other* operand's type. (When both are `Any`, identity covers it.)

3. **`Numeric` resolves to the concrete type.** `Numeric + Int` produces `Int`; `Numeric + Float` produces `Float`. The `Numeric` union collapses to whichever concrete numeric type it meets, rather than staying an unresolved union in the result.

4. **`Int` promotes to `Float`.** `Int + Float` produces `Float`. When the two concrete numeric types differ, the result is the wider one — integer arithmetic against a float yields a float, matching the runtime promotion the evaluator performs.

4a. **`Int` widens into `Decimal`, but `Float` does not.** `Decimal + Int` produces `Decimal` — an integer literal or column joins exact decimal arithmetic without loss, so `amount + 1` typechecks as `Decimal`. `Decimal` deliberately does **not** unify with `Float` or `Numeric` (which admits `Float`): mixing an exact base-10 value with a binary float is a hard type error that requires an explicit cast (`.to_decimal()` to stay exact, `.to_float()` to opt into binary precision). This is what preserves the decimal type's exactness guarantee — a lossy float can never silently contaminate a decimal computation. The same rule governs comparisons: `decimal > float` is rejected, `decimal > int` is fine.

5. **`Null` wraps.** `Null + T` produces `Nullable(T)`. Any operation involving the `Null` type produces a nullable result: meeting `Null` cannot guarantee a non-null outcome, so the result type carries the `Nullable` marker. (Runtime behavior matches — e.g. `null + 5` evaluates to `null` — and the type reflects that the result may be absent.)

6. **`Nullable` propagates.** `Nullable(A) + B` produces `Nullable(unified(A, B))`. When a nullable type meets any other type, unification recurses on the inner type `A` against `B`, then re-wraps the result in `Nullable`. Nullability is sticky: it survives the unification and re-wraps whatever the inner types unify to, so a nullable operand anywhere in an expression makes the whole result nullable.

7. **Incompatible types fail.** When no rule above applies — `String + Int`, for instance — unification fails and the typecheck phase emits a span-annotated type error naming both operand types and suggesting a coercion.

The ordering matters: `Any` and `Numeric` are resolved before the promotion and nullability rules, so by the time rules 4–6 run, both sides are concrete (or nullable-wrapped concrete) types. Rule 6's recursion is the only point the algorithm re-enters itself, and it always recurses on strictly-inner types, so unification terminates.

These rules are what let the typechecker hand Eval a fully-resolved `TypedProgram`: every binary operator, method receiver, and conditional has a single inferred result type, computed once at compile time, so the per-record evaluator never re-derives a type or discovers a mismatch mid-run.
