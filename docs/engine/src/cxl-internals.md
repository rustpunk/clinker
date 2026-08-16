# Compiler Phases & Type Unification

*User-facing view: the User Guide's "CXL Overview" and "Types & Literals" pages.*

This page is the engine-internals reference for how Clinker Expression
Language (CXL) source becomes typed planner artifacts and, later, evaluated
records. The shared front end parses, resolves, and typechecks a program.
Planner consumers may then analyze the typed tree and extract or lower
aggregate behavior before runtime evaluation. CXL is a per-record ETL
expression language — not SQL — so type errors are reported before records
flow. This page covers those boundaries, the `miette` diagnostic surface, and
unification over CXL's ten runtime value types.

`clinker-plan` is the sole execution-admission authority. It supplies the
bound row schema, compiles CXL, and decides whether the resulting pipeline may
run. The CXL crate owns expression semantics but does not parse pipeline YAML
or schedule operators. `clinker-schema` may report advisory findings from
bounded discovery and heuristic field extraction; those warnings do not
replace canonical planner parsing or admit a rejected pipeline (D-17).

One lower-layer dependency is intentionally narrow: `clinker-format` may use
only CXL's logical `Type` and document `DocPath`/`DocIndex` vocabulary. D-20
does not permit it to depend on the parser, resolver, evaluator, planner, or
other analyzers. A neutral lower-vocabulary extraction may replace this edge
later, but no broader dependency is approved now.

## Compilation and evaluation pipeline

CXL catches type errors before data processing begins. Its front-end phases are
ordered, and a failure short-circuits the remaining work: a parse error never
reaches the resolver, and a type error never reaches planner analysis or
runtime evaluation.

1. **Parse** — tokenize and build an AST from CXL source text. The lexer turns raw source into a token stream; the parser assembles those tokens into an abstract syntax tree of statements (`emit`, `let`, `filter`, `distinct`) and the expressions inside them. This is the phase that rejects the symbolic boolean operators: `&&`, `||`, and `!` are syntax errors in CXL — the language uses the `and` / `or` / `not` keywords — and that rejection happens here, at parse time, before any name or type is known.

2. **Resolve** — bind field references, validate method names, and check arity. With the AST in hand, the resolver binds each field reference to a column in the input schema, confirms every method call names a real method, and checks that each call site supplies the right number of arguments. Name and arity errors are structural — they do not depend on types — so they are settled here, ahead of type inference, which lets the typechecker assume every reference resolves and every call is well-formed.

3. **Typecheck** — infer types, validate operator compatibility, and check method receiver types. The typechecker walks the resolved tree, infers a type for every expression, and applies the [unification rules](#type-unification-rules) below at each point two types meet (a binary operator, a method receiver, a conditional's branches). It rejects incompatible combinations — applying `+` to a `String` and an `Int`, for instance — and emits a span-annotated diagnostic that names both operand types and suggests a coercion. The output of this phase is a `TypedProgram`: the AST annotated with the inferred type of every node, ready to evaluate without further inference.

4. **Analyze and extract** — planner consumers inspect the `TypedProgram` for
   execution properties and, where the node kind requires it, extract compiled
   aggregates or other lowered artifacts. This is not one universal AST rewrite:
   individual planning paths invoke the analyses they need.

**Runtime evaluation** then executes the typed or extracted artifact against
records. Statements execute top to bottom; later statements can reference
fields produced by earlier `emit` or `let` statements, and a false `filter`
excludes the record. Evaluation performs no type inference.

Array literals, map literals, and array comprehensions are ordinary expression
nodes throughout this pipeline, including inside aggregate residuals. Every AST
walker must recurse into item/value expressions, computed map keys,
comprehension sources, and predicates; otherwise schema binding, dependency
analysis, semantic identity, or lineage can silently miss an input. Runtime
construction preserves author order, rejects duplicate logical keys after
canonical escape decoding, and shares a per-record 10 MiB allocation budget and
64-container depth cap across nested constructors. The aggregate residual
evaluator enforces the same rules.

The phase split is what makes CXL's compile-time guarantee meaningful: a `cxl check transform.cxl` runs Parse → Resolve → Typecheck and reports any error with a span before a single record is read, e.g.

```text
error[typecheck]: cannot apply '+' to String and Int (at transform.cxl:12)
  help: convert one operand — use .to_int() or .to_string()
```

Because the typecheck phase produces a fully typed program, that class of type
mismatch is eliminated before evaluation rather than merely detected earlier.

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

- **`Numeric`** — an inference-only union accepting either `Int` or `Float`.
  Unification resolves it when enough context supplies a concrete numeric type.
  It may not survive into a compiled source schema: an unresolved authored
  `type: numeric` is rejected with `E158`, so source authors must declare
  `int` or `float`.
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

These rules let the typechecker hand later planner phases a resolved
`TypedProgram`: every binary operator, method receiver, and conditional has a
single inferred result type, computed once before the per-record evaluator
runs.
