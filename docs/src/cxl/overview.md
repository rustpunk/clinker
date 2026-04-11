# CXL Overview

CXL (Clinker Expression Language) is a per-record expression language designed for ETL transformations. Every CXL program operates on one record at a time, producing output fields, filtering records, or computing derived values.

**CXL is not SQL.** There are no `SELECT`, `FROM`, or `WHERE` keywords. CXL programs are sequences of statements -- `emit`, `let`, `filter`, `distinct` -- that execute top to bottom against the current record.

## Key differences from SQL

| SQL | CXL |
|-----|-----|
| `SELECT col AS alias` | `emit alias = col` |
| `WHERE condition` | `filter condition` |
| `AND` / `OR` / `NOT` | `and` / `or` / `not` (keywords) |
| `&&` / `\|\|` / `!` | Not supported -- use keywords |
| `COALESCE(a, b)` | `a ?? b` |
| `CASE WHEN ... THEN ... END` | `if ... then ... else ...` or `match { }` |

## Boolean operators are keywords

CXL uses English keywords for boolean logic, not symbols:

```bash
$ cxl eval -e 'emit result = true and false' --field dummy=1
```

```json
{
  "result": false
}
```

The operators `&&`, `||`, and `!` are **syntax errors** in CXL. Always use `and`, `or`, and `not`.

## System namespaces use `$` prefix

CXL provides built-in namespaces for accessing pipeline state, metadata, and window functions. All system namespaces are prefixed with `$`:

- `$pipeline.*` -- pipeline execution context (name, counters, provenance)
- `$meta.*` -- per-record metadata
- `$window.*` -- window function calls
- `$vars.*` -- user-defined pipeline variables

```bash
$ cxl eval -e 'emit name = $pipeline.name'
```

```json
{
  "name": "cxl-eval"
}
```

## Compile-time type checking

CXL catches type errors before data processing begins. The compilation pipeline runs four phases:

1. **Parse** -- tokenize and build an AST from CXL source text
2. **Resolve** -- bind field references, validate method names, check arity
3. **Typecheck** -- infer types, validate operator compatibility, check method receiver types
4. **Eval** -- execute the typed program against each record

Errors at any phase produce rich diagnostics with source locations and fix suggestions via `miette`.

```bash
$ cxl check transform.cxl
ok: transform.cxl is valid
```

If there are type errors, the checker reports them with spans:

```
error[typecheck]: cannot apply '+' to String and Int (at transform.cxl:12)
  help: convert one operand — use .to_int() or .to_string()
```

## A minimal CXL program

```
emit greeting = "hello"
emit doubled = amount * 2
filter amount > 0
```

This program:
1. Emits a constant string field `greeting`
2. Emits `doubled` as twice the input `amount`
3. Filters out records where `amount` is not positive

Try it:

```bash
$ cxl eval -e 'emit greeting = "hello"' -e 'emit doubled = amount * 2' \
    --field amount=5
```

```json
{
  "greeting": "hello",
  "doubled": 10
}
```

## Statement order matters

CXL statements execute sequentially. Later statements can reference fields produced by earlier `emit` or `let` statements:

```bash
$ cxl eval -e 'let tax_rate = 0.21' -e 'emit tax = price * tax_rate' \
    --field price=100
```

```json
{
  "tax": 21.0
}
```

A `filter` statement short-circuits execution -- if the condition is false, remaining statements do not run and the record is excluded from output.
