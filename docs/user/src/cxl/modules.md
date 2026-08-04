# Modules and `use`

CXL modules organize reusable constants and pure, single-expression functions.
Module files use the `.cxl` extension and are admitted while Clinker plans the
pipeline. Execution uses the admitted declarations stored in the compiled plan;
it does not read module files again.

## Module files

```cxl
# rules/shared/finance.cxl
let tax_rate = 0.21
let default_currency = "USD"

fn tax(amount) = amount * tax_rate
fn normalize_currency(value) = value.trim().upper()
```

A module may contain:

- `let` constants whose expressions depend only on other constants and pure
  CXL operations;
- `fn` declarations with named parameters and one expression body; and
- `use` declarations for other modules.

Functions cannot contain statements such as `emit`, `filter`, or `distinct`.
Recursive function calls and cyclic module imports are rejected during
planning.

## Where `use` is recognized

Planning resolves module imports from every field that carries executable CXL,
including:

- a Transform's primary expression, validation checks, and per-record log
  conditions;
- an Aggregate's expression;
- every Route condition;
- a Combine predicate and body;
- Envelope header and footer expressions;
- Reshape rule conditions, mutations, and synthesized overrides;
- Cull group-drop conditions; and
- the same fields inside reachable composition bodies.

Ordinary strings do not participate in module resolution. Node names,
validation and log messages, output paths, and other descriptive text cannot
introduce an import merely by containing text that resembles `use`.

## Importing and using a module

Module identities and member access both use dot notation:

```cxl
use shared.finance as finance

emit tax = finance.tax(amount)
emit currency = finance.default_currency
```

The alias is optional. Without `as`, the last identity segment is the alias:

```cxl
use shared.finance
emit tax = finance.tax(amount)
```

There is no `::` member syntax and no wildcard import. A missing member, calling
a constant, or reading a function without parentheses is a planning error with
the offending module and member named in the diagnostic.

## Direct imports and private dependencies

Pipeline CXL can access only modules it imports directly. A module may import
another module by its absolute logical identity:

```cxl
# rules/app/invoice.cxl
use shared.finance as finance

let standard_rate = finance.tax_rate
fn invoice_tax(amount) = finance.tax(amount)
```

```cxl
# pipeline transform
use app.invoice as invoice
emit tax = invoice.invoice_tax(amount)
```

`shared.finance` is included in the admitted transitive closure, but it is
private to `app.invoice`. The pipeline must add its own `use shared.finance` if
it needs to address that module directly. Dependencies are never re-exported.

## Rules-root selection

Clinker selects exactly one rules root for non-catalog module identities. The
precedence is:

1. explicit `clinker run --rules-path <DIR>`;
2. `pipeline.rules_path` in the pipeline YAML;
3. `[catalog].rules_root` in `clinker.toml`; then
4. the workspace-relative `rules/` default.

There is no search path and no first-match shadowing. Every relative candidate
is anchored to the selected workspace, not the process working directory or
the pipeline file's directory. See the [CLI reference](../ops/cli-reference.md#clinker-run)
and [typed workspace catalog](../pipelines/channels.md#typed-workspace-catalog).

An explicit `[catalog.rules]` entry maps a logical rule identity to a particular
workspace-contained file and takes priority over the derived
`<rules-root>/<identity segments>.cxl` path for that identity.

## Planning bounds and diagnostics

Planning loads only the direct imports and their transitive dependencies. Each
canonical module is parsed once. The default closure limits are:

| Limit | Default |
| --- | ---: |
| One module file | 1 MiB |
| Unique modules | 64 |
| Import depth | 32 |
| Total closure source | 16 MiB |

Planning fails before execution for a missing or unreadable module, invalid
UTF-8 or CXL, duplicate declarations or aliases, an import/function cycle, or a
closure that exceeds a bound. Cycle diagnostics show the complete discovered
chain so the import edge to remove is visible.

After loading the complete reachable closure, planning validates both
declaration graphs:

- constant dependencies must be acyclic; and
- function calls must be acyclic, including direct, mutual, and cross-module
  recursion.

Cycle diagnostics report the complete chain with the relevant call or
declaration locations. Imported calls are also checked at the authored call
site. The diagnostic names the logical module and member when the member is not
a function, the argument count is wrong, or the expanded function body is
ill-typed. For example, if `shared.numbers.add` takes two arguments, the
corrected call is:

```cxl
use shared.numbers as numbers
emit total = numbers.add(left, right)
```

## Source-file lifetime

Module files are an input to planning, not a runtime dependency. Once planning
succeeds, the compiled plan owns the immutable parsed declarations for every
admitted direct and transitive module. The same plan can execute repeatedly if
those source files are renamed, changed, or removed after planning. Changes take
effect only after compiling a new plan.

Removing or changing a required file before planning still fails admission.
This boundary prevents a checked plan from silently executing different module
code and keeps execution independent of filesystem path authority.

## Complete example

```cxl
# rules/etl/clean.cxl
let max_amount = 999999.99

fn normalize_name(name) = name.trim().upper()
fn safe_amount(raw) = raw.try_float() ?? 0.0
fn flag_suspicious(amount, threshold) =
  if amount > threshold then "review" else "ok"
```

```cxl
# pipeline CXL
use etl.clean as clean

emit customer = clean.normalize_name(raw_customer)
emit amount = clean.safe_amount(raw_amount)
filter amount <= clean.max_amount
emit review_flag = clean.flag_suspicious(amount, 10000)
```
