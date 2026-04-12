# Modules & use

CXL supports a module system for organizing reusable expressions. Modules contain function declarations and constant bindings that can be imported into CXL programs.

## Module files

A module is a `.cxl` file containing `fn` declarations and `let` constants. Module files live in the rules path (default: `./rules/`).

### Function declarations

Functions are pure, single-expression bodies with named parameters:

```
fn fiscal_year(d) = if d.month() < 4 then d.year() - 1 else d.year()

fn full_name(first, last) = first.trim() + " " + last.trim()

fn clamp_pct(value) = value.clamp(0, 100).round_to(1)
```

Functions are pure -- they have no side effects and always return a value.

### Module constants

Constants are `let` bindings at the module level:

```
let tax_rate = 0.21
let max_retries = 3
let default_currency = "USD"
```

### Example module file

File: `rules/shared/dates.cxl`

```
fn fiscal_year(d) = if d.month() < 4 then d.year() - 1 else d.year()

fn quarter(d) = match {
  d.month() <= 3  => 1,
  d.month() <= 6  => 2,
  d.month() <= 9  => 3,
  _               => 4
}

fn fiscal_quarter(d) = quarter(d.add_months(-3))

let fiscal_start_month = 4
```

## Importing modules

Use the `use` statement to import a module. Module paths use **dot notation** (not `::`):

```
use shared.dates as d
```

This imports the module at `rules/shared/dates.cxl` and binds it to the alias `d`.

### Import syntax

```
use module.path
use module.path as alias
```

The `as alias` clause is optional. When omitted, the last segment of the path becomes the default name.

```
use shared.dates          # access as dates::fiscal_year(...)
use shared.dates as d     # access as d::fiscal_year(...)
```

### Path resolution

Module paths are resolved relative to the rules path:

| Import | File path |
|--------|-----------|
| `use shared.dates` | `rules/shared/dates.cxl` |
| `use transforms.normalize` | `rules/transforms/normalize.cxl` |
| `use utils` | `rules/utils.cxl` |

The rules path defaults to `./rules/` and can be overridden with `--rules-path`.

## Using imported functions and constants

After importing, reference module members with `::` (double colon) syntax:

```
use shared.dates as d
use shared.finance as f

emit fiscal_year = d::fiscal_year(invoice_date)
emit quarter = d::quarter(invoice_date)
emit tax = amount * f::tax_rate
emit net = amount - tax
```

### Functions

Call functions with `alias::function_name(args)`:

```
use shared.dates as d
emit fy = d::fiscal_year(order_date)
```

### Constants

Access constants with `alias::constant_name`:

```
use shared.finance as f
emit tax = amount * f::tax_rate
```

## Restrictions

- **No wildcard imports.** `use shared.*` is not supported. Import modules explicitly.
- **Dot separator only.** Module paths use `.`, not `::`. The `::` syntax is reserved for member access after import.
- **Single expression bodies.** Functions must be a single expression -- no multi-statement bodies.
- **Pure functions.** Functions cannot use `emit`, `filter`, `distinct`, or other statement forms. They are pure computations.
- **No recursion.** Functions cannot call themselves (directly or indirectly).

## Complete example

File: `rules/etl/clean.cxl`

```
fn normalize_name(name) = name.trim().upper()

fn safe_amount(raw) = raw.try_float() ?? 0.0

fn flag_suspicious(amount, threshold) =
  if amount > threshold then "review" else "ok"

let max_amount = 999999.99
```

Pipeline CXL block:

```
use etl.clean as c

emit customer = c::normalize_name(raw_customer)
emit amount = c::safe_amount(raw_amount)
filter amount <= c::max_amount
emit review_flag = c::flag_suspicious(amount, 10000)
```
