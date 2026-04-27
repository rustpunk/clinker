# Multi-Input Combine

This recipe enriches order records with product metadata from a separate catalog stream using a `combine` node. Combine is a first-class N-ary operator: every input is declared up front, and the `where` expression uses qualified field references (`orders.product_id`, `products.product_id`) to express the join.

## Input data

`orders.csv`:

```csv
order_id,product_id,quantity,unit_price
ORD-001,PROD-A,5,29.99
ORD-002,PROD-B,2,149.99
ORD-003,PROD-A,1,29.99
ORD-004,PROD-C,10,9.99
ORD-005,PROD-B,3,149.99
```

`products.csv`:

```csv
product_id,product_name,category
PROD-A,Widget Pro,Hardware
PROD-B,DataSync License,Software
PROD-C,Cable Kit,Hardware
```

## Pipeline

`order_enrichment.yaml`:

```yaml
pipeline:
  name: order_enrichment

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: "./orders.csv"
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: quantity, type: int }
        - { name: unit_price, type: float }

  - type: source
    name: products
    config:
      name: products
      type: csv
      path: "./products.csv"
      schema:
        - { name: product_id, type: string }
        - { name: product_name, type: string }
        - { name: category, type: string }

  - type: combine
    name: enrich
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: first
      on_miss: null_fields
      cxl: |
        emit order_id = orders.order_id
        emit product_id = orders.product_id
        emit product_name = products.product_name
        emit category = products.category
        emit quantity = orders.quantity
        emit unit_price = orders.unit_price
        emit line_total = orders.quantity.to_float() * orders.unit_price
      propagate_ck: driver

  - type: output
    name: result
    input: enrich
    config:
      name: enriched_orders
      type: csv
      path: "./output/enriched_orders.csv"
```

## Run it

```bash
clinker run order_enrichment.yaml --dry-run
clinker run order_enrichment.yaml --dry-run -n 3
clinker run order_enrichment.yaml
```

## Expected output

`output/enriched_orders.csv`:

```csv
order_id,product_id,product_name,category,quantity,unit_price,line_total
ORD-001,PROD-A,Widget Pro,Hardware,5,29.99,149.95
ORD-002,PROD-B,DataSync License,Software,2,149.99,299.98
ORD-003,PROD-A,Widget Pro,Hardware,1,29.99,29.99
ORD-004,PROD-C,Cable Kit,Hardware,10,9.99,99.90
ORD-005,PROD-B,DataSync License,Software,3,149.99,449.97
```

## How combine works

A combine node declares every input in its `input:` map, binding each upstream stream to a qualifier used inside expressions:

```yaml
- type: combine
  name: enrich
  input:
    orders: orders        # qualifier: upstream_node
    products: products
  config:
    where: "orders.product_id == products.product_id"
    propagate_ck: driver
```

The `config:` block carries four fields that shape behavior:

- **`where`** -- a CXL boolean expression. Every field reference must be qualified with its input name. The expression must contain at least one cross-input equality (e.g. `orders.product_id == products.product_id`); additional range or arbitrary conjuncts can be combined with `and`.
- **`match`** -- `first` (default), `all`, or `collect`. See below.
- **`on_miss`** -- `null_fields` (default), `skip`, or `error`. Applies only to records on the driving input that find no match.
- **`cxl`** -- emit statements that shape the output row. Under `match: collect`, this field must be empty; the combine node auto-derives the output schema.

## Match modes

### `match: first`

Emit one output row per driver record, using the first matching build-side record. This is the standard 1:1 enrichment. When no match exists, the behavior is governed by `on_miss`.

```yaml
config:
  where: "orders.product_id == products.product_id"
  match: first
```

### `match: all`

Emit one output row for every matching build-side record. This is 1:N fan-out -- if a driver record matches three build records, three rows are emitted.

```yaml
- type: combine
  name: expand_benefits
  input:
    employees: employees
    benefits: benefits
  config:
    where: "employees.department == benefits.department"
    match: all
    cxl: |
      emit employee_id = employees.employee_id
      emit benefit = benefits.benefit_name
    propagate_ck: driver
```

An employee in a department with three benefits produces three output records.

### `match: collect`

Gather every matching build-side record into a single Array-typed field on the output row. The driver record appears once; the build matches are aggregated into a list. The `cxl:` body must be empty under `match: collect` -- the combine node synthesizes the output as `{ driver fields..., <build_qualifier>: Array }`.

```yaml
- type: combine
  name: gather
  input:
    orders: orders
    products: products
  config:
    where: "orders.product_id == products.product_id"
    match: collect
    cxl: ""
    propagate_ck: driver
```

Use `collect` when you need the set of matches as a single structured value (e.g. every price history row for an order). Use `all` when you need one flat row per match.

## Unmatched records (`on_miss`)

`on_miss` controls what happens to driver records with zero matches:

```yaml
config:
  where: "orders.product_id == products.product_id"
  on_miss: null_fields   # default: emit with build fields set to null
```

```yaml
config:
  where: "orders.product_id == products.product_id"
  on_miss: skip          # inner-join semantics: drop unmatched drivers
```

```yaml
config:
  where: "orders.product_id == products.product_id"
  on_miss: error         # fail the pipeline on first unmatched driver
```

Use `skip` for inner-join semantics, `null_fields` for left-join semantics, and `error` for strict referential integrity where any miss should halt processing.

## Composite keys

Chain multiple equalities with `and` to combine on more than one field. Each conjunct is a separate cross-input equality:

```yaml
- type: combine
  name: match_by_region
  input:
    sales: sales
    targets: targets
  config:
    where: |
      sales.department == targets.department
      and sales.region == targets.region
    cxl: |
      emit department = sales.department
      emit region = sales.region
      emit actual = sales.amount
      emit goal = targets.goal
    propagate_ck: driver
```

Both equalities must hold for a record pair to match.

## Equi plus residual filter

The `where` clause can mix equi predicates with additional filter conjuncts. Non-equality conjuncts are applied as a residual filter after the equi match:

```yaml
- type: combine
  name: high_value_enrichment
  input:
    orders: orders
    products: products
  config:
    where: |
      orders.product_id == products.product_id
      and orders.amount >= 100
    match: first
    on_miss: skip
    cxl: |
      emit order_id = orders.order_id
      emit product_name = products.product_name
      emit amount = orders.amount
    propagate_ck: driver
```

The equi conjunct drives the hash lookup; the `amount >= 100` conjunct is evaluated as a post-filter. At least one cross-input equality is required in every combine.

## Multi-input combine (three or more)

Combine accepts any number of inputs. Each pair of inputs that should be related needs an explicit equality in the `where` clause:

```yaml
- type: combine
  name: fully_enriched
  input:
    orders: orders
    products: products
    categories: categories
  config:
    where: |
      orders.product_id == products.product_id
      and products.category_id == categories.category_id
    match: first
    on_miss: null_fields
    cxl: |
      emit order_id = orders.order_id
      emit product_name = products.product_name
      emit category_name = categories.name
      emit amount = orders.amount
    propagate_ck: driver
```

Input order in the `input:` map is preserved, and downstream reasoning treats the first input as the default driving side unless a `drive:` hint overrides it.

## Choosing the driving input

By default the planner picks a driving (probe) input and builds hash tables for the rest. Use `drive:` to force a specific input to be the driver -- typically the larger stream, or the one whose ordering you want to preserve:

```yaml
- type: combine
  name: product_driven
  input:
    orders: orders
    products: products
  config:
    where: "orders.product_id == products.product_id"
    match: first
    drive: products
    cxl: |
      emit product_id = products.product_id
      emit product_name = products.product_name
      emit sample_order_id = orders.order_id
    propagate_ck: driver
```

With `drive: products`, the pipeline emits one row per product enriched with a matching order, instead of one row per order enriched with its product.

## Memory considerations

Build-side inputs are materialized in memory as hash tables keyed by the equi columns. For each non-driving input, plan for roughly 1.5-2x the raw CSV size in heap. A 50 MB product catalog typically uses 75-100 MB of hash-table memory. Tune with `--memory-limit`; see [Memory Tuning](../ops/memory.md) for spill thresholds and strategy overrides.
