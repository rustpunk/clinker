# Combine Nodes

Combine nodes are the N-ary record-combining operator. Every input is declared up front and bound to a qualifier; the `where:` expression matches records across inputs using qualified field references (e.g. `orders.product_id == products.product_id`); the `cxl:` body shapes the output row.

Combine is distinct from merge: merge concatenates upstream branches that share a schema, while combine joins records across inputs that have different schemas.

## Basic structure

```yaml
- type: combine
  name: enrich
  input:
    orders: orders         # qualifier: upstream node name
    products: products
  config:
    where: "orders.product_id == products.product_id"
    match: first
    on_miss: null_fields
    cxl: |
      emit order_id = orders.order_id
      emit product_name = products.product_name
      emit amount = orders.amount
```

Note the differences from other node types:

- Uses **`input:`** as a **map**, binding qualifier names to upstream node references. Other nodes use `input:` as a single string or `inputs:` as a list of strings.
- Every field reference inside `where:` and `cxl:` must be qualified (`<qualifier>.<field>`). Bare field names are a compile error.
- Using `inputs:` (plural list) on a combine node is a parse error.

## Wiring

Each entry in the `input:` map binds a qualifier to an upstream node:

```yaml
  input:
    orders: orders                  # qualifier "orders" -> source node "orders"
    products: products
    high_priority: classify.high    # qualifier "high_priority" -> route port
```

Qualifiers are local names used inside `where:` and `cxl:`; they do not need to match the upstream node name. Upstream references can be bare node names or port references from a route node.

Iteration order in the `input:` map is preserved and used as the default driver-selection order (see [Choosing the driving input](#choosing-the-driving-input) below).

## Configuration fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `where` | Yes | -- | CXL boolean expression matching records across inputs. Must contain at least one cross-input equality. |
| `match` | No | `first` | Match cardinality: `first`, `all`, or `collect`. |
| `on_miss` | No | `null_fields` | Driver-record handling on zero matches: `null_fields`, `skip`, or `error`. |
| `cxl` | Yes (except under `match: collect`) | -- | Emit statements defining the output row. Empty under `match: collect`. |
| `drive` | No | first input | Explicit driver-input qualifier. Overrides the iteration-order default. |
| `strategy` | No | `auto` | Execution strategy hint: `auto` or `grace_hash`. |

## The `where:` predicate

The `where:` expression is a CXL boolean expression evaluated for every candidate record pair across inputs. It must contain at least one **cross-input equality** -- an equality with field references from two different inputs:

```yaml
  where: "orders.product_id == products.product_id"
```

Compound predicates combine multiple conjuncts with `and`. Each conjunct is classified by the planner:

- **Equi conjunct** -- a cross-input equality (`a.x == b.y`). Drives the hash lookup or sort-merge join.
- **Range conjunct** -- a cross-input ordered comparison (`a.start <= b.ts and b.ts <= a.end`). Handled by the IEJoin algorithm when no equi conjunct constrains the same input pair.
- **Residual conjunct** -- any other CXL predicate (intra-input filter, function call, etc.). Applied as a post-filter after the equi/range match.

```yaml
  where: |
    orders.product_id == products.product_id
    and orders.amount >= 100
    and products.region == "us-east"
```

Above: the equi conjunct drives the join; `orders.amount >= 100` and `products.region == "us-east"` are applied as residuals.

At least one cross-input equality is required for every combine. Pure-range predicates without an equi conjunct are also supported via IEJoin.

## Match modes

### `match: first`

Emit one output row per driver record, using the first matching build-side record. Standard 1:1 enrichment. Default.

```yaml
  config:
    where: "orders.product_id == products.product_id"
    match: first
    cxl: |
      emit order_id = orders.order_id
      emit product_name = products.product_name
```

### `match: all`

Emit one output row for every matching build-side record. 1:N fan-out -- if a driver record matches three build records, three rows are emitted.

```yaml
  config:
    where: "employees.department == benefits.department"
    match: all
    cxl: |
      emit employee_id = employees.employee_id
      emit benefit = benefits.benefit_name
```

### `match: collect`

Gather every matching build-side record into a single Array-typed field on the output row. The driver record appears once; the build matches are aggregated into an array. The `cxl:` body must be empty under `collect` -- the combine node synthesizes the output as `{ driver fields..., <build_qualifier>: Array }`.

```yaml
  config:
    where: "orders.product_id == products.product_id"
    match: collect
    cxl: ""
```

A per-group entry limit of 10,000 prevents unbounded growth.

Use `collect` when you need the set of matches as a single structured value; use `all` when you need a flat row per match.

## Unmatched records (`on_miss`)

`on_miss` controls what happens to driver records with zero matches:

| Value | Semantics |
|-------|-----------|
| `null_fields` (default) | Build-side fields resolve to null. Driver record is still emitted. Equivalent to left-join. |
| `skip` | Driver record is dropped. Equivalent to inner-join. |
| `error` | Pipeline fails on the first unmatched driver record. |

```yaml
  config:
    where: "orders.product_id == products.product_id"
    on_miss: skip
```

`on_miss: error` is useful for strict referential integrity where any miss should halt processing. `on_miss: skip` is the inner-join shape. `on_miss: null_fields` is the left-join shape and the default.

## Composite keys

Chain multiple cross-input equalities with `and`:

```yaml
  config:
    where: |
      sales.department == targets.department
      and sales.region == targets.region
    cxl: |
      emit department = sales.department
      emit region = sales.region
      emit actual = sales.amount
      emit goal = targets.goal
```

All conjuncts must hold for a record pair to match.

## Multi-input combine (three or more)

Combine accepts any number of inputs. Each pair of inputs that should be related needs an explicit cross-input equality:

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
```

The planner builds a join tree by walking equalities pairwise and ordering the joins by selectivity.

## Choosing the driving input

The driver is the input whose records flow through one at a time during execution; the other inputs are materialized as build-side hash tables (or IEJoin index structures). By default the first input in the `input:` map is the driver.

Use `drive:` to override:

```yaml
  config:
    where: "orders.product_id == products.product_id"
    drive: products
    cxl: |
      emit product_id = products.product_id
      emit product_name = products.product_name
      emit sample_order_id = orders.order_id
```

With `drive: products`, the pipeline emits one row per product enriched with a matching order, instead of one row per order enriched with its product. Pick the driver based on which side you want to iterate over (typically the larger stream, or the one whose ordering you want to preserve).

## Strategy hint

| Value | Behavior |
|-------|----------|
| `auto` (default) | Planner picks a strategy from the predicate shape. Hash join for equi predicates; IEJoin for pure-range predicates. |
| `grace_hash` | Force grace hash join (disk-spilling partitioned hash). Applies only to pure-equi predicates; ignored on predicates with range conjuncts. |

`grace_hash` is the right hint when build-side inputs are larger than the memory budget but fit on disk after partitioning. The planner falls back automatically to grace-hash spill when an in-memory hash table approaches the RSS soft limit, so `strategy: grace_hash` is mostly an explicit assertion for performance reasoning.

## Memory considerations

Build-side inputs are materialized in memory as hash tables keyed by the equi columns. For each non-driving input, plan for roughly 1.5-2x the raw CSV size in heap. A 50 MB product catalog typically uses 75-100 MB of hash-table memory. Tune with `memory_limit:` at the pipeline level; see [Memory Tuning](../ops/memory.md) for spill thresholds and strategy overrides.

## Complete example

```yaml
pipeline:
  name: order_enrichment

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: "./data/orders.csv"
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: amount, type: float }

  - type: source
    name: products
    config:
      name: products
      type: csv
      path: "./data/products.csv"
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
        emit amount = orders.amount

  - type: output
    name: result
    input: enrich
    config:
      name: result
      type: csv
      path: "./output/enriched_orders.csv"
```

## See also

- [Multi-Input Combine](../cookbook/combine.md) -- recipe-style walkthrough with input data and expected output.
- [Merge Nodes](merge.md) -- streamwise concatenation; the right operator when inputs share a schema and no per-record matching is needed.
- [Memory Tuning](../ops/memory.md) -- memory budget, spill thresholds, and strategy overrides.
