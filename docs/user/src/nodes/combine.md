# Combine Nodes

Combine nodes are the N-ary record-combining operator. Every input is declared up front and bound to a qualifier; the `where:` expression matches records across inputs using qualified field references (e.g. `orders.product_id == products.product_id`); the `cxl:` body shapes the output row.

Combine is distinct from merge: merge concatenates upstream branches that share a schema, while combine joins records across inputs that have different schemas.

*Interactive companion: the [Combine playground](combine-explainer.html) shows, driver row by driver row, which build rows `where:` matches and what `match:` and `on_miss:` do with them, including range joins.*

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
    propagate_ck: driver
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
| `where` | Yes | -- | CXL boolean expression matching records across inputs. Must contain at least one cross-input equality **or** range conjunct (a predicate with neither is rejected at plan time — see [Predicate requirements](#the-where-predicate)). |
| `match` | No | `first` | Match cardinality: `first`, `all`, or `collect`. |
| `on_miss` | No | `null_fields` | Driver-record handling on zero predicate matches: `null_fields`, `skip`, or `error`. |
| `cxl` | Yes (except under `match: collect`) | -- | Emit statements defining the output row. Empty under `match: collect`. |
| `drive` | No | first input | Explicit driver-input qualifier. Overrides the iteration-order default. |
| `strategy` | No | `auto` | Execution strategy hint: `auto` or `grace_hash`. |
| `propagate_ck` | Yes | -- | Selects which correlation-key columns ride onto the output. `driver` keeps the driver's CK only; `all` unions every input's CK columns; `{ named: [<field>, ...] }` carries an explicit subset. See [Correlation-key propagation](#correlation-key-propagation) below. |
| `max_output_rows` | No | unlimited | Opt-in cap on the number of rows this combine may emit. When set, the run **fails loud** (diagnostic `E325`) the moment the output would exceed the cap — it never truncates to a partial result. See [Output-size cap](#output-size-cap-max_output_rows). |

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

Every combine predicate must carry at least one cross-input **equality or range** conjunct. A predicate with neither — a pure residual with no decomposable cross-input comparison — is rejected at plan time with diagnostic `E313`; there is no supported execution strategy for it. Pure-range predicates without an equi conjunct are fully supported via IEJoin.

### Non-orderable range keys

A range conjunct compares values that must be **orderable at runtime**: integers, finite floats, exact `decimal`s, dates, and datetimes. When a record's range key evaluates to a non-orderable value — SQL `NULL`, a non-finite float (`NaN`/infinity), or any other type — that record can never satisfy the range comparison, so it is routed **out** of the range match rather than joined:

> **Decimal and mixed-numeric range keys.** Exact fixed-point `decimal` range keys are fully supported on every join strategy — a monetary band join (`amount >= tier.floor and amount < tier.ceiling`) matches correctly. A range conjunct that mixes an `integer` and a `float` operand across the two inputs is also supported: the integer is compared as a float, exactly as the `>=`/`<` operators compare it elsewhere.
>
> Decimal range keys are placed on a shared fixed-point grid with up to **18 fractional digits** and an integer magnitude up to roughly **1.7 × 10²⁰** — well beyond any realistic monetary value. A decimal range value outside that grid (more than 18 fractional digits, which would truncate, or a magnitude that would overflow the grid) is **not** silently dropped: the run stops with diagnostic `E326` naming the combine, so a wrong or empty result is never emitted. Rescale or narrow the compared values if you hit it.
>
> **Datetime range keys.** `date` and `datetime` range keys compare at their native resolution — a `datetime` to the **nanosecond**, across the full representable calendar (no microsecond rounding; instants before 1677 or after 2262 stay exact). Two timestamps that differ only below the microsecond therefore match, sort, and group as **distinct** instants on every join strategy, so a sub-microsecond as-of or band lookup neither drops a boundary match nor merges two near-simultaneous events.
>
> **`abs`/`min`/`max`/`clamp` and rejected range keys (`E327`).** `abs`, `min`, `max`, and `clamp` return the `numeric` supertype (`int | float`). When **both** operands of the range conjunct recover the **same** concrete type — `abs(int) >= abs(int)`, or a `min`/`max`/`clamp` whose result can only be one type (all-`int` or all-`float`, recovered through nested calls and arithmetic such as `abs(a.x + 1)` or `abs(min(a.i, b.i))`) — the axis is exactly as safe as a plain matching-typed key and the join runs normally. Otherwise the conjunct cannot be reduced to one exact numeric axis and is rejected at **plan time** with `E327`, rather than routed to the join where it could silently drop rows or mismatch. `E327` fires when such an operand stays genuinely ambiguous — a **mixed pair** like `abs(int) >= abs(float)`, or a **per-row-ambiguous** result like `min(int, float)` that may return either the integer or the float operand — or on a **non-orderable pairing** such as a string comparison or a `decimal` compared against a `float`/`numeric`. Compare a matching-typed range key instead. (A supported int/float/decimal/date/datetime range key, including a mixed int/float **field** pair, never triggers `E327`.)

- A **driver** record with a non-orderable range key is treated as a zero-match driver and handled by [`on_miss`](#unmatched-records-on_miss) (`null_fields` / `skip` / `error`).
- A **build** record with a non-orderable range key is dropped (it can match nothing).

This is a runtime routing decision on the data, distinct from the plan-time `E313` rejection above (which is about the predicate *shape*, not the values).

## Match modes

### `match: first`

Emit one output row per driver record, using the first matching build-side record. Standard 1:1 enrichment. Default.

"First" means the earliest in the build input's arrival order: of the build records that match a driver, the one the build input delivered first. The same order sets the row order of [`match: all`](#match-all) within a driver and the element order of a [`match: collect`](#match-collect) array. It holds identically for every join strategy the planner may pick and whatever shape the `where:` predicate has. One exception remains. When the build input exceeds the memory limit, the Combine sets part of it aside on disk and splits that part into smaller pieces until each fits. If a piece still does not fit after splitting, the Combine processes it in chunks and currently decides `first`, `collect` and `on_miss` once per chunk rather than once per driver, so a driver can receive more than one `first` row or more than one `collect` row. A piece fails to fit in three cases: the build rows sharing one key value do not fit on their own; the build input has already been split into the maximum of 4,096 pieces; or splitting a piece again leaves most of its rows together, as happens when many rows share a key value or their key values happen to land in the same piece. So the exception can reach a driver even when no single key value is over the limit, for example with a large build input under a tight memory limit. This is a known defect.

- **To pick a different record, order the build input upstream.** For example, to enrich each driver with the *latest* price, deliver the build input sorted descending on its date, such as with a descending [`sort_order`](source.md) on the build Source. A Combine has no ordering option of its own.
- **A correlation key orders its Source.** Declaring `correlation_key` on the build Source makes the planner sort that Source's rows by the key before they reach the Combine, unless its declared `sort_order` already begins with the key. That order is what "first" follows. See [Correlation keys](../pipelines/correlation-keys.md#row-order).
- **An unseeded `interleave` Merge has no fixed order.** When the build input is such a Merge, its cross-input order follows arrival at run time, so "first" follows that unfixed order too. Use `concat`, or a seeded `interleave`, for a build input whose order must be the same on every run.

```yaml
  config:
    where: "orders.product_id == products.product_id"
    match: first
    cxl: |
      emit order_id = orders.order_id
      emit product_name = products.product_name
```

The `where:` predicate selects the match; the `cxl:` body is a **post-match projection** that runs once on the chosen build record. Selection and projection are separate steps: if the body filters the row out (a `filter` that fails, or a body that emits nothing), that one output row is dropped. The combine does **not** fall back to a later matching build, and the driver is **not** treated as unmatched — it matched the predicate, the body just produced no row. `on_miss` (below) never fires for such a driver; it fires only when the predicate matched nothing at all. This holds identically for every join strategy the planner may pick.

> **Behavior change.** This is a change in observable output for existing pipelines whose `where:` predicate carries a range, equi+range, or single-inequality comparison — the shapes the planner runs as a sort-merge join or an IEJoin (both the pure-range block-band path and the equi+range hash-partitioned path). On any of those three strategies, a driver that matched the predicate but whose body skipped every candidate was previously routed to `on_miss` — firing `null_fields`, `skip`, or `error`. It now silently produces no row, matching the pure-equality strategies (in-memory hash and grace hash), which already behaved this way and are unchanged. A pipeline that relied on the old routing (for example, `on_miss: error` tripping on a body-skipped driver) no longer sees it.

**When `where:` fails to evaluate.** A `where:` that raises an error for a candidate, such as a division by zero, has not said whether that candidate matches. It is neither a match nor a non-match. Under `match: first` the earliest candidate that is not a non-match decides. If it matched, the driver is enriched with it. If its `where:` failed, the driver is dead-lettered with that build row and writes no output row, even when a later candidate would match: taking the later one would publish a row that depends on an evaluation that failed. A candidate after the deciding one is not part of the driver's result, so its failure is never written. This holds on every join strategy.

### `match: all`

Emit one output row for every matching build-side record. 1:N fan-out -- if a driver record matches three build records, three rows are emitted, in the build input's arrival order (see [`match: first`](#match-first)).

```yaml
  config:
    where: "employees.department == benefits.department"
    match: all
    cxl: |
      emit employee_id = employees.employee_id
      emit benefit = benefits.benefit_name
```

Each output row depends only on its own pair, so a candidate whose `where:` fails to evaluate is dead-lettered with its build row while the driver's other matches are still emitted.

### `match: collect`

Gather every matching build-side record into a single Array-typed field on the output row. The driver record appears once; the build matches are aggregated into an array, in the build input's arrival order (see [`match: first`](#match-first)). The `cxl:` body must be empty under `collect` -- the combine node synthesizes the output as `{ driver fields..., <build_qualifier>: Array }`.

```yaml
  config:
    where: "orders.product_id == products.product_id"
    match: collect
    cxl: ""
```

A per-group entry limit of 10,000 prevents unbounded growth.

A `collect` row states the complete set of a driver's matches. If any candidate's `where:` fails to evaluate, that set is unknown, so the driver writes **no** row, neither a partial array nor an empty one; each failing candidate is dead-lettered with its build row. Candidates past the 10,000-entry limit are still checked, so every failure among them is dead-lettered too.

Use `collect` when you need the set of matches as a single structured value; use `all` when you need a flat row per match.

## Unmatched records (`on_miss`)

`on_miss` controls what happens to driver records with **zero predicate matches** — drivers for which no build-side record satisfied `where:`, because every candidate evaluated it to false or null, or because there was no candidate at all. Two kinds of driver are **not** misses and never reach `on_miss`, under any `match` mode and on every join strategy:

- A driver that matched the predicate but whose `cxl:` body skipped or failed the row (see [`match: first`](#match-first)). It simply produces no output row for that match, and a body failure is dead-lettered.
- A driver any of whose candidates failed to evaluate `where:`. Each failure is dead-lettered, and whatever `on_miss` says, it does not fire: `on_miss: error` does not stop the run and `on_miss: null_fields` writes no null-filled row, because the driver was never shown to have no match.

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
    propagate_ck: driver
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

The choice is made when the pipeline is planned, from an estimate of the inputs' size. Under `auto`, an equal-ids join runs in memory unless that estimate says the build side is too large. If the estimate is low or missing (for example, a `glob:` source whose files are not known in advance) and the build side turns out larger than the memory budget, the run stops with `E310 MemoryBudgetExceeded`; it does not switch to disk partway through ([#1337](https://github.com/rustpunk/clinker/issues/1337)). Set `strategy: grace_hash` when the build side may be larger than the memory budget: it partitions the build side to disk from the start.

## Correlation-key propagation

Combine declares which correlation-key columns its output rows carry via the required `propagate_ck` field.

```yaml
- type: combine
  name: enriched
  input:
    orders: orders
    products: products
  config:
    where: "orders.product_id == products.product_id"
    cxl: |
      emit order_id = orders.order_id
      emit product_name = products.name
    propagate_ck: driver        # driver-only (today's behavior)
```

```yaml
    propagate_ck: all           # union of every input's $ck.* columns
```

```yaml
    propagate_ck:
      named: [order_id]         # explicit subset (intersected with upstream)
```

- `driver` -- output carries only the driver input's correlation-key columns. Build-side records contribute body fields, but their group identity is consumed by the match; when a match fails, the build record's dead letter follows the failing driver's correlation group.
- `all` -- output carries every input's correlation-key columns. Use when the build side carries keys that downstream operators need to read.
- `named: [<field>, ...]` -- an explicit subset. Use to project a multi-field key down to a single field after a join.

Driver wins on a name collision: if both the driver and a build input declare the same key field, the output keeps the driver's value. See the [Correlation-key combine interaction](../pipelines/correlation-keys.md#combine-interaction) reference for how each `match` mode fills the propagated key (especially `match: collect`).

`propagate_ck` is required on every combine; pipelines without an explicit value fail to compile. Existing pipelines migrate by adding `propagate_ck: driver`, which is bit-for-bit equivalent to today's behavior.

## Output-size cap (`max_output_rows`)

`max_output_rows` is an **opt-in** ceiling on how many rows a combine may emit. It defaults to unlimited; set it to guard against a permissive or mis-specified predicate that would explode a small pair of inputs into a huge result (for example, a range join over an unexpectedly hot key producing a near cross product):

```yaml
  config:
    where: "orders.ts >= prices.effective_from"
    cxl: |
      emit order_id = orders.order_id
      emit price = prices.amount
    propagate_ck: driver
    match: all
    max_output_rows: 1000000
```

Semantics:

- **Fail-loud, never truncate.** The moment the combine would emit more than the cap, the run stops with diagnostic `E325` naming the node and the cap. It does **not** produce a capped or partial result — a silently truncated join would corrupt downstream data.
- **Independent of the memory budget.** This is a result-*size* guard, not memory pressure. A runaway join can be perfectly bounded in memory (its output spills to disk) yet still produce far more rows than intended; `max_output_rows` caps the row count regardless of bytes.
- **Covers the whole output, on every strategy.** The cap counts every emitted **output row** across all match modes and any `on_miss` rows, and is enforced identically whichever join strategy the planner picks (hash build-probe, grace-hash, sort-merge, or the IEJoin block-band).
- **`collect` counts driver rows.** Under `match: collect` a combine emits **one output row per driver row** (each carrying an array of up to 10 000 collected matches), so `max_output_rows` bounds the driver-row count, not the number of collected array elements.
- **Dead-lettered rows are not counted.** A matched pair whose `cxl:` body or residual raises a *recoverable* eval failure is routed to the DLQ, not the output, so it does not count toward the cap. Only that pair is dead-lettered: under `match: all` the driver's other matches are still evaluated and emitted, whichever join strategy runs. A failing residual is neither a match nor a miss: under `match: first` a failure on the deciding candidate is the driver's only result, and under `match: collect` it leaves the driver with no row (see [`match: first`](#match-first) and [`match: collect`](#match-collect)). The driver row and the matched build row are dead-lettered, each with its own Source and row number; each failure writes its own pair, so a driver that fails against several build rows is dead-lettered once per failure, and a build row that several failing drivers matched is dead-lettered once for each of them.
- **N-ary combines cap the final output.** A combine whose `where:` spans three or more inputs is decomposed into a chain of binary steps; `max_output_rows` guards the **final** combined output, not the intermediate chain steps.

If the large result is expected, raise the cap (or omit the field). If it is not, tighten the `where:` predicate. Run `clinker explain --code E325` for the full remediation guide.

## Memory considerations

Each non-driving (build-side) input is held in memory while the join runs, so plan for roughly 1.5–2× its file size — a 50 MB lookup table needs about 75–100 MB. An equal-ids join spills its build side to disk only when it runs as a grace hash join, which the planner chooses from its size estimate or which `strategy: grace_hash` forces. An in-memory join whose build side outgrows the budget stops the run with `E310` instead of spilling ([#1337](https://github.com/rustpunk/clinker/issues/1337)). Set the budget with `pipeline.memory.limit`; see [Memory Tuning](../ops/memory.md).

Range and equi+range predicates (the IEJoin block-band strategy) are bounded on **both input axes and the output**: each side is drained into disk-backed, key-sorted blocks, and the emitted rows accumulate in a spillable sort buffer rather than a resident vector. So a range join whose inputs — or whose result — exceed the memory budget spills automatically and completes, rather than failing. Even a single hot key whose block-pair is a near cross product streams through a bounded nested loop instead of materializing the whole candidate set. Use [`max_output_rows`](#output-size-cap-max_output_rows) if you want such a runaway result to *stop* rather than spill.

## Document boundaries

A Combine passes document boundaries through to its output, so a per-document `Aggregate` after a join still rolls up per document. A driver source that carries several documents (a `glob:` over monthly files, say) produces one roll-up per driver document after the join, not one fold spanning all of them. A document carried on both the driver and the build side opens and closes exactly once downstream. See [Document Context & Envelopes](../pipelines/envelope-and-doc-context.md) for the per-document aggregation model.

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
      propagate_ck: driver

  - type: sink
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
