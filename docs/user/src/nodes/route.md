# Route Nodes

Route nodes split a stream of records into named branches based on CXL boolean conditions. Each branch becomes an independent output port that downstream nodes can wire to using port syntax.

*Interactive companion: the [Route and Merge explainer](route-merge-explainer.html) shows, record by record, which conditions are checked and where each record goes, and how a Merge rejoins the branches.*

## Basic structure

```yaml
- type: route
  name: split_by_value
  input: orders
  config:
    mode: exclusive
    conditions:
      high: "amount.to_int() > 1000"
      medium: "amount.to_int() > 100"
    default: low
```

This creates three output ports: `split_by_value.high`, `split_by_value.medium`, and `split_by_value.low`.

## Conditions

The `conditions:` field is an ordered map of branch names to CXL boolean expressions. Each expression is evaluated against the incoming record.

```yaml
    conditions:
      priority: "urgency == \"high\" and amount > 500"
      standard: "urgency == \"medium\""
      bulk: "quantity > 100"
    default: other
```

Condition keys become the port names used in downstream `input:` wiring.

### Compile-time checking

Branch conditions are typechecked when the pipeline is compiled -- the plan-building pass you trigger with `clinker run pipeline.yaml --explain` or a bare `--dry-run` (see [Validation & Dry Run](../ops/validation.md)). Each condition is checked against the concrete column types of the route's input, so a condition that references an unknown column or compares incompatible types fails at compile time rather than partway through a run.

Compile failures surface as a CXL diagnostic keyed to the failure class -- **E202** for a branch condition that does not parse, **E203** for one that references an unknown column, and **E200** for one that compares incompatible types -- and name the offending branch, for example `split_by_value (branch high)`, so in a multi-branch route the error points at the specific branch rather than just the route node.

## Default branch

The `default:` field is **required**. Records for which no condition is true are routed to the default branch. A condition whose result is null counts as not true.

A condition that fails to evaluate is not "no match". Under `error_handling.strategy: continue` the record is dead-lettered and takes no branch, not even one whose condition held, and not the default; under `fail_fast` the run stops. In `exclusive` mode the conditions after the first true one are never evaluated, so a condition further down cannot fail for that record. See [An evaluation error is never false](../pipelines/error-handling.md#an-evaluation-error-is-never-false).

## Routing modes

### Exclusive (default)

In `exclusive` mode, conditions are evaluated in declaration order and the **first matching condition wins**. A record appears in exactly one branch. Order matters -- put more specific conditions first.

```yaml
    mode: exclusive
    conditions:
      vip: "lifetime_value > 100000"
      high: "lifetime_value > 10000"
      medium: "lifetime_value > 1000"
    default: standard
```

A customer with `lifetime_value = 50000` is not over 100000, so `vip` is not true; `high` is the first true condition and wins. A customer with `lifetime_value = 150000` is true for all three conditions, but goes to `vip` alone, because `vip` is checked first. Listing `medium` first would send both customers to `medium`.

### Inclusive

In `inclusive` mode, **all matching conditions route the record**. A single record can appear in multiple branches simultaneously.

```yaml
    mode: inclusive
    conditions:
      needs_review: "amount > 10000"
      flagged: "status == \"flagged\""
      international: "country != \"US\""
    default: standard
```

A flagged international order over 10000 would appear in `needs_review`, `flagged`, and `international` -- three copies routed to three branches.

## Downstream wiring

Downstream nodes reference route branches using **port syntax**: `route_name.branch_name`. The default branch is reached the same way, by its name. Several nodes can read the same branch; each receives every record on it.

A node whose own name equals a branch name can also reference the Route by its bare name and receives that branch: a Sink named `high` with `input: classify` reads `classify.high`.

```yaml
- type: route
  name: classify
  input: transactions
  config:
    mode: exclusive
    conditions:
      high: "amount > 1000"
      medium: "amount > 100"
    default: low

- type: transform
  name: high_value_processing
  input: classify.high
  config:
    cxl: |
      emit txn_id = txn_id
      emit amount = amount
      emit review_flag = true

- type: transform
  name: standard_processing
  input: classify.medium
  config:
    cxl: |
      emit txn_id = txn_id
      emit amount = amount

- type: sink
  name: low_value_out
  input: classify.low
  config:
    name: low_value_out
    type: csv
    path: "./output/low_value.csv"
```

## Constraints

- Give every condition a distinct name; each name is a separate port.
- Give `default` a name that no condition uses. This is not currently checked when the pipeline is planned: a `default` with the same name as a condition is folded into that condition's branch, so its records cannot be told apart from the matches.
- Declare at least one condition. A Route with none is not rejected today; every record takes the default branch.

## Complete example

```yaml
pipeline:
  name: order_routing

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: "./data/orders.csv"
      schema:
        - { name: order_id, type: int }
        - { name: region, type: string }
        - { name: amount, type: float }
        - { name: priority, type: string }

  - type: route
    name: by_region
    input: orders
    config:
      mode: exclusive
      conditions:
        domestic: "region == \"US\" or region == \"CA\""
        emea: "region == \"UK\" or region == \"DE\" or region == \"FR\""
        apac: "region == \"JP\" or region == \"AU\" or region == \"SG\""
      default: other

  - type: sink
    name: domestic_orders
    input: by_region.domestic
    config:
      name: domestic_orders
      type: csv
      path: "./output/domestic.csv"

  - type: sink
    name: emea_orders
    input: by_region.emea
    config:
      name: emea_orders
      type: csv
      path: "./output/emea.csv"

  - type: sink
    name: apac_orders
    input: by_region.apac
    config:
      name: apac_orders
      type: csv
      path: "./output/apac.csv"

  - type: sink
    name: other_orders
    input: by_region.other
    config:
      name: other_orders
      type: csv
      path: "./output/other_regions.csv"
```
