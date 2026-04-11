# Route Nodes

Route nodes split a stream of records into named branches based on CXL boolean conditions. Each branch becomes an independent output port that downstream nodes can wire to using port syntax.

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

## Default branch

The `default:` field is **required**. Records that match no condition are routed to the default branch. The default branch name must not collide with any condition key.

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

A customer with `lifetime_value = 50000` matches both `vip` and `high`, but because `exclusive` stops at first match, they go to `high` only if `vip` was checked first -- and they do, because `vip` comes first. Actually, 50000 is not > 100000, so they match `high`.

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

Downstream nodes reference route branches using **port syntax**: `route_name.branch_name`.

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

- type: output
  name: low_value_out
  input: classify.low
  config:
    name: low_value_out
    type: csv
    path: "./output/low_value.csv"
```

## Constraints

- At least 1 condition is required.
- Maximum 256 branches (conditions + default).
- Branch names must be unique.
- The `default` name must not collide with any condition key.

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

  - type: output
    name: domestic_orders
    input: by_region.domestic
    config:
      name: domestic_orders
      type: csv
      path: "./output/domestic.csv"

  - type: output
    name: emea_orders
    input: by_region.emea
    config:
      name: emea_orders
      type: csv
      path: "./output/emea.csv"

  - type: output
    name: apac_orders
    input: by_region.apac
    config:
      name: apac_orders
      type: csv
      path: "./output/apac.csv"

  - type: output
    name: other_orders
    input: by_region.other
    config:
      name: other_orders
      type: csv
      path: "./output/other_regions.csv"
```
