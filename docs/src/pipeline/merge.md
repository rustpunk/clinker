# Merge Nodes

Merge nodes combine multiple upstream branches into a single stream. They are the counterpart to route nodes -- where a route splits one stream into many, a merge joins many streams back into one.

## Basic structure

```yaml
- type: merge
  name: combined
  inputs:
    - east_data
    - west_data
  config: {}
```

Note the key differences from other node types:

- Uses **`inputs:`** (plural), not `input:` (singular).
- The `config:` block is empty -- all wiring is on the node header.
- Using `input:` (singular) on a merge node is a parse error.

## Wiring

The `inputs:` field is a list of upstream node references. These can be bare node names or port references from route nodes:

```yaml
- type: merge
  name: rejoin
  inputs:
    - process_high
    - process_medium
    - classify.low           # Port syntax for a route branch
  config: {}
```

Downstream nodes wire to the merge as a normal single-input reference:

```yaml
- type: output
  name: final_output
  input: rejoin
  config:
    name: final_output
    type: csv
    path: "./output/combined.csv"
```

## Record ordering

Records arrive in the order they are produced by upstream nodes. There is no guaranteed interleaving order between upstream branches. If you need sorted output, use a `sort_order` on the downstream output node.

## Use cases

### Reuniting route branches

The most common pattern is routing records through different processing paths and then merging them back together:

```yaml
- type: route
  name: classify
  input: orders
  config:
    mode: exclusive
    conditions:
      high: "amount > 1000"
    default: standard

- type: transform
  name: process_high
  input: classify.high
  config:
    cxl: |
      emit order_id = order_id
      emit amount = amount
      emit surcharge = amount * 0.02
      emit tier = "premium"

- type: transform
  name: process_standard
  input: classify.standard
  config:
    cxl: |
      emit order_id = order_id
      emit amount = amount
      emit surcharge = 0
      emit tier = "standard"

- type: merge
  name: all_orders
  inputs:
    - process_high
    - process_standard
  config: {}

- type: output
  name: result
  input: all_orders
  config:
    name: result
    type: csv
    path: "./output/all_orders.csv"
```

### Unioning multiple sources

Merge nodes can combine records from multiple source files that share the same schema:

```yaml
- type: source
  name: jan_sales
  config:
    name: jan_sales
    type: csv
    path: "./data/sales_jan.csv"
    schema:
      - { name: sale_id, type: int }
      - { name: amount, type: float }
      - { name: region, type: string }

- type: source
  name: feb_sales
  config:
    name: feb_sales
    type: csv
    path: "./data/sales_feb.csv"
    schema:
      - { name: sale_id, type: int }
      - { name: amount, type: float }
      - { name: region, type: string }

- type: merge
  name: all_sales
  inputs:
    - jan_sales
    - feb_sales
  config: {}

- type: aggregate
  name: totals
  input: all_sales
  config:
    group_by: [region]
    cxl: |
      emit total = sum(amount)
      emit count = count(*)
```
