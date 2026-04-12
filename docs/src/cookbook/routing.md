# Routing to Multiple Outputs

This recipe splits a stream of order records into separate output files based on business rules. High-value orders go to one file, standard orders to another.

## Input data

`orders.csv`:

```csv
order_id,customer,amount,region
ORD-001,Acme Corp,15000,US
ORD-002,Globex,450,EU
ORD-003,Initech,8500,US
ORD-004,Umbrella,22000,APAC
ORD-005,Stark Ind,950,US
ORD-006,Wayne Ent,3200,EU
```

## Pipeline

`order_routing.yaml`:

```yaml
pipeline:
  name: order_routing
  vars:
    high_value_threshold: 5000

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: "./orders.csv"
      schema:
        - { name: order_id, type: string }
        - { name: customer, type: string }
        - { name: amount, type: float }
        - { name: region, type: string }

  - type: route
    name: split_by_value
    input: orders
    config:
      mode: exclusive
      conditions:
        high: "amount >= $vars.high_value_threshold"
      default: standard

  - type: output
    name: high_value_output
    input: split_by_value.high
    config:
      name: high_value_orders
      type: csv
      path: "./output/high_value.csv"

  - type: output
    name: standard_output
    input: split_by_value.standard
    config:
      name: standard_orders
      type: csv
      path: "./output/standard.csv"
```

## Run it

```bash
clinker run order_routing.yaml --dry-run
clinker run order_routing.yaml
```

## Expected output

`output/high_value.csv`:

```csv
order_id,customer,amount,region
ORD-001,Acme Corp,15000,US
ORD-003,Initech,8500,US
ORD-004,Umbrella,22000,APAC
```

`output/standard.csv`:

```csv
order_id,customer,amount,region
ORD-002,Globex,450,EU
ORD-005,Stark Ind,950,US
ORD-006,Wayne Ent,3200,EU
```

## How routing works

### Port syntax

Route nodes produce named output ports. Downstream nodes reference these ports using dot syntax: `split_by_value.high` and `split_by_value.standard`.

The port names come from two places:
- **Condition names** in the `conditions` map (here, `high`)
- **The `default` field** (here, `standard`)

### Exclusive mode

With `mode: exclusive`, each record goes to exactly one branch. Conditions are evaluated top to bottom -- the first matching condition wins, and the record is sent to that port. Records that match no condition go to the `default` port.

### Pipeline variables

The threshold is defined in `pipeline.vars` and referenced in the CXL expression as `$vars.high_value_threshold`. This makes it easy to adjust the threshold without editing the route condition, and channel overrides can change it per environment.

## Variations

### Multiple branches

Route nodes can have any number of named branches:

```yaml
  - type: route
    name: split_by_region
    input: orders
    config:
      mode: exclusive
      conditions:
        us: "region == \"US\""
        eu: "region == \"EU\""
        apac: "region == \"APAC\""
      default: other

  - type: output
    name: us_output
    input: split_by_region.us
    config:
      name: us_orders
      type: csv
      path: "./output/us_orders.csv"

  - type: output
    name: eu_output
    input: split_by_region.eu
    config:
      name: eu_orders
      type: csv
      path: "./output/eu_orders.csv"

  # ... additional outputs for apac, other
```

### Transform before output

Insert a transform between the route and output to shape the data differently per branch:

```yaml
  - type: transform
    name: enrich_high_value
    input: split_by_value.high
    config:
      cxl: |
        emit order_id = order_id
        emit customer = customer
        emit amount = amount
        emit priority = "URGENT"
        emit review_required = true

  - type: output
    name: high_value_output
    input: enrich_high_value
    config:
      name: high_value_orders
      type: csv
      path: "./output/high_value.csv"
```

### Combining routing with aggregation

Route first, then aggregate each branch independently:

```yaml
  - type: aggregate
    name: high_value_summary
    input: split_by_value.high
    config:
      group_by: [region]
      cxl: |
        emit total = sum(amount)
        emit count = count(*)
```

This produces a per-region summary of high-value orders only.
