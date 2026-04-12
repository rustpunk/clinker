# Multi-Source Lookup

This recipe enriches order records with product information from a separate catalog file using the `lookup:` transform configuration.

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

  - type: transform
    name: enrich
    input: orders
    config:
      lookup:
        source: products
        where: "product_id == products.product_id"
      cxl: |
        emit order_id = order_id
        emit product_id = product_id
        emit product_name = products.product_name
        emit category = products.category
        emit quantity = quantity
        emit unit_price = unit_price
        emit line_total = quantity.to_float() * unit_price

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

## How lookup works

The `lookup:` configuration on a transform node loads a secondary source into memory and matches each input record against it:

- **`source`** -- the name of another source node in the pipeline (here, `products`).
- **`where`** -- a CXL boolean expression evaluated per candidate lookup row. Bare field names reference the primary input; qualified names (`products.field`) reference the lookup source.
- **`on_miss`** (optional) -- what to do when no lookup row matches: `null_fields` (default), `skip`, or `error`.
- **`match`** (optional) -- `first` (default, 1:1) or `all` (1:N fan-out).

Lookup fields are accessed using qualified syntax: `products.product_name`, `products.category`, etc.

## Unmatched records

By default (`on_miss: null_fields`), unmatched records emit with all lookup fields set to null. Other options:

```yaml
lookup:
  source: products
  where: "product_id == products.product_id"
  on_miss: skip        # drop records with no match
```

```yaml
lookup:
  source: products
  where: "product_id == products.product_id"
  on_miss: error       # fail the pipeline on first unmatched record
```

## Range predicates

Lookups support any CXL boolean expression, not just equality. This classifies employees into pay bands:

```yaml
- type: transform
  name: classify
  input: employees
  config:
    lookup:
      source: rate_bands
      where: |
        ee_group == rate_bands.ee_group
        and pay >= rate_bands.min_pay
        and pay <= rate_bands.max_pay
    cxl: |
      emit employee_id = employee_id
      emit rate_class = rate_bands.rate_class
```

## Fan-out (match: all)

When `match: all` is set, each matching lookup row produces a separate output record. This is useful for one-to-many enrichments:

```yaml
- type: transform
  name: expand_benefits
  input: employees
  config:
    lookup:
      source: benefits
      where: "department == benefits.department"
      match: all
    cxl: |
      emit employee_id = employee_id
      emit benefit = benefits.benefit_name
```

An employee in a department with 3 benefits produces 3 output records.

## Multiple lookup sources

Each transform can reference one lookup source. Chain transforms to enrich from multiple sources:

```yaml
- type: transform
  name: enrich_product
  input: orders
  config:
    lookup:
      source: products
      where: "product_id == products.product_id"
    cxl: |
      emit order_id = order_id
      emit product_name = products.product_name
      emit customer_id = customer_id
      emit amount = amount

- type: transform
  name: enrich_customer
  input: enrich_product
  config:
    lookup:
      source: customers
      where: "customer_id == customers.customer_id"
    cxl: |
      emit order_id = order_id
      emit product_name = product_name
      emit customer_name = customers.customer_name
      emit amount = amount
```

## Memory considerations

The entire lookup source is loaded into memory. For large reference files, ensure `--memory-limit` accounts for this. A 10 MB CSV lookup table uses roughly 10-20 MB of heap when loaded.
