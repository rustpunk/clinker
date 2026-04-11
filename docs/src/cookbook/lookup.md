# Multi-Source Lookup

This recipe enriches order records with product information from a separate catalog file. Clinker's analytic window feature provides the lookup mechanism.

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
      analytic_window:
        source: products
        on: product_id
        group_by: [product_id]
      cxl: |
        emit order_id = order_id
        emit product_id = product_id
        emit product_name = $window.first()
        emit category = $window.first()
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

## How the analytic window works

The `analytic_window` configuration on the transform node sets up a lookup relationship:

- **`source`** -- the name of another source node in the pipeline (here, `products`).
- **`on`** -- the join key. Both the primary input and the window source must have a column with this name.
- **`group_by`** -- the grouping key for the window. Records in the window source are grouped by this key, and `$window` functions operate within that group.

For each record in the primary input (`orders`), Clinker finds matching records in the window source where `product_id` matches, then makes window functions available:

- `$window.first()` -- first value in the matching group
- `$window.last()` -- last value in the matching group
- `$window.count()` -- number of matching records
- `$window.sum()` -- sum of numeric values in the group

The window source is loaded into memory once at pipeline start. This makes it suitable for lookup tables that fit comfortably in the memory budget -- catalogs, reference data, mapping tables.

## Key points

**Two source nodes.** The pipeline declares two independent sources. The transform node's `input` field connects it to the primary data stream (`orders`), while `analytic_window.source` connects it to the lookup data (`products`).

**Join semantics.** This is a left join. If an order references a product_id that does not exist in the products file, `$window.first()` returns null. Add null handling if your data may have unmatched keys:

```
emit product_name = if $window.count() > 0
  then $window.first()
  else "UNKNOWN"
```

**Memory considerations.** The entire window source is loaded into memory, indexed by the join key. For large reference files, ensure your `--memory-limit` accounts for this. A 10 MB CSV lookup table uses roughly 10-20 MB of memory when indexed.

## Variations

### Multiple lookup sources

A transform can reference only one analytic window. To enrich from multiple lookup tables, chain transforms:

```yaml
  - type: transform
    name: enrich_product
    input: orders
    config:
      analytic_window:
        source: products
        on: product_id
        group_by: [product_id]
      cxl: |
        emit order_id = order_id
        emit product_id = product_id
        emit product_name = $window.first()
        emit customer_id = customer_id
        emit amount = amount

  - type: transform
    name: enrich_customer
    input: enrich_product
    config:
      analytic_window:
        source: customers
        on: customer_id
        group_by: [customer_id]
      cxl: |
        emit order_id = order_id
        emit product_name = product_name
        emit customer_name = $window.first()
        emit amount = amount
```

Each transform adds fields from its respective lookup source.
