# Intra-Record Closures

This recipe shows the complete intra-record fan-out shape: an NDJSON source where each record carries an array of line items, a transform that filters items by price and then fans each remaining item into its own output record, and a flat NDJSON sink ready for downstream billing.

The pieces involved:

- Arrow-syntax [closures](../cxl/closures.md) for predicates and projections.
- [Array methods](../cxl/builtins-array.md) (`filter`, `map`) for in-place transformation.
- Bracket-index access (`it["sku"]`) for reading fields off each map element.
- [`emit each`](../cxl/emit-each.md) for fan-out.
- The Output node's [`include_unmapped`](../pipeline/output.md#unmapped-input-field-passthrough) flag for controlling which fields reach the sink.

## Input data

`orders.ndjson` -- one JSON object per line, each carrying a nested `items` array:

```ndjson
{"order_id":"O-1","customer":"alice@example.com","items":[{"sku":"a","price":10,"qty":2},{"sku":"b","price":20,"qty":1},{"sku":"c","price":3,"qty":5}]}
{"order_id":"O-2","customer":"bob@example.com","items":[{"sku":"a","price":10,"qty":1},{"sku":"d","price":50,"qty":1}]}
```

Each record has two order-level fields (`order_id`, `customer`) and an `items` array whose elements are maps with `sku`, `price`, and `qty`.

## Goal

For each order:

1. Drop items priced under $5 (a sub-threshold cutoff).
2. Fan the surviving items into one output record each, carrying the order-level identifiers plus the per-item fields.
3. Compute the per-line revenue (`unit_price * qty`) for each output record.

## Pipeline

`billing_lines.yaml`:

```yaml
pipeline:
  name: billing_lines

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      options:
        format: ndjson
      path: "./orders.ndjson"
      schema:
        - { name: order_id, type: string }
        - { name: customer, type: string }
        - { name: items, type: any }

  - type: transform
    name: filter_lines
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit customer = customer
        emit item_count = items.length()
        emit kept = items.filter(it => it["price"] >= 5)

  - type: transform
    name: explode
    input: filter_lines
    config:
      max_expansion: 10000
      cxl: |
        emit each it in kept {
          emit order_id = order_id
          emit customer = customer
          emit sku = it["sku"]
          emit unit_price = it["price"]
          emit qty = it["qty"]
          emit line_total = it["price"] * it["qty"]
        }

  - type: output
    name: lines_out
    input: explode
    config:
      name: lines_out
      type: json
      path: "./output/billing_lines.ndjson"
      options:
        format: ndjson
      include_unmapped: false
      exclude: [items, kept]

error_handling:
  strategy: continue
```

## Run it

```bash
# Validate first
clinker run billing_lines.yaml --dry-run

# Preview the first few output records
clinker run billing_lines.yaml --dry-run -n 3

# Full run
clinker run billing_lines.yaml
```

## Expected output

`output/billing_lines.ndjson`:

```ndjson
{"order_id":"O-1","customer":"alice@example.com","sku":"a","unit_price":10,"qty":2,"line_total":20}
{"order_id":"O-1","customer":"alice@example.com","sku":"b","unit_price":20,"qty":1,"line_total":20}
{"order_id":"O-2","customer":"bob@example.com","sku":"a","unit_price":10,"qty":1,"line_total":10}
{"order_id":"O-2","customer":"bob@example.com","sku":"d","unit_price":50,"qty":1,"line_total":50}
```

Order O-1's three input items collapse to two output records (the `sku=c` line was filtered out because its price was below $5). Order O-2's two items both survive the filter and produce two output records.

## How it works

**Filter stage.** The `filter_lines` transform reads each order, runs `items.filter(it => it["price"] >= 5)` to drop sub-threshold items, and stashes the survivors in a `kept` field. The closure body uses bracket indexing (`it["price"]`) because each `it` is a map; bracket indexing returns null for missing keys without aborting. The same record also carries an `item_count` projection so downstream nodes could route or audit on the original (pre-filter) item count.

**Explode stage.** The `explode` transform contains one `emit each` block over `kept`. For each surviving item, the body emits a flat record with the order-level identifiers (`order_id`, `customer`) repeated, plus the per-item fields lifted out of `it`. The body has no `filter` or nested `emit each` -- those are forbidden inside the block; pre-filter upstream as we did, or post-filter in a downstream transform.

**`include_unmapped: false`.** The default Output policy is to pass every unmapped input field through. Here we set it to `false` so the order-level `items` array (carried through from the source), the `item_count` projection, and the intermediate `kept` array (used only as the fan-out source) do not leak into the per-line output. The `exclude: [items, kept]` list provides a belt-and-suspenders defense against future renaming.

**`max_expansion: 10000`.** Caps how many output records a single input order may produce. The default is `10000`; we set it explicitly here so the value is visible in the YAML. Orders with arrays larger than the cap route to the DLQ with category `expansion_limit_exceeded` (see [Transform Nodes -> Expansion Cap](../pipeline/transform.md#expansion-cap-max_expansion)).

## Variations

### Pass through every input field

Remove `include_unmapped: false` (or set it to `true`) and the original order-level fields plus the intermediate `kept` array will appear on every output record. Useful when downstream consumers expect a complete record context, or when you need to audit what was filtered.

### Emit a single record per order with the kept-items array

Drop the `explode` transform and route `filter_lines` directly to the Output. Each output record stays at order grain, with `kept` carrying the post-filter array. This is the same pipeline minus the fan-out step.

### Reach for `.flat_map` instead of two transforms

When the per-element transformation is simple enough to fit in a single closure body, [`flat_map`](../cxl/builtins-array.md#flat_mapit--array---array) collapses the filter + project + explode pattern into one expression. It produces a flat array, which downstream nodes still see as a single field on the input record; the explicit `emit each` is what produces multiple output records.

### Rewrite a nested field in place with `.set`

When you want to keep the record at order grain but mutate a value buried inside it, the [`set`](../cxl/builtins-map.md#nested-paths) map method takes a dotted/indexed path and rewrites a single leaf, leaving every sibling untouched:

```yaml
    cxl: |
      emit order = order.set("items[0].sku", "A-100").set("ship.region", "us-east")
```

The first `set` overwrites the SKU of the first item; the second writes `ship.region`, auto-creating the `ship` map if the order had no `ship` field yet. Because `set` is copy-on-write, this builds a fresh order document without disturbing the upstream binding. A path that conflicts with the existing shape (descending into a scalar, or an array index past the end) yields `null` for that `set` rather than partially writing -- guard with [`catch`](../cxl/builtins-introspection.md) if a path may not match every record.

## See also

- [Closures](../cxl/closures.md) -- the `it => body` form.
- [Array Methods](../cxl/builtins-array.md) -- `filter`, `map`, `find`, `any`, `flat_map`, `remove`, `length`, `join`.
- [Map Methods](../cxl/builtins-map.md) -- `keys`, `values`, `merge`, `set`, `remove_field`.
- [Nested Paths](../cxl/nested-paths.md) -- bracket-index and dotted-path navigation.
- [Emit Each](../cxl/emit-each.md) -- the fan-out statement.
- [Transform Nodes](../pipeline/transform.md) -- `max_expansion` and DLQ routing.
- [Output Nodes](../pipeline/output.md) -- `include_unmapped` and field control.
