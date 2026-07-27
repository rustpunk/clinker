# 02 — Product feed normalize

**xml → csv, xml.** Read a supplier catalogue, flatten it into trade-ready
columns, and write it out in two formats — carrying a **repeated element**
through as a single multi-value field.

This is the multi-value scenario. `<category>` appears more than once per
product, and one column holds all of them.

## Run it

```bash
cargo run -p clinker-scenarios -- gen --scenario 02-product-feed-normalize
cd examples/scenarios/02-product-feed-normalize
cargo run -p clinker -- run pipeline.yaml
```

The run writes all 14 products to both destinations. Its summary reports 14
records read and 28 writes because each record is broadcast to two Output
nodes.

## The input

`data/catalog.xml` — 14 products:

```xml
<catalog supplier="Harbourline Supply" feed_date="2026-01-05" currency="USD">
  <product sku="HL-1001">
    <name>Insulated Flask 750ml</name>
    <brand>Kestrel</brand>
    <list_price_minor>2450</list_price_minor>
    <cost_minor>1470</cost_minor>
    <category>drinkware</category>
    <category>grocery</category>
    <stock_on_hand>380</stock_on_hand>
  </product>
```

Ten of the fourteen carry more than one `<category>`.

## What to look at

**One column holds every repeat.** The schema declares:

```yaml
- { name: category, type: string, multiple: true }
```

`multiple: true` is the whole mechanism. Without it, repeated elements are a
read error; with it, all occurrences collect into one field on **one** record.
The feed is *not* fanned out into a row per category — 14 products in, 14 rows
out. Fanning out is a different operation, and this is the one you want when
categories are an attribute of the product rather than the grain of the data.

**Each sink encodes the repeat in its own idiom.** The same column writes two
ways, and neither is the writer guessing:

```yaml
# csv — no cell can hold a list, so join it
join_values:
  - { field: categories, delimiter: "|" }

# xml — name the item and restore the container
join_values:
  - { field: categories, repeat_as: category, wrap_in: categories }
```

CSV gets `drinkware|grocery`. XML gets
`<categories><category>drinkware</category><category>grocery</category></categories>`
— the input shape, reconstructed. Drop the XML override and the items still
emit, but named after the column (`<categories>drinkware</categories>` twice)
and with no wrapper.

**Money is integer minor units.** The feed carries `2450`, not `24.50`, and the
display amount is derived:

```
emit list_price = list_minor.to_decimal() / "100".to_decimal()
```

Deriving once from an exact integer keeps every intermediate exact — `24.50`,
`14.70`, margin `9.80`. `margin_pct` divides in decimal for the same reason:
integer division would print a flat `40` on every row, hiding that VF-3051's
true margin is `40.02`. This is good practice regardless, and here it is also
necessary: the XML reader type-infers element text and ignores the declared
column type, so `24.50` in a `decimal` column arrives as a float expansion
([#992](https://github.com/rustpunk/clinker/issues/992)).

**Attributes and flattening.** `attribute_prefix: ""` turns the `sku` attribute
into a plain column; by default it would be `@sku`, which CXL cannot name.
Related: the repeated `<category>` is a *direct* child of `<product>` rather
than sitting in a `<categories>` container, because a nested element flattens to
a dotted column name (`categories.category`) that CXL cannot address
([#995](https://github.com/rustpunk/clinker/issues/995)) — it would be readable
but untransformable. The write side puts the container back with `wrap_in`.

## Direct multi-output broadcast

Both Output nodes read the same `normalize` result. Direct multi-output edges
are broadcast, not partitioned: `catalog.csv` and `catalog.xml` each receive all
14 records and encode the repeated category field in their own format. The
scenario gate compares both files with committed goldens and requires
`records_written = 28`.

Use a [Route](../../../docs/user/src/nodes/route.md) node when destinations
should receive different subsets instead.

## Try changing it

- Delete either Output and rerun. The remaining sink still receives every
  record, while the summary drops from 28 writes to 14.
- Remove `multiple: true` from the schema and rerun — the repeated element is
  now an error rather than a collected field.
- Drop the XML `join_values` override to see the unwrapped, column-named default.
- Give a category a `|` in it and rerun. The run **aborts** with exit 4 naming
  the offending value and the delimiter: `on_conflict` defaults to `error`, so a
  value that collides with the join delimiter refuses the record rather than
  silently producing a cell that would parse back as two categories. Set
  `on_conflict` explicitly to choose a different policy — the default is the
  safe one, not the lenient one.
