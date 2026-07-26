# 02 — Product feed normalize

**xml → csv, xml.** Read a supplier catalogue, flatten it into trade-ready
columns, and write it out in two formats — carrying a **repeated element**
through as a single multi-value field.

This is the multi-value scenario. `<category>` appears more than once per
product, and one column holds all of them.

> **This scenario currently fails its own gate**, on purpose. See
> [What is broken](#what-is-broken) below — the committed expected output states
> what the engine *should* write, and one of the two sinks does not yet get it.

## Run it

```bash
cargo run -p clinker-scenarios -- gen --scenario 02-product-feed-normalize
cd examples/scenarios/02-product-feed-normalize
cargo run -p clinker -- run pipeline.yaml
```

```
Pipeline complete: 14 total, 14 ok, 14 written, 0 dlq
```

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

## What is broken

`output/catalog.csv` comes out **empty**, and the run still exits 0 reporting
all 14 records written.

The cause is not this pipeline. A node feeding two Output nodes delivers records
to only the last-declared one; every earlier sink gets a zero-byte file, with no
diagnostic ([#996](https://github.com/rustpunk/clinker/issues/996)). Declaring
either sink alone produces correct output, and swapping their order moves the
empty file to the other one.

The scenario keeps the two-sink shape rather than working around it, because
writing one result in two formats is an ordinary thing to want and the corpus is
meant to state what the engine *should* do. `expected/catalog.csv` is therefore
the output of the single-sink variant — the correct answer — and the harness
carries a `known_broken` marker pointing at #996.

The marker is narrow by design. It names `output/catalog.csv` as the only
output allowed to differ and declares the run summary the engine currently
prints; `output/catalog.xml` stays fully gated, so the sink that works today
cannot regress unnoticed while the scenario is parked. The gate's `counters`
record the **correct** summary — 14 records to each of two sinks, so 28 writes
— which is what makes the run that fixes #996 report a stale marker rather than
a counter mismatch. A re-bless run refuses to overwrite a golden the marker
declares broken, since that file is the only record of the right answer;
regenerate it from the single-sink variant instead.

## Try changing it

- Delete the **`catalog_xml`** output and rerun. `catalog_csv` is then the only
  sink, it receives every record, and `output/catalog.csv` matches its golden
  exactly — the clearest demonstration that #996 is about having two sinks and
  not about this pipeline. (Deleting `catalog_csv` instead proves nothing: the
  CSV simply stops being written at all.)
- Remove `multiple: true` from the schema and rerun — the repeated element is
  now an error rather than a collected field.
- Drop the XML `join_values` override to see the unwrapped, column-named default.
- Give a category a `|` in it and rerun. The run **aborts** with exit 4 naming
  the offending value and the delimiter: `on_conflict` defaults to `error`, so a
  value that collides with the join delimiter refuses the record rather than
  silently producing a cell that would parse back as two categories. Set
  `on_conflict` explicitly to choose a different policy — the default is the
  safe one, not the lenient one.
