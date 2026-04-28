# per-source-ck — runnable per-source correlation-key example

This pipeline demonstrates the per-source correlation-key model: each
source declares its own `correlation_key:` field independently. The
`customers` source carries `customer_id` as its CK; the `orders`
source carries `order_id`. Combine joins them on `customer_id`, and
the joined output flows through a relaxed-CK regional aggregate.

The fixture is intentionally small — four customers, six orders —
so the joined output is easy to read and the per-region totals are
readable at a glance.

## Run it

The pipeline's `path:` fields are relative to the pipeline directory,
so invoke `clinker` from inside `examples/pipelines/per-source-ck/`:

```sh
cd examples/pipelines/per-source-ck
mkdir -p output
cargo run -p clinker -- run pipeline.yaml
```

You should see something like:

```
INFO clinker: Pipeline complete: 4 total, 2 ok, 2 written, 0 dlq
```

The `output/regional_totals.csv` payload aggregates the joined orders
per `region`:

```
region,total,n,$ck.aggregate.regional_totals
west,750,3,0
east,725,3,1
```

The trailing `$ck.aggregate.regional_totals` column is the
engine-managed synthetic CK column the relaxed aggregate stamps on
its output schema. It surfaces here only because the Output node
sets `include_correlation_keys: true`; default writer output strips
it. See [Correlation Keys](../../../docs/src/pipeline/correlation-keys.md)
for the full lifecycle and Combine `propagate_ck` semantics.

## What this fixture proves

- Multiple sources declaring different correlation keys
  (`customer_id` on `customers`, `order_id` on `orders`) compile
  cleanly. The previous pipeline-level `error_handling.correlation_key`
  field has been removed; CK is always scoped to a source.

- `propagate_ck: all` on the Combine widens the joined output schema
  with both inputs' `$ck.<field>` columns. Multi-source DLQ
  grouping then keys per-source: an `orders` failure DLQs against
  `$ck.order_id`; a `customers` failure DLQs against
  `$ck.customer_id`.

- The downstream aggregate's `group_by: [region]` omits both
  upstream CK fields, so the engine routes the pipeline through the
  relaxed-CK retraction protocol — even though the demo input has
  no failures, the orchestrator's five-phase commit body runs and
  produces the correct fast-path-equivalent output.

## See also

- [Correlation Keys](../../../docs/src/pipeline/correlation-keys.md) —
  per-source declaration, `$ck.<field>` shadow columns, multi-source
  semantics, Combine `propagate_ck` interaction.
- [`examples/pipelines/retract-demo/`](../retract-demo/) —
  end-to-end retraction example exercising both upstream and
  post-aggregate failure surfaces.
