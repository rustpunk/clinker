# Merge Nodes

Merge nodes concatenate multiple upstream branches into a single stream. They are the counterpart to route nodes -- where a route splits one stream into many, a merge joins many streams back into one.

Merge is for streamwise concatenation of inputs that share a schema. For record-level joining across inputs that have different schemas, see [Combine Nodes](combine.md).

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

## Modes

Merge's cross-input ordering discipline is selected by `config.mode`. Two modes exist; `concat` is the default.

### `concat` (default)

Predecessor records drain in **declaration order**: `inputs[0]` flows to output first, then `inputs[1]`, then `inputs[2]`, and so on. Within a single predecessor, per-source FIFO order is preserved. Output is reproducible run-to-run.

```yaml
- type: merge
  name: combined
  inputs: [east, west]
  config:
    mode: concat
```

### `interleave`

Records flow to output **as they become available** from any predecessor. Per-source FIFO is preserved within each input; cross-input order follows wall-clock arrival and is non-deterministic.

```yaml
- type: merge
  name: combined
  inputs: [east, west]
  config:
    mode: interleave
```

When every direct predecessor of an unseeded `interleave` merge is a `Source` node, the executor **fuses** the Merge into the source ingest loop — predecessor channels are polled directly and Merge consumption proceeds at live ingest rate without any intermediate buffering tier.

### Seeded interleave — `interleave_seed:`

Snapshot tests and benchmarks that need reproducible cross-input ordering can opt into a deterministic schedule:

```yaml
- type: merge
  name: combined
  inputs: [east, west]
  config:
    mode: interleave
    interleave_seed: 42
```

A seeded interleave **bypasses the fused live-channel path**. The Merge instead pre-buffers each predecessor's output into a Vec, then emits records in `fastrand`-driven order seeded by `interleave_seed`. Output is reproducible regardless of upstream timing — at the cost of opting out of live back-pressure across this Merge (see below).

## Back-pressure semantics

How a slow consumer or slow upstream reader propagates back through the DAG depends on the merge mode.

### `concat`

Each Source ingest task pushes into its own bounded `mpsc` channel (capacity 1024 records per Source). Peer sources produce **concurrently** up to that capacity — the dispatch arm just consumes from `inputs[0]`'s channel before turning to `inputs[1]`'s.

Consequences:

- **Memory:** a non-leading input can hold up to one channel's worth of buffered records before its producer blocks. Multi-input `concat` over `N` Sources may carry up to `(N - 1) × 1024` records in flight even while only one input is being drained.
- **Latency:** a record produced by `inputs[1]` while `inputs[0]` is still draining will not reach output until `inputs[0]` finishes, regardless of how fast it was produced.
- **Producer-side back-pressure:** when a non-leading input's channel fills, its reader blocks at `blocking_send`, propagating pressure back to the upstream file/network reader. The upstream is throttled even though it is not the currently-consumed input.

`concat` is the right choice when downstream consumers depend on declaration-ordered records (e.g. snapshot tests asserting on byte-identical output) or when the inputs represent ordered time partitions that must remain contiguous.

### `interleave` (unseeded)

Fused with Source predecessors, the Merge arm polls every predecessor's channel concurrently. Live back-pressure flows end-to-end:

- A slow downstream operator delays Merge consumption, which fills the predecessor channels, which blocks the Source reader tasks.
- A fast input does not wait on a slow peer — the Merge schedules whichever channel has a ready record.

When predecessors are not all Sources (e.g. Transform → Merge), fusion does not apply and the Merge consumes pre-buffered predecessor outputs in round-robin order; live back-pressure across the Merge boundary itself is unavailable in that shape, though the upstream operator's own bounded buffer still throttles its predecessors.

Unseeded `interleave` is the right choice when end-to-end latency matters and the downstream consumer is order-insensitive (e.g. an aggregator grouping on a key, or a writer that does not assert on row sequencing).

### `interleave` (seeded)

The seeded path **does not preserve live back-pressure across the Merge**: it pre-buffers each predecessor's full output into a `Vec` before emitting in `fastrand`-driven order. A slow consumer downstream of a seeded Merge will not throttle the Source readers while the buffers are still filling.

If you need both run-to-run determinism and live back-pressure, prefer asserting on the multiset of records rather than their sequence and use unseeded `interleave`, or fall back to `concat` over deterministically-declared inputs.

## Record ordering

Records arrive in the order described by the mode in use — see [Modes](#modes) and [Back-pressure semantics](#back-pressure-semantics) above. If you need sorted output regardless of merge mode, apply a `sort_order` on the downstream output node.

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

## Schema constraints across inputs

Merge concatenates streams positionally against the merge node's `output_schema` (taken from the first input). Every input must therefore agree on column shape — same column names, same `on_unmapped` policy, same `correlation_key` set.

Disagreement on the `$widened` `auto_widen` sidecar (one source uses `auto_widen`, another uses `drop` / `reject`) fails compile with **E315**. See [Auto-Widen & Schema Drift → E315](../formats/auto-widen.md#e315--merge-inputs-must-agree-on-policy) for the full diagnostic shape and remediation.
