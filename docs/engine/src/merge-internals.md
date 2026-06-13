# Merge & Back-pressure

Merge concatenates upstream branches that share a schema into a single stream. For the engine, the interesting surface is not the YAML — it is where Merge *fuses* into the Source ingest loop, where the seeded-interleave path deliberately opts out of that fused channel topology, and how back-pressure propagates (or fails to) through the bounded `mpsc` channels behind each mode. This page covers those mechanics.

*User-facing view: the User Guide's "Merge Nodes" page.*

## Fusion of `interleave` over Sources

When every direct predecessor of an **unseeded** `interleave` Merge is a `Source` node, the executor **fuses** the Merge into the source ingest loop. The predecessor channels are polled directly and Merge consumption proceeds at live ingest rate, with no intermediate buffering tier between the Source readers and the Merge arm.

This fused live-channel path is what makes end-to-end back-pressure possible across the Merge boundary (see [Back-pressure semantics](#back-pressure-semantics) below). It is also the same predicate the streaming-output path checks before it engages — see [Streaming Output Writes](output-internals.md).

When the predecessors are not *all* Sources (e.g. `Transform → Merge`), fusion does not apply: the Merge consumes pre-buffered predecessor outputs in round-robin order, and live back-pressure across the Merge boundary itself is unavailable in that shape (though the upstream operator's own bounded buffer still throttles *its* predecessors).

## Seeded interleave

Snapshot tests and benchmarks that need reproducible cross-input ordering opt into a deterministic schedule via `interleave_seed:`:

```yaml
- type: merge
  name: combined
  inputs: [east, west]
  config:
    mode: interleave
    interleave_seed: 42
```

A seeded interleave **bypasses the fused live-channel path** entirely. Instead of polling predecessor channels at ingest rate, the Merge:

1. **Pre-buffers each predecessor's full output into a `Vec`.**
2. Emits records in **`fastrand`-driven order**, seeded by `interleave_seed`.

Output is reproducible regardless of upstream timing. The cost is that the seeded path **opts out of live back-pressure across this Merge** — the buffers fill to completion before emission begins, so a slow downstream consumer cannot throttle the Source readers while those `Vec`s are still filling.

## Back-pressure semantics

How a slow consumer or a slow upstream reader propagates back through the DAG depends entirely on the Merge mode.

### `concat`

Each Source ingest task pushes into its own **bounded `mpsc` channel, capacity 1024 records per Source**. Peer sources produce *concurrently* up to that capacity — the dispatch arm consumes from `inputs[0]`'s channel before turning to `inputs[1]`'s.

Consequences:

- **Memory.** A non-leading input can hold up to one channel's worth of buffered records (1024) before its producer blocks. Multi-input `concat` over `N` Sources may carry up to `(N − 1) × 1024` records in flight even while only one input is being drained.
- **Latency.** A record produced by `inputs[1]` while `inputs[0]` is still draining will not reach output until `inputs[0]` finishes, regardless of how fast it was produced.
- **Producer-side back-pressure.** When a non-leading input's channel fills, its reader blocks at **`blocking_send`**, propagating pressure back to the upstream file/network reader. The upstream is throttled even though it is not the currently-consumed input.

`concat` is the right choice when downstream consumers depend on declaration-ordered records (snapshot tests asserting byte-identical output) or when inputs represent ordered time partitions that must remain contiguous.

### `interleave` (unseeded)

Fused with Source predecessors, the Merge arm **polls every predecessor's channel concurrently**. Live back-pressure flows end-to-end:

- A slow downstream operator delays Merge consumption → the predecessor channels fill → the Source reader tasks block.
- A fast input does not wait on a slow peer — the Merge schedules whichever channel has a ready record.

When predecessors are not all Sources, fusion does not apply: the Merge consumes pre-buffered predecessor outputs in round-robin order, and live back-pressure across the Merge boundary itself is unavailable, though each upstream operator's own bounded buffer still throttles its predecessors.

Unseeded `interleave` is the right choice when end-to-end latency matters and the downstream consumer is order-insensitive (an aggregator grouping on a key, or a writer that does not assert on row sequencing).

### `interleave` (seeded)

The seeded path **does not preserve live back-pressure across the Merge**. It pre-buffers each predecessor's full output into a `Vec` before emitting in `fastrand`-driven order, so a slow consumer downstream of a seeded Merge will not throttle the Source readers while the buffers are still filling.

If you need *both* run-to-run determinism *and* live back-pressure, prefer asserting on the multiset of records rather than their sequence and use unseeded `interleave`, or fall back to `concat` over deterministically-declared inputs.
