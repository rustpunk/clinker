# Memory Arbitration & Scheduling

*User-facing view: the User Guide's "Memory Tuning" page.*

This page is the engine-internals reference for how Clinker tracks, attributes, and reclaims memory at runtime, and how it orders simultaneously-runnable nodes to keep the resident working set bounded. It covers the `MemoryConsumer` wrapper registry, pull-mode byte attribution, the per-operator arbitration parameters the active policy reads, the bounded-memory contract for materialized stages, the `predicted_*` values that feed both `--explain` and the scheduler, and the four ranking rules the scheduler applies (with its fallback to topological order). The user-facing knobs — the `memory:` block, the `--memory-limit` flag, the backpressure-policy selection, sizing guidance, and monitoring — live in the User Guide and are intentionally not repeated here. For how each stage's buffer class (`streaming` vs `materialized`) is decided, see [Streaming vs. Blocking Stages](execution-model.md).

## How it works

Clinker tracks memory in two layers. RSS (resident set size) is sampled at chunk boundaries and supplies the primary spill / abort signal. Alongside RSS, every memory-touching operator (Source ingest channels, Aggregate hash maps, sort buffers, grace-hash partitions, sort-merge accumulators, IEJoin arrays, inline-Combine hash tables, the Reshape per-group input buffer, `node_buffers` slots, and window-runtime arenas) registers a `MemoryConsumer` wrapper with the pipeline-scoped arbitrator. Each operator owns its live byte counter and updates it on every admit / spill transition; the arbitrator queries `current_usage()` per consumer at every policy poll. This pull-mode attribution lets the policy distinguish *reclaimable* bytes (what an operator can give up right now) from currently-held bytes — a grace-hash with on-disk partitions, for instance, reports only its in-memory portion, and the Reshape buffer reports the live bytes of the groups still resident in memory.

Window-runtime arenas (the columnar backing store that analytic-window evaluation reads from) are attributed but not independently spillable: an arena is immutable once built and is freed only indirectly, when the operator that consumes its windows drains to disk. Its wrapper reports the arena's bytes so the arbitrator's attribution is complete, but ranks last among spill victims so a policy never elects an arena while any consumer that can actually pause or spill remains.

### Per-operator arbitration parameters

Each registered consumer carries two parameters the active policy reads: a **spill priority** (lower is spilled first under `Priority`) and a **back-pressure flag** (whether its producer can be paused instead). The defaults are:

| Operator class | `spill_priority` | `can_back_pressure` |
|----------------|------------------|---------------------|
| `node_buffers` slot (inter-stage buffer) | 0 | false |
| grace-hash Combine | 10 | false |
| Reshape | 15 | false |
| sort buffer / IEJoin build | 20 | false |
| sort-merge Combine | 25 | false |
| hash Aggregate | 30 | false |
| inline-hash Combine | 30 | false |
| Source ingest | N/A | true |
| streaming Aggregate | N/A | false |
| window arena | last | false |

Lower priority is spilled first, so `node_buffers` slots (priority 0) are the cheapest victim class — spilling an inter-stage buffer to disk costs one LZ4 + postcard round-trip and frees the most reclaimable bytes per call. The blocking operators climb from there: a grace-hash Combine (10) is preferred over Reshape (15), which is preferred over a sort buffer (20), which is preferred over a hash Aggregate or inline-hash Combine (30). Reshape sits between grace-hash and sort because its spill round-trip re-runs synthesis on reload — costlier to evict than grace partitions, cheaper than an external-sort merge — and it spills the raw per-group input records rather than post-processed output.

A **Source** and a **streaming Aggregate** show `spill_priority=N/A` because neither *operator* holds spillable accumulated state. A Source's `try_spill` always frees zero bytes — its only real lever is the pause its `can_back_pressure=true` advertises. A streaming Aggregate emits each group as it completes and never accumulates a spillable group table. The `N/A` here is about the operator's own state, not its downstream handoff: when a streaming stage's output rides a per-batch streaming handoff to a single consumer, that handoff registers a priority-0 consumer just like a `node_buffers` slot does, and its in-flight batches are spilled to disk one batch at a time if RSS crosses the soft threshold while they are in flight. So a streaming Aggregate's *group table* is never a spill victim, but the batches it hands downstream can be.

When memory pressure crosses the soft threshold (80 % of `limit`), the arbitrator runs the active policy to pick a victim and invokes the corresponding action: `pause()` on a back-pressureable consumer (its producer's hot loop parks on a `Condvar` until `resume`), or `try_spill(target_bytes)` on a spillable consumer (the consumer's wrapper flips a spill-requested flag the operator reads at its next batch boundary). When RSS crosses the hard limit, the engine fails fast with `E310 MemoryBudgetExceeded`.

This means:

- Pipelines always complete if disk space is available, regardless of input size.
- Performance degrades gracefully under memory pressure — you will see slower execution (and possibly disk I/O), not failures.
- The memory limit is a soft ceiling, not a hard wall. Momentary spikes may briefly exceed the limit before the policy fires.

## Bounded-memory contract for non-fused stages

A stage runs streaming — no charged per-stage `node_buffers` slot — when it hands its output to a single downstream sink Output and roots no window: fused Source → Transform → Output and Merge.interleave-of-Sources chains, plus single-branch Route, non-fused Merge, `streaming`-strategy Aggregate, and hash-build-probe Combine probe-side feeding one Output (see [Streaming vs. Blocking Stages](execution-model.md)). The remaining boundaries — multi-branch Route fan-out, a Merge or other operator whose output forks to several consumers, Composition bodies, diamond DAGs, and every blocking strategy — materialize records into per-stage `node_buffers`. Each slot registers a `NodeBufferConsumer` with the arbitrator (priority 0 — the cheapest-to-spill victim class), so the active policy's victim selection is fully attributed.

When a buffer crosses the soft threshold (80 % of the limit) the arbitrator runs the active policy. Under the default `pause`, the producer feeding the buffer is paused at its inbound channel; under `spill` or when no consumer can be paused, the slot spills to disk using the same LZ4 + postcard frame format as grace-hash sort partitions. When RSS crosses the hard limit, the engine fails fast with `E310 MemoryBudgetExceeded { node }` naming the operator whose hot loop polled the abort gate. The `explain --code E310` diagnostic covers the full diagnostic model, including the composition-involved two-shape error model.

Spill fires at the producer side of the first slot whose downstream topology permits it — single-consumer, port-less. For a Source feeding a Route, that's the Source's own slot, not the Route's per-branch slots, because the Source has the one outgoing edge that satisfies the topology rule. Per-branch slots can still spill independently when their own row-distribution drives them past the soft threshold, but the canonical case lands at the producer.

Use `clinker run --explain` to predict which stages will dominate the budget before runtime — each node carries a `buffer: streaming | materialized` annotation. Materialized nodes charge `pipeline.memory.limit` as one full-stage slot and spill the whole stage; streaming nodes charge per in-flight batch and, on a single-consumer edge, spill those batches one at a time. Both classes count against the limit and can spill — the annotation tells you the *granularity* (whole-stage vs. per-batch), not whether a stage is exempt from the budget.

## Reading `--explain` arbitration output

Alongside the `buffer:` class, every node in the **Physical Properties** stanza of `--explain` carries an `arbitration:` line giving the [per-operator parameters](#per-operator-arbitration-parameters) the arbitrator would apply at runtime. The numbers are derived at plan time — `--explain` does no I/O, so there are no live consumers to query — but they mirror the runtime values exactly, so an author can read the spill/pause model before running the pipeline.

For a fast Source feeding a slow Aggregate (the canonical bounded-memory shape), the relevant lines read:

```text
=== Physical Properties ===

source.orders:
  buffer: materialized
  arbitration: spill_priority=N/A, can_back_pressure=true, predicted_peak=1K, predicted_freed=0B, predicted_subtree_reclaim=1K

aggregation.dept_totals:
  buffer: materialized
  arbitration: spill_priority=30, can_back_pressure=false, predicted_peak=1K, predicted_freed=1K, predicted_subtree_reclaim=1K
```

The Source advertises `can_back_pressure=true` and `spill_priority=N/A`: when memory pressure rises, the arbitrator pauses the Source rather than asking it to spill (it has nothing to free). The hash Aggregate advertises the opposite — `spill_priority=30`, `can_back_pressure=false` — so it is a spill victim, ranked behind any cheaper consumer.

The three `predicted_*` values are the scheduler's inputs (see [Scheduling](#scheduling) below). `predicted_peak` is the live volume a node is expected to hold at its peak — seeded at a file-backed Source from its `path:` file's on-disk size and propagated forward. `predicted_freed` is what the node returns to the budget the instant it finishes draining: a blocking Aggregate holds its whole accumulated input (`predicted_peak=1K`) and frees it on drain (`predicted_freed=1K`), while a streaming Source carries the volume through but frees nothing the instant it drains (`predicted_freed=0B`). `predicted_subtree_reclaim` is the largest reclaim the node's downstream chain eventually unlocks: the Source frees nothing itself, but launching it is the only way to reach the point where its downstream Aggregate can drain, so it inherits that Aggregate's reclaim (`predicted_subtree_reclaim=1K`). Propagation of the subtree value stops at a convergence node — the Combine two independent chains feed — so each feeding chain keeps the distinct reclaim it owns up to the join rather than the shared post-join total. All three render `0B` when no file-size seed reached the node — a multi-file (`glob`/`regex`/`paths`) or absent/unreadable Source, or any node downstream of one. The bytes are formatted in the same binary-prefix units as `memory.limit` (`1K`, `64M`, `2G`), and the same three values appear in `--explain --format json` under `node_properties.<name>.predicted_peak_bytes`, `predicted_freed_bytes_on_complete`, and `predicted_subtree_reclaim_bytes`.

A **`=== Buffer Edges ===`** section follows, listing the `node_buffers` slot between each pair of non-fused stages. Every slot is a priority-0, non-back-pressureable `NodeBufferConsumer` — the cheapest victim class — and the `slot=` number is the stable index the executor admits into. The slot carries the producer's predicted volume (it holds the producer's materialized output and frees that whole buffer once the consumer drains it):

```text
=== Buffer Edges ===

edge source.orders -> aggregation.dept_totals:
  buffer: node_buffer (slot=0)
  arbitration: spill_priority=0, can_back_pressure=false, predicted_peak=1K, predicted_freed=1K (producer: source)
```

Reading top to bottom: under memory pressure the arbitrator first spills the inter-stage buffer (priority 0), then — if the soft threshold is still tripped — pauses the Source before it ever forces the Aggregate (priority 30) to spill. That ordering is exactly what the default `pause` policy (`BackPressurePreferred -> Priority`) encodes. Cross-reference the [per-operator table](#per-operator-arbitration-parameters) to see where any operator in your own pipeline lands.

## Scheduling

When a pipeline has several nodes that are *simultaneously runnable* — every one of their inputs is ready, so the executor could legally run any of them next — the engine picks one deterministically rather than walking topological position blindly. The common case is a single linear chain where only one node is ever runnable at a time, and there is nothing to choose. The choice matters only for a pipeline whose DAG has **multiple independent subgraphs** (for example, two unrelated Source → Aggregate branches that a later Combine or Merge joins): both branches' lead nodes become runnable together.

The engine runs one node to completion before dispatching the next. When two independent chains converge — two Source → Aggregate branches a later Combine joins — both branches' outputs must be materialized and held until the Combine consumes them, so the chain that runs *second* builds its working set while the *first* chain's output already sits in a buffer. Running the memory-heaviest chain first therefore drains and releases its large state before the lighter chain's output has to coexist with it, lowering the peak resident working set; running it last makes its large state coexist with the already-materialized output of every chain that finished before it. What the ranking also buys is *when* the frontier offers a mix of node kinds: with a blocking operator ready to drain (and reclaim its accumulated state) alongside a fresh Source about to charge a new buffer, draining first reclaims headroom before the new charge lands, and under a tight budget the engine prefers the runnable node that fits the remaining headroom over one that would overflow it.

The engine ranks the simultaneously-runnable nodes by these rules, in order:

1. **Headroom fit.** A node whose `predicted_peak` fits within the budget's remaining headroom is preferred over one that does not. Running a node that fits avoids tipping the live working set over the soft threshold and forcing a spill that a different ordering would have avoided. A node with an *unknown* peak (`predicted_peak=0B` — no file-size seed reached it) counts as fitting, because `0` is always within any headroom; this keeps an unestimated pipeline on its topological order rather than deprioritizing every node.

2. **Immediate-freed tiebreak.** Among nodes that fit equally, the one with the larger `predicted_freed` runs first. Finishing a node that returns more bytes to the budget *the instant it completes* maximizes the headroom available to everything still waiting — the same intuition as shortest-remaining-state-first. A ready blocking operator (which reclaims its accumulated state now) therefore wins over a fresh Source (which frees nothing the instant it drains), because the immediate reclaim is the headroom-minimizing choice.

3. **Subtree-reclaim tiebreak.** Among nodes that also tie on immediate freed — most importantly the fresh Sources of independent chains, which all free `0` the instant they drain — the one with the larger `predicted_subtree_reclaim` runs first. This front-loads the chain whose completion eventually frees the most: a Source's value is the reclaim its downstream Aggregate will release, so the heavier chain's Source is dispatched ahead of the lighter one even when it sorts later in topological order. Because it ranks *below* immediate freed, it never elects a fresh heavy Source over a ready light Aggregate (which would raise the peak), only between candidates whose immediate reclaim is equal.

4. **Stable-index tiebreak.** If two nodes still tie (equal fit, equal immediate freed, equal subtree reclaim — including the all-unknown case where all are `0`), the one with the lower stable node index wins. The index is each node's position in the plan's topological order — the exact sequence the executor walks the DAG — so this tiebreak is fully deterministic and independent of the machine, the thread schedule, and the order the runnable set happened to be assembled in.

**Fallback to topological order.** When no node carries a volume estimate (every `predicted_peak` is `0B`), rules 1–3 are no-ops — every node fits and every node frees the same `0` — so rule 4 alone decides, and the engine runs nodes in exactly the lowest-index / topological order it used before any volume estimates existed. This is the load-bearing guarantee: **scheduling never changes record output or branching order.** A pipeline's data output is byte-identical regardless of the predictions; the estimates only steer *which runnable node goes first* to reclaim headroom sooner, front-load the heaviest chain, and prefer fitting nodes under pressure, never *what* each node computes.

Because the predictions are a pure function of the plan shape and the input files' on-disk sizes (resolved against the pipeline file's directory, never the process working directory), the scheduling decision is identical on every machine for an identical plan over identically-sized inputs.
