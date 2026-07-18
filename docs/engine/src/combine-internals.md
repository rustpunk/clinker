# Combine Join Strategies

Combine is the N-ary record-combining operator: every input is declared up front, the `where:` predicate matches records across inputs, and the `cxl:` body shapes the output row. This page covers the parts an engine engineer reaches for when reasoning about *how* a Combine executes — the strategy selection the planner performs from the predicate shape, the heap cost of materializing build sides, how reconciled document boundaries flow through every join path, the runtime mechanics of correlation-key propagation across all four execution paths, and the join-planner statistics catalog that drives build-side selection and grace-hash partitioning.

*User-facing view: the User Guide's "Combine Nodes" page.*

## Predicate classification

The `where:` expression is a CXL boolean evaluated for every candidate record pair across inputs. The planner splits a compound `and`-predicate into three conjunct classes, and the classification is what selects the execution strategy:

- **Equi conjunct** — a cross-input equality (`a.x == b.y`). Drives the hash lookup or the sort-merge join.
- **Range conjunct** — a cross-input ordered comparison (`a.start <= b.ts and b.ts <= a.end`). Handled by the IEJoin algorithm when no equi conjunct constrains the same input pair.
- **Residual conjunct** — any other CXL predicate (intra-input filter, function call, and so on). Applied as a post-filter after the equi/range match succeeds.

At least one cross-input equality is required for every Combine, except for pure-range predicates, which IEJoin handles without an equi conjunct.

## Match selection versus the projection body

Every strategy separates two steps: **match selection** (the `where:` predicate, including any residual re-check) chooses which build records pair with a driver, and the **`cxl:` body** projects each selected pair into an output row. Under `match: first` this distinction is load-bearing and is contractually uniform across all five physical paths — in-memory hash build-probe, grace-hash, sort-merge, hash-partitioned IEJoin (equi+range), and block-band IEJoin (pure-range):

- **Selection is deterministic and budget-stable.** `first` picks a single predicate-matching build in a fixed order — the block-band path holds the minimum build-input-index candidate, sort-merge takes the lowest range-key match, and the hash paths take the first probe hit — so the chosen build is a function of the data, not of the memory-derived block/spill layout.
- **The body runs once, as a post-match projection.** A body that skips the chosen build (a `filter` that fails, an `EvalResult::Skip`) or defers on a recoverable error drops **only that output row**. The engine does not retry a later matching build, and it does not route the driver to `on_miss`: the driver matched the predicate, so it is not a zero-match driver. `on_miss` dispatch is gated purely on whether *selection* found anything — the hash and grace paths test `matched.is_empty()`, the block path routes only drivers that held no candidate, and sort-merge / equi+range mark the driver satisfied the moment a predicate-matching build is selected, before the body runs.

The earlier divergence — where the IEJoin and sort-merge paths retried later builds or routed an all-body-skip driver to `on_miss` while the hash paths did not — is retired; all strategies now match the hash paths and the User Guide.

## Strategy hint

The `strategy` config field carries a hint; the planner has final say.

| Value | Behavior |
|-------|----------|
| `auto` (default) | Planner picks a strategy from the predicate shape. Hash join for equi predicates; IEJoin for pure-range predicates. |
| `grace_hash` | Force grace hash join (disk-spilling partitioned hash). Applies only to pure-equi predicates; ignored on predicates carrying range conjuncts. |

`grace_hash` is the right hint when build-side inputs are larger than the memory budget but fit on disk after partitioning. It is mostly an explicit performance assertion rather than a behavioral switch: the planner **falls back automatically to grace-hash spill** when an in-memory hash table approaches the RSS soft limit. So `strategy: grace_hash` on a build side that would have spilled anyway changes nothing operationally — it documents the author's intent and pins the strategy regardless of the plan-time size estimate.

The choice of in-memory hash versus grace-hash for a pure-equality Combine is driven by the build-side row-count estimate (see [Join-planner statistics](#join-planner-statistics) below): a build side large enough to risk overrunning the memory limit is what tips the planner from the in-memory hash strategy to the disk-spilling grace-hash strategy.

## Memory considerations

Build-side inputs are materialized in memory as hash tables keyed by the equi columns. For each non-driving input, plan for roughly **1.5–2× the raw CSV size in heap**. A 50 MB product catalog typically occupies 75–100 MB of hash-table memory — the multiplier covers the per-key `Value` boxing, the bucket array overhead, and the per-entry chaining structure on top of the raw payload bytes.

This heap cost is the quantity the memory arbitrator charges against `pipeline.memory.limit`, and it is what the soft/hard threshold machinery watches when deciding whether to flip a pure-equi Combine to grace-hash spill. See [Memory Arbitration & Scheduling](memory-arbitration.md) for the spill thresholds, the back-pressure knob, and strategy overrides.

## Document boundaries

A Combine forwards **reconciled document boundaries** to its output on *every* strategy — the inline hash build-probe, IEJoin, grace-hash, sort-merge, and the streaming-probe path. The boundary semantics are uniform across the strategy matrix, so a downstream operator never has to know which join algorithm ran.

Concretely:

- A per-document `Aggregate` downstream of a join flushes per document. A driver source that carries several documents (a `glob:` over monthly files, say) produces one roll-up per driver document *after* the join, not one fold spanning all of them.
- A document that spans both join inputs — the same document carried on the driver and on the build side — opens and closes exactly once downstream. The boundary is reconciled, never double-fired: the join does not emit a separate open/close for the driver-side and build-side appearances of the same document.

This reconciliation is what lets the per-document aggregation model compose with joins without special-casing the operator order.

## Correlation-key propagation

Combine declares which correlation-key columns its output rows carry via the required `propagate_ck` field. The choice shapes **both** the compile-time output schema and the runtime record builder — those are the two internal surfaces an engine engineer touches when changing CK behavior.

| `propagate_ck` value | Compile-time output schema | Runtime record builder |
|----------------------|----------------------------|------------------------|
| `driver` | Carries only the driver input's `$ck.<field>` columns. | Build-side records contribute body fields only; their CK identity is consumed by the match and not copied onto the output row. |
| `all` | Carries every input's `$ck.<field>` columns. | Copies build-side CK values onto each output row alongside the body's `emit` columns. Use when the build side carries CK fields downstream operators must read. |
| `{ named: [<field>, ...] }` | Carries the explicit subset, intersected with what is actually present upstream. | Copies exactly the named subset. Use to project a multi-field CK down to a single field after a join. |

**Driver wins on a name collision.** If both the driver and a build input declare `$ck.<field>`, the column appears once on the output schema and the runtime keeps the driver's value.

`propagate_ck` is required on every Combine; a pipeline without an explicit value fails to compile.

### Match-mode interaction across the strategy paths

The propagation contract holds identically across the hash build-probe, IEJoin, grace-hash, and sort-merge paths — the record builder is shared, so a build-side CK value lands on the output row the same way regardless of which algorithm produced the match. The interaction that *does* vary is by match mode rather than by strategy:

- `match: first` / `match: all` — each emitted row is one driver × one build pairing, so the propagated `$ck.<field>` slot holds a single value (the driver's, or the build's, per the table above).
- `match: collect` — the propagated CK slot is **single-valued** (it tracks the driver's correlation-group identity), while the collected array column preserves the **full lineage** of every build match. The single-valued slot and the array column are distinct: the slot answers "which correlation group does this output row belong to," the array answers "which build records were gathered."

See the User Guide's correlation-keys reference for the per-mode lifecycle narrative; the lifecycle and rollback-narrowing mechanics on the engine side are in [Correlation Keys: Lifecycle & Rollback Narrowing](correlation-lifecycle.md).

## Join-planner statistics

When the plan carries column statistics, `--explain` ends with a `=== Statistics ===` section listing the planner-wide statistics catalog. These are the figures that drive build-side selection and grace-hash partition-bit choice, so they belong to the join planner. Every figure is tagged with its provenance, so a metadata-derived estimate is distinguishable from a record-exact measurement.

### Row counts — `[file metadata]` vs `[exec sketch]`

One line per source node, for example:

```
orders: ≈90 rows [file metadata] (informs combine build/probe + partition bits)
```

- A **`[file metadata]`** figure is derived at plan time by dividing the input file's on-disk byte length by an average-record-bytes constant, *before any record is read*. This is the same row count that drives a Combine's build-side selection and its grace-hash partition-bit choice. A build side large enough to risk overrunning the memory limit is what tips a pure-equality Combine from the in-memory hash strategy to the disk-spilling grace-hash strategy.
- An **`[exec sketch]`** figure is the exact count a source measured *during* a run, superseding the plan-time estimate.

Row counts also appear inline on each Combine's driving and build inputs (`est. 90 [file metadata] rows`).

### Column sketches — distinct counts, heavy hitters, membership filters

Three sketch kinds are populated by operators while records flow. All three are maintained by the **grace-hash Combine over its build-side join keys**, recorded under the build input's `(node, column)`:

- **Distinct-count estimate** — `product_id: 12,431 distinct [exec sketch]`.
- **Top-k heavy-hitter list** with lower-bound counts — `product_id: heavy hitters [exec sketch, lower bound]: widget=9,000, gadget=3,200, ...`. The list is a **lower bound** on frequency: a value absent from it may still be frequent, so it is only ever used to *promote* a key, never to *exclude* one.
- **Membership filter** — `product_id: membership filter, 119048 bits / 7 probes [exec sketch, sized from estimate]`. Sized up front from the build node's plan-time row-count estimate, built in the single build pass with **no per-row buffer**, and **skipped entirely when no plan-time estimate is available**.

### Honest nulls and missing sections

A statistic that was never gathered renders as `null` rather than a fabricated zero. A plan over sources whose sizes cannot be read — a `glob`/`regex` multi-file source, a network source, or a missing/unreadable file — adds **no Statistics section at all**, and (per the membership-filter rule above) skips the membership filter that the plan-time estimate would have sized. Confirm the live shape via `clinker metrics collect` after the first production run, since the planner has no group-cardinality side-table to consult before the run.
