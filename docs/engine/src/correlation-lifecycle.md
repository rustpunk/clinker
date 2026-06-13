# Correlation Key Lifecycle & Rollback Narrowing

This page is the engineer's reference for how a correlation key is born, carried, and unwound inside the executor. A correlation key declares a set of records from a single source as an atomic group: if any record in the group fails validation or processing, the whole group is sent to the DLQ. The mechanics here are the lineage substrate — `$ck.<field>` shadow columns, `(row_id, source_name)` lineage pairs, the `per_source_rollback_cursors` map, and Combine input snapshots — that the [Retraction Protocol](retraction-protocol.md) builds on. Where this page describes *how identity is tracked and how a failure narrows the rollback*, the retraction protocol describes *how an aggregate refinalizes without DLQ'ing whole groups*.

*User-facing view: the User Guide's "Correlation Keys" page.*

## Lifecycle

The engine adds a shadow column named `$ck.<field>` (one per correlation-key field) to every declaring source's schema and copies the field's value into it at ingest. From that point on, the shadow column is the authoritative group identity — if a downstream transform rewrites the user-declared correlation field, the shadow column is untouched and the group identity is preserved.

Shadow columns are an internal engine namespace. You never write `$ck.<field>` in YAML or CXL — the engine manages them. They are stripped from default writer output. To surface them for debugging, set `include_correlation_keys: true` on an output node:

```yaml
- type: output
  name: debug_out
  input: validate
  config:
    name: debug_out
    type: csv
    path: "./debug.csv"
    include_correlation_keys: true
```

A correlation key is declared per source: each source's `config:` block carries an optional `correlation_key:` field naming the column (or list of columns) whose value identifies a record's correlation group within that source. The engine widens each declaring source's schema with one `$ck.<field>` shadow column per field and stamps the user-declared value into it at ingest. A record's correlation group is identified by the tuple of values for that source's listed fields; records sharing the same tuple within the same source belong to the same group. There is no pipeline-level correlation key. A source whose declared `correlation_key:` field names a column not present in its own `schema:` block is rejected at compile time with diagnostic E153.

## Multi-source pipelines

Different sources can declare different correlation-key fields. The engine treats each source's CK identity as locally consistent: a record from `customers` is a member of the customer-id group named in its row, and a record from `orders` is a member of the order-id group named in its row, regardless of whether `customer_id` appears in `orders` or vice versa. Combine and Merge nodes that join across sources negotiate which CK columns survive into the joined output via the Combine node's `propagate_ck:` field (see [Combine Join Strategies](combine-internals.md)).

A source that declares no `correlation_key:` carries no `$ck.*` widening. Records from such a source flow through the pipeline without group identity; per-record errors DLQ on a per-record basis with no group fan-out. The orchestrator's relaxed-aggregate retraction protocol still activates if any *other* source on the same DAG carries a CK field that an aggregate's `group_by` omits — the retraction protocol scope is the DAG's lattice of `$ck.*` columns, not any single source's declaration.

## DLQ semantics

When a record fails inside a correlation group:

- The failing record produces a **trigger** DLQ entry. Its category reflects the actual failure (e.g. `type_error`, `validation_failed`).
- Every other record **from a source that contributed a trigger** to the same group produces a **collateral** DLQ entry. Collaterals carry the category `correlated`.
- Records belonging to other (clean) groups proceed normally.

A record with a null value for the correlation-key field is treated as its own per-record group: it has no peers and DLQ atomicity does not span multiple records.

A Combine output-row eval failure that the engine recovers from (under `continue` / `best_effort`, in the hash build-probe inline arm) produces entries under the `combine_output_row` category — distinct from the upstream-Transform `type_coercion_failure` because the entry carries the contributing-build lineage and rewinds both the driver and the matched build source's rollback cursor. See [Per-source rollback narrowing](#per-source-rollback-narrowing) below for the cursor-rewind detail.

The `dlq_count` counter sums triggers and collaterals.

## Per-source rollback narrowing

When two sources contribute records to the same correlation group, a failure originating from one source does NOT collaterally DLQ records from the OTHER source. The collateral fan-out is scoped to the failing source's records only.

Concretely, consider `[src_a, src_b] → merge → tfm → out` with both sources declaring `correlation_key: id`. A mid-stream Transform error fires on every `src_b` record but leaves `src_a` records untouched:

```yaml
- type: transform
  name: tfm
  input: m
  config:
    cxl: |
      emit id = id
      emit ratio = if($source.name == "src_b") then (1 / 0) else amt
```

Under per-source rollback, the dirty correlation group for each `id` value contains:

- One **trigger** DLQ entry — the `src_b` row that hit `1 / 0`.
- The `src_a` row sharing the same `id` is **spared** and reaches the output.

The engine identifies origin per record via the engine-stamped `$source.name` column. Within the failing source's records, the existing `CorrelationFanoutPolicy` (`Any` / `All` / `Primary`) determines which records DLQ — the policy semantics are unchanged. Single-source pipelines see bit-identical behavior to the pre-narrowing engine because every co-grouped record shares the failing source by construction.

Records that carry no single-source attribution — synthetic aggregate emits and Combine output rows — are NOT spared by per-source narrowing. They flow through the existing collateral path because their stamp falls back to the merged-source identity which is ambiguous about origin.

The engine also surfaces a `per_source_rollback_cursors` map on the `ExecutionReport`, keyed by source name and carrying the highest source row number that cleanly exited a forward operator. The map advances per record at the clean exit of Transform / Route / Aggregate, and rewinds per contributing source on `max_group_buffer` overflow to the lowest `row_num` any group member of that source contributed. Sources whose records all DLQ never land in the map. The map is the replay anchor for per-source resume: a downstream rerun reads each source's cursor as the floor for what must be reprocessed.

On `max_group_buffer` overflow, every record in the overflowing group still lands in DLQ (one `GroupSizeExceeded` trigger plus per-row collaterals), but the per-source rollback cursor rewinds independently per contributing source. Attributing the overflow failure itself to one source would be a fiction — every contributing source shared blame proportionally — so the DLQ shape stays group-wide while the rewind narrows per source.

The relaxed-CK aggregator's per-row lineage carries `(row_id, source_name)` pairs so a finalize-time retract scoped to one source rewinds only that source's contributions to each affected group. The source half of the pair is load-bearing under multi-source ingest: each source numbers its rows from its own monotonic counter, so two sources that both feed the same aggregate group can contribute records at identical `row_id` values. Pairing the row id with its source keeps `src_a`'s row 1 distinct from `src_b`'s row 1 when both land in one group, so a retract that must remove both reaches each one instead of collapsing the colliding ids and stranding the second source's contribution.

Combine input snapshots are captured at fold start and cleared at every Combine arm's exit (inline, IEJoin, GraceHash, SortMerge). When a Combine output-row eval fails recoverably under `continue` / `best_effort` in the hash build-probe (inline) arm — a probe-key, residual-filter, or matched / `on_miss: null_fields` body failure on one driver row — the snapshot restores each contributing source's rollback cursor to the value it held at the start of the fold (its pre-fold floor), lowering the cursor only if it had since advanced, then routes the row to the DLQ under the `combine_output_row` category. Only the sources that fed the failing row rewind; co-folded sources that did not contribute keep their forward progress. The IEJoin, grace-hash, and sort-merge arms propagate an output-eval failure as fail-fast regardless of strategy. See [Combine Join Strategies](combine-internals.md) for the per-arm execution detail.

## Group buffering

The engine buffers records per correlation group until either the group completes (all source records observed) or a failure triggers a flush. The `max_group_buffer:` field on the pipeline-level `error_handling:` block caps per-group buffering across every source's groups:

```yaml
error_handling:
  max_group_buffer: 100000     # Default: 100,000
```

Groups that exceed the cap are DLQ'd entirely with a `group_size_exceeded` trigger plus a collateral entry per buffered record. This is a backpressure boundary, not a hard error. The interaction with the `per_source_rollback_cursors` map on overflow is described under [Per-source rollback narrowing](#per-source-rollback-narrowing) above: the DLQ shape stays group-wide while the cursor rewind narrows per contributing source.

## See also

- [The Retraction Protocol](retraction-protocol.md) — how relaxed-CK aggregates refinalize affected groups instead of DLQ'ing them wholesale, and how the synthetic `$ck.aggregate.<name>` column lifts the post-aggregate retract path.
- [Operator Retraction Cost Reference](retraction-cost-reference.md) — the per-operator memory/CPU footprint under retraction, plus the `=== Retraction ===` explain block and the `correlation.retract.*` metrics counters.
- [Combine Join Strategies](combine-internals.md) — `propagate_ck:` semantics, match modes, and the per-arm snapshot/rewind behavior.
- [Memory Arbitration & Scheduling](memory-arbitration.md) — how the RSS budget and spill thresholds interact with group buffers.
