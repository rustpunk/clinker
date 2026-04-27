# Correlation Keys

A correlation key declares a set of records as an atomic group: if any record in the group fails validation or processing, the whole group is sent to the DLQ. This is the right shape for transactional data where partial processing is worse than total rejection -- the canonical example is an order with multiple line items where one bad line should reject the entire order.

This page describes the full lifecycle of a correlation key and how it interacts with each operator that can fan out, fan in, group, or join records.

## Declaration

`correlation_key:` lives at the pipeline level under `error_handling:`. It applies to every record flowing through the pipeline.

```yaml
error_handling:
  strategy: continue
  correlation_key: order_id
  dlq:
    path: "./output/rejected.csv"
```

Compound keys are declared as a list:

```yaml
error_handling:
  correlation_key: [order_id, customer_id]
```

A record's correlation group is identified by the tuple of values for the listed fields. Records sharing the same tuple belong to the same group.

## Lifecycle

The correlation key is stamped onto each record at source ingest. The engine adds a shadow column named `$ck.<field>` (one per correlation-key field) to every source schema and copies the field's value into it. From that point on, the shadow column is the authoritative group identity -- if a downstream transform rewrites the user-declared correlation field, the shadow column is untouched and the group identity is preserved.

Shadow columns are an internal engine namespace. You never write `$ck.<field>` in YAML or CXL -- the engine manages them. They are stripped from default writer output. To surface them for debugging, set `include_correlation_keys: true` on an output node:

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

## DLQ semantics

When a record fails inside a correlation group:

- The failing record produces a **trigger** DLQ entry. Its category reflects the actual failure (e.g. `type_error`, `validation_failed`).
- Every other record in the same group produces a **collateral** DLQ entry. Collaterals carry the category `correlated`.
- Records belonging to other (clean) groups proceed normally.

A record with a null value for the correlation-key field is treated as its own per-record group: it has no peers and DLQ atomicity does not span multiple records.

The `dlq_count` counter sums triggers and collaterals.

## Group buffering

The engine buffers records per correlation group until either the group completes (all source records observed) or a failure triggers a flush. The `max_group_buffer:` field caps per-group buffering:

```yaml
error_handling:
  correlation_key: order_id
  max_group_buffer: 100000     # Default: 100,000
```

Groups that exceed the cap are DLQ'd entirely with a `group_size_exceeded` trigger plus a collateral entry per buffered record. This is a backpressure boundary, not a hard error.

## Compile-time constraints

One compile-time invariant is enforced:

- **Arena execution incompatible**: the arena-evaluated execution path is incompatible with correlation grouping. Combinations are rejected at compile time.

Aggregates whose `group_by` includes every correlation-key field stay on the strict-collateral path; aggregates that omit any correlation-key field activate the retraction protocol automatically. Authors do not configure this — the engine inspects the configuration and picks the correct path. See [Aggregate interaction](#aggregate-interaction) below.

## Per-operator interactions

### Transform interaction

A transform that rewrites the user-declared correlation-key field does not change a record's group identity. The shadow column captured at ingest is what the buffer-key extractor reads, not the live field value.

```yaml
- type: source
  name: orders
  config:
    schema:
      - { name: order_id, type: string }
      - { name: amount, type: float }

# At ingest each record gets $ck.order_id = order_id

- type: transform
  name: anonymize
  input: orders
  config:
    cxl: |
      emit order_id = "REDACTED"      # writes the live field
      emit amount = amount

# Group identity is still the original order_id from $ck.order_id;
# anonymize does not collapse records into a single null-keyed group.
```

This makes the correlation-key declaration robust against routine field-rewrite logic in transforms.

### Route interaction (fan-out)

A correlation group can span multiple route branches. Group atomicity is preserved across branches: if any record in the group fails (in any branch's transform, or in the route predicate itself), the entire group is rejected from every branch.

```yaml
- type: route
  name: split
  input: validate
  config:
    mode: inclusive
    conditions:
      a: 'priority == "high"'
      b: 'priority == "low"'
    default: a

- type: output
  name: out_a
  input: split.a
  config: { ... }

- type: output
  name: out_b
  input: split.b
  config: { ... }
```

For an `inclusive` route where one record reaches both branches, a single failure in the source still DLQ's that source row exactly once -- not once per (row, output) pair. The group identity dedupes the DLQ entries at the source-row level.

A route predicate that itself fails to evaluate (e.g. type error inside the condition expression) is treated like any other failure: it triggers DLQ atomicity for the whole correlation group.

### Merge interaction (fan-in)

Merge concatenates upstream branches that share a schema. Each record carries its `$ck.<field>` shadow column unchanged through the merge. Groups originating from different upstream sources but sharing the same correlation-key value are treated as a single correlation domain downstream:

```yaml
- type: source
  name: east_orders
  config: { ... }

- type: source
  name: west_orders
  config: { ... }

- type: merge
  name: all_orders
  inputs: [east_orders, west_orders]
  config: {}
```

If `east_orders` and `west_orders` both contain rows for `order_id = ORD-42`, all of those rows are members of the same correlation group post-merge. A failure on any one of them DLQ's the whole group across both upstream sources.

### Aggregate interaction

When an aggregate's `group_by` lists every correlation-key field, the aggregate stays on the strict-collateral path: each emitted row inherits the correlation identity of its inputs and any DLQ trigger in the group rolls back every record in the group, including the aggregate output row. This is the zero-overhead default.

```yaml
error_handling:
  correlation_key: order_id

- type: aggregate
  name: order_totals
  input: validate
  config:
    group_by: [order_id]               # strict -- includes the correlation key
    cxl: |
      emit total = sum(amount)
```

When an aggregate's `group_by` omits any correlation-key field, the engine routes the aggregate through the retraction protocol automatically. A single correlation group may span multiple aggregate groups; correlation-key fields omitted from `group_by` stop being visible to downstream consumers of this aggregate's output. Authors do not configure this — the engine inspects the configuration and picks the correct path.

```yaml
error_handling:
  correlation_key: order_id

- type: aggregate
  name: dept_totals
  input: validate
  config:
    group_by: [department]             # retraction protocol is active
    cxl: |
      emit total = sum(amount)
```

Aggregate output rows on the strict path inherit the correlation meta of the records that fed them. If any input record in a correlation group fails, the surviving records in that group still flow through the aggregator and produce one aggregate row -- but that aggregate row is itself DLQ'd as a collateral and never reaches the writer.

On the retraction path, the engine retracts only the failing records and refinalizes affected groups, so the aggregate output row reflects the surviving contributions. The retraction protocol's compile-time and runtime constraints (`E15W` for non-deterministic builtins downstream, `E15Y` for `strategy: streaming` on a retraction-mode aggregate) are enforced automatically once the engine has classified the aggregate.

### Combine interaction

Every combine declares `propagate_ck:` to select which correlation-key fields its output rows carry:

- `propagate_ck: driver` -- output inherits only the driver input's correlation identity. Build-side records contribute fields to the output but their group identity is consumed by the match. Default-equivalent behavior; today's strict-correlation pipelines stay on this setting.
- `propagate_ck: all` -- output carries the union of correlation-key fields across every input. Use when the build side carries CK fields that downstream operators need to read (for example, a build-side stream is also subject to correlation-driven DLQ on its own keys).
- `propagate_ck: { named: [<field>, ...] }` -- output carries exactly the named subset, intersected with what is actually present upstream. Use to project a multi-field correlation key down to a single field after a join.

```yaml
error_handling:
  correlation_key: employee_id

- type: combine
  name: enriched
  input:
    o: orders                          # driver
    d: departments                     # build side
  config:
    where: "o.employee_id == d.employee_id"
    match: first
    on_miss: skip
    cxl: |
      emit employee_id = o.employee_id
      emit amount = o.amount
      emit dept = d.dept
    propagate_ck: driver
```

```yaml
error_handling:
  correlation_key: employee_id

- type: combine
  name: enriched_all
  input:
    o: orders
    d: departments
  config:
    where: "o.employee_id == d.employee_id"
    cxl: |
      emit employee_id = o.employee_id
      emit dept = d.dept
    propagate_ck: all                  # union of every input's CK columns
```

Under `propagate_ck: driver`, output rows from `enriched` carry the `$ck.employee_id` value from the driver record, regardless of which department record matched. A trigger error on a driver record DLQ's that driver's whole correlation group, including any combine output rows that were already produced for that group.

Under `propagate_ck: all` (or `{ named: [...] }`), the combine widens its output schema with the build-side `$ck.<field>` columns it propagates, and the runtime copies the matched build record's values into those columns. **Driver wins on a name collision**: if both the driver and a build input declare `$ck.<field>`, the column appears once on the output schema and the runtime keeps the driver's value -- the build's value would only land if the driver's slot was null, which never happens for a same-named CK field that the driver itself observes.

Match-mode interaction:

- `match: first` -- one matched build per driver row; that build's `$ck.<field>` fills the propagated slot.
- `match: all` -- one output row per matched build; each row carries its own matched build's `$ck.<field>`.
- `match: collect` -- one synthesized output row per driver. The propagated `$ck.<field>` slot is single-valued: the **first matched build's** CK fills it. Every matched build's full payload still rides inside the array column via `Value::Map`, so per-build lineage is preserved at the cost of single-valued addressing on the propagated slot.

This rule holds across all combine execution paths: the hash-join path, the IEJoin range-predicate path, the grace-hash spill path, the sort-merge path, and chained combines (combine consuming the output of another combine).

The `drive:` field on a combine selects which input is the driver. Choose the side that carries the authoritative group identity for downstream DLQ routing -- typically the larger or more transactional stream.

`propagate_ck` is a required field with no default value -- every combine must spell out which propagation mode it uses. Existing pipelines migrate by adding `propagate_ck: driver` to keep today's behavior.

### Composition interaction

A composition's body operates on records flowing in from the parent pipeline. The correlation-key shadow columns flow into composition inputs and back out the named ports unchanged. Compositions cannot define their own correlation key -- it is a pipeline-level concern.

## Operator-by-operator retraction cost reference

An aggregate whose `group_by` omits a correlation-key field activates the retraction protocol automatically. Each operator on the post-source DAG carries a different cost profile under retraction; the table below summarizes the per-operator footprint so you can size memory and pick `propagate_ck` settings before pipelines hit production.

| Operator | Retraction cost |
|---|---|
| Source | None at retraction time. The CK shadow columns are stamped at ingest; replay never re-reads the source file. |
| Transform | Re-evaluated against substituted upstream rows during sub-DAG replay. Cost = O(rows_substituted) per layer, no extra state held. Non-deterministic builtins (e.g. `now`) are rejected at compile time with E15W. |
| Aggregate (strict, `group_by ⊇ correlation_key`) | None. Strict aggregates short-circuit to today's two-phase commit body and pay zero retraction overhead. |
| Aggregate (retraction-mode, Reversible bindings) | Per-row lineage map `(input_row_id → group_index)` carried alongside accumulator state — ~8 bytes/row plus the per-group `input_rows` Vec inline cost. Retract is O(retracted_rows) reverse-op calls plus one `finalize_in_place`. Reversible accumulators: `sum`, `count`, `collect`, `any`. |
| Aggregate (retraction-mode, BufferRequired bindings) | Per-group raw contributions held until commit. Memory cost = O(input_rows × Σ binding_value_size). Retract recomputes affected groups from `contributions − retracted_rows`. BufferRequired accumulators: `min`, `max`, `avg`, `weighted_avg`. |
| Combine (driver propagation) | One propagated `$ck.<field>` slot from the driver record. No retraction state held by the combine itself; replay carries upstream deltas through. |
| Combine (`propagate_ck: all` / `named: [...]`) | Same per-row cost as driver propagation, plus the widened output schema's `$ck.<field>` columns must be re-populated on replay. Cost scales with the output schema width, not retraction frequency. |
| Window (streaming) | None — streaming windows are incompatible with a retraction-mode aggregate whose dropped CK fields overlap `partition_by`. The plan-time derivation switches such windows into buffer mode. |
| Window (buffer-mode) | Per-partition raw row buffers held until commit. Memory cost = O(largest partition × per-row-size). Retract reruns the configured `$window.*` evaluation over `partition − retracted_rows`. Covers all 13 `$window.*` builtins uniformly via wholesale recompute. |
| Output | Holds retracted rows in `correlation_buffers` until commit. Replay substitutes the post-retract row in place; clean records flush to the writer, dirty records DLQ per the resolved `correlation_fanout_policy`. |

The `--explain` output's `=== Retraction ===` section reports the live per-aggregate / per-window detail derived from the current pipeline. The `clinker metrics collect` spool reports the runtime counterpart: `correlation.retract.groups_recomputed`, `.partitions_recomputed`, `.subdag_replay_rows`, `.output_rows_retracted_total`, `.degrade_fallback_count`. Use the explain block for plan-time capacity sizing, the metrics spool for post-run confirmation.

When retraction's preconditions break at runtime (an aggregate spilled before retract reached it, or a window partition exceeded the memory budget), the orchestrator degrades to "DLQ entire affected group/partition" — the same strict-collateral DLQ shape every aggregate uses on the strict path. Each degrade increments `correlation.retract.degrade_fallback_count`; persistent non-zero values point at a tighter memory budget or a smaller correlation key cardinality.

## Debugging

To see correlation-key shadow columns in writer output:

```yaml
- type: output
  name: debug
  input: any_node
  config:
    type: csv
    path: "./debug.csv"
    include_correlation_keys: true
```

The output will contain extra columns named `$ck.<field>` (literal `$ck.` prefix in the CSV header) for each correlation-key field declared on the pipeline.

To investigate DLQ collaterals: every collateral entry's `category` is `correlated`. The trigger entry in the same group carries the actual failure category and message.

## See also

- [Error Handling & DLQ](error-handling.md) -- general DLQ configuration, fail-fast vs continue, type-error thresholds.
- [Aggregate Nodes](aggregate.md) -- group-by semantics and the strategy hint.
- [Combine Nodes](combine.md) -- driver selection and match modes.
- [Output Nodes](output.md) -- `include_correlation_keys` and other field-control flags.
