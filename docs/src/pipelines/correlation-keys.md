# Correlation Keys

A correlation key declares a set of records from a single source as an atomic group: if any record in the group fails validation or processing, the whole group is sent to the DLQ. This is the right shape for transactional data where partial processing is worse than total rejection -- the canonical example is an order with multiple line items where one bad line should reject the entire order.

This page describes how to declare a correlation key and how it behaves through each node that can fan out, fan in, group, or join records.

## Declaration

Correlation keys are declared per source. Each source's `config:` block carries an optional `correlation_key:` field naming the column (or list of columns) whose value identifies a record's correlation group within that source.

```yaml
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: ./data/orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: int }

  - type: source
    name: customers
    config:
      name: customers
      type: csv
      path: ./data/customers.csv
      correlation_key: [customer_id, region]   # multi-column key
      schema:
        - { name: customer_id, type: string }
        - { name: region, type: string }
        - { name: name, type: string }

  - type: source
    name: sensor_readings
    config:
      name: sensor_readings
      type: csv
      # No correlation_key: record-level errors land in the DLQ as
      # standalone entries with no group atomicity.
      schema:
        - { name: ts, type: date_time }
        - { name: value, type: float }
```

A record's correlation group is identified by the tuple of values for that source's listed fields. Records sharing the same tuple within the same source belong to the same group. There is no pipeline-level correlation key — declare it on each contributing source.

The group identity is captured at ingest, so **rewriting the key column in a later Transform does not change a record's group** — anonymizing or transforming `order_id` downstream still keeps the original grouping intact.

A source whose declared `correlation_key:` field names a column not present in its own `schema:` block is rejected at compile time with diagnostic **E153**. The fix is to add the field to the schema or remove it from `correlation_key:`.

## DLQ semantics

When a record fails inside a correlation group:

- The failing record produces a **trigger** DLQ entry. Its category reflects the actual failure (e.g. `type_error`, `validation_failed`).
- Every other record from the same source in that group produces a **collateral** DLQ entry, carrying the category `correlated`.
- Records belonging to other (clean) groups proceed normally.

A record with a null value for the correlation-key field is treated as its own group: it has no peers, so DLQ atomicity does not span multiple records.

The `dlq_count` counter sums triggers and collaterals.

## Group buffering

The engine buffers records per correlation group until either the group completes or a failure triggers a flush. The `max_group_buffer:` field on the pipeline-level `error_handling:` block caps per-group buffering across every source's groups:

```yaml
error_handling:
  max_group_buffer: 100000     # Default: 100,000
```

Groups that exceed the cap are DLQ'd entirely with a `group_size_exceeded` trigger plus a collateral entry per buffered record. This is a backpressure boundary, not a hard error.

## Per-operator interactions

### Route interaction (fan-out)

A correlation group can span multiple route branches. Group atomicity is preserved across branches: if any record in the group fails (in any branch's transform, or in the route predicate itself), the entire group is rejected from every branch.

For an `inclusive` route where one record reaches both branches, a single failure DLQ's that source row exactly once — not once per branch.

### Merge interaction (fan-in)

Merge concatenates upstream branches that share a schema. Records keep their correlation identity through the merge, so rows from different sources that share the same key value become one correlation group downstream: a failure on any one of them DLQ's the whole group across both sources.

### Per-source rollback narrowing

When two sources contribute records to the same correlation group, a failure originating from one source does **not** collaterally DLQ records from the other source. The collateral fan-out is scoped to the failing source's records only.

For example, with `[src_a, src_b] → merge → transform → out` where both declare `correlation_key: id`, an error that fires on a `src_b` row produces a trigger for that row while the `src_a` row sharing the same `id` is **spared** and reaches the output. Single-source pipelines behave exactly as a pipeline-wide collateral DLQ would, since every co-grouped record shares the one source.

Two cases stay group-wide rather than narrowing per source:

- **`max_group_buffer` overflow** DLQ's every record in the overflowing group — no single source is to blame for the overflow.
- **Combine output failures** DLQ the synthesized output row, which has no single-source attribution.

### Aggregate interaction

When an aggregate's `group_by` covers every correlation-key field, the aggregate stays on the **strict** path: each emitted row inherits the correlation identity of its inputs, and any DLQ trigger in the group rolls back every record in the group, including the aggregate output row.

```yaml
- type: aggregate
  name: order_totals
  input: orders                         # correlation_key: order_id
  config:
    group_by: [order_id]                # covers the key
    cxl: |
      emit total = sum(amount)
```

When an aggregate's `group_by` **omits** a correlation-key field, the engine automatically retracts only the failing records and recomputes the affected groups, so the surviving contributions still produce a correct aggregate row. You do not configure this — the engine picks the path from the `group_by` content. (One restriction: this mode cannot be combined with `strategy: streaming`, which is rejected at compile time.)

```yaml
- type: aggregate
  name: dept_totals
  input: orders                         # correlation_key: order_id
  config:
    group_by: [department]              # omits the key — surviving rows recomputed
    cxl: |
      emit total = sum(amount)
```

### Combine interaction

Every combine declares `propagate_ck:` to select which correlation-key fields its output rows carry:

- `propagate_ck: driver` — output inherits only the driver input's correlation identity. The common case; today's strict-correlation pipelines stay on this setting.
- `propagate_ck: all` — output carries the union of correlation-key fields across every input. Use when the build side carries keys that downstream operators need to read.
- `propagate_ck: { named: [<field>, ...] }` — output carries exactly the named subset. Use to project a multi-field key down after a join.

```yaml
- type: combine
  name: enriched
  input:
    o: orders                          # driver (correlation_key: employee_id)
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

How match mode fills the propagated key:

- `match: first` — the single matched build's key fills the slot.
- `match: all` — one output row per matched build, each carrying its own build's key.
- `match: collect` — one row per driver; the **first** matched build's key fills the (single-valued) slot, while every matched build's full payload still rides inside the array column.

**Driver wins on a name collision**: if both the driver and a build input declare the same key field, the output keeps the driver's value.

`propagate_ck` is a required field — every combine must spell out which mode it uses.

### Composition interaction

A composition's body operates on records flowing in from the parent pipeline; correlation identity flows into the composition inputs and back out the named ports unchanged. Compositions cannot declare their own correlation key — a key is a property of a source, not of the composition body that consumes a source's records.

## Debugging

Correlation grouping is tracked on internal columns you never write in YAML or CXL, and they are hidden from writer output by default. To surface them for debugging, set `include_correlation_keys: true` on an output node:

```yaml
- type: output
  name: debug
  input: any_node
  config:
    type: csv
    path: "./debug.csv"
    include_correlation_keys: true
```

The output then contains extra columns named `$ck.<field>` (literal prefix in the CSV header) for each declared correlation-key field.

To investigate DLQ collaterals: every collateral entry's `category` is `correlated`, and the trigger entry in the same group carries the actual failure category and message.

## See also

- [Error Handling & DLQ](error-handling.md) -- general DLQ configuration, fail-fast vs continue, type-error thresholds.
- [Aggregate Nodes](../nodes/aggregate.md) -- group-by semantics and the strategy hint.
- [Combine Nodes](../nodes/combine.md) -- driver selection and match modes.
- [Output Nodes](../nodes/output.md) -- `include_correlation_keys` and other field-control flags.
