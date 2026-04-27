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

Two compile-time invariants are enforced:

- **Aggregate group-by superset**: every correlation-key field must appear in `group_by` of every aggregate downstream of the source. The engine extends `group_by` automatically when it can; an explicit `group_by` that omits a correlation-key field produces an `E151` compile error. See [Aggregate interaction](#aggregate-interaction) below.
- **Arena execution incompatible**: the arena-evaluated execution path is incompatible with correlation grouping. Combinations are rejected at compile time.

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

Every correlation-key field must be in `group_by`. The engine auto-extends `group_by` to include the correlation-key fields when you omit them; if you specify a `group_by` that explicitly excludes a correlation-key field, the pipeline fails to compile with `E151`.

```yaml
error_handling:
  correlation_key: order_id

- type: aggregate
  name: order_totals
  input: validate
  config:
    group_by: [order_id]               # OK -- includes correlation key
    cxl: |
      emit total = sum(amount)
```

```yaml
error_handling:
  correlation_key: order_id

- type: aggregate
  name: dept_totals
  input: validate
  config:
    group_by: [department]             # E151 -- omits order_id
    cxl: |
      emit total = sum(amount)
```

The reason is structural: aggregates emit one row per group, and the emitted row must inherit the correlation identity of its inputs so that downstream DLQ logic can route it correctly. If a single aggregate group spanned multiple correlation groups, the emitted row would have ambiguous identity and could not participate in correlation rollback.

Aggregate output rows inherit the correlation meta of the records that fed them. If any input record in a correlation group fails, the surviving records in that group still flow through the aggregator and produce one aggregate row -- but that aggregate row is itself DLQ'd as a collateral and never reaches the writer.

### Combine interaction

A combine's output rows inherit the **driver** input's correlation identity. Build-side records contribute fields to the output but their group identity is consumed by the hash or sort-merge match -- they do not reach the writer directly.

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
```

Output rows from `enriched` carry the `$ck.employee_id` value from the driver record, regardless of which department record matched. A trigger error on a driver record DLQ's that driver's whole correlation group, including any combine output rows that were already produced for that group.

This rule holds across all combine execution paths: the hash-join path, the IEJoin range-predicate path, and chained combines (combine consuming the output of another combine).

The `drive:` field on a combine selects which input is the driver. Choose the side that carries the authoritative group identity for downstream DLQ routing -- typically the larger or more transactional stream.

### Composition interaction

A composition's body operates on records flowing in from the parent pipeline. The correlation-key shadow columns flow into composition inputs and back out the named ports unchanged. Compositions cannot define their own correlation key -- it is a pipeline-level concern.

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
