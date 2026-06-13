# Transform Nodes

Transform nodes apply CXL expressions to each record, producing new fields, filtering records, or both. They process one record at a time in streaming fashion with constant memory overhead.

## Basic structure

```yaml
- type: transform
  name: enrich
  input: customers
  config:
    cxl: |
      emit full_name = first_name + " " + last_name
      emit tier = if lifetime_value >= 10000 then "gold" else "standard"
      filter status == "active"
```

The `cxl:` field is required and contains a CXL program. The three core CXL statements for transforms are:

- **`emit`** -- produces an output field. Only emitted fields appear in downstream nodes.
- **`filter`** -- drops records that do not match the boolean condition.
- **`let`** -- binds a local variable for use in subsequent expressions (not emitted).

```yaml
    cxl: |
      let margin = revenue - cost
      emit product_id = product_id
      emit margin = margin
      emit margin_pct = if revenue > 0 then margin / revenue * 100 else 0
      filter margin > 0
```

## Analytic window

The `analytic_window` field enables cross-source lookups by joining a secondary dataset into the transform. The secondary source is loaded into memory and indexed by the join key.

```yaml
- type: transform
  name: enrich_orders
  input: orders
  config:
    analytic_window:
      source: products
      on: product_id
      group_by: [product_id]
    cxl: |
      emit order_id = order_id
      emit product_name = $window.first()
      emit quantity = quantity
      emit line_total = quantity * price
```

The `$window.*` namespace provides access to the windowed data. Functions like `$window.first()`, `$window.last()`, and `$window.count()` operate over the matched group.

## Validations

Declarative validation checks can be attached to a transform. They run against each record and either route failures to the DLQ (severity `error`) or log a warning and continue (severity `warn`).

```yaml
- type: transform
  name: validate_orders
  input: raw_orders
  config:
    cxl: |
      emit order_id = order_id
      emit amount = amount
      emit email = email
    validations:
      - field: email
        check: "not_empty"
        severity: error
        message: "Email is required"
      - check: "amount > 0"
        severity: warn
        message: "Non-positive amount"
      - field: order_id
        check: "not_empty"
        severity: error
```

### Validation fields

| Field | Required | Description |
|-------|----------|-------------|
| `field` | No | Restrict the check to a single field |
| `check` | Yes | Validation name (e.g. `"not_empty"`) or CXL boolean expression |
| `severity` | No | `error` (default) routes to DLQ; `warn` logs and continues |
| `message` | No | Custom error message for DLQ entries |
| `name` | No | Validation name for DLQ reporting. Auto-derived from field + check if omitted |
| `args` | No | Additional arguments as key-value pairs |

## Expansion cap (`max_expansion`)

When a transform body contains an [`emit each`](../cxl/emit-each.md) statement, every input record can fan out into multiple output records. The `max_expansion` field caps how many output records a single input record may produce -- a safety bound against unexpectedly large arrays.

```yaml
- type: transform
  name: explode_items
  input: orders
  config:
    max_expansion: 5000      # default: 10000
    cxl: |
      emit each it in items {
        emit order_id = order_id
        emit sku = it["sku"]
        emit price = it["price"]
      }
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_expansion` | `u64` | `10000` | Maximum cumulative output records per input record. |

If a single input record's `emit each` block produces more than `max_expansion` output records, the originating record routes to the DLQ with category `expansion_limit_exceeded` instead of producing a truncated or unbounded result. No partial output is emitted for that record -- the cap is enforced eagerly so the writer never sees records from a runaway expansion.

### When to tune

- **Lower** (e.g. `100`, `1000`) when input arrays are bounded by a known business rule and you want hostile or malformed input to surface as a DLQ entry rather than as a flood of downstream records.
- **Higher** (e.g. `100000`, `1000000`) when legitimate input carries large arrays -- for example, an order with a long line-item list or an event carrying a per-second pricing curve.

The DLQ category `expansion_limit_exceeded` is distinct from generic CXL evaluation failures, so DLQ-side filters and metrics can target expansion runaway specifically. See [Error Handling & DLQ](../pipelines/error-handling.md) for the wider DLQ contract.

## Batch size (`batch_size`)

A streaming-eligible transform hands its output downstream in bounded batches rather than accumulating the whole stage before the next stage runs. `batch_size` sets how many events (records plus document-boundary punctuations) a batch holds. A per-transform `batch_size` overrides the pipeline-level [`pipeline.batch_size`](../ops/memory.md#streaming-batch-size-batch_size) for this one stage; omit it to inherit the pipeline value (or the built-in default of 2048).

```yaml
- type: transform
  name: enrich
  input: orders
  config:
    batch_size: 512         # override pipeline.batch_size for this stage
    cxl: |
      emit order_id = order_id
      emit total = quantity * unit_price
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | `usize` | inherits `pipeline.batch_size` (else 2048) | Events per streaming batch for this transform. Must be `>= 1`. |

A `batch_size` of `0` is rejected at config load (a zero-event batch never flushes). Smaller batches lower the in-flight memory of a streaming stage at the cost of more per-batch bookkeeping; larger batches amortize the bookkeeping at the cost of a larger live working set. The default suits typical record widths — tune it only when a profiling run shows a streaming stage's per-batch footprint matters. See [Streaming vs. Blocking Stages](../ops/streaming-vs-blocking.md) for which stages stream and which fully materialize.

## Log directives

Log directives control diagnostic output during transform execution:

```yaml
- type: transform
  name: process
  input: validated
  config:
    cxl: |
      emit id = id
      emit result = compute(value)
    log:
      - level: info
        when: per_record
        every: 1000
        message: "Processed record"
      - level: warn
        when: on_error
        message: "Record failed processing"
      - level: debug
        when: before_transform
        message: "Starting transform"
```

### Log directive fields

| Field | Required | Description |
|-------|----------|-------------|
| `level` | Yes | `trace`, `debug`, `info`, `warn`, or `error` |
| `when` | Yes | `before_transform`, `after_transform`, `per_record`, or `on_error` |
| `message` | Yes | Log message text |
| `every` | No | Only log every N records (for `per_record` timing) |
| `condition` | No | CXL boolean expression -- only log when true |
| `fields` | No | List of field names to include in the log output |
| `log_rule` | No | Reference to an external log rule definition |

## Complete example

```yaml
- type: source
  name: employees
  config:
    name: employees
    type: csv
    path: "./data/employees.csv"
    schema:
      - { name: employee_id, type: string }
      - { name: first_name, type: string }
      - { name: last_name, type: string }
      - { name: department, type: string }
      - { name: salary, type: int }
      - { name: hire_date, type: date }

- type: transform
  name: enrich_employees
  description: "Compute display name and tenure"
  input: employees
  config:
    cxl: |
      emit employee_id = employee_id
      emit display_name = last_name + ", " + first_name
      emit department = department.upper()
      emit salary = salary
      emit annual_bonus = if salary >= 80000 then salary * 0.15
        else salary * 0.10
    validations:
      - field: employee_id
        check: "not_empty"
        severity: error
        message: "Employee ID is required"
      - check: "salary > 0"
        severity: warn
        message: "Salary should be positive"
    log:
      - level: info
        when: per_record
        every: 5000
        message: "Processing employees"
```
