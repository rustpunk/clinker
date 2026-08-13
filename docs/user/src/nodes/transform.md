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

Log directives declare bounded structured diagnostic events during transform
execution:

```yaml
- type: transform
  name: process
  input: validated
  config:
    cxl: |
      emit id = id
      emit result = compute(value)
    log:
      - name: transform.record_processed
        level: info
        when: per_record
        every: 1000
        message: "Processed record"
        fields: [id]
      - name: transform.record_failed
        level: warn
        when: on_error
        message: "Record failed processing"
      - name: transform.started
        level: debug
        when: before_transform
        message: "Starting transform"
```

### Log directive fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Stable event name: a bounded dotted identifier using ASCII letters, digits, or underscores |
| `level` | Yes | `trace`, `debug`, `info`, `warn`, or `error` |
| `when` | Yes | `before_transform`, `after_transform`, `per_record`, or `on_error` |
| `message` | Yes | Static event message, at most 1024 UTF-8 bytes. Interpolation is rejected; request record values with `fields` instead |
| `every` | For `per_record` | Positive record interval. It is required for every `per_record` event, including explicit `every: 1`, and rejected for other timings |
| `fields` | No | Up to 256 unique record field names requested as structured attributes. Available only for `per_record` and `on_error` events |
| `condition` | No | CXL boolean expression; the event fires only for records where it is true. Available only for `when: per_record`, and at most 512 UTF-8 bytes |

A transform may declare at most 32 events and request at most 256 fields in
aggregate across them. Event names and field selectors use the same grammar as
deployment field policy: dot-separated segments beginning with an ASCII letter
or underscore, followed by ASCII letters, digits, or underscores.

`fields` is the only channel by which record data reaches an event — `message`
is static text. A selector naming a field the incoming record does not carry
contributes nothing, so a directive whose selectors all miss would publish an
event with no attributes at all, which reads exactly like a run whose records
were empty.

Clinker refuses that when the pipeline compiles (E374). The rejection names the
selector, lists the columns the input row does carry, and — when your spelling
is close to one of them — gives you the corrected `fields:` line to paste:

```
[E374] transform `enrich` log[0].fields requests `orderId`, which the input
record does not carry; the upstream row has `order_id`, `amount`, `region` —
write `fields: [order_id]`
```

Selectors bind against the transform's **input** row, for `per_record` and
`on_error` alike: dispatch fires before this transform's own `cxl:` block, so a
column the transform produces cannot be requested. Request the columns it reads
instead.

The check decides what the declared schema can decide. A column that reaches
the transform through an open composition port is not visible to it, and a
selector naming one of those is still checked only at run time — counted in the
run's admission accounting under the missing-field total.

### Logging only the records you care about

`every` thins a per-record event by count. `condition` selects it by content —
use it when the interesting records are rare and you want all of them rather
than every thousandth record:

```yaml
    log:
      - name: transform.large_order
        level: info
        when: per_record
        every: 1
        condition: "amount > 1000"
        message: "large order"
        fields: [order_id, amount]
```

The two compose: `every` is applied first, then `condition`, so `every: 100`
with a condition logs every hundredth record *that also matches*.

A condition is CXL, checked when the pipeline compiles. It must resolve to a
boolean, and it is evaluated against the transform's **input** record — the one
that arrived, before this transform's own `cxl:` block runs. A field the
transform only produces is therefore not in scope; write the condition in terms
of the fields the transform reads.

A condition decides only *whether* an event fires. It cannot add anything to
one: the values that leave the process are still exactly the `fields` you
requested, each still subject to deployment policy. Narrowing a condition can
never widen what is exported.

Transform declarations name events, request fields, and may gate a per-record
event on its own input; they do not choose a destination, credentials, routing,
redaction, or sampling policy. Each requested event-field pair is denied unless
deployment observability policy explicitly allows, hashes, or replaces it.
Telemetry delivery is bounded and best effort and cannot change transform
results or published output — including a condition that fails to evaluate,
which drops its event rather than failing the run.

Every transform event also carries the fixed correlation fields
`execution_id`, `batch_id`, and `pipeline_name`. Unlike requested record
fields, these are **not** default-deny and are **not** gated by
`field_policy`: they are engine-supplied identity that never derives from a
source, and they are what makes an exported event joinable to the machine
stream and to the lineage events. A deployment that allows an event without
also writing three correlation rules still gets telemetry it can correlate.

Because they are exported verbatim, choose a `--batch-id` that is safe to send
to your collector — an identifier, not a tenant name or anything else you
would not want retained there. A `field_policy` rule naming one of these three
fields does not redact it. Source paths, records, secrets, and raw error text
are never implicit attributes.

The former `log_rule` directive key and pipeline-level `log_rules` block are
rejected. Move event identity and safe field requests into the transform's
`log:` entries as shown above; keep routing, privacy, credentials, and sampling
in deployment policy.

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
      - name: transform.employee_processed
        level: info
        when: per_record
        every: 5000
        message: "Processing employees"
        fields: [employee_id]
```
