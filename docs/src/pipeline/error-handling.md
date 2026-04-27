# Error Handling & DLQ

Clinker provides structured error handling with a dead-letter queue (DLQ) for records that fail processing. The `error_handling:` block at the top level of the pipeline YAML controls the behavior.

## Configuration

```yaml
error_handling:
  strategy: continue
  dlq:
    path: "./output/errors.csv"
    include_reason: true
    include_source_row: true
```

## Strategies

The `strategy:` field controls what happens when a record fails:

| Strategy | Behavior |
|----------|----------|
| `fail_fast` | **Default.** Stop the pipeline on the first error. |
| `continue` | Route bad records to the DLQ and keep processing good records. |
| `best_effort` | Continue processing with partial results, even if some stages produce incomplete output. |

### fail_fast

The safest strategy. Any record-level error (type coercion failure, validation error, missing required field) halts the pipeline immediately. Use this when data quality is critical and you prefer to fix issues before reprocessing.

### continue

The production workhorse. Bad records are written to the DLQ file with diagnostic metadata, and the pipeline continues processing remaining records. After the run completes, inspect the DLQ to understand and correct failures.

A pipeline that completes with DLQ entries exits with **code 2** -- this signals "pipeline completed successfully but some records were rejected." It is not a crash or internal error.

### best_effort

The most lenient strategy. Processing continues even with partial results. Use this for exploratory data analysis where completeness is less important than progress.

## DLQ configuration

The DLQ is always written as CSV, regardless of the pipeline's input/output formats.

```yaml
  dlq:
    path: "./output/errors.csv"
    include_reason: true
    include_source_row: true
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `path` | No | -- | File path for DLQ output. If omitted, DLQ records are logged but not written to file. |
| `include_reason` | No | -- | Include `_cxl_dlq_error_category` and `_cxl_dlq_error_detail` columns. |
| `include_source_row` | No | -- | Include original source fields alongside DLQ metadata. |

## DLQ columns

Every DLQ record includes these metadata columns:

| Column | Description |
|--------|-------------|
| `_cxl_dlq_id` | UUID v7 (time-ordered unique identifier) |
| `_cxl_dlq_timestamp` | RFC 3339 timestamp of when the error occurred |
| `_cxl_dlq_source_file` | Input filename that produced the failing record |
| `_cxl_dlq_source_row` | 1-based row number in the source file |
| `_cxl_dlq_stage` | Name of the transform or aggregate node where the error occurred |
| `_cxl_dlq_route` | Route branch name (if the error occurred after routing) |
| `_cxl_dlq_trigger` | Validation rule name that triggered the rejection |

When `include_reason: true` is set, two additional columns appear:

| Column | Description |
|--------|-------------|
| `_cxl_dlq_error_category` | Machine-readable error classification |
| `_cxl_dlq_error_detail` | Human-readable error description |

## Error categories

The `_cxl_dlq_error_category` column contains one of these values:

| Category | Description |
|----------|-------------|
| `missing_required_field` | A required field is absent from the record |
| `type_coercion_failure` | A value could not be converted to the expected type |
| `required_field_conversion_failure` | A required field exists but its value cannot be converted |
| `nan_in_output_field` | A computation produced NaN |
| `aggregate_type_error` | An aggregate function received an incompatible type |
| `validation_failure` | A declarative validation check failed |
| `aggregate_finalize` | An aggregate function failed during finalization |

## Advanced options

### Type error threshold

Abort the pipeline if the fraction of failing records exceeds a threshold:

```yaml
  type_error_threshold: 0.05    # Abort if >5% of records fail
```

This acts as a circuit breaker -- if your input data is unexpectedly corrupt, the pipeline stops early rather than filling the DLQ with millions of entries.

### Correlation key

Group DLQ rejections by a key field. When any record in a correlation group fails, **all records in that group** are routed to the DLQ:

```yaml
  correlation_key: order_id
```

For compound keys:

```yaml
  correlation_key: [order_id, customer_id]
```

This is useful for transactional data where partial processing of a group is worse than rejecting the entire group. For example, if one line item in an order fails validation, you may want to reject the entire order.

For the full lifecycle and per-operator semantics (route, merge, aggregate, combine), see [Correlation Keys](correlation-keys.md).

### Max group buffer

Limit the number of records buffered per correlation group:

```yaml
  max_group_buffer: 100000     # Default: 100,000
```

Groups exceeding this limit are DLQ'd entirely with a `group_size_exceeded` summary entry.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Pipeline completed successfully, no errors |
| 1 | Pipeline failed (internal error, config error, or `fail_fast` triggered) |
| 2 | Pipeline completed, but DLQ entries were produced |

Exit code 2 is not a failure -- it means the pipeline ran to completion and handled errors according to the configured strategy. Check the DLQ file for details.

## Complete example

```yaml
pipeline:
  name: order_processing
  memory_limit: "512M"

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: "./data/orders.csv"
      schema:
        - { name: order_id, type: int }
        - { name: customer_id, type: int }
        - { name: amount, type: float }
        - { name: email, type: string }

  - type: transform
    name: validate_orders
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit customer_id = customer_id
        emit amount = amount
        emit email = email
      validations:
        - field: email
          check: "not_empty"
          severity: error
          message: "Customer email is required"
        - check: "amount > 0"
          severity: error
          message: "Order amount must be positive"

  - type: output
    name: valid_orders
    input: validate_orders
    config:
      name: valid_orders
      type: csv
      path: "./output/valid_orders.csv"

error_handling:
  strategy: continue
  dlq:
    path: "./output/rejected_orders.csv"
    include_reason: true
    include_source_row: true
  type_error_threshold: 0.10
  correlation_key: order_id
```
