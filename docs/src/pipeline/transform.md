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
