# Pipeline Variables

Pipeline variables are constants defined in the YAML `pipeline:` header and accessible in CXL expressions. They provide a way to parameterize pipeline behavior without modifying CXL logic.

## Defining variables

Variables are declared in the `vars:` block of the `pipeline:` section:

```yaml
pipeline:
  name: order_processing
  vars:
    high_value_threshold: 500
    express_surcharge: 5.99
    report_title: "Monthly Orders"
    apply_discount: true
    tax_rate: 0.085
```

Variables are scalar values only: strings, numbers (integers and floats), and booleans. Complex types (arrays, objects) are not supported.

## Accessing variables in CXL

Use the `$vars.*` namespace to reference variables in CXL expressions:

```yaml
- type: transform
  name: classify_orders
  input: orders
  config:
    cxl: |
      emit order_id = order_id
      emit amount = amount
      emit is_high_value = amount > $vars.high_value_threshold
      emit surcharge = if shipping == "express" then $vars.express_surcharge else 0
      emit total = amount + surcharge
```

The `$pipeline.*` namespace also provides access to pipeline-level metadata.

## Variable semantics

Variables are **frozen at pipeline start** and **constant across all records**. They cannot be modified during execution. This makes them suitable for:

- Thresholds and cutoff values
- Fixed surcharges, tax rates, and multipliers
- Labels and titles for report headers
- Feature flags that control conditional logic

## Using variables with channels

Variables defined in `pipeline.vars` serve as defaults. The [channel system](channels.md) can override these values per client or environment without modifying the pipeline YAML:

```yaml
# Pipeline: pipeline.yaml
pipeline:
  name: invoice_processing
  vars:
    express_surcharge: 5.99
    late_fee: 25.00

# Channel: channels/acme-corp/channel.yaml
_channel:
  id: acme-corp
  name: "Acme Corp"
  active: true
variables:
  EXPRESS_SURCHARGE: "8.99"
```

When run with `--channel acme-corp`, the channel variable overrides the pipeline default.

## Complete example

```yaml
pipeline:
  name: sales_report
  vars:
    min_amount: 100
    commission_rate: 0.12
    region_label: "North America"
    include_pending: false

nodes:
  - type: source
    name: sales
    config:
      name: sales
      type: csv
      path: "./data/sales.csv"
      schema:
        - { name: sale_id, type: int }
        - { name: rep_name, type: string }
        - { name: amount, type: float }
        - { name: status, type: string }

  - type: transform
    name: compute
    input: sales
    config:
      cxl: |
        filter amount >= $vars.min_amount
        filter if $vars.include_pending then true else status == "closed"
        emit sale_id = sale_id
        emit rep_name = rep_name
        emit amount = amount
        emit commission = amount * $vars.commission_rate
        emit region = $vars.region_label

  - type: aggregate
    name: rep_totals
    input: compute
    config:
      group_by: [rep_name]
      cxl: |
        emit total_sales = sum(amount)
        emit total_commission = sum(commission)
        emit deal_count = count(*)

  - type: output
    name: report
    input: rep_totals
    config:
      name: report
      type: csv
      path: "./output/sales_report.csv"
      sort_order:
        - { field: "total_sales", order: desc }
```
