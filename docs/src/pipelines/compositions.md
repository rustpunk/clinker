# Compositions

Compositions are reusable pipeline fragments that can be imported into multiple pipelines. They encapsulate common transform patterns -- date derivations, address normalization, currency conversion -- into self-contained, testable units.

## Using a composition

A composition node in your pipeline references an external `.comp.yaml` file:

```yaml
- type: composition
  name: fiscal_dates
  input: invoices
  use: "./compositions/fiscal_date.comp.yaml"
  config:
    start_month: 4
```

The `use:` field points to the composition definition file. The `config:` block passes parameters that customize the composition's behavior for this specific invocation.

## Composition definition file

A `.comp.yaml` file declares the composition's interface -- what fields it requires from upstream and what fields it produces:

```yaml
# compositions/fiscal_date.comp.yaml
composition:
  name: fiscal_date
  description: "Derive fiscal year, quarter, and period from a date field"

  requires:
    - { name: invoice_date, type: date }

  produces:
    - { name: fiscal_year, type: int }
    - { name: fiscal_quarter, type: string }
    - { name: fiscal_period, type: int }

  params:
    - name: start_month
      type: int
      default: 1
      description: "First month of the fiscal year (1-12)"
```

### Composition fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Composition identifier |
| `description` | No | Human-readable purpose |
| `requires` | Yes | Input fields the composition needs from upstream (name + type) |
| `produces` | Yes | Output fields the composition adds to the record (name + type) |
| `params` | No | Configurable parameters with optional defaults |

## Advanced wiring

For compositions with multiple input or output ports, the node supports explicit port bindings:

```yaml
- type: composition
  name: enrich_address
  input: customers
  use: "./compositions/address_normalize.comp.yaml"
  inputs:
    primary: customers
    reference: zip_lookup
  outputs:
    normalized: next_stage
  config:
    country_code: "US"
  resources:
    zip_database: "./data/zipcodes.csv"
```

### Port and resource fields

| Field | Required | Description |
|-------|----------|-------------|
| `inputs` | No | Map of composition input ports to upstream node references |
| `outputs` | No | Map of composition output ports to downstream node references |
| `config` | No | Parameter overrides (key-value pairs) |
| `resources` | No | External resource bindings (file paths, connection strings) |
| `alias` | No | Namespace prefix for expanded node names (avoids collisions) |

## Complete example

```yaml
pipeline:
  name: invoice_pipeline

nodes:
  - type: source
    name: invoices
    config:
      name: invoices
      type: csv
      path: "./data/invoices.csv"
      schema:
        - { name: invoice_id, type: int }
        - { name: customer_id, type: int }
        - { name: invoice_date, type: date }
        - { name: amount, type: float }

  - type: composition
    name: fiscal_dates
    input: invoices
    use: "./compositions/fiscal_date.comp.yaml"
    config:
      start_month: 4

  - type: transform
    name: final_enrich
    input: fiscal_dates
    config:
      cxl: |
        emit invoice_id = invoice_id
        emit customer_id = customer_id
        emit amount = amount
        emit fiscal_year = fiscal_year
        emit fiscal_quarter = fiscal_quarter

  - type: output
    name: result
    input: final_enrich
    config:
      name: result
      type: csv
      path: "./output/invoices_enriched.csv"
```

## Current status

> **Note:** Composition support is being built in Phase 16c. The YAML shape parses and validates, but compilation currently returns a diagnostic (E100) per composition node. The documentation above reflects the intended design. Full compilation and expansion will land when Phase 16c is complete.
