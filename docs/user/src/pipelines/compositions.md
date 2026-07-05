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

### Resolving the `use:` path

A `use:` value names a `.comp.yaml` in the workspace. It is resolved
relative to the directory of the pipeline file being compiled, then
against the set of `.comp.yaml` files discovered under the workspace root,
finally falling back to a filename match. A `use:` that resolves to no
`.comp.yaml` — a typo, a wrong relative prefix, or a file that does not
exist — fails compilation with a spanned `E103` diagnostic naming the
composition node. The whole run aborts loudly; it does not silently drop
the composition and write an empty output. The same holds for the other
composition-binding errors (`E102`–`E109`): an ill-bound call site fails
compile rather than producing a run that writes zero records. Run
`clinker explain --code E103` for details.

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

### Reading config parameters in the body

A composition body reads its own config parameters as [`$config.<param>`](../cxl/system-variables.md#config-composition-config-parameters). The planner constant-folds each reference to the value resolved for that instantiation — the call site's `config:` value, or a [channel/group](channels.md) `config:` override, or the declared default — so the same composition used with different `config:` compiles to different bodies. Because the resolution happens per instantiation, a channel or group `config:` override changes what the body computes, not just the reported provenance.

### Body validation

Nodes inside a composition body are validated with the same node-scoped
config checks as top-level pipeline nodes. A body node that would be
rejected at the top level — an `envelope` wiring the not-yet-supported
`trailer:` port, a `transform` declaring a reserved variable name or a
default that does not match its declared type, an invalid log
directive, or a `batch_size: 0` — fails compilation with an `E115`
diagnostic naming the composition call site, the body file, and the
violation. Run `clinker explain --code E115` for details.

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
