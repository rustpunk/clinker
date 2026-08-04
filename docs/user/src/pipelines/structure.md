# Pipeline YAML Structure

A Clinker pipeline is a single YAML file with three top-level sections: `pipeline` (metadata), `nodes` (the processing graph), and optionally `error_handling`.

## Top-level shape

```yaml
pipeline:
  name: my_pipeline            # Required — pipeline identifier
  memory:                      # Optional — see ops/memory.md
    limit: "256M"              # Optional (K/M/G suffixes), default 512M
    backpressure: pause        # Optional, default `pause`
  vars:                        # Optional typed static configuration
    threshold: { type: int, default: 500 }
    label: { type: string, default: "Monthly Report" }
  date_formats: ["%Y-%m-%d"]   # Optional — custom date parsing formats
  rules_path: "./rules/"       # Optional — CXL module search path
  concurrency:                 # Optional
    threads: 4
    chunk_size: 1000
  metrics:                     # Optional
    spool_dir: "./metrics/"

nodes:                         # Required — flat list of pipeline nodes
  - type: source
    name: raw_data
    config:
      name: raw_data
      type: csv
      path: "./data/input.csv"
      schema:
        - { name: id, type: int }
        - { name: value, type: string }

  - type: transform
    name: clean
    input: raw_data
    config:
      cxl: |
        emit id = id
        emit value = value.trim()

  - type: output
    name: result
    input: clean
    config:
      name: result
      type: csv
      path: "./output/result.csv"

error_handling:                # Optional
  strategy: fail_fast
```

## Pipeline metadata

The `pipeline:` block carries global settings that apply to the entire run.

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Pipeline identifier. Used in logs and metrics. |
| `memory` | No | Memory-arbitrator tuning. Nested fields: `limit` (RSS budget, `K`/`M`/`G` suffixes, default `512M`) and `backpressure` (`spill`/`pause`/`both`, default `pause`). See [Memory Tuning](../ops/memory.md). |
| `vars` | No | Typed static configuration accessible in CXL via `$vars.*`. Each key declares `type` and an optional `default`; see [Scoped Variables](variables.md). |
| `date_formats` | No | List of `strftime`-style patterns for date parsing. |
| `rules_path` | No | Directory for CXL `use` module resolution. |
| `concurrency` | No | `threads` and `chunk_size` for parallel chunk processing. |
| `metrics` | No | `spool_dir` for per-run JSON metric files. |
| `date_locale` | No | Unsupported. Any explicit value is rejected with E119. Use explicit `date_formats` entries. |
| `log_rules` | No | Unsupported. Any explicit value is rejected with E124; configure runtime logging outside pipeline YAML. |
| `include_provenance` | No | Unsupported. Any explicit value is rejected with E125. Use `write_meta: true` on each intended Output. |

These three names are admitted only far enough to produce precise, spanned
diagnostics. Empty strings/maps and `false` are still explicit values and are
rejected before execution; omission is the only accepted form.

### Reserved metadata contract

The current status and locked owner are explicit:

| Field | Current status | Locked target and owner |
|-------|----------------|-------------------------|
| `date_locale` | Rejected (E119) | Remove it and express supported parsing with `date_formats:`. |
| `log_rules` | Rejected (E124) | Remove it; runtime telemetry policy is not authored in pipeline YAML. |
| `include_provenance` | Rejected (E125) | Remove it and set `write_meta: true` on each Output that needs a provenance sidecar. |

For provenance sidecars that work today, set `write_meta: true` on an Output
node. D-24 keeps that spelling. See
[Approved exceptions and rejected placeholders](https://github.com/rustpunk/clinker/blob/main/docs/ai/15_PRODUCTION_CONTRACTS.md#approved-exceptions-and-rejected-placeholders).

## The nodes list

Every pipeline has a flat `nodes:` list. Each entry is a node with a `type:` discriminator that determines its kind:

| Type | Role |
|------|------|
| `source` | Reads data from a file |
| `transform` | Applies CXL expressions to each record |
| `aggregate` | Groups and summarizes records |
| `route` | Splits records into named branches by condition |
| `merge` | Concatenates multiple upstream branches that share a schema |
| `combine` | Joins records across N inputs with `where:` predicates |
| `reshape` | Mutates or synthesizes records within correlation groups |
| `cull` | Removes whole correlation groups to a side-output port |
| `envelope` | Frames body records with optional document header and trailer streams |
| `output` | Writes records to a file |
| `composition` | Imports a reusable transform fragment |

## Node naming

Every node must have a `name:` field. Names must be unique within the pipeline and **must not contain dots** -- the dot character is reserved for port syntax (see below). Names are used for wiring, logging, and diagnostics.

## Wiring by node kind

Input fields live at the node's top level, alongside `name:` and `type:`. Their
shape is specific to the node kind:

| Node kind | Input shape |
|-----------|-------------|
| `source` | No input field |
| `transform`, `aggregate`, `route`, `reshape`, `cull`, `output` | One upstream reference in `input:` |
| `merge` | Ordered list of upstream references in `inputs:` |
| `combine` | Qualifier-to-upstream map in singular `input:` |
| `envelope` | Required `body:` plus optional `header:` and `trailer:` upstream references |
| `composition` | Required primary `input:` plus an `inputs:` map binding every required composition port; the map is authoritative for DAG wiring |

**Single upstream** -- used by ordinary one-input consumers:

```yaml
- type: transform
  name: clean
  input: raw_data       # References the source node named "raw_data"
  config: ...
```

**Port syntax** -- for consuming a specific branch from a route node, use `node.port`:

```yaml
- type: output
  name: high_value_out
  input: split.high     # Consumes the "high" branch of route node "split"
  config: ...
```

**Multiple upstreams** -- merge nodes use `inputs:` (plural) instead of `input:`:

```yaml
- type: merge
  name: combined
  inputs:
    - east_processed
    - west_processed
  config: {}
```

**Qualified inputs** -- Combine uses singular `input:` with a map whose keys
become CXL qualifiers:

```yaml
- type: combine
  name: enriched
  input:
    orders: clean_orders
    products: product_catalog
  config:
    where: "orders.product_id == products.product_id"
    match: first
    on_miss: null_fields
    cxl: |
      emit order_id = orders.order_id
      emit product_name = products.name
    propagate_ck: driver
```

Envelope and Composition have additional port semantics. See
[Envelope Nodes](../nodes/envelope.md) and [Compositions](compositions.md)
before wiring those node kinds.

**Source nodes have no input field.** They are entry points -- adding an `input:` field to a source is a parse error.

Using the wrong field or value shape for a node kind is caught at parse time by
strict deserialization.

## Optional fields on all nodes

Every node type supports these optional fields:

- **`description:`** -- human-readable text for documentation. Ignored by the engine.
- **`_notes:`** -- arbitrary metadata (JSON object). Ignored by the engine and available to external tooling.

```yaml
- type: transform
  name: enrich
  description: "Add customer tier based on lifetime value"
  _notes:
    color: "#4a9eff"
    position: { x: 300, y: 200 }
  input: customers
  config:
    cxl: |
      emit tier = if lifetime_value >= 10000 then "gold" else "standard"
```

## Strict parsing

All config structs use `deny_unknown_fields`. If you misspell a field name -- for example, writing `inputt:` instead of `input:` or `stratgy:` instead of `strategy:` -- the YAML parser rejects it immediately with a diagnostic pointing to the typo. This catches configuration errors before any data processing begins.

## Environment variable: CLINKER_ENV

The `CLINKER_ENV` environment variable can be used for conditional logic outside of pipelines (e.g., selecting channel directories or controlling CLI behavior). It is not directly referenced within pipeline YAML but is available to the channel and workspace systems.
