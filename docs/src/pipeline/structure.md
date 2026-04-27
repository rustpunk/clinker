# Pipeline YAML Structure

A Clinker pipeline is a single YAML file with three top-level sections: `pipeline` (metadata), `nodes` (the processing graph), and optionally `error_handling`.

## Top-level shape

```yaml
pipeline:
  name: my_pipeline            # Required — pipeline identifier
  memory_limit: "256M"         # Optional (K/M/G suffixes)
  vars:                        # Optional key-value pairs
    threshold: 500
    label: "Monthly Report"
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
| `memory_limit` | No | Soft RSS budget. Accepts `K`, `M`, `G` suffixes (e.g. `"512M"`). |
| `vars` | No | Scalar constants accessible in CXL via `$vars.*`. |
| `date_formats` | No | List of `strftime`-style patterns for date parsing. |
| `rules_path` | No | Directory for CXL `use` module resolution. |
| `concurrency` | No | `threads` and `chunk_size` for parallel chunk processing. |
| `metrics` | No | `spool_dir` for per-run JSON metric files. |
| `date_locale` | No | Locale for date formatting. |
| `include_provenance` | No | Attach provenance metadata to records. |

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
| `output` | Writes records to a file |
| `composition` | Imports a reusable transform fragment |

## Node naming

Every node must have a `name:` field. Names must be unique within the pipeline and **must not contain dots** -- the dot character is reserved for port syntax (see below). Names are used for wiring, logging, and diagnostics.

## Wiring: input and inputs

Nodes connect to each other through `input:` (singular) and `inputs:` (plural) fields that live at the node's top level, alongside `name:` and `type:`.

**Single upstream** -- used by transform, aggregate, route, and output nodes:

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

**Source nodes have no input field.** They are entry points -- adding an `input:` field to a source is a parse error.

Using `inputs:` on a non-merge node (or `input:` on a merge node) is caught at parse time by `deny_unknown_fields`.

## Optional fields on all nodes

Every node type supports these optional fields:

- **`description:`** -- human-readable text for documentation. Ignored by the engine.
- **`_notes:`** -- arbitrary metadata (JSON object). Ignored by the engine, used by the Kiln IDE for visual annotations and inspector panels.

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
