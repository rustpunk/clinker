# Channels

Channels enable multi-tenant pipeline customization. A single pipeline definition can be run with different configurations per client, environment, or business unit -- without duplicating or modifying the base YAML.

A `.channel.yaml` file declares a target pipeline (or composition), composition-level config knobs, and overrides/adds for the four scoped-variable registries the pipeline reads.

## Channel manifest

```yaml
# channels/staging.channel.yaml
channel:
  name: staging
  target: ./pipelines/my_pipeline.yaml

# Composition-level config knobs (DottedPath keys: alias.param)
config:
  default:
    enrich1.fuzzy_threshold: 0.85
  fixed:
    enrich1.lookup_table: "s3://acme/lookups/staging.csv"

# Variable overrides / adds (issue #45)
vars:
  static:                          # overrides + adds for $vars.*
    fuzzy_threshold:
      type: float
      default: 0.92
  pipeline:                        # overrides + adds for $pipeline.*
    cutoff_date:
      type: date
      default: "2026-01-01"
  source:                          # per-source-name overrides + adds for $source.*
    orders:
      ingest_label:
        type: string
        default: "staging"
  record:                          # overrides + adds for $record.*
    tier:
      type: string
      default: "bronze"
```

### Top-level fields

| Field | Required | Description |
|-------|----------|-------------|
| `channel.name` | Yes | Channel identifier; used in `--channel`, path templates, and the channel-identity stamp on the compiled plan. |
| `channel.target` | Yes | Path to the target pipeline (`*.yaml`) or composition (`*.comp.yaml`). |
| `config.default` / `config.fixed` | No | Composition-config overlays. `default` can be overridden by a higher layer; `fixed` cannot. |
| `vars.*` | No | See **Variable overrides** below. |

## Running with a channel

```
clinker run pipeline.yaml --channel ./channels/staging.channel.yaml
```

`--channel` loads the binding once, validates it against the compiled plan, applies the overlay, and seeds the executor's eval context before any record-stream-phase node runs. The channel name is also available as the `{channel}` token in output path templates.

If `channel.target` does not match the loaded `<config>` path, clinker emits **W104** and proceeds — the operator may have a legitimate reason to run a sibling pipeline against the same channel.

## Variable overrides

A pipeline exposes four scoped-variable registries:

| Read syntax | Lifetime | Pipeline declaration site |
|---|---|---|
| `$vars.<key>` | Frozen at pipeline start | Top-level `vars: { key: { type, default } }` |
| `$pipeline.<key>` | Pipeline-wide, mutable | Transform `declares: [{ name, scope: pipeline, type, default? }]` |
| `$source.<key>` | Per-source-file, mutable | Transform `declares: [{ name, scope: source, type, default? }]` |
| `$record.<key>` | Per-record, mutable | Transform `declares: [{ name, scope: record, type, default? }]` |

Each registry has a corresponding sub-block under `vars:` on a channel YAML. Every entry uses the same `{ type, default }` shape the pipeline declarations use:

- **Override** — entry name already exists in the registry. The channel-supplied `type` MUST equal the declared type (mismatch → **E107**). The channel `default` replaces the declared default after passing the same typecheck pipeline declarations use.
- **Add** — entry name not yet declared. The full `{ type, default }` becomes the new declaration in that registry.

Source overrides are keyed by source-node name (`vars.source.<src>.<var>`). Adds and overrides on `$source` apply to every file the named source ingests; an unknown source name produces **E111**.

### Reserved-system fields

Each scope has a small set of reserved field names that the engine populates. Channels cannot shadow these — attempting it produces **E110**, naming the offending scope and field. The reserved names are:

- **`$pipeline.*`** — `name`, `start_time`, `execution_id`, `batch_id`, `total_count`, `ok_count`, `dlq_count`, `filtered_count`, `distinct_count`
- **`$source.*`** — `file`, `row`, `path`, `count`, `batch`, `ingestion_timestamp`
- **`$record.*`** — none reserved

`$vars.*` has no reserved subset, so any name is fair game there.

### Composition-target channels

Channels that target a `.comp.yaml` may not carry a `vars:` block (composition var overlay is out of scope today) — the binding emits **E109** if `vars:` is non-empty. Channel-config knobs (`config:` block) on composition targets continue to work as before.

### Diagnostic codes

| Code | Meaning |
|------|---------|
| **E107** | Var override type mismatch (declared `T`, override declared `U`). |
| **E109** | Var overrides not supported on composition channels. |
| **E110** | Channel var shadows reserved system field for that scope. |
| **E111** | `vars.source.<src>` references a source-node name not declared in the pipeline. |
| **E230** | `sources.<src>` patch targets a source-node name not declared in the pipeline. |
| **E231** | `schema` patch `rename` / `retype` / `remove` of a column that does not exist. |
| **E232** | `schema` patch `add` of a column name that already exists. |
| **E233** | `schema` patch `rename` target collides with an existing column. |
| **E234** | `array_paths` patch `remove` of a path with no matching entry. |
| **E235** | `options` patch sets an unknown or mistyped option key for the source's format. |
| **E236** | A renamed/aliased column's exposed name collides with a real input field, which would mislocate that field. Raised at read time. |
| **W103** | Channel `config.*` key did not match any composition parameter in the compiled plan. |
| **W104** | `channel.target` does not match the `<config>` argument passed to `clinker run`. |

### Cross-Transform declaration uniqueness

`$pipeline`, `$source`, and `$record` are flat shared namespaces. The same name declared on more than one Transform's `declares:` is a config-validation error — clinker mirrors the fail-fast posture of Beam, Flink, Kafka Streams, Dagster, and post-fix dbt for shared-namespace key collisions. Authors who want shared state declare it once and reference everywhere.

## Source config patches

A channel can override any part of a **source** node's config through a
`sources:` block. Each patch is applied to the parsed pipeline config **before**
validation and compile, so the run behaves exactly as if the source YAML had
been hand-edited — every per-column, per-array, and per-option rule that applies
to hand-written config applies to the patched result.

```yaml
# channels/acme.channel.yaml
channel:
  name: acme
  target: ./pipelines/orders.yaml

sources:
  transactions:                            # source-node name
    options:
      record_path: batch_records           # set a scalar per-format option
    array_paths:                           # keyed by array path
      items:      { mode: join, separator: ";" }  # add-or-modify an entry
      line_items: remove                   # drop an entry
    schema:                                # keyed by column name
      amount:      { retype: float }       # change a column's CXL type
      cust_id:     { rename: customer_id }  # rename a column
      order_notes: remove                  # drop a column
      region:      { add: { type: string } }  # add a new column
```

The top-level key under `sources:` is a **source-node name**; an unknown name is
a compile error (**E230**). All ops are keyed and leaf-replace — there is no
deep-merge.

### `schema` ops

Keyed by column name. Exactly one op per column:

| Form | Effect | Errors |
|------|--------|--------|
| `<col>: { retype: <type> }` | Change the column's CXL type. | Unknown column → **E231** |
| `<col>: { rename: <new> }` | Expose the column under `<new>` while still reading the original physical column (a source-column alias). | Unknown column → **E231**; `<new>` collides with an existing column → **E233** |
| `<col>: remove` | Drop the column. | Unknown column → **E231** |
| `<new>: { add: { type: <type>, long_unique?: <bool> } }` | Add a new column (the map key is its name). | Name already exists → **E232** |

`<type>` is any CXL type (`string`, `int`, `float`, `bool`, `date`, `date_time`,
`array`, `map`).

`rename` is a **source-column alias**, not a bare relabel: the reader binds
declared columns to input fields by name, so a rename keeps reading the original
physical column and re-labels its value under the new name. Downstream CXL and
the output see the new name, carrying the original column's data. (A second
rename keeps the original physical binding.) This is the same alias a base
`schema:` column can declare directly with the `source_name` field:

```yaml
schema:
  # read the physical `cust_id` column, expose it downstream as `customer_id`
  - { name: customer_id, type: string, source_name: cust_id }
```

Omitting `source_name` (the common case) reads the input field whose key equals
`name`, unchanged from before.

### `array_paths` ops

Keyed by array path (the `path:` of an [array-path entry](../formats/json.md)):

| Form | Effect | Errors |
|------|--------|--------|
| `<path>: { mode: explode\|join, separator?: <str> }` | Add or modify the entry at that path. | — |
| `<path>: remove` | Drop the entry. | Unknown path → **E234** |

On an **existing** entry a `Set` is a partial modify — an omitted field keeps its
current value, so `{ separator: ";" }` on a `mode: join` entry leaves the mode
unchanged. On a **new** entry an omitted `mode` defaults to explode.

### `options` ops

A map of scalar per-format input options merged onto the source's current
`options:`. The merged options are re-validated through the format's option
struct, so an unknown or mistyped option key is rejected (**E235**) exactly as in
hand-written config. Which keys are valid depends on the source's `type:` (e.g.
`record_path` for `json`/`xml`, `delimiter` for `csv`).

### Applies on every run path

The patch is applied on the normal run path, on `clinker run --explain`, on
`clinker run --lineage`, and by the `clinker explain` provenance subcommand — no
`--channel` path compiles an unpatched config. A channel with no `sources:` block
leaves the pipeline config untouched. When patches change the effective source
config, the run's pipeline identity (`{pipeline_hash}` in output paths, lineage)
differs from the base and from other patched variants, so their outputs and
lineage do not collide.

### Scope

`sources:` patches target **top-level** source nodes. A source declared inside a
composition body is not reachable this way (patching it fails with **E230**,
naming the composition-body limitation); patch the composition's own source, or
lift the source to the top-level pipeline. Extending patches into composition
bodies is tracked as a follow-up.

## Workspace discovery

Channels are part of the broader workspace system. Clinker discovers workspaces via `clinker.toml` files, which can define the channel directory layout and other workspace-level settings.
