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
| **W103** | Channel `config.*` key did not match any composition parameter in the compiled plan. |
| **W104** | `channel.target` does not match the `<config>` argument passed to `clinker run`. |

### Cross-Transform declaration uniqueness

`$pipeline`, `$source`, and `$record` are flat shared namespaces. The same name declared on more than one Transform's `declares:` is a config-validation error — clinker mirrors the fail-fast posture of Beam, Flink, Kafka Streams, Dagster, and post-fix dbt for shared-namespace key collisions. Authors who want shared state declare it once and reference everywhere.

## Workspace discovery

Channels are part of the broader workspace system. Clinker discovers workspaces via `clinker.toml` files, which can define the channel directory layout and other workspace-level settings.
