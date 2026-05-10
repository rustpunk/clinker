# Scoped Variables

Clinker's scoped-variable system lets a pipeline read and write
named values at three lifetimes: the pipeline run, the source, and
the record. Variables are **declared statically** at pipeline top
with their type and scope, **read inline** from CXL via the `$pipeline.*`,
`$source.*`, and `$record.*` namespaces, and **written exclusively**
by a dedicated `state` node.

## The three scopes

| Scope      | Lifetime                                  | Reset             | Reader namespace        |
| ---------- | ----------------------------------------- | ----------------- | ----------------------- |
| `pipeline` | Entire pipeline run                       | Never (per run)   | `$pipeline.<key>`       |
| `source`   | One per source file (`Arc<str>`-keyed)    | Per source-file   | `$source.<key>`         |
| `record`   | A single record as it flows through nodes | Per record        | `$record.<key>`         |

`$record.<key>` is a separate namespace from `$meta.<key>`. Metadata
is written via `emit $meta.x = ...` from a transform and survives only
to the immediate downstream operator. Record-scope vars survive the
whole row pipeline (every transform along the row's path can read
them) but never serialize as output columns unless explicitly emitted
as a regular column.

## Declaring variables

Every scoped variable must be declared in the pipeline's top-level
`vars:` block, named, scoped, typed, and optionally given a default:

```yaml
pipeline:
  name: order_processing
  vars:
    pipeline:
      cutoff_date:
        type: date
        default: "2024-01-01"
      fuzzy_threshold:
        type: float
        default: 0.85
    source:
      batch_id:
        type: string
      ingestion_label:
        type: string
    record:
      fuzzy_score:
        type: float
```

Allowed types: `int`, `float`, `string`, `bool`, `date`, `date_time`.

Built-in members of each scope (`$source.file`, `$source.row`,
`$source.path`, `$source.count`, `$source.batch`,
`$source.ingestion_timestamp`; `$pipeline.start_time`,
`$pipeline.name`, `$pipeline.execution_id`, `$pipeline.batch_id`,
`$pipeline.total_count`, `$pipeline.ok_count`, `$pipeline.dlq_count`,
`$pipeline.filtered_count`, `$pipeline.distinct_count`) are reserved —
declaring a user variable with one of those names is rejected at
parse time.

## Reading variables

CXL access is identical for declared and built-in keys:

```yaml
- type: transform
  name: filter_recent
  input: orders
  config:
    cxl: |
      emit id = id
      filter received_at > $pipeline.cutoff_date
      emit batch = $source.batch_id
      emit confidence = $record.fuzzy_score
```

Reads of undeclared keys are rejected with **E200** (CXL name
resolution failed) at compile time, with a "did you mean" suggestion
that scans the declared registry.

## Writing variables: the `state` node

The only way to mutate a scoped variable is a dedicated `state`
node. The node is a **pass-through** for records — its input record
forwards unchanged on the output edge — but evaluates its `set:`
assignments and writes the results into the appropriate scope-keyed
runtime registry.

```yaml
- type: state
  name: capture_header
  input: salesforce_in
  config:
    scope: source
    set:
      - var: batch_id
        cxl: "first(this.batch)"
      - var: ingestion_label
        cxl: "$source.file.file_stem()"

- type: state
  name: row_score
  input: enrich
  config:
    scope: record
    set:
      - var: fuzzy_score
        cxl: "fuzzy_match(this.name, $pipeline.canonical_name)"
```

Inline mutation from a regular transform (`emit $pipeline.x = ...`)
is a parse error. The dedicated-node design keeps the dependency
between writers and readers visible at plan time.

## Init phase: pre-runtime population

A state node may declare `phase: init` to run **to completion**
before any runtime-phase node sees a record:

```yaml
- type: source
  name: config_src
  config:
    name: config_src
    type: csv
    path: config.csv
    schema:
      - { name: cutoff, type: int }

- type: aggregate
  name: max_agg
  input: config_src
  config:
    group_by: []
    cxl: |
      emit cap = max(cutoff)

- type: state
  name: precompute_cutoff
  input: max_agg
  config:
    scope: pipeline
    phase: init
    set:
      - var: cutoff_date
        cxl: "cap"
```

Init-phase nodes **must be terminal** — no runtime-phase node may
consume from an init-phase state node. (Init-phase state nodes can
chain through init-only descendants for compositions.) Use disjoint
Sources for init vs runtime when you need both, since a Source shared
between an init and a runtime branch only feeds the init pass.

## Compile-time validation

Scoped variables earn their architectural payoff at plan time.
Every reference and every writer is checked against a static
registry, and every cross-DAG flow is verified against the topology.

| Code | What it catches                                                        |
| ---- | ---------------------------------------------------------------------- |
| E107 | Channel var override declares a different type than the pipeline.      |
| E109 | Channel targets a composition but carries `vars:` overrides.           |
| E110 | Channel var name shadows a reserved system field for that scope.       |
| E111 | Channel `vars.source.<src>` references an unknown source-node name.    |
| E164 | An init-phase state node has a runtime descendant.                     |
| E171 | A reader is not a transitive DAG descendant of its writer.             |
| E172 | Bare `$source.<custom>` read downstream of a Merge or Combine.         |
| E173 | Composition body reads a parent scoped var without opting in.          |
| E174 | Composition `_compose.scoped_vars` declares a different type than the parent. |
| E175 | An init-phase node reads a runtime-only writer's variable.             |
| E200 | A reference to an undeclared scoped variable (resolver-level failure). |

Cross-Transform duplicate `declares:` (the same `(scope, name)` declared
on two Transforms) is rejected at config-validation time, ahead of
compilation. `$pipeline`, `$source`, and `$record` are flat shared
namespaces; declare each name once and reference it from every consumer.

Each diagnostic carries the offending span plus secondary spans
pointing at the conflicting writer or the parent declaration, so
the report shows up where the user is reading or writing — not in
some unrelated configuration block.

## Post-merge access: qualified `$source.<input>.<key>`

After a Merge or Combine, the bare `$source.<custom>` form is
ambiguous: each record carries its own source's value, but the
reader's intent is usually to compare across inputs. E172 rejects
the unqualified form and the qualified form is the legal alternative:

```yaml
- type: transform
  name: read_after_merge
  input: merged
  config:
    cxl: |
      emit id = id
      emit lt = $source.left_input.left_label
      emit rt = $source.right_input.right_label
```

The `<input_name>` segment matches the named input on the Combine
(its IndexMap key) or the upstream node name on the Merge.

## Composition opt-in

A composition body cannot see parent scoped variables by default —
the seal is enforced by E173. To pass values across the boundary,
the composition declares the schema of parent vars it consumes in
its `_compose.scoped_vars` block:

```yaml
# read_pipeline_var.comp.yaml
_compose:
  name: read_pipeline_var
  inputs:
    inp:
      schema:
        - { name: id, type: int }
  outputs:
    out: tap
  scoped_vars:
    pipeline:
      cutoff:
        type: int

nodes:
  - type: transform
    name: tap
    input: inp
    config:
      cxl: |
        emit id = id
        emit cutoff_seen = $pipeline.cutoff
```

The parent must declare `cutoff` with the matching type; mismatches
raise E174.

## What scoped variables are not

These are intentional non-features:

- **No persistence across runs.** State is in-memory only. A pipeline
  run starts with declaration defaults; the writes don't survive
  the process.
- **No inline `emit $pipeline.x` writes.** Convenience-style
  mutation from a transform body is forbidden — empirical evidence
  from comparable engines shows it leads to race conditions and
  hidden DAG dependencies.
- **No dynamic var creation.** The set of variables is closed at
  plan time, by design. This bounds memory and makes the validation
  matrix above tractable.

## Channel overrides

A channel can both override a pipeline's declaration defaults and add
new entries across all four registries (`$vars.*`, `$pipeline.*`,
`$source.*`, `$record.*`). Each registry has its own sub-block under
`vars:` on a `.channel.yaml`, and each entry uses the same
`{ type, default }` shape that pipeline-side declarations use:

```yaml
# Pipeline declarations
pipeline:
  name: orders
  vars:
    fuzzy_threshold: { type: float, default: 0.85 }   # $vars.*
nodes:
  - type: source
    name: orders_src
    config: { name: orders_src, type: csv, path: in.csv,
              schema: [{ name: id, type: int }] }
  - type: transform
    name: enrich
    input: orders_src
    config:
      declares:
        - { name: cutoff_date,  scope: pipeline, type: date,   default: "2024-01-01" }
        - { name: ingest_label, scope: source,   type: string, default: "prod" }
        - { name: tier,         scope: record,   type: string, default: "bronze" }
      cxl: |
        emit id = id

# channels/acme-prod.channel.yaml
channel:
  name: acme-prod
  target: ./pipelines/orders.yaml
vars:
  static:
    fuzzy_threshold: { type: float, default: 0.95 }
  pipeline:
    cutoff_date: { type: date, default: "2026-01-01" }
  source:
    orders_src:
      ingest_label: { type: string, default: "acme-prod" }
  record:
    tier: { type: string, default: "platinum" }
```

Override semantics (entry name already declared) require the channel's
`type` to match the declared type — mismatches produce **E107**. Add
semantics (entry name not yet declared) extend the registry with a new
declaration. `$source` overrides are keyed by source-node name; an
unknown source name produces **E111**. The reserved-name guard
(**E110**) blocks channels from shadowing system fields like
`$pipeline.execution_id` or `$source.path`. Channels that target a
`.comp.yaml` may not carry `vars:` (**E109** if they do).

See [Channels](channels.md) for the full overlay rules and the channel
manifest reference.
