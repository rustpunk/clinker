# Scoped Variables

Clinker's scoped-variable system lets a pipeline read and write
named values at three lifetimes: the pipeline run, the source, and
the record. Each variable is **declared on the Transform that writes
it** via that Transform's `declares:` block (type, scope, optional
default), **written** by the same Transform's CXL with
`emit $<scope>.<name> = ...`, and **read inline** from any downstream
node via the `$pipeline.*`, `$source.*`, and `$record.*` namespaces.

## The three scopes

| Scope      | Lifetime                                  | Reset             | Reader namespace        |
| ---------- | ----------------------------------------- | ----------------- | ----------------------- |
| `pipeline` | Entire pipeline run                       | Never (per run)   | `$pipeline.<key>`       |
| `source`   | One per source file (`Arc<str>`-keyed)    | Per source-file   | `$source.<key>`         |
| `record`   | A single record as it flows through nodes | Per record        | `$record.<key>`         |

Record-scope variables are the per-record private store: every transform
along the row's path can read them, but they never serialize as output
columns unless explicitly re-emitted as a regular column. They are written
with `emit $record.<key> = ...` from a transform that declares them.

## Declaring variables

A scoped variable is declared on the Transform that writes it, in that
Transform's `config.declares:` list. Each entry is named, scoped, typed,
and optionally given a default that satisfies reads firing before the
writer has run:

```yaml
- type: transform
  name: enrich
  input: orders
  config:
    declares:
      - { name: cutoff_date,  scope: pipeline, type: date,   default: "2024-01-01" }
      - { name: ingest_label, scope: source,   type: string, default: "prod" }
      - { name: fuzzy_score,  scope: record,   type: float }
    cxl: |
      emit id = id
      emit $pipeline.cutoff_date = "2024-01-01"
      emit $source.ingest_label = $source.file.file_stem()
      emit $record.fuzzy_score = fuzzy_match(name, $pipeline.canonical_name)
```

Allowed types: `int`, `float`, `string`, `bool`, `date`, `date_time`.

Each `(scope, name)` pair must be declared on exactly one Transform —
the same pair declared on two Transforms is rejected at config-validation
time, ahead of compilation. `$pipeline`, `$source`, and `$record` are flat
shared namespaces; declare each name once and read it from every consumer.

The pipeline's top-level `vars:` block is a **separate, flat** registry
for static configuration read via `$vars.<key>` — it does not carry the
nested `pipeline:` / `source:` / `record:` scopes:

```yaml
pipeline:
  name: order_processing
  vars:
    fuzzy_threshold: { type: float, default: 0.85 }   # read as $vars.fuzzy_threshold
```

Built-in members of each scope (`$source.file`, `$source.row`,
`$source.path`, `$source.count`, `$source.batch`,
`$source.ingestion_timestamp`; `$pipeline.start_time`,
`$pipeline.name`, `$pipeline.execution_id`, `$pipeline.batch_id`,
`$pipeline.total_count`, `$pipeline.ok_count`, `$pipeline.dlq_count`,
`$pipeline.filtered_count`, `$pipeline.distinct_count`) are reserved —
declaring a user variable with one of those names is rejected at
parse time.

### `$source.count` semantics

`$source.count` is the per-source record total for the Source that produced the
current record. The total isn't known until the source finishes, so you can't
use it during per-record evaluation: a read on a mid-stream record (in a
Transform, Route, Window, or Merge) resolves to `Null`, while reads after the
source has finished (such as a terminal aggregate emit) resolve to the per-source
total.

This means a streaming denominator like `value / $source.count` yields
`Null` on mid-stream records. If you need a running row counter, declare
a `scope: source` variable on a Transform and increment it from that
Transform's CXL instead.

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

## Writing variables

A scoped variable is written by the Transform that declares it: list the
variable in the Transform's `declares:` block and assign it from the same
Transform's CXL with `emit $<scope>.<name> = <expr>`. The Transform still
processes records normally — declaring and writing a scoped var is
additive to its ordinary `emit`/`filter` logic.

```yaml
- type: transform
  name: capture_header
  input: salesforce_in
  config:
    declares:
      - { name: batch_id,        scope: source, type: string }
      - { name: ingestion_label, scope: source, type: string }
    cxl: |
      emit id = id
      emit $source.batch_id = batch
      emit $source.ingestion_label = $source.file.file_stem()

- type: transform
  name: row_score
  input: enrich
  config:
    declares:
      - { name: fuzzy_score, scope: record, type: float }
    cxl: |
      emit id = id
      emit $record.fuzzy_score = fuzzy_match(name, $pipeline.canonical_name)
```

An `emit $<scope>.<name>` write to a variable the Transform does **not**
declare is rejected at compile time. Requiring the `declares:` entry keeps
the dependency between writers and readers visible at plan time.

## Init phase: pre-runtime population

Set `phase: init` on a Transform to pre-compute a `$pipeline.*` or
`$source.*` value from a config-file source before the main run starts:

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

- type: transform
  name: precompute_cutoff
  input: max_agg
  config:
    phase: init
    declares:
      - { name: cutoff_date, scope: pipeline, type: int }
    cxl: |
      emit cap = cap
      emit $pipeline.cutoff_date = cap
```

Init-phase nodes **must be terminal** — no runtime-phase node may
consume from an init-phase Transform. (Init-phase nodes can chain
through init-only descendants for compositions.) Use disjoint Sources
for init vs runtime when you need both: a Source shared between an init
and a runtime branch only feeds the init pass.

## Compile-time validation

Scoped variables are checked before the run starts. Every reference and
every writer is validated, and every flow from a writer to its readers is
checked against the pipeline. Each code below tells you what to fix.

| Code | What it catches                                                        |
| ---- | ---------------------------------------------------------------------- |
| E107 | Channel var override declares a different type than the pipeline.      |
| E109 | Channel targets a composition but carries `vars:` overrides.           |
| E110 | Channel var name shadows a reserved system field for that scope.       |
| E111 | Channel `vars.source.<src>` references an unknown source-node name.    |
| E164 | An init-phase Transform has a runtime descendant.                      |
| E171 | A reader is not a transitive DAG descendant of its writer.             |
| E172 | Bare `$source.<custom>` read downstream of a Merge or Combine.         |
| E173 | Composition body reads a parent scoped var without opting in.          |
| E174 | Composition `_compose.scoped_vars` declares a different type than the parent. |
| E175 | An init-phase node reads a runtime-only writer's variable.             |
| E200 | A reference to an undeclared scoped variable (resolver-level failure). |

Cross-Transform duplicate `declares:` (the same `(scope, name)` declared
on two Transforms) is rejected before the run starts. `$pipeline`,
`$source`, and `$record` are flat shared namespaces; declare each name
once and reference it from every consumer.

Each diagnostic points at the exact place you read or wrote the variable,
plus the conflicting writer or parent declaration, so the report lands
where you can act on it rather than in some unrelated configuration block.

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
- **No undeclared writes.** A Transform may only write a scoped
  variable it lists in `declares:`; an `emit $pipeline.x` to an
  undeclared name is a compile error. Requiring the declaration keeps
  every writer visible at plan time and the writer→reader dependency
  explicit in the DAG.
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

# channel/acme-prod/orders.channel.yaml
channel:
  target: ../../pipelines/orders.yaml
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

The overlay lives in the tenant's folder (`channel/acme-prod/`) and is applied
with `--channel acme-prod`; the `channel.target` field is authoritative.

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
