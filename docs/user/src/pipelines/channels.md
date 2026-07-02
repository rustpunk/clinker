# Channels

Channels make one pipeline serve many tenants. A single base pipeline is
authored once; each tenant (a **channel**) layers its own configuration,
variable defaults, and structural changes on top — without copying or editing
the base YAML. The system is built for scale: thousands of per-tenant channels
against one pipeline, with strict validation and per-value provenance.

A channel is a tenant. A **group** is a reusable overlay shared by many
channels — selected automatically from a channel's labels, or invoked by name.
Everything a channel or group contributes is expressed through two surfaces:
**value clobber** (`config:` / `vars:`) and an ordered **op list**
(`overrides:`).

## Workspace layout

Channels live in a channel-centric workspace. A `clinker.toml` at the workspace
root declares the layout roots; the rest is folders of YAML:

```
workspace/
  clinker.toml                       # declares the [channel] and [group] roots
  pipeline/       *.yaml             # base pipelines  (the pipeline-default layer)
  composition/    *.comp.yaml        # reusable sub-pipelines
  schema/         *.schema.yaml      # shared schemas
  group/          *.group.yaml       # group overlays: selector, priority, overrides
  channel/<tenant>/                  # one folder per channel; the folder name is the channel id
    channel.cfg.yaml                 # channel manifest: labels + channel-wide overlays (optional)
    <target>.channel.yaml            # per-target overlay of a pipeline
    <target>.comp.yaml               # per-target overlay of a composition
```

The channel **folder name is the channel id** — `channel/globex/` is the
`globex` channel. A `--channel globex` invocation resolves by a computed path
(`channel/globex/…`), never an O(N) scan of the workspace; the full scan is
reserved for `channels lint`.

### `clinker.toml` roots

```toml
[channel]
root = "channel"      # per-channel folders live under <root>/<channel-id>/
shard = "none"        # enumeration layout: none (default) | first-char | hash

[group]
root = "group"        # *.group.yaml definitions live here
```

Both tables are optional; omitting them defaults `[channel].root` to `channel`,
`[channel].shard` to `none`, and `[group].root` to `group`. `shard` is an
enumeration-ergonomics choice for very large channel trees (it splits the folder
fan-out); a channel is always looked up by computed path regardless of shard
scheme, so `shard` never changes resolution semantics.

## The layer model

Every value and every op is attributed to exactly one **layer**. Layers apply in
a fixed semantic order — never lexical or file order:

```
pipeline-default  <  group(s) by priority  <  channel-wide  <  channel-per-target
```

- **pipeline-default** — the base pipeline's own configuration.
- **group(s) by priority** — every group applied to the run, ordered by
  `priority` (higher priority applies later and thus wins).
- **channel-wide** — the channel manifest (`channel.cfg.yaml`): overlays that
  apply to *every* pipeline this channel runs.
- **channel-per-target** — the per-target overlay file
  (`<target>.channel.yaml`): the highest-precedence layer.

### Clobber, never deep-merge

A higher layer's value **replaces** the lower layer's value wholesale. There is
no deep-merge and no list-append: overriding a list swaps the entire list. To
override individual elements, model them as a keyed map (which the `config:` and
`overrides:` surfaces already are), not a list — so each element is addressed and
replaced by key. Every resolved value maps 1:1 back to the single layer that
supplied it, and `channels resolve` / `explain --field` report that layer.

Structural ops (`overrides:`) apply in a total order — layer precedence first,
then declaration order within a layer. Collisions are **errors, never silent
no-ops**: adding a node whose name already exists, or targeting a missing or
already-removed node, fails with a diagnostic anchored to the offending op.

Overlays are applied **pre-compile**: layers are resolved, `config`/`vars` values
are clobbered, the `overrides:` op streams are concatenated in total order and
folded over the base pipeline's node list, and only then does schema binding and
compilation run. One invocation produces one effective plan.

## Value clobber: `config` and `vars`

The value-clobber surface carries scalar overrides. It appears identically on a
group, a channel manifest, and a per-target overlay.

`config:` overrides composition **config knobs**, keyed by `alias.param` dotted
paths (the composition node's alias, then the parameter name):

```yaml
config:
  scorer.threshold: 0.95     # override the `threshold` knob of the `scorer` composition node
```

A `config:` key that matches no parameter in the compiled plan is a hard error
([E113](#diagnostics)) — a misspelled or stale key aborts the run rather than
silently doing nothing.

`vars:` overrides or adds scoped-variable defaults, using the same four scopes a
pipeline's own `vars:` block uses (`$vars.*` / `$pipeline.*` / `$source.*` /
`$record.*`). Each leaf is the same `{ type, default }` shape a pipeline
declaration uses:

```yaml
vars:
  static:                    # $vars.*
    currency: { type: string, default: "USD" }
  pipeline:                  # $pipeline.*
    cutoff_date: { type: date, default: "2026-01-01" }
  source:                    # $source.<src>.*  — outer key is the source-node name
    orders:
      ingest_label: { type: string, default: "prod" }
  record:                    # $record.*
    tier: { type: string, default: "bronze" }
```

See [Variables](variables.md) for the scoped-variable model these overlay.

## Structural ops: `overrides`

The `overrides:` surface is an **ordered list of discrete, name-addressed ops**
applied to the base pipeline's node list before compilation. Each op is a
mapping with an `op:` discriminant. Unknown keys, or keys that belong to a
different op kind, are rejected at parse time.

The op vocabulary is `add` / `remove` / `replace` / `set` / `bypass` /
`patch_schema`.

### `add` — splice in a node

Insert a new node, either inline or as a composition reference. The splice
anchor is exactly one of `after:` / `before:` / an explicit `input:`.

```yaml
overrides:
  # Inline transform, spliced after `normalize` (its former consumers now read `stamp`):
  - op: add
    node:
      type: transform
      name: stamp
      input: normalize
      config:
        cxl: "emit order_id = order_id"
    after: normalize

  # A composition, named by `alias`, with a config knob for the injected node:
  - op: add
    composition: ../composition/fraud_check.comp.yaml
    alias: fraud_check
    after: normalize
    config:
      threshold: 0.8
```

`after: X` reads from `X` and repoints `X`'s former consumers onto the new node;
`before: X` feeds `X`, taking over `X`'s former upstream. An inline node with no
splice anchor keeps its own declared `input:`. Adding a node whose name already
exists is an error.

### `remove` — delete a node and rewire

Delete a node by name, repointing its named consumers through an explicit
`rewire:` map so no dangling reference is left behind:

```yaml
overrides:
  - op: remove
    target: legacy_audit
    rewire:
      route_priority.input: product_lookup   # <consumer>.input: <new upstream>
```

Each `rewire:` key is a `<node>.input` path; each value is the replacement
upstream. Any consumer still referencing the removed node afterward is an error,
as is removing a node that does not exist.

### `bypass` — remove a linear node

Sugar for `remove` on a 1-in/1-out node: it auto-rewires the node's sole
consumer onto its sole upstream.

```yaml
overrides:
  - op: bypass
    target: legacy_audit
```

`bypass` only applies to a single-input, single-consumer node; a fan-in/fan-out
node must use the explicit `remove` op with a spelled-out `rewire:` map.

### `replace` — swap a node's definition

Replace a whole node by name, keeping its identity (and therefore every consumer
edge) intact. The replacement node's own `name:` must equal `target:`.

```yaml
overrides:
  - op: replace
    target: normalize
    node:
      type: transform
      name: normalize
      input: orders
      config:
        cxl: "emit order_id = upper(order_id)"
```

### `set` — set one field within a node

Set a single field within a named node by path. The currently addressable path
is `config.cxl` — the primary CXL body of a `transform` / `aggregate` /
`combine` node — so replacing a stage's logic wholesale is a `set`, not a
special case:

```yaml
overrides:
  - op: set
    target: route_priority
    field: config.cxl
    value: >
      emit _route = if priority_level == "urgent"
        then "priority_report" else "fulfilled_orders"
```

Any other field path is a hard error, never a silent no-op.

### `patch_schema` — shape a source's columns

Add / rename / retype / remove columns on a **source** node's declared schema,
via a **column-name-keyed map** (the map key is the column name). Each column
carries exactly one op:

```yaml
overrides:
  - op: patch_schema
    target: orders
    schema:
      amount:      { retype: float }              # change an existing column's type
      cust_id:     { rename: customer_id }         # rename (a physical->logical alias)
      order_notes: remove                          # drop an existing column (bare scalar)
      region:      { add: { type: string } }       # add a new column (map key = new name)
```

The keyed-map shape (rather than a list) is deliberate: a column op is addressed
and leaf-replaced by name, with first-class `rename` / `remove` / `add`, exactly
matching the [source-config schema patch](#source-config-patches) grammar so the
two surfaces resolve columns and their diagnostics identically.

`rename` is a **source-column alias**, not a bare relabel: the reader still binds
the original physical column and re-labels its value under the new name, so
downstream CXL and the output see the new name carrying the original column's
data. A missing column, an add that collides with an existing name, or a rename
onto an existing name are all errors ([E231–E233](#diagnostics)).

## Groups and selectors

A group (`group/<name>.group.yaml`) is a reusable overlay layer that sits
between the pipeline default and the channel layers. It carries the same two
surfaces every layer carries — `config:` / `vars:` value clobber and an
`overrides:` op list:

```yaml
group:
  name: enterprise
  match: 'tier == "enterprise"'   # optional selector; higher priority wins
  priority: 20
config:
  scorer.threshold: 0.8
overrides:
  - op: add
    node:
      type: transform
      name: fraud_stamp
      input: normalize
      config:
        cxl: "emit order_id = order_id"
    after: normalize
```

A group plays two roles under one concept:

- **Selector-derived** — when `match:` is present, the group is applied
  automatically to every channel whose labels satisfy the CXL boolean. Multiple
  matching groups are ordered by `priority` (higher wins; the default priority
  is `0`).
- **Standalone / explicit** — when `match:` is absent, the group is never
  auto-selected; it applies only when invoked by name with `--group`. Groups are
  channel-agnostic — their overrides never read channel labels — so any group can
  run standalone against the base pipeline, with or without a channel.

### Selectors are label-only CXL

`match:` is a bare [CXL](../cxl/overview.md) boolean expression evaluated in a
**restricted label-only context**: the only names in scope are the channel's
`labels`. `$record` / `$source` / `$pipeline` / `$vars` / `$doc`, window and
aggregate calls, `now`, and wildcards are all rejected, so a selector is a pure,
deterministic predicate over labels.

```yaml
match: 'region == "west" and tier == "enterprise"'
```

Labels are typed from their YAML/JSON scalar kind (string, bool, int, float), so
the typechecker rejects label/literal type mismatches. A selector that
references a label a channel does not declare is a **hard error, never a silent
`false`** — a typo surfaces as an unresolved-identifier error rather than
quietly excluding the channel.

### The channel manifest

`channel.cfg.yaml` declares a channel's identity labels and optional
channel-wide overlays (applied to every pipeline this channel runs):

```yaml
channel:
  name: globex
labels: { region: west, tier: enterprise }   # identity — drives group selectors
config:
  scorer.threshold: 0.9                        # channel-wide value clobber (optional)
vars:
  static:
    currency: { type: string, default: "USD" }
overrides: []                                  # channel-wide op list (optional)
```

Labels are **identity, never a pipeline override**. The manifest is optional: a
channel with no labels and no channel-wide overlays needs no `channel.cfg.yaml`
at all — its folder name is still its id. But a channel that groups select on
must declare the labels those selectors read, otherwise the selector errors on
the unresolved label rather than cleanly not matching.

### The per-target overlay

`<target>.channel.yaml` overlays a single pipeline (or `<target>.comp.yaml` a
composition). The `channel.target:` field is authoritative — the filename suffix
is optional and, when present, must agree:

```yaml
channel:
  target: ../../pipeline/order_fulfillment.yaml
config:
  scorer.threshold: 0.95
overrides:
  - op: patch_schema
    target: orders
    schema:
      tax_exempt: { add: { type: bool } }
```

## CLI surface

### Running with overlays

```
clinker run pipeline/order_fulfillment.yaml --group enterprise --base-dir .
```

`run` applies groups by name from the workspace (rooted at `--base-dir`, default
the current directory) and folds their resolved overrides into the plan before
execution. Overlay flags shared across `run` and `explain`:

| Flag | Meaning |
|------|---------|
| `--group <NAME>` | Force-include a group overlay by name (repeatable), with or without a channel. |
| `--no-auto-groups` | Suppress selector-derived group membership; only explicit `--group` overlays apply. |
| `--channel <FILE>` | Apply a single channel file's value overlay. See the [legacy note](#legacy-channel-file-path) below. |

`explain --field <alias.param> --group <NAME>` reports the same overlay stack for
provenance lookups, mirroring `run`.

### Inspecting overlays

`channels resolve` renders the effective post-overlay DAG for one target under a
chosen channel and/or groups, with per-value provenance — which layer supplied
each value and which group injected which node:

```
# Resolve the effective plan for the globex channel (derives matching groups from its labels)
clinker channels resolve pipeline/order_fulfillment.yaml --channel globex --base-dir .

# Preview a group overlay standalone (no channel)
clinker channels resolve pipeline/order_fulfillment.yaml --group enterprise --base-dir .
```

Here `--channel` is a **channel id** (the folder name under the channel root),
resolved by computed path; `resolve` derives that channel's matching groups from
its labels unless `--no-auto-groups` is passed.

`channels lint` compiles every channel/group overlay in the workspace and reports
every failure — the one full-tree scan in the system:

```
clinker channels lint --base-dir .
```

### Membership and labels

```
# List the channels a group's selector currently matches
clinker channels group members enterprise --base-dir .

# Stamp/overwrite a label across one or more channels (idempotent)
clinker channels label set tier=enterprise globex initech --base-dir .
```

`channels label set` takes a `key=value` assignment; the value is typed by YAML
scalar inference (`true`/`false` → bool, integers → int, decimals → float, else
string) so numeric and boolean labels compare correctly against selectors.

### Renaming a base node

`refactor rename-node` renames a base node and propagates the rename to every
overlay that references it (splice anchors, `target:`, `rewire:` keys) across the
workspace:

```
# Preview every file that would change
clinker refactor rename-node pipeline/order_fulfillment.yaml orders purchases --dry-run

# Apply it, then re-lint
clinker refactor rename-node pipeline/order_fulfillment.yaml orders purchases --base-dir .
clinker channels lint --base-dir .
```

The new name must be letters, digits, and `_` only.

## Source config patches

Independent of the overlay op engine, a channel file can patch a **source**
node's parsed config directly through a `sources:` block, applied before
validation and compile so the run behaves exactly as if the source YAML had been
hand-edited. This is the same column-keyed schema grammar `patch_schema` reuses,
plus array-path and per-format option patches.

```yaml
sources:
  transactions:                            # source-node name (unknown -> E230)
    options:
      record_path: batch_records           # set a scalar per-format option (bad key -> E235)
    array_paths:                            # keyed by array path
      items:      { mode: join, separator: ";" }   # add-or-modify an entry
      line_items: remove                    # drop an entry (unknown path -> E234)
    schema:                                 # keyed by column name
      amount:      { retype: float }
      cust_id:     { rename: customer_id }
      order_notes: remove
      region:      { add: { type: string } }
```

All ops are keyed and leaf-replace — there is no deep-merge. On an existing
`array_paths` entry a partial map is a modify (an omitted field keeps its current
value); on a new entry an omitted `mode` defaults to explode. `options` are
merged onto the source's current options and re-validated through the format's
option struct, so an unknown or mistyped key is rejected exactly as in
hand-written config. A `schema` `rename` is a source-column alias — the same
alias a base column can declare directly with `source_name:`:

```yaml
schema:
  # read the physical `cust_id` column, expose it downstream as `customer_id`
  - { name: customer_id, type: string, source_name: cust_id }
```

`sources:` patches target **top-level** source nodes; a source declared inside a
composition body is not reachable this way (patching it fails with E230). When a
patch changes the effective source config, the run's pipeline identity differs
from the base and from other patched variants, so their outputs and lineage do
not collide.

## Diagnostics

| Code | Meaning |
|------|---------|
| **E113** | A `config:` / override key matches no composition parameter in the compiled plan. A misspelled or stale key aborts the run instead of silently doing nothing. |
| **E114** | An overlay op failed to apply (missing splice anchor, duplicate node name, missing/removed `target`, invalid `set` field, invalid `bypass` node). The diagnostic is anchored to the offending op's source span, not the base pipeline. |
| **E230** | A source patch (`sources.<src>` or `patch_schema`) targets a source-node name not present in the pipeline. |
| **E231** | A schema `rename` / `retype` / `remove` of a column that does not exist. |
| **E232** | A schema `add` of a column name that already exists. |
| **E233** | A schema `rename` whose target name collides with an existing column. |
| **E234** | An `array_paths` `remove` of a path with no matching entry. |
| **E235** | An `options` patch sets an unknown or mistyped option key for the source's format. |
| **E236** | A renamed/aliased column's exposed name collides with a real input field, which would mislocate that field. Raised at read time. |

## Known behavior notes

Two current gaps are tracked as follow-up issues; both surface in the overlay
tooling today:

- **`config:` value overrides are provenance-only, not yet applied at runtime.**
  A `config:` value clobber currently writes to the compiled plan's provenance
  side-table — so `channels resolve` and `explain --field` *report* the
  overridden value and its winning layer — but lowering and the executor do not
  yet read it, so the overridden composition parameter does not change executed
  behavior. `vars:` overrides and the structural `overrides:` ops *do* reach
  runtime. This is inherited from the value-clobber mechanism; making `config:`
  values functional at runtime is tracked in issue #765.

- **A legacy single-file channel path still coexists.**<a id="legacy-channel-file-path"></a>
  `run --channel <FILE>` / `explain --channel <FILE>` accept a single channel
  *file* and apply its value overlay through the older post-compile path. Because
  a bare channel file carries no manifest labels, selector-derived groups do not
  auto-apply on `run` — only explicit `--group` overlays do (the folder-based
  `channels resolve` / `lint`, which take a channel *id*, do derive groups from
  labels). Ripping the legacy file path so label-derivation works uniformly on
  `run` is tracked in issue #764.
