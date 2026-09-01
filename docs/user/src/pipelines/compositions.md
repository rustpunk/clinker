# Compositions

Compositions are reusable pipeline fragments that can be imported into multiple pipelines. They encapsulate common transform patterns -- date derivations, address normalization, currency conversion -- into self-contained, testable units.

## Using a composition

A composition node in your pipeline references an external `.comp.yaml` file:

```yaml
- type: composition
  name: risk
  input: orders
  use: "./compositions/risk_score.comp.yaml"
  inputs:
    inp: orders
  config:
    threshold: 0.5
```

The `use:` field points to the composition definition file. The `inputs:` map
binds each declared composition input port to an upstream node. The top-level
`input:` is also required by the current node shape, but `inputs:` is the
authoritative port wiring. The `config:` block passes parameters that customize
this invocation.

### Resolving the `use:` path

A `use:` value names a `.comp.yaml` in the workspace. It is resolved
relative to the directory of the pipeline file being compiled, then
against the set of `.comp.yaml` files discovered under the workspace root,
finally falling back to a filename match. A `use:` that resolves to no
`.comp.yaml` — a typo, a wrong relative prefix, or a file that does not
exist — fails compilation with a spanned `E103` diagnostic naming the
composition node. The whole run aborts loudly; it does not silently drop
the composition and write an empty output. The same holds for the other
composition-binding errors (`E102`–`E108`): an ill-bound call site fails
compile rather than producing a run that writes zero records. Run
`clinker explain --code E103` for details.

## Composition definition file

A `.comp.yaml` file declares its interface in `_compose:` and its executable
subgraph in `nodes:`:

```yaml
# compositions/risk_score.comp.yaml
_compose:
  name: risk_score
  inputs:
    inp:
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  outputs:
    out: scored
  config_schema:
    threshold:
      type: float
      default: 0.5
      range: [0.0, 1.0]

nodes:
  - type: transform
    name: scored
    input: inp
    config:
      cxl: |
        emit order_id = order_id
        emit amount = amount
        emit high_value = amount >= $config.threshold * 2000.0
```

### Composition fields

| Field | Required | Description |
|-------|----------|-------------|
| `_compose.name` | Yes | Composition identifier |
| `_compose.inputs` | Yes | Named input ports and their minimum required schemas |
| `_compose.outputs` | Yes | Output port aliases pointing to body nodes or route ports |
| `_compose.config_schema` | No | Typed configuration parameters, defaults, and constraints |
| `_compose.scoped_vars` | No | Explicit scoped-variable names the sealed body may read from its caller |
| `_compose.resources_schema` | No | Typed resource slots; see [Resource bindings](#resource-bindings) |
| `nodes` | Yes | Unified node list for the sealed composition body |

### Reading config parameters in the body

A composition body reads its own config parameters as [`$config.<param>`](../cxl/system-variables.md#config-composition-config-parameters). The planner constant-folds each reference to the value resolved for that instantiation — the call site's `config:` value, or a [channel/group](channels.md) `config:` override, or the declared default — so the same composition used with different `config:` compiles to different bodies. Because the resolution happens per instantiation, a channel or group `config:` override changes what the body computes, not just the reported provenance.

### Explaining config provenance

Every resolved composition parameter retains its base value and each attempted
group, channel-wide, and per-target override. The winning layer, shadowed
values, fixed locks, and source spans remain attached to the stable compiled
node identity. Inspect a value with either a unique shorthand or its exact
versioned address:

```console
clinker explain pipeline.yaml --field 'risk.threshold'
clinker explain pipeline.yaml \
  --field '/v1/config/nodes/risk/fields/threshold'
```

The output includes the canonical exact address and lists layers in a stable
order. Exact addresses include every enclosing composition call. For example,
two sibling calls may each contain a local node named `shared`:

```text
/v1/config/calls/left/nodes/shared/fields/threshold
/v1/config/calls/right/nodes/shared/fields/threshold
```

In that case `shared.threshold` fails with E118 instead of selecting one by
insertion order. The diagnostic lists both exact addresses in deterministic
order; copy the intended `--field` correction. An unknown query fails with
E117 and lists only same-field candidates, never unrelated nodes or fields. An
empty query fails with E116.

Address segments use RFC 6901 escaping: `~` becomes `~0` and `/` becomes `~1`.
Unicode remains unchanged. This makes rendering and parsing lossless, including
after provenance serialization and repeated inspection of a compiled plan.

Source-schema provenance uses the parallel exact form
`/v1/schema/sources/<source>/columns/<column>/attributes/<attribute>`; the
three-part `source.column.attribute` shorthand remains available.

### Body validation

Nodes inside a composition body are validated with the same node-scoped
config checks as top-level pipeline nodes. A body node that would be
rejected at the top level — an `envelope` wiring the not-yet-supported
`trailer:` port, a `transform` declaring a reserved variable name or a
default that does not match its declared type, an invalid log
directive, or a `batch_size: 0` — fails compilation with an `E115`
diagnostic naming the composition call site, the body file, and the
violation. Run `clinker explain --code E115` for details.

A body `source` or `sink` that sets a CSV `delimiter` or `quote_char`
which is not exactly one ASCII byte is likewise rejected at compile time,
not first at run, with the same one-byte rule top-level nodes get.

A body `source` or `sink` whose `schema:` names an external
`.schema.yaml` file has that path resolved relative to the composition
file's own directory (not the invoking pipeline's), and the file's columns
are inlined before the body binds. A body Sink therefore rounds
`decimal` columns to their declared [`scale`](../nodes/sink.md#rounding-decimals-to-a-declared-scale)
at the write boundary exactly as a top-level Sink does.

### Executable example corpus

The five fragments under `examples/pipelines/compositions/` are executable
examples of the current authoring surface:

- Clean Names
- Fiscal Date Fields
- Order Classification
- Shipping Cost
- Validate Email

Each fragment uses `_compose.inputs`, `_compose.outputs`, `config_schema`, and
the unified `nodes` list. The two date-dependent examples require an explicit
`as_of_date` configuration value so their results do not depend on the day the
test runs.

The composition example test recursively inventories every `.comp.yaml` file
in that directory. Its case manifest must name exactly that discovered set:
empty or missing inventories, duplicate keys, missing or extra cases, and paths
that escape the corpus directory fail with distinct diagnostics before any
example runs. Each case is then loaded through the production composition
loader, placed in a generated pipeline, and executed by the real `clinker`
binary. The test checks the exit status, record counters, and output bytes.

`clinker run --explain` proves that a generated pipeline compiles. It does not
prove that the example produces the documented result; the executable corpus's
byte comparison is the behavior check.

## Advanced wiring

For compositions with multiple input ports, bind every declared port by name:

```yaml
- type: composition
  name: enrich_address
  input: orders
  use: "./compositions/order_product_enrich.comp.yaml"
  inputs:
    orders: orders
    products: product_catalog
```

The primary `input:` must be present for the current YAML node shape. The
planner builds composition edges from the named `inputs:` map, so that map must
contain every required port declared by `_compose.inputs`.

Downstream nodes consume a composition's declared output ports using the usual
`node.port` syntax. If there is only one output port, the bare composition node
name selects it.

### Resource bindings

A composition declares the external capabilities it needs as typed slots. The
currently admitted kind is `file`; it requires read capability and a run-local
file opener:

```yaml
_compose:
  name: order_lookup
  inputs:
    input: { schema: [{ name: order_id, type: string }] }
  outputs: { out: order_reference }
  config_schema: {}
  resources_schema:
    orders:
      kind: file
      required: true

nodes:
  - type: source
    name: order_reference
    config:
      name: order_reference
      type: csv
      resource: orders
      schema: [{ name: order_id, type: string }]
```

`resource:` is an explicit body-Source-to-slot link. The slot must be declared
by the enclosing `_compose.resources_schema`; the Source name and its format do
not select a resource implicitly. A resource-backed body Source must not also
declare `path`, `glob`, `regex`, or `paths`, because the bound catalog resource
is its only external target. Every authored body Source must declare
`resource:`. Composition input ports are separate synthetic roots: to consume
caller-provided rows, declare `_compose.inputs.<port>` and set a downstream
node's `input: <port>` instead of authoring a Source node for that port.

Top-level Sources are unchanged: they continue to require exactly one direct
matcher. `resource:` on a top-level Source is rejected until a separate
top-level binding surface is designed.

The workspace supplies a concrete, secret-free descriptor under a logical
identity in `clinker.toml`:

```toml
[catalog.resources.shared_orders]
kind = "file"
path = "data/orders.csv"
access = "read"
```

The call site binds only the declared slot to that logical identity:

```yaml
- type: composition
  name: lookup
  input: orders
  use: ./compositions/order_lookup.comp.yaml
  inputs: { input: orders }
  resources: { orders: shared_orders }
```

Resource descriptors are strict. Unknown kinds or fields, inline objects,
unknown catalog identities, undeclared slots, missing required slots, and
kind/capability mismatches fail planning. A call site cannot contain a path,
credential profile, secret, token, or opened handle. File descriptors must
remain inside the workspace and the catalog is admitted under fixed entry and
descriptor-byte limits.

Planning retains the winning logical identity and every attempted overlay
layer for each binding. That identity also participates in the semantic plan
fingerprint. For each authored body Source, planning compiles a distinct
call-site-scoped instance carrying only the slot, logical identity, resource
kind, required capabilities, opener family, run lifetime, provenance, and
stable logical dataset identity. It does not retain the catalog's physical
path. During `clinker run`, the CLI resolves that credential-free `file`
requirement at the workspace edge and transfers an opaque single-use reader
factory to the executor. The executor acquires the complete compiled group,
opens all of its Sources before starting any of them, and streams their finite
records through the ordinary bounded Source path. An open/read failure or
interruption closes every opened session and releases the group. This surface
still does not select credentials: a group requiring credentials fails before
runtime effects because no credential-profile option exists yet.

Ordinary composition calls do not have `outputs:` or `alias:` fields. Either
key fails with E377 at its authored location. Use `_compose.outputs` for the
public output contract and the composition node's `name` for its
caller-visible namespace. `add.alias` remains valid only inside an overlay
`add` operation, where it names the inserted node.

### Call-site fields

| Field | Required | Description |
|-------|----------|-------------|
| `input` | Yes | Primary upstream required by the current node shape |
| `use` | Yes | Path to the `.comp.yaml` definition |
| `inputs` | Yes for declared ports | Map of composition input ports to upstream node references |
| `config` | No | Parameter overrides (key-value pairs) |
| `resources` | No | Declared slot to logical `[catalog.resources]` identity; scalar values only |
| `outputs` | Rejected | E377: declare ports under `_compose.outputs` and use `node.port` downstream |
| `alias` | Rejected | E377: use this composition node's `name` as the namespace |

### Contract status

The bounded catalog, typed file slot/binding, overlay provenance, stable
logical dataset identity, and E377 call-surface rejection implement the
planning half of D-12 through D-16. Credential references, credential
resolution, and runtime handle activation are not implemented; a resource
binding is therefore a validated planning contract, not permission to perform
I/O.

See the canonical
[composition-resource and call-site contract](https://github.com/rustpunk/clinker/blob/main/docs/ai/15_PRODUCTION_CONTRACTS.md#composition-resources-and-call-site-surface)
for status, evidence, compatibility impact, and the AUTH-01 boundary.

## Complete example

```yaml
pipeline:
  name: order_pipeline

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: "./data/orders.csv"
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }

  - type: composition
    name: risk
    input: orders
    use: "./compositions/risk_score.comp.yaml"
    inputs:
      inp: orders
    config:
      threshold: 0.5

  - type: sink
    name: result
    input: risk
    config:
      name: result
      type: csv
      path: "./output/scored_orders.csv"
```
