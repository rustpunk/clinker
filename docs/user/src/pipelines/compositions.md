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
| `_compose.resources_schema` | No | Reserved file-resource declarations; see [Resource status](#resource-status) |
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

A body `source` or `output` that sets a CSV `delimiter` or `quote_char`
which is not exactly one ASCII byte is likewise rejected at compile time,
not first at run, with the same one-byte rule top-level nodes get.

A body `source` or `output` whose `schema:` names an external
`.schema.yaml` file has that path resolved relative to the composition
file's own directory (not the invoking pipeline's), and the file's columns
are inlined before the body binds. A body `output` therefore rounds
`decimal` columns to their declared [`scale`](../nodes/output.md#rounding-decimals-to-a-declared-scale)
at the write boundary exactly as a top-level output does.

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

### Resource status

Composition resources are reserved surface, not a working runtime facility.
The declaration model currently recognizes only `kind: file`, and the parser
accepts call-site `resources:`. Planner resource validation is currently a
stub, and bindings are not resolved or consumed during execution. Connection
strings and other resource kinds are unsupported. Do not put operational
dependencies in `resources:` until runtime resource binding is implemented.

The call-site parser also accepts `outputs:` and `alias:` fields, but they do
not currently remap declared output ports or namespace expanded body nodes.
Use `_compose.outputs` for the public output contract and the composition
node's `name` for its caller-visible namespace.

### Call-site fields

| Field | Required | Description |
|-------|----------|-------------|
| `input` | Yes | Primary upstream required by the current node shape |
| `use` | Yes | Path to the `.comp.yaml` definition |
| `inputs` | Yes for declared ports | Map of composition input ports to upstream node references |
| `config` | No | Parameter overrides (key-value pairs) |
| `resources` | Reserved | Parsed but not validated or consumed at runtime |
| `outputs` | Reserved | Parsed but does not override `_compose.outputs` |
| `alias` | Reserved | Parsed but does not namespace body nodes |

### Locked replacement contract

D-12 through D-16 lock the replacement for these inert surfaces, but it has
not been implemented yet:

- Resource kinds come from a bounded typed registry. Each registered kind owns
  its descriptor schema, validation, runtime opener, capabilities, redaction,
  provenance, tests, and documentation; unknown kinds fail closed (D-12).
- Concrete resources live in a named catalog. `_compose.resources_schema`
  declares typed slots, and call-site `resources:` binds those slots to catalog
  names instead of embedding definitions or secrets (D-13).
- Channel and group overlays may rebind a slot to another catalog name under
  existing precedence and fixed-lock rules. Unknown slots and catalog names
  fail before execution, and provenance records attempted and winning bindings
  (D-14).
- Planning validates descriptors, capabilities, bindings, and non-secret
  compile inputs. Run preflight resolves secret references and opens run-local
  handles before data side effects; plans never store secret values or live
  handles (D-15).
- Ordinary call-site `outputs:` and `alias:` are rejected. `_compose.outputs`
  remains the public output contract, the composition node `name` remains its
  caller-visible namespace, and the distinct channel/group insertion alias is
  unaffected (D-16).

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

  - type: output
    name: result
    input: risk
    config:
      name: result
      type: csv
      path: "./output/scored_orders.csv"
```
