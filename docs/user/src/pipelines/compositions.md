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
| `_compose.inputs` | Yes | Named input ports and the columns the body may use; see [Input ports](#input-ports) |
| `_compose.outputs` | Yes | Output port aliases pointing to body nodes or route ports |
| `_compose.config_schema` | No | Typed configuration parameters, defaults, and constraints |
| `_compose.scoped_vars` | No | Explicit scoped-variable names the sealed body may read from its caller |
| `_compose.resources_schema` | No | Typed resource slots; see [Resource bindings](#resource-bindings) |
| `nodes` | Yes | Unified node list for the sealed composition body |

### Input ports

A call behaves as if the composition's nodes were written in the pipeline at
the call site. Each input port receives the caller's rows as they are: every
column the caller has, in the caller's order. The port's `schema:` lists the
columns the composition needs, and those are the only columns of the caller's
rows its nodes may use. Every other column passes through the composition
untouched and comes out in the same place, the way a Transform passes through
the columns it does not `emit`.

For the `risk_score` composition above, a call fed rows of `order_id`,
`region`, `amount` gets back `order_id`, `region`, `amount`, `high_value`:
`region` passes through even though the port does not declare it.

A Sink with `include_unmapped: false` reading the call writes only the columns
the node behind the composition's first output port emits, as it would after
the same nodes written inline. Some shapes do not yet match the inline nodes:
a call carries only the rows of its composition's first output port, so a
composition whose output reads a Route, one with several output ports, and a
Cull's `removed_to` rows do not reach the pipeline as they would inline
([#1315](https://github.com/rustpunk/clinker/issues/1315)); and an Aggregate
inside a composition fed by a source that declares a `correlation_key` stops
with `E314` where the inline Aggregate runs.

A port's types are what the composition's nodes are checked against; what a
call outputs follows what the caller passed. Each node is checked with every
column the port declares at its declared type, at every call, so a composition
that passes the check compiles wherever its ports accept the caller's rows.
The rows a call then carries out are the composition's nodes run on the
caller's values: a column the composition does not write leaves the call with
the type it arrived with, and a column it writes has the type its nodes give
it from the caller's values, as the same nodes written in the pipeline would.
A column declared `any` (or `{ nullable: any }`) is checked with the type the
call provides, so a node that uses it in a way only some types support
compiles at some calls and not at others; the type error names the
composition and the port, because it depends on the call. Its help prints the
node's line with the column converted to the type the node needs, such as
`emit u = v.to_string().upper()`, which compiles whatever type a call provides
as long as the values convert. Columns the port
does not declare keep the caller's types on the way through.

A column declared `{ nullable: <type> }` says the composition copes with an
empty value in that column. Its nodes are checked as if the value could be
empty, so a node that cannot take an empty value, such as `filter qty > 6`
or a Combine `where:` comparing `a.qty > 6`, fails with `E200` at every call,
and the fix is to handle the empty case in the composition:
`filter (qty ?? 0) > 6`, or `where: '(a.qty ?? 0) > 6'`. A call whose column is never empty
keeps that guarantee through the call, so a later composition that needs a
never-empty `qty` still accepts it. A call whose column can be empty gets
results that can be empty, and a later composition that needs a never-empty
value says so with `E102`.

The same check applies to a composition called inside another composition.
If the outer composition declares `qty` as `{ nullable: int }` and passes it
unchanged to an inner composition whose port declares `qty` as `int`, the
outer composition fails with `E102` at every call, even when every caller's
`qty` is never empty: the outer composition is checked as if `qty` could be
empty, and the inner one does not accept an empty `qty`. The error names the
outer composition's port as the cause. Declare `qty` as never empty on that
port, `- { name: qty, type: int }`, when the outer composition should only be
called with a value; to keep accepting empty values, handle them before the
inner call, or declare the inner port `{ nullable: int }` if the inner
composition copes with an empty `qty`.

Each call checks the caller's rows against the port: every declared column
must be there, with the declared type, with the non-empty form of a
`{ nullable: <type> }` declaration, or with any type for `any`. Otherwise the
call fails with `E102`. Nothing is converted at the port, so a caller's `int`
column does not fit a `float` port column, and a caller column with no declared
type fits only `any`; the help prints the `schema:` line or the `emit` that
fixes the call.

A node inside the composition that uses a column its port does not declare
fails compilation with `E169` at every call whose rows carry that column, and
the help prints the line to add under `_compose.inputs.<port>.schema`. A call
whose rows do not carry the column fails with the ordinary unknown-name error,
such as `E203`. A Reshape `set` or `overrides` entry counts as using the column
it rewrites. A column the composition creates itself, with an `emit` or as an
aggregate result, is its own, and later nodes may use it. When the composition
writes a column with the same name as a caller column its port does not
declare, the composition's value replaces the caller's and the compiler warns
with `W101`. Nodes may be listed in any order; each is bound after the nodes it
reads. A node that reads itself or sits on a cycle fails with `E107`, and one
whose `input:` names neither a node nor an input port of the composition fails
with `E004`. A node that reads an input port declared `required: false` needs
every call to bind that port: a call that leaves it unbound fails with `E104`,
and the help prints the `inputs:` entry to add. Only a port no node reads may
be left unbound. Run `clinker explain --code E169` for details.

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

A body `sink` cannot be combined with a Source that declares
`dlq_granularity: document`: document-level dead-lettering needs every Sink at
pipeline level, and the pipeline fails compilation with `E378`. Run
`clinker explain --code E378` for how to move the Sink out of the body.

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
contain every required port declared by `_compose.inputs`, and every port
declared `required: false` that a node of the composition reads (`E104`).

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
