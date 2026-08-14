# Explain Plans

The `--explain` flag prints the execution plan -- the DAG of nodes, their connections, and the parallelism strategy the optimizer has chosen -- without reading any data.

## Text format

```bash
clinker run pipeline.yaml --explain
# or explicitly:
clinker run pipeline.yaml --explain text
```

The text format shows a human-readable summary of the execution plan:

```
Execution Plan: customer_etl
============================

Node 0: customers (Source, parallel: file-chunked)
  -> transform_1

Node 1: transform_1 (Transform, parallel: record)
  -> route_1

Node 2: route_1 (Route, parallel: record)
  -> [high] output_high
  -> [default] output_standard

Node 3: output_high (Output, parallel: serial)

Node 4: output_standard (Output, parallel: serial)
```

Key information shown:

- **Node index and name** -- the topological position in the DAG
- **Node type** -- Source, Transform, Aggregate, Route, Merge, Output, Composition
- **Parallelism strategy** -- how the optimizer plans to execute the node
- **Connections** -- downstream nodes, with port labels for route branches
- **Buffer class** (Physical Properties section) -- `buffer: streaming` for a node that hands its output straight to a single downstream consumer, or `buffer: materialized` for one that holds a whole stage's output in an inter-stage buffer. See [Streaming vs. Blocking Stages](streaming-vs-blocking.md) for the distinction.

The buffer class is a pre-runtime signal for memory pressure: a `materialized` node holds its rows against `pipeline.memory.limit` and may spill to disk once the budget is tight, while a `streaming` node holds only a small in-flight slice. Use the annotation alongside `--memory-limit` / `pipeline.memory.limit` to predict which stages will dominate memory before running the pipeline.

## JSON format

```bash
clinker run pipeline.yaml --explain json
```

Standard output carries only the JSON document: plan warnings and other human
diagnostics are written to stderr in this format and in `dot`, so redirecting
stdout into a parser is safe. Read stderr as well if you want to see them --
under `--explain text` they remain on stdout alongside the plan.

Produces a machine-readable JSON object for programmatic consumption. Useful for:

- CI pipelines that need to assert plan properties
- Custom dashboards that visualize execution plans
- Diffing plans between config versions

```bash
# Compare plans before and after a config change
clinker run old.yaml --explain json > plan_old.json
clinker run new.yaml --explain json > plan_new.json
diff plan_old.json plan_new.json
```

## Graphviz DOT format

```bash
clinker run pipeline.yaml --explain dot
```

Produces a [Graphviz](https://graphviz.org/) DOT graph. Pipe it to `dot` to render an image:

```bash
# PNG
clinker run pipeline.yaml --explain dot | dot -Tpng -o pipeline.png

# SVG (scalable, good for documentation)
clinker run pipeline.yaml --explain dot | dot -Tsvg -o pipeline.svg

# PDF
clinker run pipeline.yaml --explain dot | dot -Tpdf -o pipeline.pdf
```

This requires the `graphviz` package to be installed on the system.

The resulting diagram shows:

- Nodes as labeled boxes with type and parallelism annotations
- Edges as arrows with port labels where applicable
- Branch/merge fan-out and fan-in structure

## When to use explain

- **During development** -- verify the DAG shape matches your mental model before writing test data.
- **After adding route or merge nodes** -- confirm branch wiring is correct.
- **When tuning parallelism** -- check which strategy the optimizer selected for each node.
- **In code review** -- generate a DOT diagram and include it in the PR for visual confirmation.

Explain parses the YAML and builds the plan without opening runtime readers or
processing records. Planning may inspect source metadata or matchers for cost
estimates, but it does not create pipeline outputs.

```bash
clinker run pipeline.yaml --explain       # parse, compile, print the plan
clinker run pipeline.yaml --dry-run       # parse and compile without printing the plan
```

Both commands perform the same compile-time checks: schema binding, CXL type
checking, DAG wiring, and plan-time source and output gates. `--explain` also
renders the compiled plan; bare `--dry-run` is the quieter validation form.
Neither command opens runtime readers, processes records, or creates pipeline
outputs.

## Retraction section

If at least one Aggregate has a `group_by` that omits a correlation-key field, the output includes a `=== Retraction ===` block. It lists which aggregates and windows use group-atomic retraction (see [Correlation Keys](../pipelines/correlation-keys.md)) and a rough per-row memory estimate for each, so you can gauge the memory cost before a production run. The block is absent on pipelines that don't use this mode.

Exact group sizes are unknown until the pipeline runs, so treat the estimates as a planning aid and confirm the live shape with `clinker metrics collect` after the first run.

## Statistics

When the plan carries column statistics, the output ends with a `=== Statistics ===` section. Each figure is tagged with where it came from:

- **Row counts** — an estimate per source. A `[file metadata]` figure is estimated from the input file's size before any record is read; a `[exec sketch]` figure is an exact count measured during an actual run. These row counts are what the optimizer uses to pick a Combine's [join strategy](../nodes/combine.md#strategy-hint).
- **Column sketches** — distinct-value counts and frequent-value hints that a Combine gathers over its join keys while records flow, used to speed up matching.

A statistic that was never gathered renders as `null` rather than a fabricated zero — for example, a multi-file `glob` source or a network source whose size cannot be read adds no Statistics section at all.

## Field provenance

`clinker explain <pipeline> --field <path>` traces where a single resolved value
comes from across every configuration layer, printing the winning layer plus
each shadowed layer and its source span. The path arity selects what is traced:

- **`<node>.<param>`** (two parts) — a composition config parameter, resolved
  across composition defaults and channel/group overlays.
- **`<source>.<column>.<attribute>`** (three parts) — a **source-schema
  attribute** (`type`, `scale`, `precision`, `format`, `width`, `required`, …),
  resolved across the schema-provenance layers `Base < Pipeline < Group <
  Channel`. `Base` is the source's own declared `schema:`; the higher layers are
  the [`patch_schema`](../pipelines/channels.md#patch_schema--shape-a-sources-columns)
  overlay ops each channel/group applies.

```bash
# Where does the `scale` on the orders source's `amount` column come from?
clinker explain pipeline.yaml --field orders.amount.scale

# Resolve the same attribute with a channel overlay applied first.
clinker explain pipeline.yaml --field orders.amount.scale --channel acme_prod
```

```
Field: orders.amount.scale

  Resolved value: 2

  Provenance chain (outermost to innermost):
  [WON] Channel               →  2  (line 12)
        Pipeline              →  0  (shadowed)  (line 5)
        Base                  →  0  (shadowed)
```

The `[WON]` marker names the layer whose value survives; shadowed layers show
what they proposed. An unknown source, column, or attribute is rejected with a
hint listing the valid names at that level.

## Reading a plan-time failure

A pipeline that fails a plan-time check never reads any input. The failure is
printed before the run starts, and it carries four things:

```
E363

  × source 'src': `record_path` "$.rows" starts with the JSONPath root marker
  │ `$.`, which is not part of the grammar; `record_path` is a dot-separated
  │ path of object keys, descended from the document root (for example
  │ `data.rows`). Write "rows" instead
   ╭─[pipeline.yaml:4:1]
 3 │ nodes:
 4 │   - type: source
   · ────────┬───────
   ·         ╰── declared here
 5 │     name: src
   ╰────
  help: `record_path` on a `json` source is a dot-separated path of object
        keys descended from the document root: no `$.` JSONPath root marker,
        no leading `/`, and no empty segments. It takes precedence over
        `format:`, so pair it with `format: object` or leave `format:` off.
        Omit `record_path` entirely and the reader auto-detects the document
        shape. Run `clinker explain --code E363` for the full grammar.
```

- **The code** (`E363`) heads the report. Where a page exists for it, hand it
  to `clinker explain --code` for the worked example.
- **The message** names the offending input and the rule it broke.
- **The source line** is quoted from your YAML, with the offending node
  underlined.
- **The `help:` paragraph** names the fix. When the gate does not already say
  so, a `See: clinker explain --code <CODE>` line is appended.

Warnings are reported the same way but marked `⚠` rather than `×`, so an
advisory is distinguishable from the diagnostic that stopped the run.

The same report is printed under `--explain`, which compiles the plan before
printing it.

Two notes on where the snippet comes from:

- A pipeline that pulls in a composition body is reported without the quoted
  source line. A plan-time diagnostic carries a line number but not which file
  it belongs to, so rather than risk underlining an unrelated line, the report
  gives the code, message and help alone.
- A channel/group overlay suppresses the snippet only when it rewrites the
  compiled config through structural ops, source patches, or composition
  `config:` values. A selection that contributes only runtime vars leaves the
  pipeline document unchanged, so its snippet remains safe and is retained.
- Bare `--dry-run` compiles the plan and prints the same report without reading
  source data.

## Looking up diagnostic codes

`clinker explain --list` enumerates every registered diagnostic in stable code
order. Each entry includes its code, severity, status, category, retryability,
meaning, and correction. Narrow the list with exact filters:

```bash
clinker explain --list
clinker explain --list --status retired-reserved
clinker explain --list --category source-and-expression
```

The status vocabulary is closed:

| Status | Meaning |
|--------|---------|
| `active` | The code describes a condition in the current authoring surface. |
| `retired-reserved` | The old condition is no longer accepted, but its identifier remains permanently reserved and still explains the paste-ready correction. |

Categories are `configuration`, `composition`, `source-and-expression`,
`execution-and-format`, `terminal-authoring`, `security`, and `advisory`.
Unknown or empty filter values fail; a valid combination that matches no code
also fails instead of printing an ambiguous empty result.

`clinker explain --code <CODE>` prints the same registry-owned descriptor as
the list view, followed by a longer detail page when one exists:

```bash
clinker explain --code E15Y   # retraction-mode aggregate incompatible with strategy: streaming
clinker explain --code E376   # retired type: output spelling; use type: sink
```

Not every registered code has a longer page. A registered code without one is
still valid and prints its complete descriptor plus `Detail page: none`; only a
code absent from the registry is unknown. The `See: clinker explain --code
<CODE>` line is appended to a diagnostic only when the longer page exists.

List and code discovery are static authoring metadata. They do not compile a
pipeline, inspect records, or render runtime values or secrets. Use only the
`clinker explain` spelling: there is no separate diagnostic command.
