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

Explain runs instantly because it only parses the YAML and builds the plan -- no data is touched. Pair it with `--dry-run` for full config validation:

```bash
clinker run pipeline.yaml --explain       # inspect plan
clinker run pipeline.yaml --dry-run       # validate config
```

## Retraction section

If at least one Aggregate has a `group_by` that omits a correlation-key field, the output includes a `=== Retraction ===` block. It lists which aggregates and windows use group-atomic retraction (see [Correlation Keys](../pipelines/correlation-keys.md)) and a rough per-row memory estimate for each, so you can gauge the memory cost before a production run. The block is absent on pipelines that don't use this mode.

Exact group sizes are unknown until the pipeline runs, so treat the estimates as a planning aid and confirm the live shape with `clinker metrics collect` after the first run.

## Statistics

When the plan carries column statistics, the output ends with a `=== Statistics ===` section. Each figure is tagged with where it came from:

- **Row counts** — an estimate per source. A `[file metadata]` figure is estimated from the input file's size before any record is read; a `[exec sketch]` figure is an exact count measured during an actual run. These row counts are what the optimizer uses to pick a Combine's [join strategy](../nodes/combine.md#strategy-hint).
- **Column sketches** — distinct-value counts and frequent-value hints that a Combine gathers over its join keys while records flow, used to speed up matching.

A statistic that was never gathered renders as `null` rather than a fabricated zero — for example, a multi-file `glob` source or a network source whose size cannot be read adds no Statistics section at all.

## Looking up diagnostic codes

`clinker explain --code <CODE>` prints the documentation for any registered error or warning code, including retraction-specific codes:

```bash
clinker explain --code E15Y   # retraction-mode aggregate incompatible with strategy: streaming
```

The full set of codes is enumerated in the error returned when an unknown code is passed.
