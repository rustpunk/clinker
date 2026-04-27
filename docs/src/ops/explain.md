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

Pipelines whose at least one Aggregate has a `group_by` that omits a correlation-key field get a `=== Retraction ===` block in the text output. The engine selects the retraction-mode path automatically based on `group_by` content; the block is silent on every other pipeline, so strict-correlation and non-correlated `--explain` output stays identical to today's text.

The block opens with a one-line summary -- `retraction enabled — N relaxed aggregates, M buffer-mode windows, fanout policy: <policy>.` -- followed by one block per retraction-mode Aggregate and one per buffer-mode window index.

Per retraction-mode Aggregate the block reports:

- the resolved accumulator path (`Reversible` or `BufferRequired`),
- the per-row lineage memory cost (`~8 bytes/row` for Reversible, `n/a` for BufferRequired which holds raw contributions instead),
- the worst-case degrade fallback when retraction's preconditions break at runtime.

Per buffer-mode window index the block reports:

- the source name and `partition_by` fields,
- the per-row buffer cost in `Value` slots over the index's arena fields,
- the worst-case partition memory ceiling under degrade.

Group cardinality is honestly surfaced as "unknown at plan time" -- the planner has no group-cardinality side-table to consult before the run. Use the [operator-by-operator retraction cost reference](../pipeline/correlation-keys.md#operator-by-operator-retraction-cost-reference) and the per-row figures the explain block prints for capacity planning, then confirm the live shape via `clinker metrics collect` after the first production run.

## Looking up diagnostic codes

`clinker explain --code <CODE>` prints the documentation for any registered error or warning code, including retraction-specific codes:

```bash
clinker explain --code E15W   # non-deterministic CXL builtin downstream of a retraction-mode aggregate
clinker explain --code E15Y   # retraction-mode aggregate incompatible with strategy: streaming
```

The full set of codes is enumerated in the error returned when an unknown code is passed.
