# Column Lineage

The `--lineage` flag builds the pipeline's **column-level lineage** -- which source columns each output column is derived from, and which source columns influence the output as a whole -- and writes it as [OpenLineage](https://openlineage.io) events. Like `--explain`, it compiles the plan and exits **without reading any data**, so the lineage is derived statically from the pipeline definition.

```bash
# Write to a file
clinker run pipeline.yaml --lineage lineage.ndjson

# Write to stdout (pipe into other tooling)
clinker run pipeline.yaml --lineage -
```

## Output format

The output is [NDJSON](https://github.com/ndjson/ndjson-spec) (one JSON object per line) conforming to the OpenLineage `2-0-2` core spec. A run is described by a **`START`** event followed by a **`COMPLETE`** event that share one `runId`:

```json
{"eventType":"START","run":{"runId":"019f030d-0b3e-7ee1-86ec-1bb5b4a2776b"},"job":{"namespace":"clinker","name":"audit_join","facets":{"clinker_pipeline":{"sourceHash":"7fd096a9..."}}}, ...}
{"eventType":"COMPLETE","run":{"runId":"019f030d-0b3e-7ee1-86ec-1bb5b4a2776b"}, "inputs":[...], "outputs":[{"namespace":"file","name":".../audit_report.csv","facets":{"columnLineage":{ ... }}}]}
```

- **`runId`** is a UUID v7 minted for this export and shared by both events. Because `--lineage` is a *static, plan-derived* export, the `START`/`COMPLETE` pair describes the pipeline's lineage, not an executed data run — no rows are processed and the two events share one timestamp. A separate `clinker run` mints its own `runId`. (Live run-lifecycle emission with real timing is a planned follow-up.)
- **`job.namespace`** is `clinker`; **`job.name`** is the pipeline name. The pipeline's content hash rides in the `clinker_pipeline` job facet (`sourceHash`), not the job name -- so the name stays stable across edits while runs of the same definition remain correlatable.
- **`inputs`** are the source datasets; **`outputs`** are the sink datasets. Filesystem datasets use the `file` namespace with the resolved path as the name; a network source falls back to the `clinker` namespace plus the node name.
- The `columnLineage` facet is attached to each **output** dataset on the `COMPLETE` event.

## Reading the `columnLineage` facet

The facet has two parts, mirroring the OpenLineage `ColumnLineageDatasetFacet`:

```json
"columnLineage": {
  "fields": {
    "amount": { "inputFields": [
      { "namespace":"file", "name":".../audit_orders.csv", "field":"amount",
        "transformations":[{"type":"DIRECT","subtype":"IDENTITY"}] }
    ]}
  },
  "dataset": [
    { "namespace":"file", "name":".../audit_orders.csv", "field":"order_id",
      "transformations":[{"type":"INDIRECT","subtype":"JOIN"}] }
  ]
}
```

- **`fields`** -- **DIRECT** (value-derivation) lineage, keyed per output column: the source columns each output column's *value* is computed from. A rename (`emit full = name`) or a multi-hop chain collapses to the originating source column.
- **`dataset`** -- **INDIRECT** (influence) lineage for the dataset as a whole: source columns that shaped *which rows* exist, via filtering, joining, grouping, or sorting -- collected once rather than duplicated across every column.

Each transformation carries a `type` (`DIRECT` / `INDIRECT`) and a `subtype` (`IDENTITY`, `TRANSFORMATION`, `AGGREGATION`, `JOIN`, `GROUP_BY`, `FILTER`, `SORT`, `CONDITIONAL`).

## When to use

- **Impact analysis** -- before changing a source schema, see which outputs and columns depend on it.
- **Auditing & governance** -- feed the OpenLineage events into a catalog (e.g. Marquez) to track data provenance.
- **Review** -- attach the lineage of a new pipeline to a PR to confirm the intended derivations.

Because `--lineage` reads no data, it runs instantly and works on a pipeline whose inputs do not yet exist.

## Limitations

Lineage is derived from the compiled plan, so a few constructs are approximated:

- A **Composition** node is treated opaquely: every output column is taken to derive from every composition input column.
- **Envelope** / `$doc` provenance is best-effort same-name passthrough.
- INDIRECT influence covers route/cull predicates, join keys, aggregate grouping, and correlation sort. An aggregate's pre-aggregation row `filter`, a transform-inline `filter`, and Reshape `order_by` / `partition_by` are not (yet) attributed as influence.
- Constant and `count(*)` columns (which have no source input) are omitted from `fields`; engine-stamped columns (`$ck.*`, `$meta.*`, `$source.*`) are skipped, mirroring the default writer.
