# Column Lineage

The `--lineage` flag builds the pipeline's **column-level lineage** -- which source columns each output column is derived from, and which source columns influence the output as a whole -- and writes it as [OpenLineage](https://openlineage.io) events. Like `--explain`, it compiles the plan and exits **without reading any data**, so the lineage is derived statically from the pipeline definition.

```bash
# Write to a file
clinker run pipeline.yaml --lineage lineage.ndjson

# Write to stdout (pipe into other tooling)
clinker run pipeline.yaml --lineage -
```

There are two emission modes:

- **`--lineage`** -- a *static, plan-derived* export. It compiles the plan and exits without reading data, so it runs instantly and describes the pipeline's lineage rather than a specific execution.
- **`--lineage-events`** -- *live run-lifecycle* emission. It runs the pipeline and emits a `START` when the run begins and a terminal `COMPLETE` / `FAIL` / `ABORT` when it ends, carrying real timing and row counts. See [Live run events](#live-run-events) below.

Both modes share the same column-lineage facet and the same on-the-wire OpenLineage shape; the live mode wraps it in real run-lifecycle events. Pushing those events to a live OpenLineage HTTP endpoint (a catalog such as Marquez) is a separate, planned transport and is not yet available.

## Output format

The output is [NDJSON](https://github.com/ndjson/ndjson-spec) (one JSON object per line) conforming to the OpenLineage `2-0-2` core spec. A run is described by a **`START`** event followed by a **`COMPLETE`** event that share one `runId`:

```json
{"eventType":"START","run":{"runId":"019f030d-0b3e-7ee1-86ec-1bb5b4a2776b"},"job":{"namespace":"clinker","name":"audit_join","facets":{"clinker_pipeline":{"sourceHash":"7fd096a9..."}}}, ...}
{"eventType":"COMPLETE","run":{"runId":"019f030d-0b3e-7ee1-86ec-1bb5b4a2776b"}, "inputs":[...], "outputs":[{"namespace":"file","name":".../audit_report.csv","facets":{"columnLineage":{ ... }}}]}
```

- **`runId`** is a UUID v7 minted for this export and shared by both events. Because `--lineage` is a *static, plan-derived* export, the `START`/`COMPLETE` pair describes the pipeline's lineage, not an executed data run — no rows are processed and the two events share one timestamp. A separate `clinker run` mints its own `runId`. (For real timing and row counts tied to an actual execution, use [`--lineage-events`](#live-run-events).)
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

- **`fields`** -- **DIRECT** (value-derivation) lineage, keyed per output column: the source columns each output column's *value* is computed from. A rename (`emit full = name`), a multi-hop chain, or a path through a **composition** body (including nested compositions) collapses to the originating source column. A column whose value derives from an **envelope** read (`$doc.<section>.<field>`, bare / indexed / inside a larger expression) gets a DIRECT input field on the originating source dataset whose `field` is the rendered `$doc.…` path -- so envelope-derived columns trace back to the document section they came from.
- **`dataset`** -- **INDIRECT** (influence) lineage for the dataset as a whole: source columns that shaped *which rows* exist, via filtering, joining, grouping, or sorting -- collected once rather than duplicated across every column.

Each transformation carries a `type` (`DIRECT` / `INDIRECT`) and a `subtype` (`IDENTITY`, `TRANSFORMATION`, `AGGREGATION`, `JOIN`, `GROUP_BY`, `FILTER`, `SORT`, `CONDITIONAL`).

## Live run events

`--lineage-events <PATH>` **runs the pipeline** and emits OpenLineage run events tied to that actual execution, as NDJSON to a file path (or `-` for stdout):

```bash
clinker run pipeline.yaml --lineage-events events.ndjson
```

Unlike `--lineage` (which exits before reading data), this processes data, so it cannot be combined with `--lineage`, `--explain`, `--dry-run`, or `-n`.

A run emits a `START` when it begins, then exactly one terminal event when it ends:

- **`START`** -- written and flushed **before** the run body executes, so a crash mid-run still leaves an observable open run. It carries the input and output datasets by identity (no facets yet).
- **`COMPLETE`** -- the run finished. It carries the input datasets and the output datasets with their `columnLineage` facets, exactly like the static export.
- **`FAIL`** -- the run errored. It carries the standard OpenLineage `errorMessage` run facet with the failure message.
- **`ABORT`** -- the run was interrupted (e.g. a `SIGINT`/`SIGTERM` shutdown) and drained what it could before unwinding.

```json
{"eventType":"START","eventTime":"2026-07-03T17:00:00Z","run":{"runId":"019f..."}, "inputs":[...], "outputs":[...]}
{"eventType":"COMPLETE","eventTime":"2026-07-03T17:00:04Z","run":{"runId":"019f...","facets":{"clinker_runStats":{"recordsRead":1000,"recordsWritten":970,"recordsDlq":30,"durationMs":4210}}}, "outputs":[{"...":"...","facets":{"columnLineage":{ ... }}}]}
```

Key differences from the static export:

- **`runId`** is the run's **`execution_id`** (a UUID v7) — the same identity used across clinker's provenance sidecars and metrics spool, so an orchestrator can correlate the lineage events with the run's other artifacts.
- The `START` and terminal events carry **distinct** `eventTime`s (run begin and run end), not one shared timestamp.
- The terminal event carries a **`clinker_runStats`** run facet — a clinker-defined facet with `recordsRead`, `recordsWritten`, `recordsDlq`, and `durationMs`. Counts are pipeline-wide run totals, not per-output.
- On `FAIL`, the run also carries the standard **`errorMessage`** run facet (`ErrorMessageRunFacet` `1-0-0`) with the failure message.

A terminal event is always emitted for a started run: if the process fails to reach a clean terminal (for example, an output-commit error after the executor finished), the run is still closed out as a `FAIL` so no `START` is left dangling. Emission is best-effort *after* the run's data outputs are committed — a lineage-sink write error is logged as a warning and does not fail a run whose outputs already landed.

## When to use

- **Impact analysis** -- before changing a source schema, see which outputs and columns depend on it.
- **Auditing & governance** -- feed the OpenLineage events into a catalog (e.g. Marquez) to track data provenance.
- **Review** -- attach the lineage of a new pipeline to a PR to confirm the intended derivations.

Because `--lineage` reads no data, it runs instantly and works on a pipeline whose inputs do not yet exist.

## Limitations

Lineage is derived from the compiled plan, so a few constructs are approximated:

- A column-grain `$doc` read is traced as DIRECT lineage (see [`fields`](#reading-the-columnlineage-facet) above) in a transform projection, a combine body, a composition body, and an **aggregate** emit, attributed only to a source whose envelope declares the section. A `$doc` read in an **influence predicate** -- a route condition, a cull `drop_group_when`, or a combine `where` -- is surfaced as INDIRECT influence (`FILTER` for route and cull, `JOIN` for combine). Two `$doc` cases remain uncovered: a whole-section **envelope echo** (an output header/footer regenerated from a source document section, with no output column or expression); and any `$doc` reference in a **Reshape** rule, which the compiler rejects outright (Reshape re-runs its rules after a per-group spill that drops envelope context), so there is no Reshape envelope lineage to produce.
- A **match: collect** combine declared without a projection body produces coarse column lineage: each collected column derives (as `TRANSFORMATION`) from *every* build-side column, because there is no body expression to pin the exact source column.
- A **multi-record-type** source (one physical file carrying several record shapes over a single superset schema) is modeled as **one** dataset, matching its single physical identity -- lineage is not split per record type.
- INDIRECT influence covers route/cull predicates, join keys, aggregate grouping, and correlation sort over **record columns** (and `$doc` envelope terms in route/cull/combine predicates, as above). An aggregate's pre-aggregation row `filter`, a transform-inline `filter`, and Reshape `order_by` / `partition_by` are not (yet) attributed as influence.
- Constant and `count(*)` columns (which have no source input) are omitted from `fields`; engine-stamped columns (`$ck.*`, `$meta.*`, `$source.*`) are skipped, mirroring the default writer.
