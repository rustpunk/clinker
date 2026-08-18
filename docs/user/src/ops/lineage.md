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

Both modes share the same column-lineage facet and the same on-the-wire
OpenLineage shape; the live mode wraps it in real run-lifecycle events. In
external identity mode, complete events can also cross the independently
bounded delivery worker described below. That worker owns only the selected
file or stdout sink; it does not share the OTLP Collector worker or its memory
arena.

## Dataset identity preflight

Both flags require an explicit `[observability.lineage]` identity policy in the
workspace `clinker.toml`. The default `identity_mode = "external"` requires one
exact binding for every emitted Source and Sink node. A binding uses either a
canonical datasource or a complete catalog namespace/name pair:

```toml
[observability.lineage]
identity_mode = "external"

[[observability.lineage.dataset]]
node = "source_customers"
canonical_datasource = "s3://warehouse/customers"

[[observability.lineage.dataset]]
node = "output_customers"
catalog_namespace = "analytics"
catalog_name = "customers_clean"
```

A source or output declared **inside a composition body** needs its own
binding, keyed by the call site it belongs to:

```toml
[[observability.lineage.dataset]]
node = "enrich_orders.reference_prices"
canonical_datasource = "s3://warehouse/prices"
```

Body node names live in their own scope and may legally repeat a top-level
name, so the key is `<composition node>.<body source>` rather than the bare
name — two call sites of one body can be pointed at different files, and each
gets its own identity.

`.` joins a call site to a body node, and a key never has to disambiguate that
join from a node's own name: a `.` in a node name is refused at plan time with
`E010`, for every node kind and inside composition bodies too. `node =
"enrich.ref"` therefore always addresses the source `ref` inside composition
node `enrich`.

A `\` that belongs to a node's own name is written `\\` in the key, so the key
format stays unambiguous on its own rather than by relying on the naming rule.
The same escape covers `.` — `node = "enrich\\.ref"` (in TOML, `\\` is a
literal backslash; the literal string `'enrich\.ref'` says the same thing)
would address a node whose own name is `enrich.ref` — but no pipeline the
planner accepts can produce that key. Node names without `\` — nearly all of
them — are unaffected.

A node whose key cannot be written as a binding — over 128 bytes once the call
site is joined to it — is refused by name, naming the limit. The correction is
to rename the pipeline nodes the key is built from; there is no binding that
can carry an over-long key.

`#` is reserved in an authored dataset **name** (`catalog_name`, or the name
half of a canonical datasource) because it separates a multi-record source's
record types from their base dataset. Without the restriction a name like
`payments#detail` would collide with record type `detail` of a source bound to
`payments`, and the two would merge in the catalogue — attributing one
dataset's columns to the other. Namespaces are unaffected.

Clinker validates all required bindings before opening the lineage sink or,
for `--lineage-events`, discovering sources and creating output attempts.
Missing, duplicate, partial, ambiguous, or invalid bindings fail as
`observability.configuration.invalid`; rejected values and physical paths are
not copied into the diagnostic. The complete observability policy, including
the required OTLP and authentication tables, is documented under
[lineage identity](metrics.md#lineage-identity).

The destination file is emptied once the exporter has started, before any
event is written. Point each run at a path you are willing to overwrite, and
copy a record you want to keep before re-running against it. This applies to
both `--lineage` and `--lineage-events`.

What that means for a run that produces no events:

- Refused **before** the exporter starts — an invalid pipeline, a rejected
  configuration, a lineage binding that does not resolve — the file is
  untouched and still holds the previous run's events.
- Refused **after** the exporter starts, or fails before its first event, the
  file is empty. An empty file means this run wrote nothing; it never means
  the previous run's result still stands.
- A plan-only `--lineage` export that wrote nothing removes the destination,
  so no zero-byte artifact is left for a later step to publish. This applies
  only to a regular file: a destination that always reports zero length, such
  as `/dev/null` or a FIFO, is a successful export and is never removed. It is
  also skipped when the export ran out of flush time, because the exporter may
  still be writing.

If a consumer must distinguish "this run produced no lineage" from "an older
run's lineage is still here", give each run its own destination path rather
than relying on the state of a shared one.

Path-derived dataset names remain available only through the exact local
compatibility spelling below. This mode is visibly labeled on stderr and is
for local diagnostics, not external delivery:

```toml
[observability.lineage]
identity_mode = "local_diagnostic_paths"
```

## Output format

The output is [NDJSON](https://github.com/ndjson/ndjson-spec) (one JSON object per line) conforming to the OpenLineage `2-0-2` core spec. A run is described by a **`START`** event followed by a **`COMPLETE`** event that share one `runId`:

```json
{"eventType":"START","run":{"runId":"019f030d-0b3e-7ee1-86ec-1bb5b4a2776b","facets":{"clinker_batch":{"batchId":"batch-42"}}},"job":{"namespace":"clinker","name":"audit_join","facets":{"clinker_pipeline":{"sourceHash":"7fd096a9..."},"clinker_semanticPlan":{"algorithm":"blake3","semanticSchemaVersion":1,"digest":"4e8c..."}}}, ...}
{"eventType":"COMPLETE","run":{"runId":"019f030d-0b3e-7ee1-86ec-1bb5b4a2776b","facets":{"clinker_batch":{"batchId":"batch-42"}}},"job":{"namespace":"clinker","name":"audit_join","facets":{"clinker_pipeline":{"sourceHash":"7fd096a9..."},"clinker_semanticPlan":{"algorithm":"blake3","semanticSchemaVersion":1,"digest":"4e8c..."}}}, "inputs":[...], "outputs":[{"namespace":"analytics","name":"audit_report","facets":{"columnLineage":{ ... }}}]}
```

- **`runId`** is a UUID v7 minted for this export and shared by both events. Because `--lineage` is a *static, plan-derived* export, the `START`/`COMPLETE` pair describes the pipeline's lineage, not an executed data run — no rows are processed and the two events share one timestamp. A separate `clinker run` mints its own `runId`. (For real timing and row counts tied to an actual execution, use [`--lineage-events`](#live-run-events).)
- Correlation is copied from one immutable CLI lifecycle snapshot: `runId` is the generated execution ID, the clinker-defined **`clinker_batch`** run facet carries the caller/generated `batchId`, and the **`clinker_semanticPlan`** job facet carries the effective fingerprint algorithm, schema version, and digest. Static and live events do not independently mint or parse these identities.
- **`job.namespace`** is `clinker`; **`job.name`** is the pipeline name. The pipeline's content hash rides in the `clinker_pipeline` job facet (`sourceHash`), not the job name -- so the name stays stable across edits while runs of the same definition remain correlatable.
- **`inputs`** are the source datasets; **`outputs`** are the sink datasets. External mode uses the exact configured canonical or catalog identities, so relocating a pipeline does not change its lineage graph. Explicit `local_diagnostic_paths` compatibility mode instead uses the `file` namespace with resolved paths (and falls back to the `clinker` namespace plus the node name for a network source).
- The dataset namespace/name identifies the stable collection. A concrete
  logical partition or location would be emitted as the standard role-specific
  input/output subset facet — which rides under `inputFacets` on an input and
  `outputFacets` on an output, the positions its schema names, not under the
  dataset-level `facets` — and an explicitly authorized alias as the standard
  symlinks facet, which is a plain dataset facet and does ride under `facets`; neither is ever inferred from worker paths, attempt paths,
  hashes, or process context. **No pipeline emits either facet today** -- the
  workspace config exposes no subset or symlink fields, so nothing can
  authorize one.
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

## Multi-record sources

A **multi-record flat file** carries several record shapes in one physical file, discriminated by a lead `record_type` column. Record types differ in their *columns*, not in which rows they select, so each is treated as **its own logical dataset** rather than as a subset of one flat superset dataset:

- Each record type is a dataset named `<dataset>#<id>` -- the source's **bound dataset identity** with the record type's `id` as a `#` fragment. Under `identity_mode = "external"` that is the configured canonical or catalog identity (namespace `s3://payments-lake`, name `raw/payments#detail`), so no filesystem path enters the name; under `local_diagnostic_paths` it is the resolved file path (`.../payments.txt#detail`). Its columns are exactly that record type's declared columns, so an output column that derives from a detail-record field traces to `…#detail`, and one from a header field traces to `…#header`.
- A column declared by **several** record types (unified into one superset column) lists **each** owning `#<id>` dataset as an input field, so a derived output column traces to every record type it could have come from.
- The engine-stamped `record_type` **discriminator** lead column belongs to the container rather than to any one record type, so it stays on the **base** dataset (no fragment) -- a `Route` that branches on `record_type` still references `{<base>, record_type}`.
- The run's `inputs` list the base dataset followed by each `#<id>` record-type dataset, in record-type declaration order. Declaring them is load-bearing, not cosmetic: a lineage consumer resolves a `columnLineage` input field only against datasets the run declared as inputs, so a record-type dataset left out of `inputs` would have its column edges silently dropped on ingest.

A record type's `parent` / `join_key` -- the intra-file hierarchy linking a child record type to its parent -- is not emitted as a lineage edge, since no plan operation performs that join.

## Live run events

`--lineage-events <PATH>` **runs the pipeline** and emits OpenLineage run events tied to that actual execution, as NDJSON to a file path (or `-` for stdout):

```bash
clinker run pipeline.yaml --lineage-events events.ndjson
```

Unlike `--lineage` (which exits before reading data), this processes data, so it cannot be combined with `--lineage`, `--explain`, `--dry-run`, or `-n`.

> **Prefer a file path for a clean stream.** With `-` (stdout), the run's own stdout output — for example the per-stage spill-volume summary — interleaves with the event lines, so stdout is not pure NDJSON. Writing to a file keeps the events unmixed.

A run emits a `START` when it begins, then exactly one terminal event when it ends:

- **`START`** -- offered to the lineage path **before** the run body executes. In local diagnostic mode it is written synchronously; in external mode it is admitted non-blockingly to the bounded lineage queue and can be dropped under the configured policy. It carries the input and output datasets by identity plus the shared batch and semantic-plan correlation facets; no completed dataset facets exist yet.
- **`COMPLETE`** -- the run finished. It carries the input datasets and the output datasets with their `columnLineage` facets, exactly like the static export.
- **`FAIL`** -- the run errored. It carries the standard OpenLineage `errorMessage` run facet and the clinker-defined `clinker_failure` facet. Both are derived from the same bounded, sanitized classification used by machine supervision; the latter adds stable `code`, `category`, and `retryAdvice` fields.
- **`ABORT`** -- the run was interrupted (e.g. a `SIGINT`/`SIGTERM` shutdown) and drained what it could before unwinding.

```json
{"eventType":"START","eventTime":"2026-07-03T17:00:00Z","run":{"runId":"019f...","facets":{"clinker_batch":{"batchId":"batch-42"}}},"job":{"facets":{"clinker_semanticPlan":{"algorithm":"blake3","semanticSchemaVersion":1,"digest":"4e8c..."}}}, "inputs":[...], "outputs":[...]}
{"eventType":"COMPLETE","eventTime":"2026-07-03T17:00:04Z","run":{"runId":"019f...","facets":{"clinker_batch":{"batchId":"batch-42"},"clinker_runStats":{"recordsRead":1000,"recordsWritten":970,"recordsDlq":30,"durationMs":4210}}},"job":{"facets":{"clinker_semanticPlan":{"algorithm":"blake3","semanticSchemaVersion":1,"digest":"4e8c..."}}}, "outputs":[{"...":"...","facets":{"columnLineage":{ ... }}}]}
```

Key differences from the static export:

- **`runId`** is the run's **`execution_id`** (a UUID v7) — the same identity used across clinker's provenance sidecars and metrics spool, so an orchestrator can correlate the lineage events with the run's other artifacts.
- Both events carry the same **`clinker_batch`** run facet and **`clinker_semanticPlan`** job facet. Together with `runId`, their batch ID, execution ID, and semantic fingerprint tuple come from the run's single CLI-owned lifecycle source and exactly match the optional machine stream when both are enabled.
- The `START` and terminal events carry **distinct** `eventTime`s (run begin and run end), not one shared timestamp.
- The terminal event carries a **`clinker_runStats`** run facet — a clinker-defined facet with `recordsRead`, `recordsWritten`, `recordsDlq`, and `durationMs`. Counts are pipeline-wide run totals, not per-output.
- On `FAIL`, the run also carries the standard **`errorMessage`** run facet (`ErrorMessageRunFacet` `1-0-0`) plus **`clinker_failure`**, with the shared sanitized message and stable failure code/category/retry advice.

Every started run that reaches a handled executor or publication boundary records one terminal snapshot; output-commit errors close as `FAIL` from that same source. Terminal emission is best-effort after the run's authoritative publication decision. A lineage admission drop or sink write, flush, or deadline failure is reported on standard error and does not fail a run whose outputs already landed. A process crash can still leave only a delivered `START` event.

### External delivery boundary

External identity mode is also the boundary for the independently bounded
lineage worker. Each complete event is serialized under
`lineage.max_event_bytes` and offered without blocking to a queue capped by
`lineage.queue_bytes`; a full queue or oversized event drops the newest event.
One synchronous worker owns the selected file or stdout sink, and shutdown
waits no longer than `lineage.flush_timeout_ms`. Its typed outcome distinguishes
normal shutdown, write failure (including permission errors), flush failure,
and deadline expiry, with accepted, dropped, and full counters reported
separately.

Within that deadline the worker stops taking **new** events off the queue
halfway through, keeping the rest of the budget to finish the event it is
already writing. A destination too slow to keep up therefore receives a file
that is **short** — missing its last events — rather than one that ends inside
a half-written record, which an NDJSON reader cannot parse. Nothing is added to
the deadline: the whole flush still ends within `lineage.flush_timeout_ms`. A
destination that stops accepting bytes altogether cannot be waited on, so in
that one case the file may end mid-record, and the delivery outcome reports
that separately from its counters. It has no access to the telemetry arena or Collector worker.

#### What the run prints

A delivery that lost events, ended on anything other than a normal shutdown, or
left its destination inside a record prints one line on standard error:

```text
clinker: lineage delivery outcome: status=deadline-exceeded error_kind=none accepted=2 dropped=0 full=0 records_complete=false
```

`records_complete` is the completeness of the **file**, where the counters
beside it are the completeness of the **export**. It is `true` on every normal
shutdown and on the slow-destination path above — a short file is still valid
NDJSON — and `false` only when the worker was abandoned inside a write. That is
the one state a consumer cannot determine for itself: a truncated NDJSON file
simply ends, with nothing in it to say more was coming, and the counters cannot
tell you either, because a run that gave up on a slow destination reports the
same accepted total whether or not the last record made it out whole.

Being a condition rather than a count, it breaks the clean-run silence on its
own: a run that dropped no events still prints this line if it left the
destination unreadable. A normal shutdown that dropped nothing and ended on a
record boundary prints nothing at all.

The plan-only `--lineage` export, whose whole invocation *is* the export, says
the same thing in prose when it misses its flush deadline, and its correction
follows from it. A short export ends "on a record boundary, short by the events
that never got out" and is simply re-run. An export that "ends inside a record
and is not readable as NDJSON" must be discarded rather than published as this
run's lineage, and only then re-run.

A sink write or flush failure leaves the same two files, and says so the same
way. Where the deadline path ran out of time on an export that was otherwise
going fine, here the destination itself refused, so two separate facts are
reported: what the destination was left holding, and where the retry should
point. An export that "ends on a record boundary and is readable as NDJSON" is
reported without a disposal instruction; one that "ends inside a record and is
not readable as NDJSON" must be discarded first, and the correction says so
ahead of the retry advice. The retry advice itself is unchanged — a permanent
refusal (permission denied, read-only filesystem, a directory) asks for a
different destination, anything else asks for a re-run — because re-running
against a destination that has just refused a write may refuse it again.

Nothing is said about a destination that is not there: a failure that wrote no
bytes at all leaves an empty file, which this path removes so a publish step
cannot upload it as the run's lineage, and the diagnostic then describes no
file.

This external worker is not a second identity mode and does not make local
paths suitable as catalog identity. The explicit `local_diagnostic_paths`
mode remains a synchronous compatibility path for local file or console
inspection and cannot enter the external delivery worker.

Lineage uses logical dataset bindings, not working-directory or attempt paths.
It copies the batch ID, execution ID, semantic fingerprint, and terminal facts
from the same immutable lifecycle snapshot used by machine supervision and
OTLP, while keeping its delivery result independent. A lineage fault cannot
change final or DLQ bytes, exit status, machine terminal payload, publication
inventory, visible final set, or retained failed-attempt evidence. Event values
also remain outside this identity-only payload; telemetry field policy is
enforced before Collector queue admission. If `[observability]` is absent,
neither the lineage worker nor the Collector worker exists.

## When to use

- **Impact analysis** -- before changing a source schema, see which outputs and columns depend on it.
- **Auditing & governance** -- feed the OpenLineage events into a catalog (e.g. Marquez) to track data provenance.
- **Review** -- attach the lineage of a new pipeline to a PR to confirm the intended derivations.

Because `--lineage` reads no data, it runs instantly and works on a pipeline whose inputs do not yet exist.

## Limitations

Lineage is derived from the compiled plan, so a few constructs are approximated:

- A column-grain `$doc` read is traced as DIRECT lineage (see [`fields`](#reading-the-columnlineage-facet) above) in a transform projection, a combine body, a composition body, and an **aggregate** emit, attributed only to a source whose envelope declares the section. A `$doc` read in an **influence predicate** -- a route condition, a cull `drop_group_when`, or a combine `where` -- is surfaced as INDIRECT influence (`FILTER` for route and cull, `JOIN` for combine). Two `$doc` cases remain uncovered: a whole-section **envelope echo** (an output header/footer regenerated from a source document section, with no output column or expression); and any `$doc` reference in a **Reshape** rule, which the compiler rejects outright (Reshape re-runs its rules after a per-group spill that drops envelope context), so there is no Reshape envelope lineage to produce.
- A **match: collect** combine declared without a projection body produces coarse column lineage: each collected column derives (as `TRANSFORMATION`) from *every* build-side column, because there is no body expression to pin the exact source column.
- INDIRECT influence covers route/cull predicates, join keys, aggregate grouping, and correlation sort over **record columns** (and `$doc` envelope terms in route/cull/combine predicates, as above). An aggregate's pre-aggregation row `filter`, a transform-inline `filter`, and Reshape `order_by` / `partition_by` are not (yet) attributed as influence.
- Constant and `count(*)` columns (which have no source input) are omitted from `fields`; engine-stamped columns (`$ck.*`, `$meta.*`, `$source.*`) are skipped, mirroring the default writer.
