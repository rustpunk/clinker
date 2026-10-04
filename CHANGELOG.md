# Changelog

All notable changes to Clinker are tracked here.

## Unreleased

### Changed — Cull and Reshape order each group like a Sink sort

A Cull or Reshape `order_by` now orders the rows of each group by the same
rule a Sink `sort_order` uses (see
[How values are ordered](docs/user/src/nodes/sink.md#how-values-are-ordered)),
and applies the `null_order` written there, `last` by default for `asc` and
`desc` alike. Output changes where a group's ordering key holds:

- **A null under `desc`, or with `null_order` omitted.** Nulls came first
  under `desc`; they now come last unless the field says
  `null_order: first`.
- **An authored `null_order: first`.** It was accepted and then ignored; it
  now places the nulls.
- **NaN, `-0.0`, and a column mixing integers with floats or decimals.** An
  integer and a float used to compare as equal, and a NaN could stop the
  comparison, so the rows around them came out in no fixed order. They now
  take their place in the value order.

Closes [#1281](https://github.com/rustpunk/clinker/issues/1281).

### Changed — null_order: drop is accepted only on a Sink sort_order

`null_order: drop` excludes records whose key is null, which only a Sink's
`sort_order` is for. On a field that only orders records it is now refused
when the pipeline is planned, with the reason and one fix: delete
`null_order: drop` and add a Transform whose whole `config` is the printed
`config: { cxl: "filter not <field>.is_null()" }` line, before the node, or
after a Source. Node and field names print in double quotes, escaped as in
every other diagnostic:

- **Cull and Reshape `order_by`.** `drop` used to be accepted and silently
  ignored. It is now an E200 error naming the node and the field:

  ```text
  cull "dedupe": `null_order: drop` is not allowed on `order_by` for field "txn_date": `order_by` only orders the rows of a group, placing nulls `first` or `last`, and cannot remove a row. To remove the rows whose "txn_date" is null, delete `null_order: drop` and add a Transform before this node with `config: { cxl: "filter not txn_date.is_null()" }`.
  ```

- **Source `sort_order`.** `drop` was already refused; the error now also
  gives the fix, the printed filter Transform after the Source, and is
  reported as E200 at the Source like the other ordering-only fields (it used
  to be an E003 "node property derivation failed" error with no location).
- **Transform `analytic_window.sort_by`.** `drop` used to silently take
  null-key rows out of the window partition, so the window functions never
  saw them, while the Transform still wrote those rows. It is now an E200
  error at the Transform, and `null_order` there is `first` or `last` only.
  To leave those rows out, filter them in a Transform before the windowed
  one; unlike the old behaviour, that also removes them from its output:

  ```text
  transform "running": `null_order: drop` is not allowed on `analytic_window.sort_by` for field "amount": `sort_by` only orders the rows of a window partition, placing nulls `first` or `last`, and cannot remove a row. To remove the rows whose "amount" is null, delete `null_order: drop` and add a Transform before this node with `config: { cxl: "filter not amount.is_null()" }`.
  ```

  Two windows that differ only in `null_order` now each read partitions in
  their own order; they used to share one index sorted by whichever came
  first.

The filter is printed only for a field CXL can name as it is: one identifier
of ASCII letters, digits and `_`, not starting with a digit and not a CXL
keyword. For any other field, such as `order id`, `filter` or a flattened
`Address.City`, the error prints no CXL, since that text would not parse or,
for a dotted name, would read another value and drop every row. Its one
next step is the `source_name:` line it prints for that column: in the
column's Source schema entry, set `name` to a new identifier and add that
line, then use the new name wherever the pipeline names the column. Planning
again prints the filter on the new name:

```text
source "orders": `null_order: drop` is not allowed on `sort_order` for field "order id": a Source `sort_order` only states the order its records arrive in, placing nulls `first` or `last`, and verifying it cannot discard a record. To remove the rows whose "order id" is null, first give the column a name CXL can write: in its Source schema entry, set `name` to a new identifier and add `source_name: "order id"`, then use the new name wherever the pipeline names this column; planning again prints the filter to add. A CXL name is one identifier of ASCII letters, digits and `_`, not starting with a digit and not a CXL keyword.
```

Cull and Reshape `order_by` also accept a bare field name, as a Sink or
Source `sort_order` does: `order_by: [txn_date]` is
`order_by: [{ field: txn_date }]`.

For Rust callers, `validate_source_sort_policy` returns the validated fields
(`Vec<OrderField>`) instead of `()`, and `PlanNode::Cull` and
`PlanNode::Reshape` carry the validated `order_by: Vec<OrderField>`. The
window index types (`RawIndexRequest`, `IndexSpec`, `find_index_for`) and
`pipeline::sort::{sort_partition, is_sorted}` take `OrderField`s, and
`sort_partition` takes the positions as `&mut [u64]`.

### Fixed — a rejected document's rows are dead-lettered once under `dlq_granularity: document`, and no held row is lost

Under `dlq_granularity: document`, a Sink could write a document's records
before a node on another branch rejected the document, a row that only a
later Sink held was lost from the dead-letter output, and a failed document's
failing rows had to fit in memory.

- Every Sink now runs after every other node, so each document's verdict is
  final before any Sink writes one of its records. `--explain` lists the
  Sinks last, and dead-letter rows that other nodes write come before the
  rows a Sink writes.
- Each source row of a rejected document is dead-lettered once, however many
  Sinks held it, and `dlq_count` counts it once. Rows that only a later Sink
  held are no longer lost. The exception is a record that an Aggregate, a
  Combine or a Reshape failure already dead-lettered: it is written again, as
  a `document_rejected` row, when a Sink on another branch also received it
  and its document is rejected, and `dlq_count` counts that record twice
  ([#1232](https://github.com/rustpunk/clinker/issues/1232)).
- A failed document's failing rows are held, charged to the memory budget,
  until the document is rejected, and move to one file in the spill
  directory when the budget needs the memory. Past
  `storage.spill.disk_cap_bytes` the run fails with E320; it fails with E310
  only when one more row does not fit with every held row already on disk.
- The record of which rows a rejected document has already written is
  charged to the memory budget too. It never spills, so it ends in E310 if
  it would pass the limit once every held row is on disk.
- A row failure inside an Aggregate, a Combine or a Reshape still
  dead-letters only that record and does not reject its document
  ([#1232](https://github.com/rustpunk/clinker/issues/1232)). A Sink on
  another branch that also received that record writes it again as a
  `document_rejected` row when another failure rejects its document, and
  publishes it when none does. The rows a Combine or an Aggregate writes are
  not yet held back by their document's verdict
  ([#1317](https://github.com/rustpunk/clinker/issues/1317)), nor are the
  rows that pass through a Reshape (#1232). See "Not
  covered" under "Document-level DLQ" in the error-handling reference.

### Changed — a Sink inside a composition body no longer compiles under `dlq_granularity: document`

**Breaking change.** A pipeline whose Source declares
`dlq_granularity: document` and whose composition body declares a Sink is
now refused at compile time with E378. The refusal keeps the guarantee that
no Sink writes a rejected document's records for when body Sinks write
([#1242](https://github.com/rustpunk/clinker/issues/1242)); today a body Sink
writes nothing in a run, so moving it to the pipeline is also how its output
gets written.

Where moving the Sink to the pipeline through a new composition output port
runs today, the error's help prints that move ready to paste, including the
Sink's own configuration. For a Sink that reads a Transform and writes only
the columns it emits (`include_unmapped: false` with no `mapping:`), the
printed move writes those columns as a `mapping:`, so the moved Sink writes
the same columns.
Otherwise the help names
`clinker explain --code E378`, which shows how to declare the Sink's work at
pipeline level.

### Fixed — `--explain` reports what a `dlq_granularity: document` run streams

Under `dlq_granularity: document`, `--explain` reported every Sink, and any
stage that would hand its output to a streaming consumer, as
`buffer: streaming`, though the run streams none of them. Every
Sink now reports `buffer: materialized`, since it holds each open document's
records until the document's verdict, and no Transform, Merge, Route,
Aggregate or Combine reports `buffer: streaming`.

### Changed — a Combine's `match: first`, `all` and `collect` follow the build input's arrival order on every join strategy

`match: first` now picks the earliest matching build record in the build
input's arrival order, whichever join strategy the planner picks. The hash
and grace-hash strategies, which run every equality-only Combine, used to
pick the most recently arrived matching record instead, while the range
strategies picked the earliest; adding a range condition every record
satisfies could therefore change which record enriched a driver. The same
order now sets the row order of `match: all` within a driver and the element
order of a `match: collect` array.

- To pick the latest record, deliver the build input sorted descending, for
  example with a descending `sort_order` on the build Source.
- A `correlation_key` on the build Source sorts its rows by the key before
  they reach the Combine, and "first" follows that order.

### Fixed — a Combine `where:` that fails to evaluate is neither a match nor a miss

A Combine residual that fails to evaluate is neither a match nor a miss. Its
pair is dead-lettered and, whatever the join strategy:

- a driver with no matching candidate and at least one failing one no longer
  reaches `on_miss`: `on_miss: error` no longer stops a `continue` run with
  E319 (the run completes with exit code 2), and `null_fields` no longer adds
  a null-filled row beside the failure;
- `match: first` stops at the first candidate, in build arrival order, that
  is not a non-match: a failing one is the driver's only result instead of a
  later candidate being taken, and a failure of a candidate after the chosen
  one is no longer written (the IEJoin strategies wrote it);
- `match: collect` writes no row for a driver with a failing candidate,
  neither a partial array nor an empty one, and a driver past the 10,000
  entry limit still has every failure among its remaining candidates
  written;
- `match: all` still emits the driver's successful pairs.

On range and equi+range joins, a `match: all` driver whose bodies all skip or
fail no longer reaches `on_miss`, matching equality joins. Under `fail_fast`
a failing residual stops the run with its evaluation error, never E319.

`records_ok`, `records_written` and `max_output_rows` no longer count the
null-filled, later-candidate or partial-array rows these drivers used to
write; under `match: first` the dead-letter counts fall by the failures
after each deciding candidate.

A Route branch condition follows the same rule, which it already did: a
failing condition takes no branch and not the default.

On a hash Combine whose driver streams straight in, the failures of a
`match: all` hot key are held until the probe finishes, and they now count
against `pipeline.memory.limit` as they accumulate, so a key whose residual
mostly fails ends in a memory-limit error rather than growing past the limit
unseen.

### Fixed — a `where:` conjunct beside one or two range conjuncts is applied on the IEJoin strategies

A Combine whose `where:` held one or two range conjuncts and a conjunct that
is neither an equality nor a range, such as `a.lo <= b.v and a.x / b.y > 1`,
never applied that conjunct when the planner picked an IEJoin strategy:
every pair within the ranges matched. The conjunct is now evaluated for
every pair, as on the other strategies.

### Changed — a correlation key writes one dead-letter row per failure

A correlation key no longer removes, merges or relabels a failure row. Every
failure writes its own trigger row under a key exactly as it does without
one, triggering field and value included; the key only adds the rows a
failing group condemns. Every `_cxl_dlq_trigger_id` names a trigger row the
run wrote.

- A Combine driver that fails against several build rows is written once per
  failure, each copy followed by the build row of that failure. A build row
  now reports its own Source and row number on every join strategy, and
  under a key it is held with its failing driver's group: it never condemns
  its own group, is not counted a second time against `max_group_buffer`,
  and is not retracted from relaxed Aggregates.
- A row that fails on two inclusive Route branches writes two trigger rows,
  one per branch, including in an overflowing group.
- Every join strategy writes the same rows for the same failing input. The
  hash build-probe strategy used to dead-letter a driver at its first
  failing match and drop the output of its other matches; under
  `match: all` it now evaluates every matched pair like the other
  strategies: each failing pair is one failure, and the driver's successful
  matches are still written. IEJoin and sort-merge failures now name the
  driver's row in their error detail, as the hash strategies do.
- Under a key, `dlq_count`, `records_dlq` and the `dlq.max_rate` numerators
  therefore rise to the counts the same failures give without a key, plus
  the rows the failing groups condemn.

### Changed — records group by exact numeric value, and NaN is one group

Every place that puts records into groups now decides whether two values
are the same group by the rule sorting uses (see
[How values are ordered](docs/user/src/nodes/sink.md#how-values-are-ordered)):
Aggregate `group_by`, Cull and Reshape `partition_by`, a window's `group_by`,
correlation keys, `distinct` and output splitting. Output changes where a
group key holds:

- **Integers above 2^53.** Distinct integers are distinct groups however
  large. Integers used to be grouped through a float, so neighbours such as
  `9007199254740992` and `9007199254740993` merged into one group.
- **An integer and a decimal of equal value** are one group, as an integer
  and a float of equal value already were. They used to be two groups.
- **Negative zero.** `-0.0` and `0.0` remain one group.
- **NaN.** Every NaN key, whatever its sign, is one group, separate from the
  null group. A NaN key used to stop the run in Aggregate, Cull, window
  partitions and output splitting; to fail the record in `distinct`, handled
  like any other evaluation error under `error_handling`; and to join the
  null group in Reshape and in correlation keys.
- **The written group value.** A group reports the value of its first-arriving
  row, the same with or without spilling to disk. An integer group-by column is
  now written as integers (JSON `42` where it was `42.0`), and integers above
  2^53 are written exactly where they were rounded. A column that holds both
  integers and floats writes each group as its first row held it.
- **Session-window Aggregates.** A session-window Aggregate grouped by an
  integer column, or by a column holding integers among other numbers,
  writes its groups in a different order than before. The order is the same
  on every run, but it is not the value order: a group keyed `10` can be
  written before one keyed `9`. Session groups are written in key order in a
  later release.

Reshape still puts empty strings and array- or map-valued cells in its null
group, unlike Cull
([#1022](https://github.com/rustpunk/clinker/issues/1022)).

### Fixed — sorting places NaN, negative zero and mixed numbers by one rule

A Sink or Source `sort_order` and a window's `sort_by` now order every value
by one rule, described in
[How values are ordered](docs/user/src/nodes/sink.md#how-values-are-ordered).
Output changes where a sort key holds:

- **NaN.** Every NaN, whatever its sign, is one value that sorts after `inf`
  in ascending order and first in descending order. A NaN used to compare
  equal to every value, which made it a barrier: the records around it could
  be left unsorted, and the output could depend on the memory limit, because a
  sort that spilled to disk split the records at different places.
- **Negative zero.** `-0.0` and `0.0` are equal, so they keep their arrival
  order.
- **Integers, floats and decimals in one column.** They compare by exact
  value: the integer `9007199254740993` sorts after the float
  `9007199254740992.0`, and an integer and a decimal of equal value are equal.
  A sort that spilled to disk used to compare an integer with a float by raw
  bytes of different meaning.
- **Values of different types in one column** order by a fixed rank: booleans,
  numbers, strings, dates, datetimes, arrays, maps. They used to compare equal.
- **Leap seconds.** A leap-second datetime sorts with the instant one second
  later, as a sort that spilled to disk already did.

Nulls are still placed only by `null_order`.

### Changed — terminal Output nodes are now Sinks

**Breaking YAML and Rust API change.** The terminal destination node is now
authored as `type: sink`. The retired `type: output` spelling is rejected with
E376 and a source-located `type: sink` correction; it is not retained as an
alias.

Rust callers must migrate `OutputConfig` to `SinkConfig`, `OutputBody` to
`SinkBody`, `PipelineNode::Output` to `PipelineNode::Sink`, and
`PipelineConfig::output_configs()` to `PipelineConfig::sink_configs()`. The
compiled form is `PlanNode::Sink` with `PlanSinkPayload`.

Only the terminal-node concept changed. Output ports, output fields and
projections, serialized output formats, command output, writer results, and
OpenLineage output datasets keep their established vocabulary.

### Fixed — dry-run now performs the documented compile check

Bare `clinker run --dry-run` now compiles the plan and applies channel/group
overlays before returning, while still stopping before runtime source discovery,
reader and writer setup, or record processing. Compilation may inspect source
metadata or matchers for planning estimates. This restores the behavior the CLI
reference, explain pages, and deployment examples already promised: CXL parsing
and type checking, schema binding, DAG wiring, and plan-time gates all run during
a dry-run.

### Changed — compile failures retain structured diagnostics

**Rust API change.** Plan compile failures now use
`PipelineError::PlanDiagnostics` instead of flattening their code, help text,
severity, and spans into `PipelineError::Compilation`. Channel-overlay failures
use `PipelineError::OverlayDiagnostics` so renderers do not blame the pipeline
file for an overlay error. Downstream exhaustive matches on `PipelineError`
must handle both variants.

`clinker_core_types::Diagnostic::error` and `Diagnostic::warning` now enforce
the compile-time diagnostic registry in debug builds. A code passed to either
public constructor must be registered with the matching severity; an unknown or
mismatched code triggers a debug assertion. Downstream code that constructs
Clinker diagnostics must register its codes before upgrading.

`PlanDiagnostics` also records whether its line-only spans are safe to resolve
against the pipeline document. Untrusted spans remain in the structured value
for consumers that can attribute them; the CLI simply omits the source snippet.
### Changed — an Output's `mapping:` is an ordered sequence, and its direction is fixed

**Breaking change to a hand-written YAML key.** `mapping:` was a map of column
name to column name. It is now a sequence, one item per output column:

```yaml
mapping:
  - order_id                # carried through under its own name
  - sold_to: customer_id    # written as `sold_to`, read from `customer_id`
```

A bare scalar carries a column through unchanged; a single-key pair renames.
Declaration order is the output column order — which the map form could not
express at all. Columns the block does not list are appended after it when
`include_unmapped: true` (the default) and dropped when it is `false`.

**The pair direction is `output_name: source_column` — output on the left.**
That is the direction the user guide always documented and the plan layer
always assumed; the executor's rename pass implemented the reverse, so a block
written to the documentation renamed nothing and the run still exited 0. Both
halves now agree on the documented direction.

Migration, both mechanical:

- Put `- ` in front of each line. A pair whose two sides are the same column
  collapses to a bare name.
- **Swap the two sides of each remaining pair.** The engine looked map entries
  up by the incoming field name, so the key was the *source* column:
  `customer_id: sold_to` renamed `customer_id` to `sold_to`, which is now
  `- sold_to: customer_id`.

A map-valued `mapping:` is rejected at compile time with **E364**, and the
message prints your own block already converted — lifted, collapsed, and with
each pair swapped as above. The one block that swap is wrong for is one written
to follow the *old documentation*, which described the opposite direction: such
a block matched no incoming field and so renamed nothing at all, and the
diagnostic says so. It has no behaviour to preserve — swap those pairs back to
what you originally meant.

`mapping:` is now a column **selection**, not a rename overlay. Four silent
outcomes are therefore compile errors:

- a repeated output name (**E364**);
- an empty block, `mapping: {}` or `mapping: []` (**E364**) — it declares an
  output with no columns; remove the key to write every upstream column;
- an output name that `include_unmapped: true` would also carry through
  (**E364**) — the file would carry the column twice and readers would resolve
  the passthrough copy, losing the renamed value;
- an item naming a column that does not exist at that point in the pipeline
  (**E365**, with the available column list and a `did you mean`).

`exclude:` naming a column the mapping *produces* is deliberately **not** an
error. `exclude:` matches incoming column names, so it removes the upstream
column of that name and leaves the mapped one standing — which is exactly the
fix the collision diagnostic above prescribes.

A `mapping:` item may name an `auto_widen` drift column when the output sets
`include_unmapped: true`, which is what expands the sidecar to top-level
columns; under `include_unmapped: false` the sidecar stays packed and the item
is rejected. Similarity to a declared name does not change that waiver: edit
distance can suggest a spelling only after absence is known, and a real drift
column may happen to be similar. **W365** reports the item after the run if no
written record supplied it. Column names in `mapping:` are matched bare — there
is no qualified `input.column` spelling.

Rust callers must also migrate `OutputConfig.mapping` from
`Option<IndexMap<String, String>>` to `Option<OutputMapping>` (constructed from
`Vec<MappingEntry>`). `OutputSpec.mapping` is now a `Vec<MappingEntry>`, and
`ExecutionReport` struct literals must supply the new `advisories` field.

Run `clinker explain --code E364` or `clinker explain --code E365` for the full
pages.

### Changed — every record writes every column an Output's `mapping:` declares

**Breaking change for streams whose records differ in shape.** When a record
does not carry an item's source column, that column is now written **empty**
rather than omitted. Previously such a record passed through without the column,
so the file's shape depended on the data.

The declared column set is the same for every record, in declaration order. That
follows from what the surface already promises: `mapping:` is the author's
statement of which columns the file carries and in what order, and a column that
vanishes on some rows contradicts both. It also makes the output schema a
function of the config rather than of whichever record happened to arrive first —
which matters, because most write paths derive the file's header from exactly
that first record.

Affected: a multi-record-type source, a composition body's open row, and columns
reaching the sink through the `auto_widen` sidecar. A homogeneous stream, where
every record carries every mapped column, is unchanged.

If a `mapping:` block relied on the old behaviour to produce a
per-record-variable column set, remove the items for the columns that vary and
let `include_unmapped: true` append them instead — that path still follows the
data.

### Added — end-of-run reporting for an Output's `mapping:` block

Two advisory warnings, printed to standard error when a run finishes. Neither
changes the exit code: both describe a file that was written and is readable,
and by the time a stream ends the run's other outputs have already been flushed.

- **W365** — a `mapping:` item whose source column *no record* carried, so the
  item wrote an empty column in every row. This replaces a write-boundary
  **E365** that aborted such runs. The abort tested the wrong thing: it checked
  the established output schema, which on most write paths is derived from the
  first record, so it killed runs whose first record merely happened to be
  sparse while staying silent about a column absent from every record after the
  first. Tracking resolution across the whole stream separates the two cases
  exactly — a misspelling is carried by no record, an ordinary sparse column is
  carried by some record and is not reported.
- **W366** — an upstream column dropped because a `mapping:` output name
  occupies its place in the header. The mapped value still wins, unchanged; what
  is new is that the displaced column is named rather than lost in silence.
  Where the planner can enumerate the columns reaching that output, the same
  collision remains an **E364** at compile time.

Run `clinker explain --code W365` or `clinker explain --code W366` for the full
pages.

### Removed — the `best_effort` error strategy

**Breaking change.** `error_handling.strategy` now accepts exactly `fail_fast`
and `continue`. The third spelling, `best_effort`, is gone.

It never had behaviour of its own. The runtime made one decision per record
failure — propagate it, or dead-letter it and keep going — and `best_effort`
took the same branch as `continue` at every site, so the two produced identical
DLQ entries and identical exit codes. The documentation claimed otherwise (that
`best_effort` continued "without writing error records", and that it was "the
most lenient strategy"), which made the config surface look like it offered a
third disposition that the engine could not deliver.

Replace `strategy: best_effort` with `strategy: continue`. A pipeline still
carrying the old value is rejected at config-validation time with a message
naming the replacement and pointing at the offending line, rather than a bare
unknown-value error.

A genuine partial-success mode — one that actually differs from `continue` —
can be designed on its own merits later; nothing about this removal forecloses
it.

### Changed — three channel-overlay conditions moved to their own diagnostic codes

**Diagnostic code change.** Three conditions raised while resolving a channel or
group overlay shared a code with an unrelated composition-binding check. Because
a failure now prints `See: clinker explain --code <CODE>`, sharing sent readers
of one condition to a page describing the other — for a pipeline that may not
use compositions at all. Each condition now has its own code and its own page:

| Condition | Was | Now |
|---|---|---|
| Channel var declaration changes an existing type, or its default does not match its declared type | `E107` | `E116` |
| Channel var name shadows a reserved `$pipeline.*` / `$source.*` field | `E110` | `E117` |
| `vars.source` block keyed by a source the pipeline does not declare | `E111` | `E118` |

The old codes keep their original meanings — `E107` a cycle in the flat
post-expansion graph, `E110` an extraction selection naming a node absent from
the DAG, `E111` a composition body with zero nodes — and are still emitted for
those. Only the overlay conditions moved.

Tooling that greps run output or CI logs for `E107`, `E110`, or `E111` to detect
an overlay misconfiguration needs to match the new codes instead. Nothing in
pipeline or channel YAML changes.

### Changed — JSON output expands dotted column names into nested objects

**Behaviour change.** A JSON output previously emitted every column name
verbatim, so a column named `customer.name` became the literal key
`"customer.name"`. It now expands into `{"customer": {"name": …}}`, matching
what the XML writer has always done with the same column set — so a pipeline
that reads nested JSON and writes JSON reproduces its input shape. This applies
unconditionally; there is no option to keep the old output, because a per-output
flag would mean the same column name meant different things at different
outputs.

Any pipeline whose output schema carries a dotted column name emits a different
JSON shape than before. This includes columns produced by the JSON and XML
readers' flattening, and — under `include_correlation_keys: true` — the
engine-stamped `$ck.<field>` columns, which now nest under a `"$ck"` object.

- To emit a key that genuinely contains a `.`, escape the separator in the
  column name: a column declared `a\.b` writes the single key `"a.b"`. The full
  grammar, including the reserved `[`, is documented at
  `docs/user/src/cxl/field-paths.md`.
- Two column names that cannot both be expanded — a column `a` holding a value
  alongside a column `a.b` needing `a` to be a container, or two spellings of
  the same path — are now refused before any byte is written, naming both
  columns, on the JSON **and** XML writers. The XML writer previously emitted
  two sibling elements for that column set, which its own reader then rejected
  on the way back in.
- A column name carrying a malformed escape (a `\` not part of `\.`, `\[`, or
  `\\`, as in a column literally named `C:\temp`) is likewise refused, with the
  corrected spelling in the message.

Known gap: the readers still join flattened path segments without escaping them,
so a source key that literally contains a `.` arrives as an unescaped column
name and writes back nested. The read-side inverse is tracked at
<https://github.com/rustpunk/clinker/issues/920>.

### Added — scoped variables and the `state` node

- Three-scope variable system: `pipeline`, `source`, and `record`.
  Each scope has its own lifetime (run / source-file / record),
  reader namespace (`$pipeline.*`, `$source.*`, `$record.*`), and
  runtime registry. See `docs/src/pipeline/variables.md` for the
  full reference.
- New top-level `vars:` block declares each variable's name, scope,
  type, and optional default. Reads typecheck against the declared
  registry; writes are rejected if the variable isn't declared with
  that scope.
- New `state` node — the only construct that can mutate a scoped
  variable. The node is a pass-through for records but evaluates
  per-assignment CXL programs and writes results into the
  scope-keyed runtime registry.
- New `phase: init` mode on the `state` node. Init-phase nodes (and
  their transitive ancestors) run to completion before any
  runtime-phase node sees a record. Use case: pre-load lookup
  tables, derive cutoffs from a config source.
- Qualified post-merge syntax `$source.<input_name>.<key>` for
  reading source-scope variables across a Merge or Combine
  boundary, paired with E172 rejecting the unambiguous bare form.
- Composition body opt-in via `_compose.scoped_vars`. Parent scoped
  variables are sealed from composition bodies by default; bodies
  must declare what they consume in their signature, and types must
  match (E174).
- New diagnostics: E164, E170, E171, E172, E173, E174, E175.
  Each carries primary spans on the offending reference and
  secondary spans pointing at the conflicting writer or parent
  declaration.
- `$record.<key>` writes use a dedicated 64-key channel separate
  from `$meta.*`'s 64-key channel, so heavy `$meta` use can't
  starve `$record` writes (and vice versa).
