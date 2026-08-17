# Changelog

All notable changes to Clinker are tracked here.

## Unreleased

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
