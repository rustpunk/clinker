# Changelog

All notable changes to Clinker are tracked here.

## Unreleased

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
| Channel var override disagrees with the pipeline's declared type, or its default does not match that type | `E107` | `E116` |
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
