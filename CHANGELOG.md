# Changelog

All notable changes to Clinker are tracked here.

## Unreleased

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
name and writes back nested. Tracked separately.

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
