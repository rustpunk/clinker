# Changelog

All notable changes to Clinker are tracked here.

## Unreleased

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
