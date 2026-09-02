# Validation and Admission

`clinker-plan` is the authority that admits a pipeline to execution. A pipeline
is executable only after the planner has parsed canonical YAML, bound schemas
and compositions, type-checked Clinker Expression Language (CXL), and produced
a `CompiledPlan`.

## Canonical planner validation

```bash
clinker run pipeline.yaml --explain text
```

This compiles the complete pipeline through `clinker-plan`, the sole authority
that can admit it for execution. The command checks:

- YAML structure and required fields
- CXL syntax and compile-time type checking
- Schema compatibility between connected nodes
- DAG wiring (no cycles, dangling inputs, or missing nodes)
- Plan-time source and output configuration gates

No runtime readers are opened and no output files are created. Planning may
inspect available file metadata or evaluate matchers for cost estimates. The
command exits with code 0 only after the planner produces a `CompiledPlan`, and
with code 1 for a configuration, schema, or plan diagnostic. Admission does not
prove that later input decoding or I/O will succeed, and the rendered plan does
not prove output correctness.

Composition resource descriptors and bindings are part of this admission. The
planner checks the bounded `[catalog.resources]` table, declared
`_compose.resources_schema` slots, call-site and overlay logical identities,
kind/capability compatibility, required slots, fixed locks, and recursive
composition bodies without resolving credentials or opening handles.

An ordinary composition call containing `alias:` or `outputs:` fails during
strict YAML parsing with E377 at the authored location. Replace `alias:` with
the composition node's `name:`. Declare ports under `_compose.outputs` and
refer to them downstream as `<composition-node-name>.<port>`. The separate
`add.alias` field remains valid only within an overlay `add` operation.

Bare `--dry-run` performs the same planner compilation without rendering the
plan:

```bash
clinker run pipeline.yaml --dry-run
```

Prefer `--explain text` when reviewing a schema change because the resulting
plan is visible evidence of what the planner admitted. See [Explain
Plans](explain.md) for text, JSON, and DOT plan output.

## Guessing numeric types and repeated source fields

`numeric` is an authoring-only placeholder. Runtime planning still rejects it
with E158; use `clinker guess` to collect the real readers' parser evidence and
produce an exact patch, then review and apply that patch before compilation.

```bash
clinker guess pipeline.yaml
clinker guess pipeline.yaml --field orders.amount --field orders.tax
clinker guess pipeline.yaml --channel production --check
clinker guess pipeline.yaml --field orders.amount --write
```

With no selector, the base pipeline is inspected. Exactly one `--channel ID`
or `--group NAME` selects an effective configuration; the two options conflict.
Repeatable `--field node.column` selectors narrow literal `numeric` leaves or
select one concrete column from a single-record CSV, JSON, or XML source for
multiplicity review. Numeric selectors retain their existing meaning and can
represent more than one authored multi-record leaf; the report gives every
exact owner address separately. Concrete multiplicity candidates are limited
to directly authored columns that do not already declare `multiple: true`.

The default preview is deterministic, bounded, and read-only. It freezes the
configured stable file order (name ascending by default) and reports the
fixed-size identity of its normalized paths, order, and sizes, then allocates
four file opens, 1,024 records, and 8 MiB of admitted file sizes globally in
round-robin source/file order. Each selected reader is pinned to its discovered
length: a pre-open mismatch, truncation, or growth fails instead of expanding
the preview, and no format reader receives bytes beyond that admitted length.
Multi-pass formats may report more physical `bytes_read` because each bounded
pass rereads the same admitted input. The manifest itself is capped at 4,096 files;
narrow a matcher or use `files.take_first` /
`files.take_last` if the selected set is larger. The YAML/configuration cap
limits candidates to 100,000 source-schema leaves. Per owner, at most eight
representative observations are retained, each with at most 128 bytes of
numeric lexeme evidence. Coverage retains at most four file details per source
and reports aggregate sampled, truncated, uncovered, and unreported counts for
the rest. Multiplicity inference retains counters and a fixed interpretation
set, never field values or a raw sample corpus. More than 16 distinct CSV
delimiter candidates is review-only. These fixed bounds are also printed in
the JSON report when they apply.

The manifest identity covers normalized path, configured order, and discovered
size; it is not a content hash or a compare-and-swap proof. Preview and check
perform no edit. Write additionally streams an exact BLAKE3 snapshot of every
file in the capped manifest before evidence collection and compares it again
after collection and immediately before publication.

`--check` uses the same frozen, capped manifest but reads every selected file
and record. It is exhaustive over that manifest rather than subject to the
preview's open/record/byte sampling budgets. `--write` is equally exhaustive
and edits only when exactly one resolved owner is directly authored in the
base pipeline. Numeric evidence may replace one literal `numeric` leaf.
Multiplicity evidence may set one column's `multiple: true` and, for CSV, add
one complete `split_values` entry using the proven delimiter and activated
escape. Both are one owner mutation. An overlay, external/generated owner,
alias, interpolation, existing conflicting split declaration, no-op
already-multiple column, symlink, non-local input, multiple owners, unresolved
evidence, or changed snapshot leaves the pipeline untouched and reports the
patch with exit 3. The edit is reparsed and compared with a typed expected
configuration, so comments, ordering, spans, and every unrelated scalar remain
unchanged.

Publication holds an advisory `fs4` lock on the stable sibling
`<CONFIG>.clinker-guess.lock` file through a sibling-temp flush/fsync, final
exact byte/semantic/input revalidation, atomic replacement, and parent directory
fsync. The lock file remains beside the config so cooperating replacements keep
one lock inode across renames; it must remain regular, non-symlinked, and
owner-only on Unix. This catches any change visible at the final comparison,
but is not a kernel-enforced content-conditional rename: a writer that ignores
the advisory lock can still race after the final check. Use one cooperating
configuration writer per file.

Numeric votes come only from the parser-owned observations used by the shared
runtime reader construction. Exact integers vote `int`; finite, representation-
safe values vote `float`. Mixed integer/float evidence resolves to `float` only
when every integer is exactly representable there. Numeric defaults vote
through the schema parser. Accepted missing/null/empty states abstain but remain
reported, forbidden absence is a conflict, and all-no-value evidence remains
unresolved. No confidence threshold or statistical guess is used.

### Repeated-value evidence

Multiplicity is proved per logical record; counts from separate records are
never added together. The production reader runs against a temporary schema
clone so it can retain ordered repeated values for observation without changing
the effective pipeline:

- XML becomes conclusive when one record contains two or more sibling elements
  at the selected path. A sibling in each of two records is still unconfirmed.
- JSON becomes conclusive when one record contains an array longer than one.
  Null, empty, and one-element arrays remain unconfirmed.
- CSV becomes conclusive only when exactly one delimiter/activated-escape
  interpretation parses and re-encodes every non-null cell to the original
  bytes in the source's declared character set, and at least one cell produces
  multiple values. Two surviving interpretations are review-only.

For example, each selected `tags` field below uses the same existing schema
surface:

```yaml
schema:
  - name: tags
    type: string
```

Conclusive XML has two siblings in one `row`:

```xml
<root><row><tags>a</tags><tags>b</tags></row></root>
```

Conclusive JSON has an array longer than one:

```json
[{"tags": []}, {"tags": ["a"]}, {"tags": ["a", "b"]}]
```

Conclusive CSV has one lossless interpretation:

```csv
tags
a|b
plain
```

The corresponding safe CSV edit reuses the normal multi-value syntax:

```yaml
split_values:
  - field: tags
    delimiter: "|"
schema:
  - name: tags
    type: string
    multiple: true
```

These inputs remain review-only or unconfirmed and cannot write:

```json
[{"tags": []}, {"tags": ["a"]}, {"tags": ["b"]}]
```

```csv
tags
a|b;c
d|e;f
```

Run the exhaustive gate before requesting a write:

```bash
clinker guess pipeline.yaml --field values.tags --check
clinker guess pipeline.yaml --field values.tags --write
```

| Exit | Meaning |
|------|---------|
| 0 | Preview completed, including a preview with unresolved owners; exhaustive check resolved every owner; or write published its one safe edit. |
| 1 | Configuration or selection error. |
| 3 | Exhaustive check is unresolved, or write emitted a patch but did not safely edit. |
| 4 | Source discovery, reader, I/O, signal-handler, or report-output failure. |
| 130 | Interrupted before a complete report could be emitted. |

Inspect `outcome`, every owner-level `unresolved_reasons` entry, coverage, and
the emitted patch. A preview exit of 0 is not proof that every selected field
resolved; use `--check` when the exit status must enforce that condition.

## Preview options are not yet a bounded preview

The CLI accepts `--dry-run -n N` and `--dry-run-output PATH`, but those options
are not currently wired to a bounded preview. Supplying `-n` proceeds through
the ordinary execution path instead of enforcing a record limit, and
`--dry-run-output` is parsed without redirecting preview output.

Do not use either option to protect a production destination. Until bounded
preview is implemented and tested, run representative data with an explicitly
isolated destination you can inspect.

## Advisory workspace schema analysis

`clinker-schema` is a separate advisory authoring library. It reuses the
canonical typed YAML parser for pipeline structure and schema references, but
it is not called by `clinker run`. Its warnings and coverage status cannot
admit or reject execution, and an advisory result never overrides the planner.

| Status | Exact meaning |
|--------|---------------|
| `analyzed` | Every applicable facet represented by the advisory model was inspected. This is not planner acceptance. |
| `partial` | Some applicable content was inspected, but an unsupported shape or bounded limit left a gap. Read the attached reasons, then run the canonical planner check. |
| `skipped` | The artifact had no applicable advisory content, such as no external schema reference or no fields to inspect. This is not success. |
| `failed` | The artifact could not be inspected safely or structurally, such as a read, parse, or reference-resolution failure. Diagnose the reason and still use the planner as the execution authority. |

Known advisory limits are explicit rather than silently treated as valid:

- The advisory schema model covers linked external `.schema.yaml` metadata.
  Inline column lists, generated schemas, multi-record schemas, and
  planner-owned external schema shapes remain planner concerns; an applicable
  unsupported external shape reports `partial`.
- Transform field scanning is a conservative heuristic. It does not reproduce
  CXL parsing, name resolution, schema flow, composition binding, or type
  checking; when linked schema content is otherwise analyzable, a transform
  therefore makes that advisory coverage `partial`.
- Array element types and object shapes without declared child fields are not
  modeled completely. Format matching is also unavailable for fixed-width and
  SWIFT sources, and the advisory Parquet token is not a supported pipeline
  source format.
- Analysis retains at most 10,000 field descriptors to depth 64, 4,096 schema
  references, and 1,024 reasons. Reaching a bound reports `partial` rather than
  dropping the gap.

After reading an advisory report, make the authoring decision against the
canonical result:

```bash
clinker run pipeline.yaml --explain text
```

## Recommended runtime-validation workflow

1. Run `clinker run pipeline.yaml --explain text` for planner admission and
   inspect the compiled DAG.
2. Use bare `--dry-run` only when a quiet canonical compile check is preferable.
3. Run representative data against an isolated destination and inspect it.
4. Run the full job only after checking the representative result and
   destination policy.
