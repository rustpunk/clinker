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

Bare `--dry-run` performs the same planner compilation without rendering the
plan:

```bash
clinker run pipeline.yaml --dry-run
```

Prefer `--explain text` when reviewing a schema change because the resulting
plan is visible evidence of what the planner admitted. See [Explain
Plans](explain.md) for text, JSON, and DOT plan output.

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

## Recommended workflow

1. Run `clinker run pipeline.yaml --explain text` for planner admission and
   inspect the compiled DAG.
2. Use bare `--dry-run` only when a quiet canonical compile check is preferable.
3. Run representative data against an isolated destination and inspect it.
4. Run the full job only after checking the representative result and
   destination policy.
