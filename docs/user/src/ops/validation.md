# Validation and Admission

`clinker-plan` is the authority that admits a pipeline to execution. A pipeline
is executable only after the planner has parsed canonical YAML, bound schemas
and compositions, type-checked Clinker Expression Language (CXL), and produced
a `CompiledPlan`.

## Compile validation

```bash
clinker run pipeline.yaml --dry-run
```

This validates everything that can be checked without processing records:

- YAML structure and required fields
- CXL syntax and compile-time type checking
- Schema compatibility between connected nodes
- DAG wiring (no cycles, dangling inputs, or missing nodes)
- Plan-time source and output configuration gates

No runtime readers are opened and no output files are created. Planning may
inspect available file metadata or evaluate matchers for cost estimates. The
command exits with code 0 when the planner admits the pipeline and code 1 for a
configuration, schema, or plan diagnostic. Admission does not prove that later
input decoding or I/O will succeed.

Use `--explain` for the same compile-time checks plus a rendered plan:

```bash
clinker run pipeline.yaml --explain
```

See [Explain Plans](explain.md) for text, JSON, and DOT plan output.

## Preview options are not yet a bounded preview

The CLI accepts `--dry-run -n N` and `--dry-run-output PATH`, but those options
are not currently wired to a bounded preview. Supplying `-n` proceeds through
the ordinary execution path instead of enforcing a record limit, and
`--dry-run-output` is parsed without redirecting preview output.

Do not use either option to protect a production destination. Until bounded
preview is implemented and tested, run representative data with an explicitly
isolated destination you can inspect.

## Advisory workspace schema analysis

`clinker-schema` is a separate advisory authoring library. It is not called by
`clinker run`, and its warnings cannot admit or reject execution. Its coverage
is narrower than the planner's:

- It discovers external `.schema.yaml` files and scans pipeline text for simple
  `schema:` references instead of parsing pipelines through the canonical YAML
  boundary.
- It checks linked source formats and uses a conservative text heuristic for
  field references in Transform CXL. It does not reproduce planner name
  resolution, schema flow, composition binding, or type checking.
- Inline, generated, multi-record, or otherwise unlinked schemas are skipped.
- Discovery reports schema parse/read failures separately, while unsupported
  pipeline analysis can yield no advisory findings.

A tool presenting these results must distinguish **analyzed**, **partial**,
**skipped**, and **failed** coverage rather than treating silence as success.

## Recommended workflow

1. Run `clinker run pipeline.yaml --dry-run` for planner admission.
2. Use `--explain` when you also want to inspect the compiled DAG.
3. Run representative data against an isolated destination and inspect it.
4. Run the full job only after checking the representative result and
   destination policy.
