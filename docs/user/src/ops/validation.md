# Validation & Dry Run

Clinker provides three levels of pre-flight checking so you can catch problems before committing to a full run.

## Config-only validation

```bash
clinker run pipeline.yaml --dry-run
```

This checks the configuration document itself. Among what it catches:

- YAML structure, required fields, and unknown-key rejection
- Per-node config validation — option values and mutually exclusive settings,
  such as a file source declaring no `path`/`glob`/`regex`/`paths` matcher, or
  more than one
- The pipeline-level memory thresholds
- An output envelope naming a section none of its feeding sources declares

No records are read. No output files are created. The command exits with code 0
on success or code 1 with a diagnostic on failure.

**`--dry-run` stops before the plan is compiled.** It does *not* type-check CXL,
bind schemas, check that connected nodes agree on columns, resolve the DAG, or
run the plan-time gates that need the compiled plan — which includes the DLQ
rate bounds (`E318`) and the check that no two output destinations resolve to
the same file (`E322`). A pipeline that `--dry-run` accepts can still fail the
moment a real run starts.

## Plan compilation

```bash
clinker run pipeline.yaml --explain
```

This is the stronger config check, and the one to reach for after a YAML edit.
It compiles the plan — schema binding, CXL parsing, name resolution and type
checking, DAG wiring, and every plan-time gate — then prints the execution plan
and exits without reading data. Anything a real run would reject before its
first record, `--explain` rejects here, with the diagnostic's code, help text
and the offending YAML line. See [Explain Plans](explain.md).

## Record preview

```bash
clinker run pipeline.yaml --dry-run -n 10
```

This reads the first 10 records from each source and processes them through the full pipeline -- transforms, aggregations, routing, and output formatting. Results are printed to stdout.

The record preview exercises the runtime evaluation path, catching issues that config-only validation cannot:

- CXL expressions that are syntactically valid but fail at runtime (e.g., calling a string method on an integer)
- Data format mismatches between the declared schema and actual file contents
- Unexpected null values in required fields

### Save preview to file

```bash
clinker run pipeline.yaml --dry-run -n 100 --dry-run-output preview.csv
```

The output format matches what the pipeline's output node would produce, so `preview.csv` shows you exactly what the full run will write.

## Recommended workflow

Use the levels in sequence before every production run:

1. **`--explain`** -- compile the plan; catch config, schema and CXL type
   errors instantly.
2. **`--dry-run -n 10`** -- verify output shape and values against real data.
3. **Full run** -- execute with confidence.

Bare `--dry-run` belongs in this sequence only as a fast syntax check on a
half-written file; it is `--explain` that gates a change.

This three-step pattern is especially valuable when:

- Editing CXL expressions in transform or aggregate nodes
- Changing source schemas or swapping input files
- Adding or removing nodes from the pipeline DAG
- Modifying route conditions

## Combining with explain

You can also inspect the execution plan before running:

```bash
clinker run pipeline.yaml --explain
```

This shows the DAG structure, parallelism strategy, and node ordering without reading any data. See [Explain Plans](explain.md) for details.

The typical full pre-flight sequence is:

```bash
clinker run pipeline.yaml --dry-run          # check the config document
clinker run pipeline.yaml --explain          # compile the plan, inspect the DAG
clinker run pipeline.yaml --dry-run -n 10    # preview with data
clinker run pipeline.yaml --force            # run for real
```
