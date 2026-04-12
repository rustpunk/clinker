# Validation & Dry Run

Clinker provides two levels of pre-flight validation so you can catch problems before committing to a full run.

## Config-only validation

```bash
clinker run pipeline.yaml --dry-run
```

This validates everything that can be checked without reading data:

- YAML structure and required fields
- CXL syntax and compile-time type checking
- Schema compatibility between connected nodes
- DAG wiring (no cycles, no dangling inputs, no missing nodes)
- File path resolution (existence checks for inputs)

No records are read. No output files are created. The command exits with code 0 on success or code 1 with a diagnostic message on failure.

**Use this after every YAML edit.** It runs in milliseconds and catches the majority of configuration mistakes.

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

Use both validation levels in sequence before every production run:

1. **`--dry-run`** -- catch configuration and type errors instantly.
2. **`--dry-run -n 10`** -- verify output shape and values against real data.
3. **Full run** -- execute with confidence.

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
clinker run pipeline.yaml --explain          # inspect the DAG
clinker run pipeline.yaml --dry-run          # validate config
clinker run pipeline.yaml --dry-run -n 10    # preview with data
clinker run pipeline.yaml --force            # run for real
```
