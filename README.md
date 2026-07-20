# Clinker

Clinker is a pure-Rust, **bounded-memory batch DAG executor** for finite,
ETL-style jobs. It reads finite inputs (CSV, JSON/NDJSON, XML, fixed-width, and
several EDI/messaging formats such as HL7, X12, EDIFACT, and SWIFT), drives
them through a directed acyclic graph of transformation nodes one record at a
time, and exits when the inputs are drained. It ships as a single static binary
with no interpreter, no runtime, and no install dependencies.

Pipelines are declared in **YAML**. Per-record transformation logic is written
in **CXL**, a small expression language purpose-built for ETL. Planning
(parsing, schema binding, CXL type-checking, DAG lowering) is a separate phase
from runtime execution, so a pipeline is fully validated before any data is
read.

## Three pillars

Clinker's scope is fixed by three permanent commitments; unbounded streaming,
daemon/service mode, distributed execution, and worker-process pools are
explicit non-goals.

1. **Finite inputs.** Files are the canonical shape. Finite-cursor network
   sources (paginated REST APIs with page/record caps) fit the same model —
   they exhaust their cursor and hit EOF. Unbounded sources (Kafka, Kinesis,
   `tail -f`-style followers) are out of scope.
2. **Finite jobs.** A run begins when you invoke `clinker run`, drains the DAG,
   and exits with a status code. There is no long-running daemon and no service
   surface.
3. **Single process.** One `clinker` invocation is one OS process. Parallelism
   happens *inside* the process via `std::thread` and Rayon; Clinker does not
   spawn worker processes or coordinate a cluster.

## Why Clinker

- **Single binary, zero dependencies.** No JVM, no Python, no package manager.
  Linux, macOS, and Windows are all tested in CI.
- **Bounded memory.** A run is charged against a configurable RSS budget
  (default 512 MB). Non-fused stage boundaries buffer against that same
  envelope; blocking operators (Aggregate, sort, grace-hash Combine) spill to
  disk under memory pressure and fail fast with `E310 MemoryBudgetExceeded` at
  the hard limit rather than OOM-killing the process.
- **Reproducible output.** Given the same input and pipeline, Clinker produces
  byte-identical output across runs.
- **Operability first.** Per-stage metrics, dead-letter queues for failed
  records, explain plans, and structured exit codes for scripting.

## Binaries

| Binary    | Purpose                                                    |
|-----------|------------------------------------------------------------|
| `clinker` | Validate and run pipelines against real data.              |
| `cxl`     | Check, evaluate, and format CXL expressions interactively. |

## A taste of Clinker

Given `employees.csv`:

```csv
id,name,department,salary
1,Alice Chen,Engineering,95000
2,Bob Martinez,Marketing,62000
3,Carol Johnson,Engineering,88000
4,Dave Williams,Sales,71000
```

and this pipeline in `my_first_pipeline.yaml`:

```yaml
pipeline:
  name: salary_report

nodes:
  - type: source
    name: employees
    config:
      name: employees
      type: csv
      path: "./employees.csv"
      schema:
        - { name: id, type: int }
        - { name: name, type: string }
        - { name: department, type: string }
        - { name: salary, type: int }

  - type: transform
    name: classify
    input: employees
    config:
      cxl: |
        emit id = id
        emit name = name
        emit department = department
        emit salary = salary
        emit level = if salary >= 90000 then "senior" else "junior"

  - type: output
    name: report
    input: classify
    config:
      name: salary_report
      type: csv
      path: "./salary_report.csv"
```

run it with:

```bash
clinker run my_first_pipeline.yaml
```

which writes `salary_report.csv`:

```csv
id,name,department,salary,level
1,Alice Chen,Engineering,95000,senior
2,Bob Martinez,Marketing,62000,junior
3,Carol Johnson,Engineering,88000,junior
4,Dave Williams,Sales,71000,junior
```

Useful flags on `clinker run`:

- `--dry-run` — parse the YAML, resolve the DAG, and type-check every CXL
  expression against the declared schemas without reading any data.
- `--dry-run -n 2` — additionally read the first two records and print the
  results to the terminal.
- `--explain` — print the execution plan (DAG topology, per-node strategy, and
  schema propagation) instead of running.

Beyond `run`, the CLI also offers `explain` (field provenance and error-code
lookup), `metrics` (collect spooled execution metrics), `channels`
(multi-tenant overlay tooling), and `refactor`.

## Build from source

Clinker requires **Rust 1.91+** (edition 2024). The pinned toolchain lives in
`rust-toolchain.toml`, so `rustup` fetches the right version automatically.

```bash
git clone https://github.com/rustpunk/clinker.git
cd clinker

# Install the pipeline executor and the CXL tool
cargo install --path crates/clinker
cargo install --path crates/cxl-cli

# Run the full workspace test suite
cargo test --workspace
```

There are no C build dependencies.

## Repository layout

Clinker is a Cargo workspace. The main crates:

- `crates/clinker` — the `clinker` CLI.
- `crates/cxl`, `crates/cxl-cli` — the CXL language (parser, resolver,
  type-checker, evaluator) and its standalone tool.
- `crates/clinker-plan` — YAML config, validation, schema binding, CXL compile,
  and DAG lowering into a typed `CompiledPlan`.
- `crates/clinker-exec` — the runtime executor: operator dispatch, memory
  arbitration, spill, backpressure, metrics, and DLQ.
- `crates/clinker-record`, `crates/clinker-format` — the record/value model and
  the streaming format readers/writers.
- `crates/clinker-net`, `crates/clinker-channel`, `crates/clinker-schema`,
  `crates/clinker-lineage` — integration crates (finite REST source,
  multi-tenant overlays, schema workspace, column lineage).

See [docs/ai/20_CRATE_MAP.md](docs/ai/20_CRATE_MAP.md) for the full crate map
and layering rules.

## Documentation

- **User Guide** — authoring and running pipelines:
  [docs/user/src/README.md](docs/user/src/README.md), starting with
  [Your First Pipeline](docs/user/src/getting-started/first-pipeline.md) and
  [Installation](docs/user/src/getting-started/installation.md).
- **Engine Internals** — the execution model, memory arbitration, and operator
  strategies: [docs/engine/src/README.md](docs/engine/src/README.md).
- **Non-goals** — what Clinker deliberately does not do:
  [docs/user/src/non-goals.md](docs/user/src/non-goals.md).
- **Contributing / architecture** — [AGENTS.md](AGENTS.md) and the AI
  onboarding docs under [docs/ai/](docs/ai/00_READ_THIS_FIRST.md).
- **Changelog** — [CHANGELOG.md](CHANGELOG.md).

Both books are mdBook projects (`docs/user`, `docs/engine`) and render with
`mdbook build`.

## License

Clinker is licensed under the MIT license.
