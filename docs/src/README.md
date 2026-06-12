# Clinker

Clinker is a pure-Rust, bounded-memory **batch DAG executor** for CSV, JSON,
XML, and fixed-width data. It reads finite inputs, drives them through a
directed acyclic graph of transformation nodes one record at a time, and exits
when the inputs are drained. It ships as a single static binary with no
interpreter, no runtime, and no install dependencies.

Pipelines are declared in YAML. Data transformation logic is written in CXL, a
custom expression language purpose-built for ETL. Together they replace legacy
tools like Informatica, SSIS, Talend, and NiFi with something deterministic,
lightweight, and easy to reason about.

## What Clinker is, plainly

**A finite batch executor with per-record streaming evaluation, not a
long-running stream processor.** A pipeline run is a job: Sources read until
EOF, the DAG drains, the process exits. Within a run, stateless operators
(Transform, Route, most Combine probe-side work, Output) evaluate records one
at a time without accumulating per-record state. Every stage is charged
against the configured RSS budget. Fused Source → Transform → Output paths
run streaming with no per-stage materialization; non-fused boundaries
(Route fan-out, Merge fan-in, Composition bodies, diamond DAGs) materialize
records into per-stage buffers that charge against the same envelope. The
engine spills buffers to disk at 80% of the limit and fails fast with
`E310 MemoryBudgetExceeded` at the hard limit, naming the offending
producer. Blocking operators (Aggregate, sort, grace-hash Combine)
accumulate state inside that same budget and **spill to disk** when soft
and hard memory thresholds trip, rather than OOM-killing the process.

If you have used Flink, Kafka Streams, or Beam in unbounded mode: Clinker is
not that. There are no watermarks against wall-clock time, no infinite-source
semantics, no exactly-once delivery across restarts. The closest prior art is
Pentaho Kettle / Apache Hop, Embulk, Singer, Benthos in batch mode, and Vector
running file-to-file -- finite ETL jobs with per-record evaluation and a hard
memory ceiling.

**Three pillars of what Clinker is:**

1. **Finite inputs.** Files (CSV, JSON, XML, fixed-width) are the canonical
   shape. Finite-cursor network sources (paginated REST APIs, SQL `SELECT`
   cursors) fit the same model -- they exhaust their cursor and EOF.
   Unbounded sources (Kafka topics, Kinesis streams, Server-Sent Events,
   webhooks, `tail -f`-style file followers) are out of scope and will
   remain so.
2. **Finite jobs.** A pipeline run begins when you invoke `clinker run`,
   drains the DAG, and exits with a status code. No long-running daemon,
   no service surface, no infinite event loop.
3. **Single process.** One `clinker` binary invocation is one operating-
   system process. Parallelism happens *inside* the process via threads
   (`std::thread`, Rayon). Clinker does not spawn worker processes, does
   not coordinate a cluster, and does not shuffle data between machines.
   Scale by giving the host more cores, more RAM, and more disk -- the
   DuckDB / Polars / Kettle model. If a single host genuinely can't fit
   the work, partition the input by file or by key and run multiple
   `clinker` invocations from a shell script; that's a five-line bash
   script, not an architectural addition.

## Why Clinker?

**Single binary, zero dependencies.** Download it, run it. No JVM, no Python,
no package manager. Runs on Linux, macOS, and Windows out of the box — CI
builds and tests on all three, and the spill, staging, and RSS-sampling
layers have platform-specific paths so behavior is consistent across them.

**Good neighbor on busy servers.** Clinker enforces a strict memory ceiling
(default 512 MB) so it can run alongside JVM applications, databases, and other
services without competing for RAM. Aggregation spills to disk when memory
pressure rises.

**Reproducible output.** Given the same input and pipeline, Clinker produces
byte-identical output across runs. No nondeterminism from thread scheduling,
hash randomization, or floating-point reordering.

**Operability-first design.** Per-stage metrics, dead-letter queues for error
records, explain plans for understanding execution, and structured exit codes
for scripting. Built for production from day one.

**Two binaries:**

| Binary    | Purpose                                                |
|-----------|--------------------------------------------------------|
| `clinker` | Run pipelines against real data                        |
| `cxl`     | Check, evaluate, and format CXL expressions interactively |

## A taste of Clinker

Here is a complete pipeline that reads a customer CSV, filters to active
customers, classifies them into tiers, and writes the result:

```yaml
pipeline:
  name: customer_etl

nodes:
  - type: source
    name: customers
    config:
      name: customers
      type: csv
      path: "./data/customers.csv"
      schema:
        - { name: customer_id, type: int }
        - { name: first_name, type: string }
        - { name: last_name, type: string }
        - { name: status, type: string }
        - { name: lifetime_value, type: float }

  - type: transform
    name: enrich
    input: customers
    config:
      cxl: |
        filter status == "active"
        emit customer_id = customer_id
        emit full_name = first_name + " " + last_name
        emit tier = if lifetime_value >= 10000 then "gold" else "standard"

  - type: output
    name: result
    input: enrich
    config:
      name: enriched
      type: csv
      path: "./output/enriched_customers.csv"
```

Run it:

```bash
clinker run customer_etl.yaml
```

That is the entire workflow. No project scaffolding, no configuration files, no
compile step. One YAML file, one command.

## Next steps

- [Installation](getting-started/installation.md) -- download the binary and verify it works
- [Your First Pipeline](getting-started/first-pipeline.md) -- build and run a pipeline step by step
- [Key Concepts](getting-started/concepts.md) -- understand the mental model behind Clinker pipelines
