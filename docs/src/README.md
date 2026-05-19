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
at a time without accumulating per-record state. The DAG executor does
materialize intermediate buffers between non-fused stages, so memory footprint
scales with the largest intermediate stage's output rather than total input
size; fused streaming paths (Source → Transform → Output without fan-out)
avoid intermediate materialization entirely. Blocking operators (Aggregate,
sort, grace-hash Combine) accumulate state inside the configured RSS budget
and **spill to disk** when soft and hard memory thresholds trip, rather than
OOM-killing the process.

If you have used Flink, Kafka Streams, or Beam in unbounded mode: Clinker is
not that. There are no watermarks against wall-clock time, no infinite-source
semantics, no exactly-once delivery across restarts. The closest prior art is
Pentaho Kettle / Apache Hop, Embulk, Singer, Benthos in batch mode, and Vector
running file-to-file -- finite ETL jobs with per-record evaluation and a hard
memory ceiling.

## Why Clinker?

**Single binary, zero dependencies.** Download it, run it. No JVM, no Python,
no package manager. Works on any Linux server out of the box.

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
