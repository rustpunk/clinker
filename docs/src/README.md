# Clinker

Clinker is a pure-Rust, memory-bounded CLI ETL engine for streaming
transformation of CSV, JSON, XML, and fixed-width data. It ships as a single
static binary with no interpreter, no runtime, and no install dependencies.

Pipelines are declared in YAML. Data transformation logic is written in CXL, a
custom expression language purpose-built for ETL. Together they replace legacy
tools like Informatica, SSIS, Talend, and NiFi with something deterministic,
lightweight, and easy to reason about.

## Why Clinker?

**Single binary, zero dependencies.** Download it, run it. No JVM, no Python,
no package manager. Works on any Linux server out of the box.

**Good neighbor on busy servers.** Clinker enforces a strict memory ceiling
(default 256 MB) so it can run alongside JVM applications, databases, and other
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
