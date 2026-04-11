# Your First Pipeline

This walkthrough builds a pipeline from scratch, runs it, and explores the
tools Clinker provides for validating and understanding pipelines before they
touch real data.

## 1. Create sample data

Save the following as `employees.csv`:

```csv
id,name,department,salary
1,Alice Chen,Engineering,95000
2,Bob Martinez,Marketing,62000
3,Carol Johnson,Engineering,88000
4,Dave Williams,Sales,71000
```

## 2. Write the pipeline

Save the following as `my_first_pipeline.yaml`:

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

This pipeline has three nodes:

1. **`employees`** (source) -- reads the CSV file and declares the schema.
2. **`classify`** (transform) -- passes all fields through and adds a `level`
   field based on salary.
3. **`report`** (output) -- writes the result to a new CSV file.

The `input:` field on each consumer node wires the DAG together. Data flows
from `employees` through `classify` to `report`.

## 3. Validate before running

Before processing any data, check that the pipeline is well-formed:

```bash
clinker run my_first_pipeline.yaml --dry-run
```

Dry-run parses the YAML, resolves the DAG, and type-checks all CXL expressions
against the declared schemas. If there are errors -- a typo in a field name, a
type mismatch, a missing `input:` reference -- Clinker reports them with
source-location diagnostics and stops. No data is read.

## 4. Preview records

To see what the output will look like without writing files, preview a few
records:

```bash
clinker run my_first_pipeline.yaml --dry-run -n 2
```

This reads the first 2 records from the source, runs them through the pipeline,
and prints the results to the terminal. Useful for sanity-checking
transformations before committing to a full run.

## 5. Understand the execution plan

To see how Clinker will execute the pipeline:

```bash
clinker run my_first_pipeline.yaml --explain
```

The explain plan shows the DAG topology, the order nodes will execute, per-node
parallelism strategy, and schema propagation through the pipeline. This is
valuable for understanding complex pipelines with routes, merges, and
aggregations.

## 6. Run it

```bash
clinker run my_first_pipeline.yaml
```

Clinker reads `employees.csv`, applies the transform, and writes
`salary_report.csv`. The output:

```csv
id,name,department,salary,level
1,Alice Chen,Engineering,95000,senior
2,Bob Martinez,Marketing,62000,junior
3,Carol Johnson,Engineering,88000,junior
4,Dave Williams,Sales,71000,junior
```

Alice's salary of 95,000 meets the threshold, so she is classified as
`senior`. Everyone else is `junior`.

## What just happened

The pipeline executed as a streaming process:

1. The source node read `employees.csv` one record at a time.
2. Each record flowed through the `classify` transform, which evaluated the CXL
   block to produce the output fields.
3. The output node wrote each transformed record to `salary_report.csv`.

At no point was the entire dataset loaded into memory. This is how Clinker
processes files of any size under its memory ceiling.

## Next steps

- [Key Concepts](concepts.md) -- understand the building blocks of Clinker
  pipelines
- [Pipeline YAML Structure](../pipeline/structure.md) -- full reference for
  pipeline configuration
- [CXL Overview](../cxl/overview.md) -- learn the expression language in depth
