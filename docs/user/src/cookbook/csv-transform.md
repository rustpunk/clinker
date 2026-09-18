# CSV-to-CSV Transform

This recipe reads employee data from a CSV file, computes salary tiers using CXL expressions, and writes the enriched result to a new CSV file.

## Input data

`employees.csv`:

```csv
id,name,department,salary
1,Alice Chen,Engineering,95000
2,Bob Martinez,Marketing,62000
3,Carol Johnson,Engineering,88000
4,Dave Williams,Sales,71000
5,Eva Brown,Marketing,58000
6,Frank Lee,Engineering,102000
```

## Pipeline

`salary_tiers.yaml`:

```yaml
pipeline:
  name: salary_tiers

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
        emit salary_band = match {
          salary >= 100000 => "100k+",
          salary >= 90000 => "90-100k",
          salary >= 70000 => "70-90k",
          _ => "under 70k"
        }

  - type: sink
    name: report
    input: classify
    config:
      name: report
      type: csv
      path: "./output/salary_report.csv"

error_handling:
  strategy: fail_fast
```

## Run it

```bash
# Validate first
clinker run salary_tiers.yaml --dry-run

# Preview output
clinker run salary_tiers.yaml --dry-run -n 3

# Full run
mkdir -p output
clinker run salary_tiers.yaml
```

At revision `3b343a4e`, bounded preview of this example can fail with an internal
node-buffer cleanup error; the ordinary run produces the output below. See
[the preview limitation](../ops/validation.md#bounded-execution-preview).

## Expected output

`output/salary_report.csv`:

```csv
id,name,department,salary,level,salary_band
1,Alice Chen,Engineering,95000,senior,90-100k
2,Bob Martinez,Marketing,62000,junior,under 70k
3,Carol Johnson,Engineering,88000,junior,70-90k
4,Dave Williams,Sales,71000,junior,70-90k
5,Eva Brown,Marketing,58000,junior,under 70k
6,Frank Lee,Engineering,102000,senior,100k+
```

## Key points

**Schema declaration.** The source node declares the schema explicitly with typed columns. This enables compile-time type checking of CXL expressions -- if you write `salary + name`, the type checker catches the error before any data is read.

**Emit statements.** A Transform adds or replaces the fields it emits. Other input
columns can pass through. To select the public output columns deliberately, use
the Sink's `mapping`/`exclude` settings and `include_unmapped: false`; an emit list
alone is not a data-leakage boundary.

**Match expressions.** The `match` block evaluates conditions top to bottom and returns the value of the first matching arm. The `_` wildcard is the default case and must appear last.

**Error handling.** The `fail_fast` strategy aborts the pipeline on the first record error. For production pipelines processing dirty data, consider `continue` instead, which routes the failing record to the dead-letter queue and keeps going -- see [Error Handling & DLQ](../pipelines/error-handling.md).

## Variations

### Filtering records

Add a `filter` statement to exclude records:

```yaml
  - type: transform
    name: classify
    input: employees
    config:
      cxl: |
        filter salary >= 60000
        emit id = id
        emit name = name
        emit salary = salary
```

Records where `salary < 60000` are dropped silently -- they do not appear in the output or the DLQ.

### Computed columns with type conversion

```yaml
      cxl: |
        emit id = id
        emit name = name
        emit monthly_salary = (salary.to_float() / 12.0).round_to(2)
        emit salary_display = "$".concat(salary.to_string())
```

The `.to_float()` conversion is required because `salary` is declared as `int` and division by a float literal requires matching types.
