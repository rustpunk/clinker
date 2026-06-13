# File Splitting

This recipe demonstrates splitting large output files into smaller chunks, optionally keeping related records together.

## Basic record-count splitting

Split output into files of at most 5,000 records each:

```yaml
pipeline:
  name: monthly_report

nodes:
  - type: source
    name: transactions
    config:
      name: transactions
      type: csv
      path: "./data/transactions.csv"
      schema:
        - { name: id, type: int }
        - { name: date, type: string }
        - { name: department, type: string }
        - { name: amount, type: float }
        - { name: description, type: string }

  - type: output
    name: split_output
    input: transactions
    config:
      name: monthly_report
      type: csv
      path: "./output/report.csv"
      split:
        max_records: 5000
        naming: "{stem}_{seq:04}.{ext}"
        repeat_header: true
```

### Output files

```
output/report_0001.csv  (5000 records + header)
output/report_0002.csv  (5000 records + header)
output/report_0003.csv  (remaining records + header)
```

### Naming pattern variables

| Variable | Description | Example |
|----------|-------------|---------|
| `{stem}` | Base filename without extension | `report` |
| `{ext}` | File extension | `csv` |
| `{seq:04}` | Zero-padded sequence number (width 4) | `0001` |

The `path` field provides the template: `./output/report.csv` means stem is `report` and ext is `csv`.

### Header behavior

When `repeat_header: true`, each output file includes the CSV header row. This is the recommended setting -- each file is self-contained and can be processed independently.

## Grouped splitting

Keep all records with the same group key value in the same file:

```yaml
      split:
        max_records: 5000
        group_key: "department"
        naming: "{stem}_{seq:04}.{ext}"
        repeat_header: true
        oversize_group: warn
```

With `group_key: "department"`, the splitter ensures that all records for a given department land in the same output file. A new file starts only at a group boundary (when the department value changes), even if the current file has not reached `max_records` yet.

### Oversize group policy

If a single group contains more records than `max_records`, the `oversize_group` setting controls behavior:

| Policy | Behavior |
|--------|----------|
| `warn` (default) | Log a warning and write all records for the group into one file, exceeding the limit |
| `error` | Stop the pipeline with an error |
| `allow` | Silently allow the oversized file |

For example, if `max_records` is 5,000 but the Engineering department has 7,000 records, the `warn` policy produces a file with 7,000 records and logs a warning.

## Byte-based splitting

Split by file size instead of record count:

```yaml
      split:
        max_bytes: 10485760  # 10 MB per file
        naming: "{stem}_{seq:04}.{ext}"
        repeat_header: true
```

The splitter estimates the current file size and starts a new file when the limit is approached. The actual file size may slightly exceed the limit because the current record is always completed before splitting.

## Combined limits

Use both `max_records` and `max_bytes` together -- whichever limit is reached first triggers a new file:

```yaml
      split:
        max_records: 10000
        max_bytes: 5242880   # 5 MB
        naming: "{stem}_{seq:04}.{ext}"
        repeat_header: true
```

This is useful when record sizes vary widely. Short records might produce a tiny file at 10,000 records, while long records might hit the byte limit well before 10,000.

## Full pipeline example

A complete pipeline that reads a large transaction file, filters it, and splits the output:

```yaml
pipeline:
  name: split_transactions

nodes:
  - type: source
    name: transactions
    config:
      name: transactions
      type: csv
      path: "./data/all_transactions.csv"
      schema:
        - { name: id, type: int }
        - { name: date, type: string }
        - { name: department, type: string }
        - { name: category, type: string }
        - { name: amount, type: float }

  - type: transform
    name: current_year
    input: transactions
    config:
      cxl: |
        filter date.starts_with("2026")

  - type: output
    name: chunked
    input: current_year
    config:
      name: transactions_2026
      type: csv
      path: "./output/transactions_2026.csv"
      split:
        max_records: 5000
        group_key: "department"
        naming: "{stem}_{seq:04}.{ext}"
        repeat_header: true
        oversize_group: warn
```

```bash
clinker run split_transactions.yaml --force
```

## Practical considerations

- **Downstream consumers.** Splitting is useful when the receiving system has file size limits (e.g., an upload API that accepts files up to 10 MB) or when parallel processing of chunks is desired.

- **Record ordering.** Records within each output file maintain their original order from the pipeline. Across files, the sequence number (`{seq}`) indicates the order.

- **Group key sorting.** For `group_key` to work correctly, the input should ideally be sorted by the group key. If the input is not sorted, records for the same group may appear in multiple files. Pre-sort with a transform if needed, or accept the split-group behavior.

- **Overwrite behavior.** Use `--force` when re-running a pipeline with splitting enabled. Without it, the pipeline aborts if any of the output chunk files already exist.
