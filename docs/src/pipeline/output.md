# Output Nodes

Output nodes write processed records to files. They are the terminal nodes of a pipeline -- every pipeline path must end at an output (or records are silently dropped).

## Basic structure

```yaml
- type: output
  name: result
  input: transform_node
  config:
    name: output_stage
    type: csv
    path: "./output/result.csv"
```

The `type:` field selects the output format: `csv`, `json`, `xml`, or `fixed_width`.

## Field control

By default, output nodes write only the fields explicitly emitted by upstream transforms. Several options control which fields appear and how they are named.

### Include unmapped fields

```yaml
    include_unmapped: true    # Default: false
```

When `true`, fields that were not explicitly emitted by transforms but exist on the record are included in the output. Useful for pass-through pipelines where you want all original fields plus a few computed ones.

### Include correlation-key shadow columns

```yaml
    include_correlation_keys: true    # Default: false
```

When the pipeline declares `error_handling.correlation_key: <field>`, the engine adds shadow columns named `$ck.<field>` to the schema. These shadow columns preserve correlation-group identity through transforms that may rewrite the user-declared field. They are an internal engine namespace and are stripped from output by default.

Set `include_correlation_keys: true` to surface the shadow columns in the writer output -- typically for debugging correlation-group routing or auditing DLQ behavior. See [Correlation Keys](correlation-keys.md) for the full lifecycle.

### Field mapping

Rename fields at output time without changing upstream CXL:

```yaml
    mapping:
      "Customer Name": "full_name"
      "Order Total": "amount"
```

Keys are output column names; values are the source field names from upstream.

### Excluding fields

Remove specific fields from output:

```yaml
    exclude: [internal_id, _debug_flag, temp_calc]
```

### Header control (CSV)

```yaml
    include_header: true      # Default: true
```

Set to `false` to omit the CSV header row.

### Null handling

```yaml
    preserve_nulls: false     # Default: false
```

When `false`, null values are written as empty strings. When `true`, nulls are preserved in the output format's native null representation (e.g., `null` in JSON).

## Metadata inclusion

Control whether per-record `$meta.*` metadata fields appear in output:

```yaml
    include_metadata: all       # Include all metadata fields
```

```yaml
    include_metadata: none      # Default -- strip all metadata
```

```yaml
    include_metadata:
      - source_file             # Include only listed metadata keys
      - source_row
```

Metadata fields are prefixed with `meta.` in the output.

## Output format options

### CSV

```yaml
- type: output
  name: csv_out
  input: processed
  config:
    name: csv_out
    type: csv
    path: "./output/result.csv"
    options:
      delimiter: "|"
```

### JSON

```yaml
- type: output
  name: json_out
  input: processed
  config:
    name: json_out
    type: json
    path: "./output/result.json"
    options:
      format: ndjson           # array | ndjson
      pretty: true             # Pretty-print JSON
```

- `array` (default) -- writes a single JSON array containing all records.
- `ndjson` -- writes one JSON object per line.

### XML

```yaml
- type: output
  name: xml_out
  input: processed
  config:
    name: xml_out
    type: xml
    path: "./output/result.xml"
    options:
      root_element: "data"
      record_element: "row"
```

### Fixed-width

```yaml
- type: output
  name: fw_out
  input: processed
  config:
    name: fw_out
    type: fixed_width
    path: "./output/result.dat"
    schema: "./schemas/output.schema.yaml"
    options:
      line_separator: crlf
```

Fixed-width output requires a format schema defining field positions and widths.

## Sort order

Sort records before writing:

```yaml
    sort_order:
      - { field: "name", order: asc }
      - { field: "amount", order: desc, null_order: last }
```

| Sort option | Values | Default |
|-------------|--------|---------|
| `order` | `asc`, `desc` | `asc` |
| `null_order` | `first`, `last`, `drop` | `last` |

- `first` -- nulls sort before all non-null values.
- `last` -- nulls sort after all non-null values.
- `drop` -- records with null sort keys are excluded from output.

Shorthand: a bare string defaults to ascending with nulls last:

```yaml
    sort_order:
      - "name"
      - { field: "amount", order: desc }
```

## File splitting

Split output into multiple files based on record count, byte size, or group boundaries:

```yaml
- type: output
  name: split_output
  input: processed
  config:
    name: split_output
    type: csv
    path: "./output/result.csv"
    split:
      max_records: 10000
      max_bytes: 10485760           # 10 MB
      group_key: "department"       # Never split mid-group
      naming: "{stem}_{seq:04}.{ext}"
      repeat_header: true           # Repeat CSV header in each file
      oversize_group: warn          # warn | error | allow
```

### Split configuration fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `max_records` | No | -- | Soft record count limit per file |
| `max_bytes` | No | -- | Soft byte size limit per file |
| `group_key` | No | -- | Field name -- never split within a group sharing this key value |
| `naming` | No | `"{stem}_{seq:04}.{ext}"` | File naming pattern. `{stem}` is the base name, `{seq:04}` is a zero-padded sequence number, `{ext}` is the file extension |
| `repeat_header` | No | `true` | Repeat CSV header row in each split file |
| `oversize_group` | No | `warn` | What to do when a single key group exceeds file limits |

At least one of `max_records` or `max_bytes` should be specified for splitting to have any effect.

### Oversize group policies

- `warn` (default) -- log a warning and allow the oversized file.
- `error` -- stop the pipeline.
- `allow` -- silently allow the oversized file.

When `group_key` is set, the split point is the first group boundary after the threshold is reached (greedy). Without `group_key`, files are split at the exact limit.

## Complete example

```yaml
- type: output
  name: department_reports
  input: enriched_employees
  config:
    name: department_reports
    type: csv
    path: "./output/employees.csv"
    mapping:
      "Employee ID": "employee_id"
      "Full Name": "display_name"
      "Department": "department"
      "Annual Salary": "salary"
    exclude: [internal_flags]
    include_header: true
    sort_order:
      - { field: "department", order: asc }
      - { field: "display_name", order: asc }
    split:
      max_records: 5000
      group_key: "department"
      naming: "employees_{seq:03}.csv"
      repeat_header: true
```
