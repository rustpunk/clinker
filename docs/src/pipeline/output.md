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

### Include widened (auto_widen) fields

```yaml
    include_widened: true    # Default: false
```

When `true`, fields the source's `on_unmapped: auto_widen` policy absorbed into the per-record `$widened` sidecar map are expanded back to top-level columns at the sink. With `include_widened: false` (the default), the sidecar slot is stripped and only user-declared / explicitly-emitted columns reach the writer.

The flag composes independently with `include_correlation_keys: true` — see below. See [Auto-Widen & Schema Drift → Output controls](auto-widen.md#output-controls) for the full specification, cross-format flow examples, and the writer-rejection contract for `Value::Map` payloads.

### Include correlation-key shadow columns

```yaml
    include_correlation_keys: true    # Default: false
```

When the pipeline declares `error_handling.correlation_key: <field>`, the engine adds shadow columns named `$ck.<field>` to the schema. These shadow columns preserve correlation-group identity through transforms that may rewrite the user-declared field. They are an internal engine namespace and are stripped from output by default.

Set `include_correlation_keys: true` to surface the shadow columns in the writer output -- typically for debugging correlation-group routing or auditing DLQ behavior. See [Correlation Keys](correlation-keys.md) for the full lifecycle.

`include_correlation_keys` does **not** surface the `$widened` sidecar — `include_widened` is the separate flag for that. The two are independent: each, both, or neither can be set.

### Writer rejection of `Value::Map` payloads

CSV, XML, and fixed-width writers refuse records carrying a `Value::Map` payload at any column slot, raising `FormatError::UnserializableMapValue { format, column }`. JSON serializes `Value::Map` natively as a nested object.

The typical cause is a `$widened` sidecar reaching the writer because the Output node forgot `include_widened: true`. See [Auto-Widen & Schema Drift → Writer rejection](auto-widen.md#writer-rejection-of-valuemap-payloads) for the rejection contract and remediation routes.

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

## Streaming writes under fused `Merge.interleave`

When a single Output sits directly downstream of a `Merge` whose mode is `interleave` and whose every direct predecessor is a `Source`, the executor takes a streaming path: a bounded `tokio::sync::mpsc::channel` connects the Merge arm to the writer task, and `Writer::write_record` fires per record as Merge emits, concurrent with Merge production.

The buffered alternative — which still runs for every other Output topology — waits until the Merge arm has accumulated every record before invoking the writer. With a slow upstream Source that defeats the live back-pressure the `Merge.interleave` fusion provides at the Source-channel layer: each record sits in `node_buffers[merge]` until the slow Source finishes.

### Topology

```yaml
- type: source
  name: src_a
  config: { type: csv, path: a.csv, schema: ... }
- type: source
  name: src_b
  config: { type: csv, path: b.csv, schema: ... }
- type: merge
  name: merged
  inputs: [src_a, src_b]
  config:
    mode: interleave        # required
- type: output
  name: out
  input: merged
  config:
    name: out
    type: csv
    path: out.csv
```

The streaming path is selected automatically — there is no opt-in setting. Pipelines that don't match the topology keep the buffered path.

### Eligibility

Every condition must hold for the streaming path to engage; if any fails, the buffered path runs:

- The Output has exactly one incoming edge, and that predecessor is a `Merge` with `mode: interleave`.
- Every direct predecessor of that Merge is a `Source` (same predicate the fused `Merge.interleave` arm uses for its live `tokio::select!`).
- The Merge has no other downstream consumer besides this one Output (no fan-out).
- The Output is not in the init-phase ancestor closure.
- The OutputConfig has no `split:` block — splitting writers manage their own file rotation lifecycle.
- The writer is registered in the single-file writer registry (not `fan_out_per_source_file`).
- No `Source` in the pipeline declares a correlation key — the correlation-buffered output path defers writes to `CorrelationCommit` and is incompatible with per-record write.

### Back-pressure flow

Under the streaming path, back-pressure flows end-to-end:

```
writer slow → mpsc::Sender::send().await yields
             → Merge arm yields
             → Source mpsc::Receiver fills
             → Source ingest task blocks on send
```

The bounded handoff channel between Merge and Output (256 slots) and the existing per-Source ingest channels (issue #67) form a single pace-bound chain from the underlying `Write` sink back to the source reader. A slow file system, a saturated network sink, or a deliberately-paced writer no longer accumulates records in pipeline-internal `Vec`s; the upstream readers slow down to match.

### Counter semantics

Counter behavior under the streaming path matches the buffered Output arm exactly: `records_written` increments once per `Writer::write_record` call, `ok_count` counts distinct source `row_num`s reaching the Output, and `dlq_count` is unaffected (DLQ entries originate upstream). Stage metrics (`SchemaScan`, `Write`, `Projection`) accumulate into the same fields the buffered path uses; the dispatcher folds the streaming task's per-task accounting back into the run-wide totals at end of DAG.

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
