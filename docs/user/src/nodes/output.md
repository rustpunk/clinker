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

The `type:` field selects the output format: `csv`, `json`, `xml`, `fixed_width`, `edifact`, `x12`, `hl7`, or `swift`. The `edifact`, `x12`, and `swift` writers reconstruct one interchange/message envelope around emitted records; the `hl7` writer re-emits HL7 v2 segments and optionally wraps them in batch/file envelopes. See [EDIFACT Format](../formats/edifact.md), [X12 Format](../formats/x12.md), [HL7 v2 Format](../formats/hl7.md), and [SWIFT MT Format](../formats/swift.md).

Structured single-writer outputs (`edifact`, `x12`, `hl7`, and `swift`) accept one concrete document grain per output file. A multi-file source or multi-input merge feeding one of these outputs is rejected instead of being silently written as one merged envelope. To write multiple structured documents, consolidate them deliberately with an Envelope node first or route each document to a separate output path.

## Field control

Output nodes can either pass every upstream field through to the writer or restrict output to the fields the upstream transform explicitly emitted. Several options control which fields appear and how they are named.

### Unmapped input field passthrough

```yaml
    include_unmapped: false    # Default: true
```

When `true` (the default), every field on an input record that the upstream transform did not explicitly emit still passes through to the output unchanged. This includes fields the source's `on_unmapped: auto_widen` policy absorbed into the per-record `$widened` sidecar map -- their contents expand back to top-level columns at the sink.

When `false`, only fields named by an `emit` statement in the upstream transform appear in the output. The `$widened` sidecar slot is stripped and undeclared input fields are dropped.

#### Migration notice

The default flipped from `false` to `true` in a recent release (see [issue #90](https://github.com/rustpunk/clinker/issues/90)). Pipelines that relied on the previous behavior -- where output records contained only the fields explicitly emitted upstream -- must now set `include_unmapped: false` explicitly to restore that shape.

The flag composes independently with `include_correlation_keys: true` -- see below. See [Auto-Widen & Schema Drift -> Output controls](../formats/auto-widen.md#output-controls) for the full specification and cross-format flow examples.

#### Worked example

Suppose the upstream source emits records with `order_id`, `customer_id`, `amount`, and `region`, and a transform that emits only one derived field:

```yaml
- type: transform
  name: classify
  input: orders
  config:
    cxl: |
      emit amount_bucket = if amount >= 1000 then "high" else "low"
```

With `include_unmapped: true` (the default), each output record carries `order_id`, `customer_id`, `amount`, `region`, and `amount_bucket`. With `include_unmapped: false`, each output record carries only `amount_bucket`. The transform's CXL is unchanged in both cases -- the Output node decides the field set.

### Include correlation-key shadow columns

```yaml
    include_correlation_keys: true    # Default: false
```

When a source declares a `correlation_key:`, the engine tracks correlation-group identity on hidden columns that are stripped from output by default. Set `include_correlation_keys: true` to surface them in the writer output — typically for debugging correlation-group routing or auditing DLQ behavior. See [Correlation Keys](../pipelines/correlation-keys.md).

`include_correlation_keys` does **not** surface auto-widened columns -- `include_unmapped` is the separate flag for that. The two are independent: each, both, or neither can be set.

### Nested columns and non-JSON writers

The CSV, XML, fixed-width, EDIFACT, X12, and HL7 writers can only write flat scalar columns; a nested value reaching one of them fails with an `UnserializableMapValue` error. JSON writes nested values natively. The usual cause is an auto-widened column reaching a non-JSON writer with `include_unmapped: false` — see [Auto-Widen & Schema Drift](../formats/auto-widen.md#writer-errors-on-unexpanded-columns) for the fix.

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

JSON numbers cannot represent non-finite floats; a record carrying `NaN` or
an infinity fails the write with a JSON error instead of silently becoming
`null`. See [JSON Format](../formats/json.md#non-finite-floats).

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

Fixed-width output requires a format schema defining field positions and
widths. Fields land at their declared byte ranges with gaps space-filled —
see [Fixed-Width Format](../formats/fixed-width.md#writing-fixed-width-output)
for the layout semantics.

### EDIFACT

```yaml
- type: output
  name: edi_out
  input: messages
  config:
    name: edi_out
    type: edifact
    path: "./out/result.edi"
    options:
      interchange: ["UNOA:1", "SENDER", "RECEIVER", "240101:1200", "REF1"]
      message_type: "ORDERS:D:96A:UN"
      write_una: false
      segment_newline: true
```

The EDIFACT writer reconstructs the interchange envelope around emitted
records, recomputing the `UNT`/`UNZ` control counts and echoing the
control references, and release-escapes any element data that carries a
service character. The `UNB` header comes from `interchange` (literal
elements) or `interchange_from_doc` (echoed from a `$doc` section). An
interchange is a single envelope, so an `edifact` output cannot be
combined with a `split:` block — the combination is rejected at
config-validation time (`E323`). See [EDIFACT Format](../formats/edifact.md) for the
full option reference, the record schema, and the round-trip semantics.

### HL7 v2

```yaml
- type: output
  name: hl7_out
  input: messages
  config:
    name: hl7_out
    type: hl7
    path: "./out/result.hl7"
    options:
      file_header: ["^~\\&", "LAB", "HOSP", "EHR", "HOSP", "20240102", "FILE7"]
      batch_header: ["^~\\&", "LAB", "HOSP", "EHR", "HOSP", "20240102", "BATCH3"]
      segment_newline: true
```

The HL7 writer re-emits the `MSH` and body segments from the record
stream, escaping any field data that carries a delimiter character (`|` →
`\F\`, `^` → `\S\`, and so on). When a `file_header` (or
`file_header_from_doc`) or `batch_header` is configured the writer wraps the
messages in an `FHS..FTS` file or `BHS..BTS` batch and recomputes the
closing `BTS`/`FTS` counts. A batch/file envelope is a single structure, so
an `hl7` output cannot be combined with a `split:` block — the combination
is rejected at config-validation time (`E339`). See
[HL7 v2 Format](../formats/hl7.md) for the full option reference, the record schema,
the MSH off-by-one, and the round-trip semantics.

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

## Streaming writes after an interleave Merge

When a single Output sits directly after a `Merge` with `mode: interleave` whose inputs are all Sources, records are written to disk as they arrive rather than being buffered until the merge finishes. This keeps memory flat and lets a slow writer naturally pace the upstream readers.

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

This is automatic — there is no setting to enable it. It applies only to this exact shape: one interleave Merge of Sources feeding one non-splitting Output, in a pipeline without correlation keys. Any other topology buffers as usual, with identical output either way.

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
