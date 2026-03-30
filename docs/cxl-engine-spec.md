# CXL Streaming ETL Engine — Engineering Specification v2

## 1. Mission & Constraints

A memory-efficient, high-throughput CLI ETL tool written in pure Rust. Processes CSV, XML, and JSON files (≤100MB typical) and outputs to CSV, XML, or JSON. Runs on servers alongside 2–6 concurrent JVM processes each consuming ~2GB RAM.

### Hard Constraints

- **Pure Rust**: Zero C-compiled dependencies anywhere in the dependency tree. `unsafe` blocks in Rust are acceptable where unavoidable (allocators, syscall wrappers) but must be audited and minimized.
- **Memory ceiling**: Default 512MB RSS budget. Configurable via `--memory-limit`. Must never silently exceed the configured limit — spill to disk or halt with a diagnostic error.
- **No Apache Arrow**: No columnar memory model. Row-oriented streaming throughout.
- **Deterministic output**: Given the same inputs and config, output must be byte-identical across runs (stable sort, deterministic field ordering, no thread-ordering leaks into output).

### Target Workload Profile

- Input files: 1–5 per pipeline, ≤100MB each (XML and CSV dominant, occasional JSON)
- Nesting: XML/JSON may contain nested arrays 2–3 levels deep
- Record counts: 10K–2M records per file
- Transformation complexity: field mapping, type coercion, string manipulation, date arithmetic, conditional branching, cross-source lookups, window functions (lag/lead/aggregates)
- Concurrency environment: 2–6 JVM processes on the same host, each ~2GB. Clinker must be a good neighbor.

---

## 2. Crate Selection

Every crate must be pure Rust (no transitive C dependencies).

| Purpose | Crate | Notes |
|---|---|---|
| CLI | `clap` (derive) | Argument parsing |
| Serialization | `serde`, `serde_json` | Core data model serialization |
| YAML config | `serde-saphyr` | Pure Rust, 1000+ test suite, budget constraints for DoS protection |
| CSV I/O | `csv` | Streaming reader/writer, configurable delimiter/quoting |
| XML I/O | `quick-xml` | Pull-parser, streaming, namespace-aware |
| CXL parser | Hand-rolled Pratt parser | Top-down operator precedence with recursive-descent for statements and match/if expressions. Chosen over PEG combinators (`chumsky`, `pest`) for surgical 4-part error messages (what / where / why / how-to-fix) and panic-mode recovery. No external parser crate dependency in `cxl`. |
| Error reporting | `miette` | Contextual diagnostics with source spans (replaces thiserror+anyhow) |
| Async HTTP | `tokio`, `reqwest` (rustls) | Isolated to API intercept phase only. `reqwest` must use `rustls` feature, not `native-tls` (which links OpenSSL). |
| Date/time | `chrono` | Date parsing, arithmetic, formatting |
| Regex | `regex` | Pure Rust regex engine |
| Parallelism | `rayon` | Work-stealing thread pool for chunk-level parallelism |
| Channels | `crossbeam-channel` | Bounded MPMC channels for pipeline stage communication |
| Hashing | `ahash` | Fast, DoS-resistant hashing for HashMaps (pure Rust). NOT relied on for cross-platform determinism — output ordering uses Vec/IndexMap, never HashMap iteration. |
| Ordered map | `indexmap` | Insertion-order-preserving map for Record overflow fields. Emit-order output. |
| Compression | `lz4_flex` | Pure Rust LZ4 for spill file compression |
| Temp files | `tempfile` | Spill file management |
| Tracing | `tracing`, `tracing-subscriber` | Structured logging |
| UUID | `uuid` (v7) | DLQ row identifiers and `pipeline.execution_id`. UUID v7 is time-ordered (monotonically increasing, timestamp-prefixed) — DLQ rows sort chronologically and execution IDs sort by run time. |
| Glob | `glob` | Multi-file source patterns |
| Decimal | `rust_decimal` | Optional: fixed-point arithmetic for financial data |
| Signal handling | `ctrlc` | Pure Rust. Graceful SIGINT/SIGTERM handling |
| RSS (macOS) | `mach2` | Pure Rust FFI to Mach kernel API. `cfg(target_os = "macos")` only. |
| RSS (Windows) | `windows-sys` | Pure Rust FFI to Win32 API. `cfg(target_os = "windows")` only. |

### Crate-to-workspace mapping

| External crate | Used by workspace crate(s) |
|---|---|
| `serde`, `serde_json`, `chrono`, `ahash`, `indexmap` | `clinker-record`, `cxl`, `clinker-format`, `clinker-core` |
| `regex` | `cxl` |
| `csv`, `quick-xml` | `clinker-format` |
| `serde-saphyr`, `rayon`, `crossbeam-channel`, `lz4_flex`, `tempfile`, `glob`, `uuid`, `ctrlc`, `mach2`, `windows-sys` | `clinker-core` |
| `clap`, `tracing-subscriber` | `clinker`, `cxl-cli` |
| `miette`, `tracing` | All crates |
| `tokio`, `reqwest` | `clinker-core` (v2 only — API intercept) |

### Crates explicitly excluded

| Crate | Reason |
|---|---|
| `arrow`, `arrow-csv` | Columnar model removed |
| `rhai` | Replaced by CXL |
| `serde_yaml_bw` | Depends on `unsafe-libyaml` (C-translated) |
| `serde_yaml`, `serde_yml` | Archived / unsound |
| `mimalloc` | C allocator. Use Rust's default system allocator; benchmark first, then evaluate `dlmalloc-rs` if needed. |
| `sonic-rs` | Uses SIMD intrinsics via C; `serde_json` is sufficient for ≤100MB |
| `bumpalo` | Arena allocator was for Arrow batch lifetime management; not needed |
| `sqlx` | Deferred to v2 (database sources) |
| `chumsky` | PEG combinator; good error recovery but error message quality inferior to hand-rolled Pratt parser for a small DSL |
| `pest` | PEG; same tradeoff as chumsky |
| `winnow` | Considered as Pratt foundation; rejected in favour of fully hand-rolled for maximum error control |

---

## 3. Data Model

All types in this section live in the `clinker-record` crate — the shared foundation of the entire workspace. Both `cxl` and `clinker-format` depend on `clinker-record` directly, eliminating value type conversions at crate boundaries.

### Value

```rust
// In clinker-record::value
#[derive(Debug, Clone, Serialize)]
// PartialEq: custom impl using f64::to_bits() — NaN == NaN, -0.0 != 0.0
// Deserialize: custom impl trying DateTime→Date→String for JSON strings, i64→f64 for numbers
pub enum Value {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(Box<str>),
    Date(chrono::NaiveDate),
    DateTime(chrono::NaiveDateTime),
    Array(Vec<Value>),
}
```

Design notes:
- `Box<str>` not `Arc<str>` — saves 8 bytes per string (no refcount). Values are not shared across records in the normal pipeline flow. If profiling reveals clone pressure from window lookups on wide string fields, the upgrade path is a `Value::SharedString(Arc<str>)` variant — cheap clone, backward-compatible addition. Do not add it preemptively.
- `Date`/`DateTime` as first-class variants — eliminates runtime parsing in CXL date functions.
- `i64` for integers. Overflow produces a CXL runtime error with row context.
- No `Map` variant. Nested objects are flattened during input parsing via `array_paths`.
- Numeric variants (`Integer`, `Float`) and temporal variants (`Date`, `DateTime`) are stack-only — clone is a bitwise copy with no heap involvement. The only clone cost is for `String` (heap) and `Array` (heap + recursive). Window lookups on numeric and date fields are therefore free to clone.

### Schema

```rust
// In clinker-record::schema
pub struct Schema {
    columns: Vec<Box<str>>,
    index: HashMap<Box<str>, usize, ahash::RandomState>,
}
```

Shared across all records in a batch via `Arc<Schema>`. O(1) name→index lookup. Column names stored once.

### Record

```rust
// In clinker-record::record
pub struct Record {
    schema: Arc<Schema>,
    values: Vec<Value>,
    overflow: Option<Box<indexmap::IndexMap<Box<str>, Value>>>,  // emit-order preserved
}
```

Design notes:
- Schema-indexed positional storage. `get("field")` → schema index lookup → `values[idx]`.
- `overflow` holds fields added by CXL transforms not in the original schema. Uses `IndexMap` (not `HashMap`) to preserve emit statement order — overflow fields appear in output in the order they were emitted. `None` in the common case (no heap allocation). Overflow field names never collide with schema field names — `emit` targeting a known schema field updates `values[idx]`; `emit` targeting an unknown field writes to `overflow`. For output (Phase 3 projection), CXL-emitted values always take precedence over unchanged source values: if `emit Status = "active"` fires for a schema field, the emit value is used for output; unmodified schema fields pass through as-is. Output field ordering: schema fields first (in `Schema.columns` Vec order), then overflow fields (in emit order via IndexMap).
- No `context` field from the old design. Provenance metadata (`_source_file`, `_source_row`, `_source_batch`, `_ingestion_timestamp`) is carried as a `RecordProvenance` struct in the evaluator context, not as `Record` fields — see §3 Record Provenance.
- `Record` must be `Send + Sync` for rayon parallelism.

### MinimalRecord (Phase 1 only)

```rust
// In clinker-record::minimal
pub struct MinimalRecord {
    fields: Vec<Value>,  // Only the fields needed for indexing, positional
}
```

Used during the Skinny Pass. Contains only the fields specified in the "Indices to Build" list from the AST compiler. Typical size: 2–4 fields × ~40 bytes = 80–160 bytes per record (vs 500+ bytes for a full Record).

`MinimalRecord` carries no field names — it is intentionally thin. Field name→position mapping is owned by the `Arena` that wraps it (see §7.1). The evaluator never accesses a `MinimalRecord` directly; it goes through `Arena::resolver_at(idx)` which returns a view that implements `FieldResolver`.

**Position stability**: Field positions in `MinimalRecord` are fixed for the entire lifetime of a given `Arena`. Position `i` in `fields` always maps to the same field name throughout the Arena. The position mapping is the "Indices to Build" list from Phase E in index order (see §6.1). This guarantee is what makes pointer-based window lookups (`Arena::resolver_at(usize)`) safe across Phase 1.5 sort and Phase 2 reads.

### Arena (Phase 1 only)

```rust
// In clinker-core::pipeline::arena
pub struct Arena {
    pub schema: Arc<Schema>,          // Field names + positions for the indexed subset only
    pub records: Vec<MinimalRecord>,  // Flat record store; indexed by usize pointers
}
```

`Arena` is the pairing of a `Vec<MinimalRecord>` with the schema that gives those positional fields their names. It lives in `clinker-core` (not `clinker-record`) because it has no meaning without pipeline context.

`Arena::resolver_at(&self, idx: usize) -> ArenaRecordView<'_>` returns a zero-allocation stack view:

```rust
// In clinker-core::pipeline::arena (private)
struct ArenaRecordView<'a> {
    values: &'a [Value],
    schema: &'a Schema,
}
impl FieldResolver for ArenaRecordView<'_> {
    fn resolve(&self, name: &str) -> Option<Value> {
        self.schema.index(name).and_then(|i| self.values.get(i)).cloned()
    }
    // resolve_qualified: delegates to resolve() — MinimalRecords have no source prefix
    fn resolve_qualified(&self, _source: &str, field: &str) -> Option<Value> {
        self.resolve(field)
    }
}
```

### Record Provenance

Reserved metadata fields are available on every record throughout the pipeline. They are resolved by `FieldResolver` before any schema field lookup — they can never be shadowed by source data (field names beginning with `_source_` or `_pipeline_` are reserved; Phase A reports a parse error if a source schema or `emit` statement declares such a field).

**Provenance fields** (per-record, static after Phase 1):

| Field | Type | Description |
|---|---|---|
| `_source_file` | `String` | Path to the source file, relative to working directory |
| `_source_row` | `Integer` | 1-based row number within the source file (post-explosion: each exploded row gets its own row number) |
| `_source_count` | `Integer` | Total record count from this source file (known after Phase 1, from Arena size) |
| `_source_batch` | `String` | UUID v7 generated at pipeline start. Optionally overridden by `--batch-id` CLI flag. Identical for every record in the same run. |
| `_ingestion_timestamp` | `DateTime` | `NaiveDateTime` when the pipeline started. Identical for every record in the same run. |

**Pipeline counters** (updated at chunk boundaries during Phase 2):

| Field | Type | Description |
|---|---|---|
| `_pipeline_total_count` | `Integer` | Total records across all sources (static, sum of all Arena sizes) |
| `_pipeline_ok_count` | `Integer` | Records successfully processed so far |
| `_pipeline_dlq_count` | `Integer` | Records routed to DLQ so far |

**Chunk-boundary update model**: `_pipeline_ok_count` and `_pipeline_dlq_count` are updated between chunks, not per-record. All records within a chunk see the same counter values (as of the start of that chunk). This preserves deterministic output — no record's CXL evaluation depends on the parallel execution order of other records in the same chunk. `_pipeline_total_count` and `_source_count` are static (known after Phase 1).

**Storage model**: Provenance is not stored inside `Record` or `MinimalRecord`. It lives as two structs in the evaluator context:

```rust
// In clinker-record::provenance
pub struct RecordProvenance {
    pub source_file: Arc<str>,                      // Shared across all records from the same file
    pub source_row: u64,                            // Unique per record
    pub source_count: u64,                          // Total records from this source file
    pub source_batch: Arc<str>,                     // Shared across all records in the run
    pub ingestion_timestamp: chrono::NaiveDateTime, // Shared across all records in the run
}

// In clinker-core::pipeline::counters
pub struct PipelineCounters {
    pub total_count: u64,   // Static after Phase 1
    pub ok_count: u64,      // Updated at chunk boundaries
    pub dlq_count: u64,     // Updated at chunk boundaries
}
```

`source_file` and `source_batch` are `Arc<str>` — no per-record heap allocation. `source_row` is `u64` (8 bytes). `ingestion_timestamp` is `NaiveDateTime` (12 bytes, stack-only clone). `PipelineCounters` is 24 bytes, stack-only, snapshotted at each chunk boundary and shared as a read-only value across the chunk's `par_iter_mut`.

**CXL access**: All provenance and counter fields are usable anywhere a field reference is valid:

```
route if _source_file.contains("Q1")
emit Region = _source_file.capture("region_(\\w+)")
emit Batch  = _source_batch
emit error_rate = _pipeline_dlq_count / _pipeline_total_count
```

`_source_file` supports all standard String methods, including the path decomposition methods (see §5.5).

**Phase 1 population**: `RecordProvenance` is created once when the streaming reader opens a source file. `source_row` increments for each record emitted from that file (including explosion — each exploded row gets a unique `source_row`). `source_count` is set to the Arena's final record count for that source. `source_batch` and `ingestion_timestamp` are computed once at pipeline start and shared via `Arc`. `PipelineCounters.total_count` is the sum of all Arena sizes after Phase 1 completes.

**Output passthrough**: Setting `include_provenance: true` on an output appends the provenance fields (`_source_file`, `_source_row`, `_source_count`, `_source_batch`, `_ingestion_timestamp`) and pipeline counters (`_pipeline_total_count`, `_pipeline_ok_count`, `_pipeline_dlq_count`) as the last columns of every output record. These columns never appear in output via `include_unmapped` — only via `include_provenance`.

---

### GroupByKey (clinker-core internal)

```rust
// In clinker-core::pipeline::index — NOT in clinker-record
#[derive(Hash, Eq, PartialEq, Clone)]
pub(crate) enum GroupByKey {
    Null,
    Bool(bool),
    Integer(i64),
    Float(u64),          // f64::to_bits() — makes NaN detectable and the type hashable
    String(Box<str>),
    Date(chrono::NaiveDate),
    DateTime(chrono::NaiveDateTime),
}
```

`GroupByKey` is the key type for `SecondaryIndex` (see §7.1). It is **not** part of the public `clinker-record` data model. Conversion from `Value` to `GroupByKey` happens only at Secondary Index insertion time and applies two rules:

1. **Numeric normalization**: A `Value::Integer` in a `group_by` position where the field is inferred as `Numeric` is widened to `GroupByKey::Float` via `(n as f64).to_bits()`. This prevents split partitions when the same logical value appears as `"135"` in some rows and `"135.0"` in others. A field pinned to `integer` via `schema_overrides` keeps `GroupByKey::Integer`.
2. **NaN rejection**: If `f64::from_bits(bits).is_nan()`, halt with a hard pipeline error (exit code 3) — NaN in a group_by field is a data integrity problem, not a row-level error suitable for DLQ.

---

## 4. Configuration Schema (YAML)

```yaml
pipeline:
  name: <string>                    # Telemetry/logging namespace; available as pipeline.name in CXL
  memory_limit: <string>            # e.g., "512MB", "2GB". Overridden by --memory-limit CLI flag.
  vars:                             # Optional user-defined pipeline constants (see §5.8)
    <string>: <integer | float | boolean | string>
    # Each key becomes pipeline.VARNAME in CXL. Values are typed by their YAML representation:
    #   fiscal_year: 2026           → pipeline.fiscal_year  (Int)
    #   rate: 1.075                 → pipeline.rate         (Float)
    #   active: true                → pipeline.active       (Bool)
    #   region: "WEST"              → pipeline.region       (String)
    # String values that look like dates are NOT auto-parsed to Date — call .to_date() in CXL.
    # Names must not collide with built-ins: start_time, name, execution_id.
    # Evaluated once at config load time, before any data is read. Immutable at runtime.
  date_formats: [<string>]           # Priority-ordered list of date/datetime parse formats (chrono strftime).
                                    # Used by .to_date() / .to_datetime() when called WITHOUT an explicit fmt.
                                    # Tried in order; first successful parse wins.
                                    # Default: ["%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M:%S"]
                                    # Per-field schema `format:` overrides this chain entirely.
  date_locale: <us | eu>            # Hint for auto-detect heuristic. Default: us (month-first for MM/dd).
                                    # Only used when date_formats is not declared AND no field-level format.
                                    # us: ambiguous "01/02/2024" → January 2. eu: → February 1.
  rules_path: <string>              # Module search path for .cxl files (see §5.9). Default: ./rules/
  log_rules: <string>               # Path to external log rules YAML (see §5.10). Optional.
  sort_output:                      # Optional global sort before flushing to disk
    - field: <string>
      order: <ascending | descending>
      nulls: <first | last>         # Default: last
  concurrency:
    worker_threads: <integer | "auto">  # Rayon pool size. Default: min(num_cpus - 2, 4)
    chunk_size: <integer>               # Records per parallel chunk. Default: 1024
    source_threads: <integer | "auto">  # Concurrent source reads. Default: number of inputs

inputs:
  - name: <string>                  # Namespace alias for CXL (e.g., source_name.field)
    type: <csv | xml | json | fixed_width>
    path: <string>                  # Supports ${ENV_VAR} interpolation and glob patterns
    schema: <string | map>          # Schema file path OR inline schema map (see §4.1)
                                    # Required for fixed_width and multi-record inputs.
                                    # Optional for csv, json, xml — parser infers from data + CXL usage.
    schema_overrides:               # Strategic merge patches on top of external schema (see §4.1)
      - name: <string>             # Match field by name; merge specified keys
        <key>: <value>             # Override any field property (type, width, format, etc.)
        alias: <string>            # Rename field for output
        drop: <boolean>            # Remove field from schema
        record: <string>           # Scope override to specific record type (multi-record only)
    options:                        # Format-specific options
      # CSV options
      delimiter: <string>           # Default: ","
      quote_char: <string>          # Default: '"'
      has_header: <boolean>         # Default: true
      encoding: <utf-8 | latin1 | windows-1252>  # Default: utf-8
      # XML options
      record_path: <string>         # XPath-like path to record elements (e.g., "Orders/Order")
      attribute_prefix: <string>    # Prefix for attribute fields. Default: "@"
      namespace_handling: <strip | qualify>  # Default: strip
      # JSON options
      format: <array | ndjson | object>  # Default: auto-detect (see below)
      # Auto-detect algorithm: skip leading whitespace; if first byte is '[' → array;
      # if first byte is '{' → ndjson (each line is a JSON object, confirmed by reading line 1);
      # record_path specified → object (navigate to array at path). On ambiguous input, fail with
      # a diagnostic suggesting the explicit format key.
      record_path: <string>         # JSON pointer to array (e.g., "/data/records")
      # Fixed-width options (only when type: fixed_width)
      line_separator: <newline | crlf | none>  # Default: newline
      trim: <boolean>               # Trim trailing whitespace from each line. Default: true
    array_paths:                    # Paths to nested arrays that should explode into rows
      - path: <string>             # e.g., "items" or "items.details"
        mode: <explode | join>      # explode = one row per element; join = comma-delimited string
        separator: <string>         # For join mode. Default: ","
    sort_order: [<string>]          # Declare pre-sorted fields (enables optimizations)

outputs:
  - name: <string>
    type: <csv | xml | json>
    path: <string>
    include_unmapped: <boolean>     # Passthrough unmapped source fields. Default: false
    include_provenance: <boolean>   # Append all provenance + pipeline counter columns as the last
                                    # output columns (see §3 Record Provenance). Default: false
    preserve_nulls: <boolean>       # Emit empty XML tags / JSON nulls. Default: false
    include_header: <boolean>       # CSV: include header row. Default: true
    mapping:                        # Rename fields: {internal: output}
      <string>: <string>
    exclude: [<string>]             # Drop these fields from output
    options:                        # Format-specific output options
      # CSV
      delimiter: <string>
      # XML
      root_element: <string>
      record_element: <string>
      # JSON
      pretty: <boolean>             # Default: false
      format: <array | ndjson>      # Default: array

transformations:
  - name: <string>
    description: <string>
    local_window:                   # Optional — hoisted by AST compiler
      source: <string>              # Input name (alias from inputs[].name). May be same as or different from primary.
      on: <expr | [expr]>           # Required when source != primary input. Omit for same-source windows.
                                    # One CXL expression (or a list of expressions for compound keys) evaluated
                                    # against the PRIMARY record. The result is matched by equality against the
                                    # reference source's group_by index.
                                    # Arity must match group_by: scalar on ↔ scalar group_by, list ↔ list.
                                    # Examples:
                                    #   on: customer_id                     # simple field ref
                                    #   on: upper(customer_id)              # computed key
                                    #   on: [customer_id, record_type]      # compound key (group_by must also be a list)
                                    # Field refs in on-expressions resolve against the primary record schema.
                                    # Qualified refs (e.g., orders.customer_id) are also valid.
      group_by: <string | [string]> # Field(s) of the SOURCE used to partition/index during Phase 1.
                                    # For same-source windows: the current record's own group_by values are the key.
                                    # For cross-source windows: the reference source is indexed by these fields;
                                    # the on-expression produces the matching key from the primary record.
      sort_by: <string | [string]>
      order: <ascending | descending>
      nulls: <first | last | drop>
    log:                              # Per-transform log directives (see §5.10)
      - level: <trace | debug | info | warn | error>
        when: <before_transform | after_transform | per_record | on_error>
        message: <string>            # Template: {field_name} interpolation
        condition: <string>          # CXL boolean expression. Optional.
        fields: [<string>]           # Additional structured KV pairs. Optional.
        every: <integer>             # Sampling (per_record only). Optional.
        log_rule: <string>           # Reference to named log rule. Optional.
    validations:                      # Declarative field validations (see §5.11)
      - field: <string>             # Field to validate. Optional for cross-field checks.
        check: <string>             # Module function ref or inline CXL bool expression
        args: {<string>: <value>}   # Named args for function checks. Optional.
        severity: <error | warn>    # error = DLQ, warn = log + continue
        message: <string>           # Template for DLQ reason or log message. Optional.
    cxl: |                          # Multi-line CXL script
      <cxl statements>

error_handling:
  strategy: <fail_fast | continue | best_effort>  # Default: continue
  dlq:
    path: <string>                  # Default: errors.csv
    include_reason: <boolean>       # Append error reason column. Default: true
    include_source_row: <boolean>   # Include raw source data. Default: true
  type_error_threshold: <float>     # Fraction of total records (0.0–1.0). Default: 0.1 (10%).
                                    # Overridable: YAML config value, then --error-threshold CLI flag (highest precedence).
                                    # Only meaningful with strategy: continue.
                                    # Denominator = total record count from Phase 1 (arena.records.len()).
                                    # When error_count / total_records > threshold → halt (exit 3). Strictly greater-than.
                                    # 0.0 = halt after first DLQ write (1/N > 0.0 is true, so one row written then stop).
                                    #
                                    # Semantics by strategy:
                                    # - fail_fast: halt immediately on first error. No DLQ write. For dev/debugging.
                                    #   type_error_threshold is ignored.
                                    # - continue: DLQ bad rows and keep processing. Halt when error rate exceeds
                                    #   threshold. 0.0 = write first error to DLQ then halt (distinct from
                                    #   fail_fast — the DLQ row IS written).
                                    # - best_effort: DLQ all bad rows, never halt regardless of error rate.
                                    #   type_error_threshold is ignored.
```

### Environment variable interpolation

All string values in the YAML support `${VAR_NAME}` syntax, resolved at config load time. Missing variables cause a config error (not silent empty string).

### 4.1 Unified Schema System

Clinker uses a single schema language for all input formats. Schemas define field names, types, positions (fixed-width), validation rules, and multi-record structure. The schema system has three layers:

```
External schema file  →  schema_overrides (inline patches)  →  CXL type inference (runtime)
```

**When schema is required vs optional:**

| Input type | Schema required? | Why |
|---|---|---|
| `fixed_width` | **Required** | No other way to know field positions and widths |
| Multi-record (any type) | **Required** | Discriminator and per-type field layouts must be declared |
| `csv` | Optional | Header row provides field names; types inferred from CXL usage |
| `json` | Optional | Structure self-describing; types inferred from values + CXL |
| `xml` | Optional | Element/attribute structure self-describing |

When optional and omitted, the parser infers field names from the data (CSV headers, JSON keys, XML elements) and all fields default to `String`. CXL type coercion and `to_*` methods handle conversion at evaluation time.

#### Schema resolution

The `schema:` key on an input accepts either:
- **A file path** (string ending in `.yaml` or `.yml`): load and parse the external schema file.
- **An inline map** (with a `fields:` or `records:` key): use the schema directly.

When `schema:` is a file path AND `schema_overrides:` is present, the override entries are applied on top of the loaded schema using **strategic merge by field `name`**:

1. For each override entry, find the field in the base schema with matching `name` (and `record:` if scoped).
2. If found: deep-merge the override's keys onto the base field. Only specified keys change; unspecified keys are preserved.
3. If not found: append as a new field.
4. If `drop: true`: remove the field (or entire record type if `name` is absent and only `record:` is specified).

**Fail-fast**: If `schema:` is an inline map and `schema_overrides:` is also present, Phase A reports a config error — overrides are only meaningful with an external base file.

#### Schema file format

External schema files use YAML with the following structure:

```yaml
# schemas/benefits_feed.yaml
version: "1.0"                         # Schema version (for evolution tracking)
format: fixed_width                    # fixed_width | csv | json | xml
encoding: utf-8                        # Default: utf-8

# Record type discriminator (multi-record files only)
discriminator:
  start: 0                             # Byte position of discriminator value
  width: 4                             # Width in bytes
  # OR for delimited files:
  # field: record_type                 # Column name containing discriminator

# Reusable field templates
defs:
  standard_id:
    type: string
    width: 10
    required: true
    justify: right
    pad: "0"

  date_field:
    type: date
    width: 8
    format: "%Y%m%d"

# Single-record schema (no discriminator)
fields:
  - name: employee_id
    start: 0
    inherits: standard_id              # Pull in all props from defs.standard_id
  - name: hire_date
    start: 10
    inherits: date_field

# OR multi-record schema (with discriminator)
records:
  - id: EEID                           # Record type identifier
    tag: "EEID"                        # Discriminator value that identifies this type
    description: "Employee master"
    fields:
      - name: record_type
        start: 0
        width: 4
        type: string
      - name: employee_id
        start: 4
        inherits: standard_id
      - name: first_name
        start: 14
        width: 25
        type: string
        justify: left
      - name: hire_date
        start: 39
        inherits: date_field

  - id: PLAN
    tag: "PLAN"
    description: "Benefit plan enrollment"
    parent: EEID                       # Declares parent-child relationship
    join_key: employee_id              # Field linking child → parent
    fields:
      - name: record_type
        start: 0
        width: 4
        type: string
      - name: employee_id
        start: 4
        inherits: standard_id
      - name: plan_code
        start: 14
        width: 6
        type: string
      - name: effective_date
        start: 20
        inherits: date_field
      - name: premium
        start: 28
        width: 10
        type: decimal
        precision: 10
        scale: 2
        justify: right
        pad: "0"

  - id: TRLR
    tag: "TRLR"
    description: "File trailer"
    fields:
      - name: record_type
        start: 0
        width: 4
        type: string
      - name: total_count
        start: 4
        width: 8
        type: integer
        justify: right
        pad: "0"

# Optional: structural ordering constraints (validation only, not required for parsing)
structure:
  - record: EEID
    count: "1+"
  - record: PLAN
    count: "0+"
  - record: TRLR
    count: "1"
```

**`fields:` vs `records:`**: A schema file has either a top-level `fields:` (single-record) or `records:` (multi-record). Never both — Phase A config error if both are present.

#### Field properties

All field properties except `name` are optional (except `start` + (`width` or `end`) for fixed-width):

| Property | Type | Applies to | Description |
|---|---|---|---|
| `name` | String | All formats | Field name. Required on every field. |
| `type` | String | All formats | One of: `string`, `integer`, `float`, `decimal`, `boolean`, `date`, `datetime`, `object`, `array`. Default: `string`. |
| `required` | Bool | All formats | If true and missing → DLQ. Default: false. |
| `format` | String | All formats | Parse format for date/datetime (chrono strftime). |
| `coerce` | Bool | All formats | Attempt type coercion (e.g., string "42" → integer). Default: true. |
| `default` | Any | All formats | Fallback value when field is missing or null. |
| `enum` | [String] | All formats | Allowed values. Values outside enum → DLQ. |
| `alias` | String | All formats | Rename field for output. CXL uses `name`; output uses `alias`. |
| `inherits` | String | All formats | Pull all properties from the named `defs:` entry. Inline properties override inherited ones. |
| `start` | Int | Fixed-width | 0-based byte position. Required for fixed-width. |
| `width` | Int | Fixed-width | Field width in bytes. Mutually exclusive with `end`. |
| `end` | Int | Fixed-width | Exclusive end position. Mutually exclusive with `width`. |
| `justify` | String | Fixed-width | `left` or `right`. Default: `left` for strings, `right` for numerics. |
| `pad` | String | Fixed-width | Padding character. Default: `" "` (space). |
| `precision` | Int | Decimal | Total digits. |
| `scale` | Int | Decimal | Digits after decimal point. |
| `path` | String | XML | Relative XPath to the element/attribute (e.g., `"customer/name"`, `"@id"`). |

**`width` vs `end`**: Exactly one must be present for fixed-width fields. If both are specified, Phase A reports a config error: `field 'X' specifies both width and end — use one or the other`. `end` is exclusive: `start: 4, end: 14` = bytes 4–13 (width 10).

#### `inherits:` mechanism

`defs:` defines named field templates. `inherits: template_name` on a field pulls in all properties from the template. Properties specified directly on the field override inherited ones:

```yaml
defs:
  standard_id:
    type: string
    width: 10
    required: true

records:
  - id: EEID
    fields:
      - name: employee_id
        start: 4
        inherits: standard_id          # type: string, width: 10, required: true
        width: 12                      # overrides inherited width: 10 → 12
```

Resolution order: `defs` properties first, then field-level properties win. `inherits` chains are not supported (a `defs` entry cannot itself use `inherits`). Phase A config error if the named template does not exist in `defs`.

#### Multi-record files

A multi-record file contains lines belonging to different record types, identified by a `discriminator`. Each record type is a separate logical source in the pipeline:

- **CXL access**: `input_name.record_type_id.field_name` — e.g., `benefits.EEID.employee_id`
- **Window source**: `local_window.source: benefits.EEID` references the EEID record type from the `benefits` input
- **Phase 1**: The reader scans the file once, dispatching each line to the matching record type based on the discriminator. Each record type gets its own `Arena` and `SecondaryIndex`, exactly like a separate input source.

**`parent:` and `join_key:`** on a record type declare the parent-child relationship. This is informational metadata that:
1. Enables Phase E to auto-generate the `local_window` configuration when the transform references the child record type
2. Documents the relationship for `--explain` output
3. Does NOT constrain the pipeline — you can still write arbitrary cross-type window lookups

**`discriminator:`** matching rules:
- **Fixed-width**: `start` + `width` define the byte range. The value at that range is compared to each record type's `tag`.
- **CSV**: `field` names the column. The column value is compared to each record type's `tag`.
- **Match**: Exact string equality after trimming. No regex or glob — discriminators should be simple type codes.
- **Unmatched lines**: If a line's discriminator value doesn't match any `tag`, the line is skipped with a warning. If `error_handling.strategy` is `fail_fast`, this is a hard error.

**`structure:`** is optional ordering validation. When present, the parser validates that record types appear in the declared order with the declared cardinality. Violations are reported as config-level warnings (not DLQ errors), because real-world files often arrive unsorted — the two-pass architecture handles any ordering.

#### Schema for structured formats (JSON, XML, CSV)

When `schema:` is provided for a structured format, it adds type annotations, validation, and field metadata on top of the parser's inferred structure:

**JSON schema example** (inline):
```yaml
inputs:
  - name: events
    type: json
    path: data/events.ndjson
    schema:
      fields:
        - name: event_id
          type: string
          required: true
        - name: timestamp
          type: datetime
          format: "%Y-%m-%dT%H:%M:%S"
        - name: payload
          type: object
          fields:                          # nested object schema
            - name: user_id
              type: integer
            - name: action
              type: string
              enum: [click, view, purchase]
        - name: tags
          type: array
```

**XML schema example** (with `path:` for element mapping):
```yaml
inputs:
  - name: orders
    type: xml
    path: data/orders.xml
    options:
      record_path: "Orders/Order"
    schema:
      fields:
        - name: order_id
          path: "@id"                      # XML attribute
          type: string
          required: true
        - name: customer_name
          path: "customer/name"            # relative XPath from record element
          type: string
        - name: total
          path: "totals/grand_total"
          type: decimal
          precision: 10
          scale: 2
```

**CSV schema example** (external file + override):
```yaml
inputs:
  - name: transactions
    type: csv
    path: data/transactions.csv
    schema: schemas/transaction_base.yaml
    schema_overrides:
      - name: amount
        type: decimal                      # override base's string → decimal
        scale: 2
      - name: region                       # add field not in base schema
        type: string
        enum: [EAST, WEST, CENTRAL]
```

**Multi-record CSV** (discriminator on a column):
```yaml
inputs:
  - name: hr_export
    type: csv
    path: data/hr_combined.csv
    schema:
      discriminator:
        field: record_type                 # CSV column name
      records:
        - id: EMP
          tag: "EMP"
          fields:
            - name: record_type
              type: string
            - name: employee_id
              type: string
              required: true
            - name: full_name
              type: string
        - id: PLAN
          tag: "PLAN"
          parent: EMP
          join_key: employee_id
          fields:
            - name: record_type
              type: string
            - name: employee_id
              type: string
            - name: plan_code
              type: string
```

#### Integration with existing features

- **`schema_overrides:` on inputs replaces the old top-level `schema_overrides:`**. Per-input schemas are more precise — the old global overrides couldn't distinguish fields with the same name across different inputs.
- **`array_paths:`** remains on the input, not in the schema. Array explosion is a pipeline-level concern, not a schema concern.
- **`sort_order:`** remains on the input. It describes the physical file's ordering, not the schema.
- **Provenance fields** (`_source_file`, etc.) are not part of the schema — they are injected by the pipeline independently.
- **Fixed-width reader**: new module in `clinker-format`. Reads lines, applies the schema's field positions, produces `Record` objects with the schema's field names and parsed types.
- **Multi-record dispatcher**: reads each line, matches the discriminator, routes to the appropriate record type's Arena during Phase 1 and record stream during Phase 2.

---

## 5. CXL Language Specification

### 5.1 Design Principles

CXL is a domain-specific language for row-level data transformation. It is:
- **Expression-oriented**: Every construct produces a value.
- **Statically analyzable**: The AST compiler can determine types, field dependencies, and parallelism safety before any data flows.
- **Null-safe**: Null propagation is explicit via the coalesce operator (`??`). Operators on null produce null (not exceptions), unless short-circuited.

### 5.2 Formal Grammar (PEG)

```peg
program     = use_stmt* statement (NEWLINE statement)* NEWLINE?
statement   = let_stmt / emit_stmt / trace_stmt / expr
use_stmt    = "use" module_path ("as" IDENT)?     # Import a .cxl module (see §5.9)
              # use validators                     → qualified: validators.fn_name()
              # use shared::dates as dates         → aliased: dates.fn_name()
module_path = IDENT ("::" IDENT)*                 # Maps to filesystem: rules/shared/dates.cxl
let_stmt    = "let" IDENT "=" expr
emit_stmt   = "emit" IDENT "=" expr              # Explicit output field binding
trace_stmt  = "trace" trace_level? trace_guard? string_lit   # Logging statement (see §5.10)
trace_level = "info" / "warn" / "error" / "debug"            # Default: trace
trace_guard = "if" expr                           # Conditional — message only evaluated when true

expr        = coalesce
coalesce    = ternary ("??" ternary)*
ternary     = "if" or_expr "then" expr "else" expr / or_expr
or_expr     = and_expr ("or" and_expr)*
and_expr    = not_expr ("and" not_expr)*
not_expr    = "not" comparison / comparison
comparison  = additive (comp_op additive)?
comp_op     = "==" / "!=" / ">" / "<" / ">=" / "<="
additive    = term (("+" / "-") term)*
              # "+" on two Strings concatenates (same as .concat()). Lint warning CXL-W0310
              # if either operand is untyped (from CSV without schema_overrides).
term        = unary (("*" / "/" / "%") unary)*
unary       = "-" atom / atom
atom        = primary postfix*
primary     = literal / window_primary / match_expr / IDENT / "(" expr ")"
              # IDENT resolves in Phase B: let-bound variable, field ref, or pipeline.* namespace.
              # Reserved words are NOT valid as IDENT (see below).
postfix     = "." IDENT ("(" (expr ("," expr)*)? ")")?
              # no parens  → field access (source qualifier, record field, pipeline.* member)
              # with parens → method call; desugars to method_fn(receiver, args...)
              # Examples:
              #   customer_id.upper()          method call → upper(customer_id)
              #   customer_id.substring(0, 5)  method call with args
              #   orders.customer_id           field path (no parens)
              #   pipeline.start_time          pipeline namespace access (no parens)

literal     = string_lit / float_lit / int_lit / bool_lit / null_lit / date_lit / "now"
string_lit  = "'" ([^'\\] / "\\'")*  "'"           # Single-quoted; \' escapes embedded quote
            / "\"" ([^"\\] / "\\\"")*  "\""         # Double-quoted; \" escapes embedded quote
float_lit   = [0-9]+ "." [0-9]+
int_lit     = [0-9]+
bool_lit    = "true" / "false"
null_lit    = "null"
date_lit    = "#" [0-9]{4} "-" [0-9]{2} "-" [0-9]{2} "#"   # e.g., #2024-01-15#
              # "now" → wall-clock DateTime at point of row evaluation. NOT deterministic.
              # For a deterministic run timestamp use pipeline.start_time (see §5.8).

match_expr  = "match" expr? "{" match_arm+ "}"
              # Without expr: condition form — each arm pattern is a Bool expression.
              # With expr:    value form    — each arm pattern is compared == to expr.
match_arm   = match_pattern "=>" expr ","?         # trailing comma optional on last arm
match_pattern = expr / "_"                         # _ is the catch-all wildcard; must be last arm

window_primary = "window" "." window_fn
              # window is a reserved keyword. Followed by postfix* from atom for field access
              # and method chains: window.lag(1).Amount → window_primary + ".Amount" postfix

window_fn   = positional_fn / aggregate_fn / iterable_fn
positional_fn = "first()" / "last()"
              / "lag(" expr ")" / "lead(" expr ")"  # Offset expression (must evaluate to integer)
              # Positional fns return Record?. Field access via postfix: window.lag(1).Amount
              # Null propagation: postfix on null positional result → null. Standard rule.
              # Direct null test: window.lag(1) == null (valid — == is exempt from null prop)
              # Postfix on aggregate/iterable → Phase C type error (Int/Bool has no fields)
aggregate_fn  = "count()" / "sum(" expr ")" / "avg(" expr ")"
              / "min(" expr ")" / "max(" expr ")"
iterable_fn   = "any(" predicate_expr ")" / "all(" predicate_expr ")"
predicate_expr = expr
              # 'it' is a reserved identifier inside predicate_expr.
              # it          → the current iteration element (implements FieldResolver)
              # it.Amount   → field access on the iteration element
              # Unqualified field refs resolve against the PRIMARY record (not the partition).
              # To access partition-element fields use it.FieldName explicitly.
              # To use the primary record's value: bind to let before the window call.
              # window.* expressions inside predicate_expr are a Phase C compile error.
              #
              # Examples:
              #   window.any(it.Amount > 1000)          # partition element's Amount
              #   let my_amt = Amount                   # primary record's Amount
              #   window.any(it.Amount > my_amt)        # compare partition to primary

IDENT       = [a-zA-Z_][a-zA-Z0-9_]*
              # Reserved words (not valid as IDENT):
              # let  emit  if  then  else  and  or  not  match  use  as  fn  trace
              # null  true  false  now  it  window  pipeline  _
NEWLINE     = "\n" / "\r\n"
COMMENT     = "#" [^\n]*                           # Line comments

# Module file grammar (.cxl files only — not valid in inline CXL blocks)
module      = module_decl*
module_decl = fn_decl / let_stmt
fn_decl     = "fn" IDENT "(" param_list? ")" "=" expr
param_list  = IDENT ("," IDENT)*
              # fn is_valid_email(val) = val.matches('^[\\w.]+@[\\w.]+$')
              # fn in_range(val, lo, hi) = val >= lo and val <= hi
              # fn clamp(val, lo, hi) = if val < lo then lo else if val > hi then hi else val
              # Functions are pure: no emit, no trace, no side effects.
              # Parameters are untyped in v1 (type inferred at call site).
              # Module-level let bindings are constants: let DEFAULT_REGION = "EAST"
```

### 5.3 Operator Precedence (highest to lowest)

| Level | Operators | Associativity |
|---|---|---|
| 1 | Unary `-`, `not` | Right |
| 2 | `*`, `/`, `%` | Left |
| 3 | `+`, `-` | Left |
| 4 | `==`, `!=`, `>`, `<`, `>=`, `<=` | Non-associative |
| 5 | `and` | Left |
| 6 | `or` | Left |
| 7 | `if/then/else` | Right |
| 8 | `??` (coalesce) | Right |

`match` and `if/then/else` are keyword expressions, not operators. They are lower precedence than all binary operators — a `match` or `if` always extends as far right as possible. Wrap sub-expressions in `()` when using them as operands.

### 5.4 Type System

CXL uses a simple static type system with 7 types: `Null`, `Bool`, `Int`, `Float`, `String`, `Date`, `DateTime`.

**Null propagation**: In CXL, null means "this field has no value in this record." When absent data enters a computation, the result is absent — null propagates through all binary operators (except `??` and `==`/`!=`). This forces the developer to handle absent data explicitly rather than silently producing wrong results.

- Arithmetic: `5 + null` → null, `Amount * Rate` where either is null → null
- Comparison: `Amount > 100` where Amount is null → null (not false — the comparison could not be evaluated)
- Logical: `A and null` → null (unless A is false, in which case false); `A or null` → null (unless A is true)
- `not null` → null
- `null == null` → `true` (special case — null fields are equal to other null fields)
- `null != null` → `false` (derived from above)
- `null == anything_non_null` → `false`
- `null != anything_non_null` → `true`

Use `??` to provide a default when null is expected: `Amount ?? 0`. Use `field == null` to test for null directly (returns `true`/`false`, never null).

**Implicit coercion rules** (applied automatically, no explicit cast needed):
- `Int` + `Float` → `Float` (Int promoted)
- `Int` in string context (concat) → `String`
- `String` in comparison with `Int`/`Float` → attempt parse the String as the Numeric type, null on failure. Coercion is always String → Numeric, never Numeric → String. `"3.14" < 4` parses `"3.14"` as Float → `3.14 < 4.0` → `true`. `"abc" == 42` parse fails → null propagates → null (not `false`; the result is unknown, not definitively unequal).
- `String` in date context → attempt parse using schema_overrides format or ISO 8601

**Float serialization (output)**: Clinker uses `serde_json`'s default float serializer, which is backed by the `ryu` algorithm (dtolnay). ryu produces the *shortest decimal representation that round-trips exactly through IEEE 754 binary64*, guaranteed deterministic across platforms and Rust versions. Example: `1.0f64` → `"1.0"`, `1.23e40f64` → `"1.23e40"`. The byte-identical output guarantee (§1) holds for all finite floats without any custom serialization. NaN and Infinity in output fields are a DLQ event (see §10.1) — they never reach the serializer.

**Explicit conversion methods** use a `to_*` / `try_*` split:
- `expr.to_int()`, `expr.to_float()`, `expr.to_string()`, `expr.to_date(fmt?)`, `expr.to_bool()` — **strict**: conversion failure is a CXL runtime error, routed per `error_handling.strategy` (DLQ or halt).
- `expr.try_int()`, `expr.try_float()`, `expr.try_date(fmt?)`, `expr.try_bool()` — **lenient**: conversion failure silently returns `null`. Use with `??` for inline defaults: `value.try_int() ?? 0`.
- `.catch(default)` — method-chain sugar: `expr.catch(v)` is equivalent to `expr ?? v`. Useful when chaining conversions: `amount.try_float().catch(0.0)`.
- `to_string()` never fails — every `Value` variant has a string representation.

**`now` wall-clock semantics**: `now` returns the system wall-clock `DateTime` at the point of row evaluation. It is not deterministic — two pipeline runs at different times will produce different values. The byte-identical output guarantee (§1) explicitly excludes expressions containing `now`. For a deterministic run timestamp, use `pipeline.start_time` (see §5.8).

### 5.5 Built-in Method Library

Built-in functions are invoked as methods on a receiver via postfix chaining: `receiver.method(args)`.
There are no standalone built-in function calls — `upper(s)` is invalid; write `s.upper()` instead.
Module functions (from `.cxl` files) are called with qualified names: `validators.is_valid_email(Email)` — see §5.9.

**String methods** (receiver: `String`):

| Method | Signature | Description |
|---|---|---|
| `.concat(b, ...)` | `String.(String...) → String` | Append strings. Nulls become `""`. Prefer `a + b` for two strings. |
| `.trim()` | `String.() → String` | Strip leading/trailing whitespace |
| `.trim_start()` | `String.() → String` | Strip leading whitespace |
| `.trim_end()` | `String.() → String` | Strip trailing whitespace |
| `.upper()` | `String.() → String` | Uppercase |
| `.lower()` | `String.() → String` | Lowercase |
| `.replace(find, repl)` | `String.(String, String) → String` | Replace all occurrences |
| `.substring(start, len?)` | `String.(Int, Int?) → String` | 0-indexed substring. Omit `len` for rest of string. |
| `.left(n)` | `String.(Int) → String` | First `n` characters. |
| `.right(n)` | `String.(Int) → String` | Last `n` characters. |
| `.length()` | `String.() → Int` | Character count |
| `.starts_with(prefix)` | `String.(String) → Bool` | Prefix test |
| `.ends_with(suffix)` | `String.(String) → Bool` | Suffix test |
| `.contains(sub)` | `String.(String) → Bool` | Literal substring test — fast path, no regex engine |
| `.split(delim)` | `String.(String) → Array` | Split into array of strings |
| `.join(delim)` | `String.(String) → String` | Join receiver (treated as iterable) with delimiter |
| `.find(pattern)` | `String.(String) → Bool` | Regex substring test — true if pattern is found **anywhere** in the field value. |
| `.matches(pattern)` | `String.(String) → Bool` | Regex full-value test — true only if pattern covers the **entire** field value (implicit `^...$`). Use for format validation. |
| `.capture(pattern, group?)` | `String.(String, Int?) → String?` | Extract first regex match found anywhere in the field. `group` defaults to 0 (full match). Null if no match. |
| `.format(template)` | `String.(String) → String` | Format string with template interpolation. |
| `.pad_left(len, char?)` | `String.(Int, String?) → String` | Left-pad to `len` chars. Default pad char: `" "`. |
| `.pad_right(len, char?)` | `String.(Int, String?) → String` | Right-pad to `len` chars. |
| `.repeat(n)` | `String.(Int) → String` | Repeat string `n` times. |
| `.reverse()` | `String.() → String` | Reverse character order. |

**Regex dialect** (applies to `.find`, `.matches`, and `.capture`): Rust `regex` crate v1.x. Linear time O(m×n) — no catastrophic backtracking. Supported: character classes, quantifiers, alternation, Unicode categories, inline flags. NOT supported: lookahead, lookbehind, backreferences. Input must be valid UTF-8 (CXL `String` values always are).

- **`.contains(sub)`** — literal string only, no regex. Fast path (no regex engine). Use for simple substring checks.
- **`.find(pattern)`** — unanchored regex: `Notes.find("urgent")` returns true if the pattern is found anywhere in the field value.
- **`.matches(pattern)`** — fully anchored regex: `Code.matches("[A-Z]{2}\\d{4}")` returns true only when the pattern covers the entire field value. Behaves as if the pattern is wrapped in `^(?:...)$`.
- **`.capture(pattern, group?)`** — extracts the first match found anywhere (same search semantics as `.find`).
- **Inline flags**: `(?i)` case-insensitive, `(?m)` multiline `^`/`$`, `(?s)` dot-matches-newline.
- **`.` behavior**: does not match `\n` by default; use `(?s)` to enable.
- **Compile errors**: malformed regex patterns are Phase A parse errors (not runtime errors).

**Path decomposition methods** (receiver: `String`): Treat the string as a file path. All return `null` if the string is not a valid path or the component is absent (e.g., `.extension()` on a path with no extension). Available on any `String` — particularly useful on `_source_file`.

| Method | Signature | Description |
|---|---|---|
| `.file_name()` | `String.() → String?` | Final path component including extension. `"data/orders.csv".file_name()` → `"orders.csv"` |
| `.file_stem()` | `String.() → String?` | Final path component without extension. `"data/orders.csv".file_stem()` → `"orders"` |
| `.extension()` | `String.() → String?` | Extension without the leading dot. `"data/orders.csv".extension()` → `"csv"`. Null if none. |
| `.parent()` | `String.() → String?` | Full parent directory path. `"data/batch/orders.csv".parent()` → `"data/batch"` |
| `.parent_name()` | `String.() → String?` | Immediate parent directory name only. `"data/batch/orders.csv".parent_name()` → `"batch"` |

**Array methods** (receiver: `Array`):

| Method | Signature | Description |
|---|---|---|
| `.join(delim)` | `Array.(String) → String` | Join elements into a string |
| `.length()` | `Array.() → Int` | Element count |

**Numeric methods** (receiver: `Int` or `Float`):

| Method | Signature | Description |
|---|---|---|
| `.abs()` | `Numeric.() → Numeric` | Absolute value |
| `.round(decimals?)` | `Float.(Int?) → Float` | Round to N decimal places. Default: 0. |
| `.ceil()` | `Float.() → Int` | Ceiling |
| `.floor()` | `Float.() → Int` | Floor |
| `.min(b)` | `Numeric.(Numeric) → Numeric` | Scalar min (not window aggregate) |
| `.max(b)` | `Numeric.(Numeric) → Numeric` | Scalar max |
| `.round_to(n)` | `Float.(Int) → Float` | Round to `n` decimal places. Same as `.round(n)` but reads more clearly in chains. |
| `.clamp(min, max)` | `Numeric.(Numeric, Numeric) → Numeric` | Clamp value to `[min, max]` range. |

**Date / DateTime methods** (receiver: `Date` or `DateTime`):

| Method | Signature | Description |
|---|---|---|
| `.format_date(fmt)` | `Date.(String) → String` | Format using chrono format codes (e.g., `"%Y-%m-%d"`) |
| `.year()` | `Date.() → Int` | Extract year |
| `.month()` | `Date.() → Int` | Extract month (1–12) |
| `.day()` | `Date.() → Int` | Extract day of month (1–31) |
| `.hour()` | `DateTime.() → Int` | Extract hour (0–23). Only valid on DateTime receiver. |
| `.minute()` | `DateTime.() → Int` | Extract minute (0–59). Only valid on DateTime receiver. |
| `.second()` | `DateTime.() → Int` | Extract second (0–59). Only valid on DateTime receiver. |
| `.add_days(n)` | `Date.(Int) → Date` | Add N days |
| `.add_months(n)` | `Date.(Int) → Date` | Add N months |
| `.add_years(n)` | `Date.(Int) → Date` | Add N years |
| `.diff_days(other)` | `Date.(Date) → Int` | Days between dates (`self - other`) |
| `.diff_months(other)` | `Date.(Date) → Int` | Months between dates |
| `.diff_years(other)` | `Date.(Date) → Int` | Full years between dates |

**Type conversion methods** (receiver: `Any`):

Strict (`to_*`) — failure is a CXL runtime error, routed per `error_handling.strategy`:

| Method | Signature | Description |
|---|---|---|
| `.to_int()` | `Any.() → Int` | Convert to integer |
| `.to_float()` | `Any.() → Float` | Convert to float |
| `.to_string()` | `Any.() → String` | Stringify. Never fails. |
| `.to_bool()` | `Any.() → Bool` | `"true"/"yes"/"1"/"on"` → true; `"false"/"no"/"0"/"off"` → false |
| `.to_date(fmt?)` | `Any.(String?) → Date` | Parse date. **With `fmt`**: uses exactly that chrono strftime format. **Without `fmt`**: three-layer resolution — (1) if field has schema `format:`, use that; (2) else try each format in `pipeline.date_formats` in order; (3) else auto-detect heuristic (ISO 8601, then slash-delimited using `date_locale` for MM/dd vs dd/MM). First success wins; all fail → strict error. |
| `.to_datetime(fmt?)` | `Any.(String?) → DateTime` | Parse datetime. Same three-layer resolution as `to_date()`, but tries datetime formats first (formats containing `%H`/`%M`/`%S`), then falls back to date-only formats with midnight time. |

Lenient (`try_*`) — failure returns `null` silently:

| Method | Signature | Description |
|---|---|---|
| `.try_int()` | `Any.() → Int?` | Parse to integer. Null on failure. |
| `.try_float()` | `Any.() → Float?` | Parse to float. Null on failure. |
| `.try_bool()` | `Any.() → Bool?` | Parse to bool. Null on unrecognised input. |
| `.try_date(fmt?)` | `Any.(String?) → Date?` | Parse to date. Null on failure. |
| `.try_datetime(fmt?)` | `Any.(String?) → DateTime?` | Parse to datetime. Null on failure. |

Introspection:

| Method | Signature | Description |
|---|---|---|
| `.is_null()` | `Any.() → Bool` | True if value is null |
| `.is_empty()` | `Any.() → Bool` | True if value is an empty string (`""`) or empty array (`[]`). Returns false for null (use `.is_null()` for null checks). |
| `.type_of()` | `Any.() → String` | Returns `"null"`, `"bool"`, `"int"`, `"float"`, `"string"`, `"date"`, `"datetime"`, `"array"` |
| `.catch(default)` | `Any.(Any) → Any` | If receiver is null, return `default`. Sugar for `receiver ?? default`. |

**Debugging** (receiver: `Any`):

| Method | Signature | Description |
|---|---|---|
| `.debug(prefix)` | `Any.(String) → Any` | **Passthrough**: evaluates receiver, emits a `tracing::trace!` event with `prefix`, the value, `_source_row`, and `_source_file`, then returns the value unchanged. If receiver is null, logs `"null"` as the value and returns null. Silent unless `--log-level=trace` or `RUST_LOG=cxl=trace`. Safe to leave in production code — zero overhead when trace is disabled. |

**Multi-branch conditional** — `match` keyword expression (not a method):

```
# Condition form — each arm is a Bool expression; no subject
match {
  score >= 90 => "A",
  score >= 80 => "B",
  score >= 70 => "C",
  _           => "F",
}

# Value form — arms are compared == to the subject expression
match tier {
  "gold"   => price * 0.8,
  "silver" => price * 0.9,
  _        => price,
}
```

Rules: `_` catch-all arm is required (Phase C error if missing). All arms must produce compatible types. Arms are evaluated top-to-bottom; first match wins.

### 5.6 Window Functions

Window functions operate on the pre-built Arena and Secondary Index from Phase 1. They are only valid inside a CXL block that has an associated `local_window` definition.

**Positional:**
- `window.first()` → `Record?` — First record in the partition.
- `window.last()` → `Record?` — Last record in the partition.
- `window.lag(n)` → `Record?` — Record n positions before current. Returns null at partition boundaries.
- `window.lead(n)` → `Record?` — Record n positions after current.

Field access on positional results uses the `("." IDENT)?` suffix from the grammar:
- `window.lag(1).Amount` → `Value?` — null-propagates if lag(1) is null (partition boundary).
- `window.lag(1) == null` → `Bool` — valid direct null test; `==` is exempt from null propagation.
- `window.count().Amount` → **Phase C type error** — aggregates return scalars, not Record?.

**Aggregates:**
- `window.count()` → `Int`
- `window.sum(field_expr)` → `Numeric`
- `window.avg(field_expr)` → `Float`
- `window.min(field_expr)` → `Value`
- `window.max(field_expr)` → `Value`

**Empty partition behavior** (no records match the group_by key — the partition has zero entries): `count()` → `0`; `sum()` → `0` (additive identity — zero things summed is zero); `avg()` → `null`; `min()` → `null`; `max()` → `null`. Distinct from the case where records exist but all field values are null — that case propagates null through the aggregate normally (null propagation applies within the sum, producing null if any operand is null). `sum` of zero records gives `0` without requiring `?? 0`; `avg`/`min`/`max` of zero records give `null` because the result is genuinely undefined.

**Type constraints on aggregate `field_expr`**: `sum` and `avg` require a Numeric argument. If `field_expr` has a statically-known non-Numeric type (e.g., `Date`, `Bool`, `Array`), Phase C reports a compile error: `sum/avg requires a Numeric expression; got Array`. If the type is dynamically unknown at compile time (e.g., a `Numeric`-inferred field whose runtime value is an Array due to an `explode: join` path), the runtime DLQ event fires: "aggregate type error: sum/avg received non-Numeric value in field X". `min` and `max` accept any non-Array type (they compare using the natural ordering for their variant); passing an Array to `min`/`max` is a Phase C compile error when statically detectable, runtime DLQ otherwise.

**Iterables:**
- `window.any(predicate_expr)` → `Bool` — Short-circuits on first true.
- `window.all(predicate_expr)` → `Bool` — Short-circuits on first false.
- `window.collect(field_expr)` → `Array` — Collect field values from all partition records into an array.
- `window.distinct(field_expr)` → `Array` — Collect unique field values from all partition records into an array.

Inside `predicate_expr`, unqualified field references resolve against each **partition element** (the iteration record), not the primary record being transformed. This is enforced by Phase B (Resolve) switching schema context when it enters a `predicate_expr` node.

```
# it refers to each partition element; Amount (unqualified) = current primary record
emit any_high = window.any(it.Amount > 1000)

# Compare partition element against current record's value — both explicit
let my_amount = Amount
emit has_higher_peer = window.any(it.Amount > my_amount)
```

`window.*` expressions are **not valid inside `predicate_expr`** — Phase C compile error. Nested window context is not supported.

**Cross-source lookups:**

A cross-source window has `local_window.source` pointing to a **different** input than the one being transformed. The `on` field bridges the two sources.

**How matching works:**
- Phase 1 indexes the reference source by its `group_by` field(s) → `HashMap<Vec<GroupByKey>, Vec<usize>>`
- Phase 2, for each primary record: evaluate the `on` expression(s) against the primary record → produce a `Vec<GroupByKey>` lookup key
- Look up that key in the reference source's SecondaryIndex
- The matched `Vec<usize>` is the partition; window functions operate over it

The join is always **equality** on the computed key. No range joins or inequality joins in v1.

```yaml
# Example: for each order, look up window data from the matching customer
local_window:
  source: customers         # reference source
  on: customer_id           # evaluated against the current order — produces the lookup key
  group_by: id              # customers indexed by id; on-result must match this type
  sort_by: signup_date
  order: ascending
cxl: |
  let customer_name = window.first().name
  let order_count   = window.count()
  emit customer      = customer_name
  emit orders_placed = order_count
```

**Type constraint**: The type of the `on` expression must be compatible with the type of the `group_by` field in the reference source. Phase C reports a compile error if they are mismatched (e.g., `on` produces a String but `group_by` field is Date).

**Compound keys**: When `group_by` is a list, `on` must also be a list of the same arity. Each expression in the `on` list maps positionally to the corresponding `group_by` field.

**Zero matches**: If the lookup key is not found in the reference index, all positional functions return null and aggregates return their empty-partition defaults (see §5.6 aggregates).

**One-to-many matches**: The full matched set is the partition. `window.first()` returns the first match (by `sort_by` order), `window.count()` returns the total match count. For a single-row lookup, use `window.first().field_name`.

**Same-source windows** (when `source` == primary input, `on` omitted): the current record's own `group_by` field values are used directly as the lookup key — no cross-source resolution needed.

### 5.7 CXL Example

```yaml
transformations:
  - name: enrollment_transform
    description: Compute enrollment deltas and status
    local_window:
      source: enrollments
      group_by: [member_id, plan_id]
      sort_by: effective_date
      order: ascending
      nulls: last
    cxl: |
      # Type coercion from CSV strings (strict — failure DLQs the row)
      let eff_date = Effective_Date.to_date("%m/%d/%Y")
      let end_date = End_Date.try_date("%m/%d/%Y") ?? #2099-12-31#

      # Window lookups
      let prev_end = window.lag(1).End_Date ?? #2099-12-31#
      let is_first = window.lag(1) == null

      # Business logic
      let gap_days = if not is_first then eff_date.diff_days(prev_end) else 0
      let status = if end_date < now then "terminated" else "active"
      let plan_count = window.count()

      # String assembly
      let member_label = Last_Name.upper() + ", " + First_Name

      # Emit output fields (only emit bindings are written to output)
      emit member_name = member_label
      emit enrollment_status = status
      emit gap_from_prior = gap_days
      emit total_plan_enrollments = plan_count
```

### 5.8 Pipeline Metadata

The `pipeline` namespace provides read-only execution context accessible from any CXL expression. `pipeline` is a reserved identifier; all access is via postfix field access or method chaining.

**Built-in pipeline properties:**

| Expression | Type | Description |
|---|---|---|
| `pipeline.start_time` | `DateTime` | Timestamp when `clinker` began executing. Fixed for the entire run — deterministic. Use this instead of `now` when you need a consistent run timestamp. |
| `pipeline.name` | `String` | From `pipeline.name` in the YAML config. |
| `pipeline.execution_id` | `String` | UUID v7 generated at startup. Unique per run, time-ordered across runs. Useful for audit columns and log correlation. |
| `pipeline.total_count` | `Int` | Cumulative number of source records read so far across all sources. Snapshotted at chunk boundaries — all records in a chunk see the same value. |
| `pipeline.ok_count` | `Int` | Cumulative number of records successfully processed (not routed to DLQ). Per-source running total, snapshotted at chunk boundaries. |
| `pipeline.dlq_count` | `Int` | Cumulative number of records routed to DLQ. Per-source running total, snapshotted at chunk boundaries. |
| `pipeline.source_file` | `String` | File path of the source file this record came from. Shared across all records from the same file (Arc-allocated). |
| `pipeline.source_row` | `Int` | 1-based row number of this record in its source file. |

**User-defined pipeline variables:**

Declare constants under `pipeline.vars` in the YAML config. They are available in every CXL block as `pipeline.VARNAME`. Type is inferred from the YAML value (integer, float, bool, or string).

```yaml
pipeline:
  name: enrollment_pipeline
  vars:
    fiscal_year: 2026
    cutoff_date: "2026-01-01"
    region: "WEST"
    rate_multiplier: 1.075
```

```
# In any CXL block:
emit year        = pipeline.fiscal_year             # Int
emit region      = pipeline.region                  # String
let cutoff       = pipeline.cutoff_date.to_date()   # String → Date via method chain
emit adjusted    = amount * pipeline.rate_multiplier
```

**Rules:**
- `pipeline.*` is read-only. Assignment to any `pipeline.*` path is a Phase C compile error.
- User variable names must not collide with built-in properties (`start_time`, `name`, `execution_id`, `total_count`, `ok_count`, `dlq_count`, `source_file`, `source_row`). Phase A (config parse) errors on collision.
- Variable values are evaluated once at startup, before any data is read.
- `pipeline.cutoff_date` above is a `String` — call `.to_date()` or `.try_date()` to convert.

### 5.9 CXL Modules

CXL modules are external `.cxl` files containing reusable functions and constants. They are the primary mechanism for rule reuse across pipelines.

#### Module files

A module file contains only `fn` declarations and `let` bindings — no `emit`, `trace`, or side effects:

```
# rules/validators.cxl
fn is_valid_email(val) = val.matches('^[\\w.]+@[\\w.]+$')
fn in_range(val, lo, hi) = val >= lo and val <= hi
fn clamp(val, lo, hi) = if val < lo then lo else if val > hi then hi else val
fn normalize_name(val) = val.trim().upper()
let DEFAULT_REGION = "EAST"
let MAX_SALARY = 10000000
```

**Rules for `fn`:**
- Functions are pure. No `emit`, `trace`, or `.debug()` calls inside function bodies.
- Parameters are untyped in v1 — type is inferred at each call site during Phase B.
- Return type is inferred from the body expression.
- Recursive calls are not supported (Phase C error). Functions are single-expression bodies.
- Function names must be unique within a module. Duplicate → Phase A parse error.

**Rules for module-level `let`:**
- Constants only — evaluated once at module load time (Phase A).
- Only literal values and expressions over other module-level `let` bindings.
- No field references, no `pipeline.*`, no `window.*`.

#### `use` statement

Import a module at the top of any inline CXL block:

```
use validators                          # qualified: validators.is_valid_email(Email)
use shared::date_helpers as dates       # aliased: dates.parse_date(raw, fmt)
```

- `use` must appear before any `let`, `emit`, or `trace` statements. Phase A parse error otherwise.
- The module path maps to the filesystem: `shared::date_helpers` → `{rules_path}/shared/date_helpers.cxl`.
- **Module search path**: defaults to `./rules/` relative to the pipeline YAML. Overridable via `pipeline.rules_path` in config or `--rules-path` CLI flag.
- **Qualified access**: `module_name.function_name(args)` or `module_name.CONSTANT`.
- **No wildcard imports**: `use validators::*` is not supported in v1. All access is qualified.
- **No cross-file imports**: A `.cxl` module file cannot `use` another module. This keeps the dependency graph flat (one level deep) and avoids circular import resolution. If a function in `validators.cxl` needs a helper from `utils.cxl`, extract the shared logic into both call sites or move it to a shared module that both inline CXL blocks import.

#### Module compilation

Modules are parsed and type-checked at Phase A alongside inline CXL. The compiler:
1. Reads the pipeline YAML, collects all `use` statements from all CXL blocks.
2. Deduplicates module paths — each `.cxl` file is parsed once, even if used by multiple transforms.
3. Stores parsed modules in `Arc<ModuleAst>` (shared read-only across transforms).
4. During Phase B, function calls like `validators.is_valid_email(Email)` are resolved against the module's function registry. Unresolved → Phase B error with suggestion.
5. During Phase C, function body types are checked against each call site's argument types.

**Error diagnostics**: Module errors get the same 4-part format (what/where/why/how-to-fix) as inline CXL. The source span points into the `.cxl` file, not the YAML.

#### Pipeline config

```yaml
pipeline:
  rules_path: rules/              # Module search path. Default: ./rules/
```

CLI override: `--rules-path path/to/rules/`

### 5.10 Logging and Tracing

CXL provides four levels of logging, from pipeline lifecycle down to individual expression debugging.

#### Level 1: Pipeline log directives (YAML)

Configured per-transform in the pipeline YAML:

```yaml
transformations:
  - name: enrich_plans
    log:
      - level: info
        when: before_transform
        message: "Starting {_transform_name}: {_source_count} records"
      - level: info
        when: after_transform
        message: "Done: {_pipeline_ok_count} ok, {_pipeline_dlq_count} errors ({_transform_duration_ms}ms)"
      - level: warn
        when: after_transform
        condition: "_pipeline_dlq_count > 0"
        message: "{_pipeline_dlq_count} records failed in {_transform_name}"
      - level: debug
        when: per_record
        every: 10000
        message: "Processing record {_source_row}: {employee_id}"
        fields: [employee_id, plan_code]
```

**Log directive properties:**

| Property | Type | Description |
|---|---|---|
| `level` | String | `trace`, `debug`, `info`, `warn`, `error`. Required. |
| `when` | String | `before_transform`, `after_transform`, `per_record`, `on_error`. Required. |
| `message` | String | Template string. `{field_name}` resolves against current record or pipeline metadata. |
| `condition` | String | CXL boolean expression. Directive only fires when true. Optional. |
| `fields` | [String] | Additional field values to include as structured key-value pairs in the log event. Optional. |
| `every` | Int | Sampling: emit every Nth invocation. Only meaningful with `when: per_record`. Optional. |
| `log_rule` | String | Reference to a named log rule (see Level 2). Mutually exclusive with inline properties except as overrides. |

**Available template variables:**
- All provenance fields: `{_source_file}`, `{_source_row}`, `{_source_count}`, `{_source_batch}`, `{_ingestion_timestamp}`
- All pipeline counters: `{_pipeline_ok_count}`, `{_pipeline_dlq_count}`, `{_pipeline_total_count}`
- All record fields: `{employee_id}`, `{amount}`, etc.
- Transform context: `{_transform_name}`, `{_transform_duration_ms}`
- Pipeline metadata: `{pipeline.name}`, `{pipeline.execution_id}`

#### Level 2: Reusable log rules (external YAML)

Define log directives once, reference from any pipeline:

```yaml
# rules/log_rules.yaml
lifecycle:
  level: info
  when: before_transform
  message: "Starting {_transform_name}: {_source_count} records"

error_summary:
  level: warn
  when: after_transform
  condition: "_pipeline_dlq_count > 0"
  message: "{_pipeline_dlq_count} records routed to DLQ"
  fields: [_pipeline_dlq_count, _pipeline_ok_count]

high_value_alert:
  level: warn
  when: per_record
  condition: "amount > 100000"
  message: "High-value transaction: {employee_id} amount={amount}"
  fields: [employee_id, amount, _source_file, _source_row]
```

**Pipeline config:**
```yaml
pipeline:
  log_rules: rules/log_rules.yaml
```

**Usage with override:**
```yaml
transformations:
  - name: enrich
    log:
      - log_rule: lifecycle                # use as-is
      - log_rule: high_value_alert
        condition: "premium > 50000"       # override the condition
        message: "High premium: {employee_id} = {premium}"  # override the message
```

**Override semantics**: Same merge model as schema overrides — the log rule provides defaults, inline keys win. The `log_rule` key is cleared after expansion.

#### Level 3: `.debug()` passthrough method (in CXL expressions)

Wraps any expression, logs it via `tracing::trace!`, returns the value unchanged:

```
emit total = (employee_premium + employer_premium).debug("total_premium")
# → [TRACE cxl] total_premium = 1842.50  (row 312, benefits.dat:PLAN)

# Debug intermediate values
let rate = (salary.debug("raw_salary") / 2080).debug("hourly_rate")

# Debug a window lookup
emit parent_name = window.first().debug("matched_parent").full_name

# Debug inside a match
emit tier = match {
  score.debug("score") >= 90 => "A",
  score >= 80 => "B",
  _ => "C"
}.debug("tier")
```

**Behavior:**
- Emits `tracing::trace!` — silent by default, enabled via `--log-level=trace` or `RUST_LOG=cxl=trace`.
- Each event includes: prefix, stringified value, `_source_row`, `_source_file`, record type (multi-record).
- Zero overhead when trace is disabled — `tracing` short-circuits before evaluating the format string.
- Safe to leave in production CXL. No effect on expression semantics.
- `.debug()` is **not** allowed inside `fn` bodies (module functions are pure).

#### Level 4: `trace` statement (in CXL blocks)

Standalone logging without wrapping an expression:

```
trace "Processing {employee_id} with salary {salary}"

# Conditional — message only evaluated when guard is true
trace if premium > 50000 "High premium: {employee_id} = {premium}"
trace if parent == null "Orphan record: {employee_id} has no parent"

# Level override — default is trace; can promote
trace warn if premium > 100000 "Extreme premium: {employee_id}"
trace info "Batch {_source_batch}: {_source_file.file_name()}"

# Progress sampling
trace if _source_row % 10000 == 0 "Progress: {_source_row} of {_source_count}"
```

**Behavior:**
- `trace` is a statement — no return value, no effect on data flow.
- `{field_name}` in the message string resolves against the current record's `FieldResolver` + provenance.
- Default level is `trace`. Level keyword (`info`, `warn`, `error`, `debug`) promotes the event.
- The `if` guard short-circuits: the message string is not evaluated unless the condition is true.
- `trace` is **not** allowed inside `fn` bodies (module functions are pure).
- In parallel evaluation (rayon chunks), log events include `tracing::Span` context for stage + chunk correlation.

### 5.11 Declarative Validations

Validations are declared in the pipeline YAML and reference CXL functions from modules (§5.9) or inline CXL expressions. They run during Phase 2 after `let` bindings but before `emit` statements.

```yaml
validations:
  - field: employee_id
    check: validators.not_null
    severity: error
    message: "Employee ID is required"

  - field: salary
    check: validators.in_range
    args: { lo: 0, hi: 10000000 }
    severity: error
    message: "Salary {salary} out of range for {employee_id}"

  - field: email
    check: validators.is_valid_email
    severity: warn
    message: "Invalid email for {employee_id}: {email}"

  - check: "effective_date > pipeline.cutoff_date.to_date()"
    severity: warn
    message: "Record predates cutoff: {employee_id} eff={effective_date}"
```

**Validation properties:**

| Property | Type | Description |
|---|---|---|
| `field` | String | Field to validate. Optional — omit for cross-field checks. |
| `check` | String | Module-qualified function name (e.g., `validators.not_null`) OR inline CXL boolean expression. Required. |
| `args` | Map | Named arguments passed to the check function. Keys map to function parameters by name. Values must be YAML literals (int, float, string, bool) or `pipeline.*` references — not field references (use inline `check:` expressions for field-dependent validation). Only valid when `check` is a function reference. |
| `severity` | String | `error` (route to DLQ) or `warn` (log + continue). Required. |
| `message` | String | Template string for DLQ reason or log message. `{field_name}` interpolation. Optional. |

**Execution model:**
- When `check` is a function reference: the function is called with the field value as the first argument, plus any `args`. The function must return `Bool`. `false` = validation failure.
- When `check` is an inline expression: evaluated as a CXL boolean expression against the current record. `false` = failure.
- When `field` is specified: the field value is passed as the first argument to the check function. When `field` is omitted: the check expression has access to all record fields.
- `severity: error` — record is routed to DLQ with the `message` as `_cxl_dlq_error_detail`. Error category: `validation_failure`.
- `severity: warn` — a `tracing::warn!` event is emitted; record continues processing normally.
- Validations run in declaration order. A `severity: error` validation short-circuits — remaining validations for that record are skipped.

**Phase C checks:**
- If `check` references a module function, Phase C verifies the function exists and the `args` match its parameter count.
- If `field` is specified, Phase C verifies the field exists in the source schema (or warns if schema is inferred).
- If the check function's return type is statically known to be non-Bool, Phase C reports a compile error.

---

## 6. AST Compiler

The AST compiler runs once at startup before any data streams. It is a pure function: `(YAML config, CXL source texts) → ExecutionPlan | CompileError`.

Phases A–C live entirely in the `cxl` crate (no pipeline knowledge). Phases D–F bridge `cxl` and `clinker-core` — the `cxl` crate produces a typed AST with dependency metadata, and `clinker-core` consumes that to build the `ExecutionPlan`.

### 6.1 Compilation Phases

**Phase A — Parse** (`cxl`): Each CXL block is parsed by a hand-rolled Pratt parser (top-down operator precedence + recursive descent for statements, `match`, and `if/then/else`) into an untyped AST. The parser uses panic-mode error recovery — on a syntax error it skips to the next statement boundary and continues, collecting all errors before returning. Parse errors are emitted as `miette` diagnostics with the 4-part structure: **what** (error label), **where** (source span + line/col), **why** (contextual note), **how to fix** (help suggestion). Example:

```
× expected expression, found keyword `emit`
   ╭─[pipeline.yaml:14:7]
14 │   let status = emit "active"
   ·                ^^^^ keyword not valid here
   ╰─
  help: `emit` starts an output binding statement; did you mean to assign with `let`?
```

**Phase B — Resolve** (`cxl`): Field references are resolved against input schemas (provided by `clinker-core` via the `FieldResolver` trait). Unresolved fields produce an error listing similar field names (fuzzy match via edit distance). Inside a `predicate_expr` node (argument to `window.any()` / `window.all()`): unqualified field refs resolve against the **primary record** as normal; the reserved identifier `it` resolves against the `local_window.source` schema (the current iteration element). `it` outside a `predicate_expr` is a Phase B error. Let-bound variables are always visible regardless of context. `window.*` expressions inside a predicate are flagged as a Phase C error.

**Phase C — Type check** (`cxl`): Constraint-based type inference. Every expression node gets a type annotation. Conflicts are reported as compile errors (e.g., `date_diff(Amount, End_Date, "days")` — first argument is Numeric, not Date). Additional Phase C checks: (1) Every `match` expression must contain a `_` catch-all arm. (2) `window.*` expressions inside a `predicate_expr` (argument to `window.any` / `window.all`) are illegal — nested window context is not supported. (3) A CXL block whose associated output has `include_unmapped: false` and contains no `emit` statements produces a compile error: `transform 'NAME' produces no output fields — add at least one \`emit\` statement or set \`include_unmapped: true\` on the output`.

**Phase D — Dependency analysis** (`cxl::analyzer`, consumed by `clinker-core`):
- Identify which inputs each CXL block reads (field references).
- Identify cross-source dependencies: a transformation with `local_window.source != primary_input` depends on the reference source completing Phase 1 before Phase 2 can begin. The `on` expression is also resolved here — its field refs must exist in the primary source's schema (Phase B already ensured this, but Phase D records the dependency edge formally).
- Build a source dependency DAG.
- Classify each transformation for parallelism safety (see §8.3).
- Type-check `on` expression result type against the reference source's `group_by` field type(s). Arity mismatch (list vs. scalar, or wrong list length) is a compile error.

**Phase E — Index planning** (`clinker-core::plan`):
- Extract all `local_window` blocks.
- Deduplicate identical windows (same source, group_by, sort_by) that differ only in order into a single index.
- Produce the "Indices to Build" list for Phase 1.

**Phase F — Emit ExecutionPlan** (`clinker-core::plan`):

```rust
// In clinker-core::plan::execution_plan
pub struct ExecutionPlan {
    pub source_dag: Vec<SourceTier>,         // Topologically sorted source tiers
    pub indices_to_build: Vec<IndexSpec>,     // Phase 1 work items
    pub transforms: Vec<TransformPlan>,       // Phase 2 execution units (contains cxl::TypedAst)
    pub output_projections: Vec<OutputSpec>,  // Phase 3 projection rules
    pub parallelism: ParallelismProfile,      // Thread pool config + per-transform classification
}
```

**`TypedAst` must be `Send + Sync`**: The compiled AST is constructed once at startup, placed in an `Arc<TypedAst>`, and shared as an immutable read-only value across all rayon worker threads during Phase 2 chunk processing. `cxl::TypedAst` must therefore implement both `Send` (can be transferred to other threads) and `Sync` (can be referenced from multiple threads simultaneously). This is enforced at compile time. Any AST node type that cannot be made `Send + Sync` (e.g., types containing `Rc<T>`, `Cell<T>`, or raw pointers) is disallowed in the type system. The evaluator calls take `&TypedAst` (shared immutable reference) — no interior mutability is permitted in the AST type hierarchy.

### 6.2 The --explain Flag

`clinker --explain pipeline.yaml` prints:
- Parsed AST for each CXL block (pretty-printed)
- Type annotations for every expression
- Source dependency DAG (as text or mermaid diagram)
- Indices to build (fields, grouping, sort)
- Parallelism classification per transform
- Estimated memory budget breakdown

No data is read. This is a compile-only mode.

The `cxl-cli` crate provides a standalone equivalent: `cxl check script.cxl` parses and type-checks a CXL script without a pipeline config, useful for rapid language development and CI validation of CXL scripts before deployment.

### 6.3 Dry-run Mode

`clinker --dry-run -n 100 pipeline.yaml` runs the full pipeline on the first 100 records per input. Validates types, CXL evaluation, output format. Writes output to stdout (or a temp file with `--dry-run-output`).

---

## 7. Execution Pipeline

### 7.1 Phase 1: Skinny Pass (Indexing)

Stream each input file. Extract only the fields listed in "Indices to Build." Do NOT hold full records.

**The Arena**: An `Arena` struct (see §3) per source — a `Vec<MinimalRecord>` paired with an `Arc<Schema>` for the indexed field subset. Each MinimalRecord contains only the fields specified in the "Indices to Build" list. Memory cost: ~100–200 bytes per record × 2M records = ~200–400MB worst case. For typical ≤100MB inputs with ≤500K records, this is 50–100MB.

**Secondary Indices**: `HashMap<Vec<GroupByKey>, Vec<usize>>` per window definition. Key = compound group_by field values converted to `GroupByKey` (see §3). Value = vector of usize pointers into the Arena.

**GroupByKey conversion rules at insertion time**:
- `Numeric` fields (inferred type `Int | Float`, no schema_override pin): always widen to `GroupByKey::Float` via `f64::to_bits()`. This ensures `"135"` and `"135.0"` map to the same partition key regardless of which appears first in the file.
- Fields pinned to `integer` via `schema_overrides`: use `GroupByKey::Integer`. A Float value in this position is a type error (routed per error strategy).
- `NaN` values: hard pipeline error, halt immediately (exit code 3). NaN in a group_by key is a data integrity failure.
- `String`, `Bool`, `Date`, `DateTime`: converted directly to their `GroupByKey` equivalents.
- `Array`: rejected at AST compile time — arrays are not valid group_by keys.

**Null handling**: Records missing a required group_by key are NOT inserted into the index. They will produce null for all window lookups in Phase 2.

**Array explosion**: `array_paths` entries with `mode: explode` are applied during Phase 1 streaming, at record-parse time. Each element of the named array path becomes a separate `MinimalRecord` in the Arena. The explosion rules are identical in Phase 2 (see §7.4) — the Arena record count therefore equals the Phase 2 record stream length, with every exploded row having a 1-to-1 correspondence between its Arena entry and its Phase 2 record. Window functions that `group_by` a field inside an exploded array are fully supported; each exploded row is indexed independently. `mode: join` entries are NOT exploded — the array is collapsed to a single `String` value in the MinimalRecord.

**Source-level parallelism**: Independent sources (those not referenced as cross-source lookups by other sources being indexed) run Phase 1 concurrently on separate OS threads via `std::thread::scope`.

### 7.2 Phase 1.5: Pointer Sorting

After indexing, iterate over each Secondary Index's value vectors (`Vec<usize>`).

Sort pointers in-place by looking up their sort_by fields in the Arena. Apply `nulls: first | last | drop` logic:
- `first`: Nulls sort before all non-null values.
- `last`: Nulls sort after all non-null values.
- `drop`: Remove null entries from the pointer vector entirely.

This is a CPU-bound operation on small vectors (partition sizes). Parallelize across partitions via `rayon::par_iter_mut` on the HashMap values.

**`sort_order` optimization**: If a source declares `sort_order: [field_a, field_b]` in config, and a window definition for that source has `sort_by: [{field: field_a, ...}, {field: field_b, ...}]` with matching fields and directions, Phase 1.5 pointer sorting for that window is **skipped** — the Arena was built in source order, which is already the correct partition order. This is the "declared pre-sorted" optimization: Phase E emits the window with `already_sorted: true` and Phase 1.5 skips the sort for those partitions. If `sort_order` only partially matches, the optimization does not apply.

### 7.3 Phase 1.7: API Intercept (Optional — v2)

Deferred to v2. When implemented:
- Config schema in YAML under a top-level `api_sources:` block
- Unique request parameters collected into `HashSet` during Phase 1
- Isolated `tokio` runtime fires concurrent requests with rate limiting, retry, and circuit breaker
- Responses streamed into a Virtual Arena with its own Secondary Index
- Auth: Bearer, Basic, API Key, OAuth2 (with token refresh)
- Error handling: configurable per-request (DLQ, retry, halt)

### 7.4 Phase 2: Fat Pass (Streaming Transformation)

Re-read the primary input file from disk. This time, read full records.

**Array explosion**: `array_paths` entries with `mode: explode` are applied during Phase 2 streaming using the same rules as Phase 1. A source record containing N array elements at an explode path produces N separate records in the stream. `mode: join` collapses elements to a single comma-delimited `String` field. The explosion step happens before CXL evaluation — the record passed to the evaluator is already the post-explosion row. Because Phase 1 and Phase 2 apply identical explosion rules, window partition lookups are always correct: each exploded row's Arena index corresponds exactly to its position in the Phase 2 stream.

**Chunk-based processing**: Read `chunk_size` records (default 1024) into a `Vec<Record>`. Process the chunk. Write results. Drop the chunk. Memory: ~1–5MB per chunk.

**Execution context** (per record):

```rust
pub struct ExecutionContext<'a> {
    pub record: &'a mut Record,
    pub arena: &'a Arena,
    pub indices: &'a SecondaryIndices,
    pub partition_position: usize,    // This record's position in its partition
    pub partition_key: Vec<Value>,    // Pre-computed group_by key
}
```

**Parallelism within chunks**: If the AST compiler classified a transform as `Stateless` or `IndexReading`, the chunk is processed via `rayon::par_iter_mut`. Otherwise, sequential.

**DLQ routing**: If a field is `required: true` in schema_overrides and missing, the raw record is written to the DLQ file immediately (streaming — no buffering). The record is skipped for transformation.

**Type error handling**: Governed by `error_handling.strategy`:
- `fail_fast`: Halt on first type error. No DLQ write. Produce a `miette` diagnostic with CXL rule name, row number, field name, expected type, actual value. For development/debugging only.
- `continue` (default): Route type-error rows to DLQ, continue processing. Halt when error rate (`error_count / total_records`) is strictly greater than `type_error_threshold` (default: 0.1 = 10%). Total record count is known from Phase 1 (Arena size). Checked at chunk boundaries.
- `best_effort`: Log type errors, skip the failing expression (field gets null), DLQ the row, never halt.

### 7.5 Phase 3: Projection Layer (Output)

**Field selection**:
- `emit` bindings from CXL become output fields.
- If `include_unmapped: true`, all source fields not explicitly excluded are also included.
- `mapping` renames are applied.
- `exclude` fields are dropped.
- **Compile-time guard (Phase C)**: If a CXL block produces no output fields — no `emit` statements AND `include_unmapped: false` on every targeted output — Phase C reports a compile error. An empty output is never silently emitted as zero-field records.

**Multi-cast**: A single transformed record can be broadcast to multiple outputs. Each output applies its own mapping/exclude/options independently. The record is cloned once per additional output (not per field).

**Null preservation**:
- `preserve_nulls: true` → XML: `<tag/>`, JSON: `"field": null`, CSV: empty cell
- `preserve_nulls: false` → XML: tag omitted, JSON: key omitted, CSV: empty cell (CSV always emits the column)

**Streaming vs. buffered output**:
- No `sort_output` → stream directly to output writers. O(1) memory.
- With `sort_output` → buffer all transformed records, sort, then flush. For large outputs, use external merge sort: sort chunks of `spill_threshold / estimated_record_size` records in memory, spill sorted chunks to temp files as NDJSON+LZ4, k-way merge via loser tree, stream merged output to final file.

---

## 8. Parallelism Architecture

### 8.1 Thread Pool Configuration

```rust
// Created once at startup, shared across the pipeline
let pool = rayon::ThreadPoolBuilder::new()
    .num_threads(config.concurrency.worker_threads)
    .thread_name(|i| format!("cxl-worker-{i}"))
    .build()
    .expect("Failed to create thread pool");
```

Default `worker_threads`: `min(num_cpus::get().saturating_sub(2), 4)`. This reserves cores for JVM processes. Overridable via config or `--threads` CLI flag.

### 8.2 Four Levels of Parallelism

**Level 1 — Source-level** (Phase 1): Independent inputs read and indexed concurrently via `std::thread::scope`. One OS thread per source. Arena and indices are constructed per-thread, then moved into a shared read-only `ExecutionContext`.

**Level 2 — Partition-level** (Phase 1.5): Pointer sorting across partitions parallelized via `rayon::par_iter_mut` on the HashMap values. Each partition's `Vec<usize>` is sorted independently.

**Level 3 — Chunk-level** (Phase 2): Within each chunk of N records, `rayon::par_iter_mut` processes records concurrently (for Stateless/IndexReading transforms). `par_iter_mut` is a blocking call — the calling thread does not proceed to the next chunk until all records in the current chunk are processed. This is the **chunk barrier**: chunks are always dispatched, processed, and forwarded to the writer in strict sequence. No reorder buffer is needed.

**Level 4 — Stage-level** (Phase 2, future): When multiple independent CXL transform blocks exist (no field dependencies between them), each block can run as a separate "lane" operating on the same chunk concurrently. Results merge at the projection layer. Deferred to v2 — implement sequential multi-transform first.

### 8.3 Parallelism Safety Classification

The AST compiler tags each CXL transform block:

| Classification | Criteria | Execution |
|---|---|---|
| `Stateless` | No `window.*` references, no cross-source lookups | `rayon::par_iter_mut` on chunks |
| `IndexReading` | References `window.*` but only reads from immutable Arena | `rayon::par_iter_mut` on chunks (safe because Arena is read-only after Phase 1) |
| `Sequential` | Contains inter-record state (running accumulators, row counters via `window.count()` used as a running counter) | Single-threaded, record-by-record |
| `CrossSource` | References a different input's index | `rayon::par_iter_mut` after dependency source completes Phase 1 |

The `--explain` flag prints this classification per transform.

### 8.4 Output Serialization

Output writing is always single-threaded per output file. The pattern:

```
[Phase 2 threads] --chunk--> [bounded channel] --> [writer thread]
```

Each output gets its own writer thread (for multi-cast) connected via a bounded `crossbeam::channel`. Channel capacity: 4 chunks (back-pressure if the writer falls behind). Chunk ordering is preserved (chunks enter the channel in sequence number order).

For `sort_output`, chunks are collected into a `Vec<Record>`, sorted after all chunks arrive, then streamed to the writer.

### 8.5 Deterministic Output Guarantee

Output order matches input order because:
- Chunks are read and dispatched sequentially by the main thread.
- `rayon::par_iter_mut` is a blocking barrier — the main thread does not advance to the next chunk until the current one is fully processed.
- Chunks therefore enter the §8.4 output channel in the exact order they were read.
- Within a chunk, `par_iter_mut` mutates records in-place in the `Vec<Record>`; positions are stable across parallel mutation, so record order within a chunk is preserved.

No reorder buffer is required. Given the same inputs and config, output is byte-identical across runs.

**v2 note**: If concurrent chunk dispatch (pipeline model) is added — e.g., to overlap I/O and compute on large files — a reorder buffer becomes necessary. The `ordered-channel` crate (pure Rust, `BinaryHeap`-backed, MIT/Apache-2.0) solves this cleanly: workers send `(seq_num, chunk)` pairs in any order; the receiver always reads them in sequence. Revisit if profiling shows I/O–compute overlap is worth the complexity.

---

## 9. Spill-to-Disk Strategy

### 9.1 When Spilling Occurs

The `MemoryBudget` tracks RSS using platform-native APIs. Spilling is triggered when:
- RSS exceeds `spill_threshold` (default: 60% of memory budget, ~245MB at default 512MB budget)
- OR a blocking stage (sort, distinct) accumulates more than `spill_threshold` bytes of in-memory data

**Cross-platform RSS tracking**:

| Platform | API | Cost | Notes |
|---|---|---|---|
| Linux | `/proc/self/statm` (file read) | ~1µs | Virtual file, kernel-provided. No crate dependency. |
| macOS | `mach_task_basic_info` via `mach2` crate | ~1µs | Mach kernel API. `mach2` is pure Rust FFI to the OS (no compiled C). |
| Windows | `K32GetProcessMemoryInfo` via `windows-sys` crate | ~1µs | Win32 API. `windows-sys` is pure Rust FFI bindings (no compiled C). |

All three APIs return resident set size with comparable accuracy. The `MemoryBudget` module provides a single `fn rss_bytes() -> Option<u64>` that compiles to the correct platform implementation via `#[cfg(target_os)]`. On unsupported platforms, `rss_bytes()` returns `None` and the budget falls back to allocation-counting only (tracking bytes allocated by the pipeline's own data structures, without OS-level RSS).

**Polling frequency**: RSS is checked once per chunk, at the start of each Phase 2 chunk loop iteration. All platform APIs complete in ~1µs — negligible at chunk boundaries.

### 9.2 Spill File Format

NDJSON compressed with LZ4 frames via `lz4_flex`. One record per line, serialized via `serde_json`.

Rationale: Arrow IPC would be faster but violates the "no Arrow" constraint. NDJSON+LZ4 is simple, debuggable (you can decompress and read spill files), and fast enough for ≤100MB inputs. The serialization cost is amortized by the I/O savings of not holding everything in memory.

### 9.3 External Merge Sort

When `sort_output` requires sorting more data than fits in memory:

1. Read chunks of records into memory (sized to `spill_threshold / estimated_record_size`)
2. Sort each chunk in memory via `sort_by` with the compound key comparator
3. Write each sorted chunk to a temp file (NDJSON+LZ4)
4. K-way merge: open all chunk files simultaneously, use a loser tree comparator
5. Stream merged output to the final output file

The loser tree performs O(log k) comparisons per output record, where k = number of spill files. For typical workloads (2–4 spill files), this is 1–2 comparisons per record.

**Spill file limit (`k_max = 16`)**: At most 16 spill files are held open simultaneously. This cap leaves ample headroom within default OS file descriptor limits (Linux: 1024 soft, macOS: 256 soft, Windows: 512 CRT) on servers where Clinker shares the host with other processes.

**Cascade merge**: If a sort produces more than 16 spill files (possible when `--memory-limit` is very low or data volumes exceed the typical workload), a preliminary merge pass runs before the final merge:
1. Merge files `[0..16)` using the loser tree → `tmp_merged_0` (single temp file)
2. Merge `[tmp_merged_0, 16..32)` → `tmp_merged_1`
3. Repeat until ≤ 16 files remain
4. Final k-way merge of remaining files → output

At no point are more than 17 FDs open for spill files (16 readers + 1 writer). Total comparisons remain O(n log k) over all passes. Cascade temp files use the same NDJSON+LZ4 format and are cleaned up immediately after each pass completes.

---

## 10. Error Handling & Diagnostics

### 10.1 Error Types

| Error | Behavior | Output |
|---|---|---|
| Config parse error | Halt before any data processing | `miette` diagnostic with YAML source span |
| CXL compile error | Halt before any data processing | `miette` diagnostic with CXL source span, type info, suggestions |
| Missing required field | Route row to DLQ, continue (unless `fail_fast`) | DLQ row with reason "missing required field: X" |
| Type coercion failure | Depends on `error_handling.strategy` | DLQ row or null substitution |
| `required: true` field — conversion failure | DLQ always, regardless of `error_handling.strategy` | DLQ row with reason "required field conversion failure: X (expected Y, got 'Z')". A bad value on a required field is equivalent to a missing value — the row is invalid regardless of strategy. `best_effort` does not substitute null for required fields. |
| I/O error (file not found, permission) | Halt immediately | `miette` diagnostic with file path |
| Memory budget exceeded | Trigger spill or halt if spill fails | Warning log, then error if unrecoverable |
| NaN in group_by field | Halt immediately (not DLQ) | `miette` diagnostic: field name, row number, source file. Exit code 3. Data integrity failure — NaN in a group_by key makes indexing undefined. |
| Aggregate type error | Route row to DLQ | DLQ row with reason "aggregate type error: sum/avg received non-Numeric value in field X". Fires when runtime type differs from Phase C static inference (e.g., explode:join produces Array). |
| NaN or Infinity in output field | Route row to DLQ, continue | DLQ row with reason "non-finite float in output field: X". The value never reaches the serializer. |

### 10.2 Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success, zero errors |
| 1 | Config or CXL compile error (no data processed) |
| 2 | Partial success — some rows routed to DLQ |
| 3 | Fatal data error — fail_fast triggered, error rate threshold exceeded, or NaN in group_by key |
| 4 | I/O or system error |

### 10.3 Signal Handling

- `SIGINT` (Ctrl-C): Flush all in-progress output buffers, close output files cleanly, write DLQ summary, exit with appropriate code. No partial/corrupt output files.
- `SIGTERM`: Same as SIGINT.
- Implementation: `ctrlc` crate (pure Rust) sets an `AtomicBool` flag checked at chunk boundaries.

### 10.4 DLQ File Format

The DLQ file is a CSV (default: `errors.csv`). Column layout:

| Column | Type | Description |
|--------|------|-------------|
| `_cxl_dlq_id` | String | UUID v7 — unique, time-ordered DLQ row identifier |
| `_cxl_dlq_timestamp` | String | ISO 8601 datetime when the row was DLQ'd (`%Y-%m-%dT%H:%M:%S`) |
| `_cxl_dlq_source_file` | String | Source file path — taken from `RecordProvenance.source_file` (see §3 Record Provenance) |
| `_cxl_dlq_source_row` | Integer | 1-based row number — taken from `RecordProvenance.source_row` |
| `_cxl_dlq_error_category` | String | One of: `missing_required_field`, `type_coercion_failure`, `required_field_conversion_failure`, `nan_in_output_field`, `aggregate_type_error`, `validation_failure` |
| `_cxl_dlq_error_detail` | String | Full error message (human-readable) |
| *(source fields)* | varies | All original source fields in schema order, serialized as their raw string values (pre-transformation). For XML/JSON sources: a single column `_cxl_source_record` containing the raw record serialized as a JSON string. |

All `_cxl_` columns are always present regardless of `include_reason` and `include_source_row` config flags. Those flags control whether the last two logical groups (error columns and source fields) are written: `include_reason: false` suppresses `_cxl_dlq_error_category` and `_cxl_dlq_error_detail`; `include_source_row: false` suppresses all source field columns. The first four columns (`_cxl_dlq_id`, `_cxl_dlq_timestamp`, `_cxl_dlq_source_file`, `_cxl_dlq_source_row`) are always written.

### 10.5 Progress Reporting

For files with known size, report progress to stderr:
```
[cxl] orders.xml: Phase 1 indexing... 150,000/500,000 records (30%) [12.3s]
[cxl] orders.xml: Phase 2 transforming... 250,000/500,000 records (50%) [8.1s, ~8.1s remaining]
```

Throttled to 1 update/second. Disabled with `--quiet`.

---

## 11. Workspace Architecture

The project is a Cargo workspace monorepo with six crates. The dependency graph is strictly one-directional — no cycles, no reverse dependencies. Every crate has a clear reason to exist: shared data model, language, language tooling, I/O, orchestration, CLI.

### 11.1 Directory Structure

```
clinker/
├── Cargo.toml                          # Workspace root
├── Cargo.lock                          # Single lockfile for the entire project
├── crates/
│   ├── clinker-record/                 # Shared data model (the lingua franca)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # Re-exports Value, Schema, Record, MinimalRecord
│   │       ├── value.rs                # Value enum, type coercion, Display, Serialize/Deserialize
│   │       ├── schema.rs              # Schema (column names + index), Arc sharing
│   │       ├── record.rs              # Record (schema-indexed Vec<Value> + overflow)
│   │       └── minimal.rs             # MinimalRecord (Phase 1 subset)
│   │
│   ├── cxl/                            # The CXL language (zero pipeline/I/O dependencies)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # Public API: parse(), check(), evaluate()
│   │       ├── lexer.rs                # Token definitions
│   │       ├── parser.rs              # hand-rolled Pratt parser → untyped AST
│   │       ├── ast.rs                  # AST node types
│   │       ├── types.rs                # Type system, constraint inference, checking
│   │       ├── resolver.rs             # Field reference resolution (trait-based)
│   │       ├── analyzer.rs             # Dependency analysis, parallelism classification
│   │       ├── functions.rs            # Built-in function registry and type signatures
│   │       └── evaluator.rs            # Runtime AST evaluation against trait contexts
│   │
│   ├── cxl-cli/                        # Standalone CXL validator / REPL
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs                 # cxl check, cxl eval, cxl fmt commands
│   │
│   ├── clinker-format/                 # I/O adapters (pure readers/writers, no pipeline logic)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # FormatReader / FormatWriter traits
│   │       ├── csv.rs                  # CSV reader/writer (streaming, configurable delimiter/quoting/encoding)
│   │       ├── xml.rs                  # XML reader/writer (pull-parser, namespace handling, attributes)
│   │       ├── json.rs                 # JSON reader/writer (array, NDJSON, nested object)
│   │       ├── fixed_width.rs          # Fixed-width reader/writer (schema-driven field extraction)
│   │       ├── multi_record.rs         # Multi-record dispatcher (discriminator matching, per-type routing)
│   │       ├── schema.rs               # Schema loading, inherits resolution, override merging (§4.1)
│   │       ├── detect.rs               # Auto-detect JSON format (array vs NDJSON vs object)
│   │       └── options.rs              # FormatOptions structs (shared across reader/writer)
│   │
│   ├── clinker-core/                   # Config, pipeline orchestration, execution engine
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # Public API: execute(), explain(), dry_run()
│   │       ├── error.rs               # ClinkerError enum, miette integration
│   │       ├── config/
│   │       │   ├── mod.rs              # Top-level PipelineConfig deserialization
│   │       │   ├── input.rs            # InputConfig, format options
│   │       │   ├── output.rs           # OutputConfig, mapping, projection
│   │       │   ├── transform.rs        # TransformConfig, local_window
│   │       │   ├── error_handling.rs   # ErrorStrategy, DLQ config
│   │       │   ├── concurrency.rs      # Thread pool, chunk size config
│   │       │   └── env.rs              # ${VAR} interpolation
│   │       ├── plan/
│   │       │   ├── mod.rs
│   │       │   ├── execution_plan.rs   # ExecutionPlan struct
│   │       │   ├── index_planner.rs    # Window deduplication, "Indices to Build"
│   │       │   └── source_dag.rs       # Source dependency DAG, topological sort
│   │       └── pipeline/
│   │           ├── mod.rs
│   │           ├── executor.rs         # Orchestrates Phase 1 → 1.5 → 2 → 3
│   │           ├── arena.rs            # Vec<MinimalRecord>, Arena construction
│   │           ├── index.rs            # Secondary index (HashMap) construction
│   │           ├── context.rs          # ExecutionContext (implements cxl traits)
│   │           ├── chunk.rs            # Chunk reading, sequence numbering
│   │           ├── memory.rs           # MemoryBudget, cross-platform RSS tracking (§9.1), spill triggers
│   │           ├── sort.rs             # In-memory sort, external merge sort
│   │           ├── spill.rs            # NDJSON+LZ4 spill file read/write
│   │           ├── loser_tree.rs       # K-way merge comparator
│   │           ├── dlq.rs              # Dead letter queue writer
│   │           └── progress.rs         # Progress bar / reporting
│   │
│   └── clinker/                        # CLI binary (thin wrapper)
│       ├── Cargo.toml
│       └── src/
│           └── main.rs                 # Arg parsing, logging setup, calls clinker_core::execute()
│                                       # CLI flags (all override YAML config):
│                                       #   --memory-limit <e.g. "512MB">
│                                       #   --threads <int>
│                                       #   --error-threshold <float 0.0–1.0>
│                                       #   --batch-id <string>  (overrides auto-generated UUID v7)
│                                       #   --rules-path <dir>   (module search path, default: ./rules/)
│                                       #   --log-level <level>  (trace|debug|info|warn|error)
│                                       #   --explain            (print execution plan, exit)
│                                       #   --dry-run            (validate config + CXL, exit)
│
├── tests/                              # Workspace-level integration tests
│   ├── fixtures/                       # Sample CSV, XML, JSON, YAML pipeline configs
│   └── integration.rs
└── docs/
    └── spec.md                         # This document
```

### 11.2 Crate Dependency Graph

```
clinker (binary)
  └── clinker-core (library)
        ├── cxl (library)
        │     └── clinker-record (library)
        ├── clinker-format (library)
        │     └── clinker-record (library)
        └── clinker-record (library)

cxl-cli (binary)
  ├── cxl (library)
  └── clinker-record (library)
```

Every arrow points down. No cycles. `clinker-record` is the foundation that everything builds on. `cxl` and `clinker-format` are peers that share the data model but know nothing about each other. `clinker-core` orchestrates them.

### 11.3 Crate Boundary Rules

| Crate | Responsibility | Does NOT know about |
|---|---|---|
| `clinker-record` | `Value`, `Schema`, `Record`, `MinimalRecord`, type coercion, serialization | CXL, pipelines, file formats, config, I/O |
| `cxl` | CXL parsing, AST, type checking, expression evaluation, built-in function signatures | Pipelines, file formats, config YAML, I/O, memory budgets |
| `cxl-cli` | Interactive CXL script validation, REPL, `cxl check` / `cxl eval` / `cxl fmt` | Pipelines, file formats, config |
| `clinker-format` | CSV/XML/JSON/fixed-width readers and writers, multi-record dispatcher, schema loading, format auto-detection, streaming I/O | CXL, pipelines, config YAML, memory budgets |
| `clinker-core` | Config parsing, execution planning, pipeline orchestration, Arena, indexing, sorting, spilling, memory management, DLQ | CLI argument parsing, logging subscriber setup |
| `clinker` | CLI entry point, argument parsing, logging subscriber setup, `main()` | Nothing — it's the top-level binary |

### 11.4 Trait Boundaries

#### Between clinker-record and cxl

The `cxl` crate operates on `clinker_record::Value` directly — no conversion layer, no duplicate type. The CXL evaluator returns `Value`, the built-in functions accept and return `Value`, and the type checker maps to `Value` variants.

**Why `FieldResolver::resolve` returns `Option<Value>` (owned) rather than `Option<&Value>`:**

Returning a borrowed `&Value` would require a lifetime parameter threading through every evaluator call site — `fn eval<'a>(&self, expr: &Expr, ctx: &'a dyn FieldResolver) -> Value` — mixing owned (operator results, literals) and borrowed (field reads) at every node. The ergonomic cost is high and the benefit is small: even with `Option<&Value>`, the evaluator clones at the first operator that consumes the value (e.g., `Amount + 1` clones to produce `Integer(n + 1)` regardless). For the expected workload (2–4 indexed fields, mostly numeric/date types that are stack-only clones), owned `Value` is the right call. Measure before changing.

#### Between cxl and clinker-core

The `cxl` crate defines traits that `clinker-core` implements to inject pipeline context into the evaluator:

```rust
// In cxl::resolver

use clinker_record::Value;

/// Resolve a field name to a value from the current record.
pub trait FieldResolver {
    fn resolve(&self, name: &str) -> Option<Value>;
    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value>;
}

/// Access window partition data (Arena + Secondary Index).
pub trait WindowContext {
    /// Positional functions return a boxed resolver to avoid lifetime entanglement
    /// with the Arena. One heap allocation per call site (not per record or per field).
    fn first(&self) -> Option<Box<dyn FieldResolver>>;
    fn last(&self) -> Option<Box<dyn FieldResolver>>;
    fn lag(&self, offset: usize) -> Option<Box<dyn FieldResolver>>;
    fn lead(&self, offset: usize) -> Option<Box<dyn FieldResolver>>;
    fn count(&self) -> i64;
    fn sum(&self, field: &str) -> Value;
    fn avg(&self, field: &str) -> Value;
    fn min(&self, field: &str) -> Value;
    fn max(&self, field: &str) -> Value;
    fn any(&self, predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool;
    fn all(&self, predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool;
}
```

The positional functions (`first`, `last`, `lag`, `lead`) return `Option<Box<dyn FieldResolver>>` rather than `Option<&dyn FieldResolver>`. The borrowed form would require the returned reference to be tied to `&self`'s lifetime, which cannot be expressed without GATs and would complicate the `cxl` evaluator's call sites. A single `Box` allocation per window positional call is the accepted cost — it is O(1) per expression evaluation, not O(records).

`clinker-core::pipeline::context` implements `FieldResolver` for `Record` (delegates to `record.get()`) and `WindowContext` for `ExecutionContext` (delegates to `Arena::resolver_at()` wrapped in `Box`). The `cxl` evaluator never sees a `Record`, an `Arena`, or a `HashMap` — it works entirely through these trait interfaces.

**`it` binding in `any` / `all` predicates**: The `&dyn FieldResolver` argument passed to the predicate closure *is* the `it` variable. The CXL evaluator compiles a `predicate_expr` node into a Rust closure `|it: &dyn FieldResolver| -> bool`. Inside that closure, unqualified field refs (e.g., `Amount`) resolve against the **captured** outer `EvalContext` (the primary record); `it.Amount` resolves against the closure argument (the current iteration element from the partition). This mapping is an internal evaluator concern — the trait boundary is clean. The `WindowContext` impl iterates the partition's `Vec<usize>`, calling `Arena::resolver_at(idx)` for each element and passing it into the predicate. No `it` resolver is allocated outside a `predicate_expr` evaluation.

#### Between clinker-format and clinker-core

The `clinker-format` crate defines reader/writer traits. `clinker-core` calls them:

```rust
// In clinker_format

use clinker_record::{Record, Schema};

/// Streaming record reader. Yields records one at a time.
pub trait FormatReader: Send {
    /// Peek at the schema (reads headers/first element, resets position).
    fn schema(&mut self) -> Result<Arc<Schema>>;
    /// Yield the next record, or None at EOF.
    fn next_record(&mut self) -> Result<Option<Record>>;
}

/// Streaming record writer. Consumes records one at a time.
pub trait FormatWriter: Send {
    fn write_record(&mut self, record: &Record) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}
```

`clinker-core` constructs a `CsvReader`, `XmlReader`, or `JsonReader` (all from `clinker-format`) based on the config, then calls `next_record()` in a loop. It never touches `csv::Reader`, `quick_xml::Reader`, or `serde_json::StreamDeserializer` directly — format-specific logic is fully encapsulated.

### 11.5 Why NOT extracted into separate crates

The following components remain inside `clinker-core` because they lack independent testability, have a single consumer, or are too small to justify a crate boundary:

| Component | Lines (est.) | Reason to keep in clinker-core |
|---|---|---|
| Arena + Secondary Index | ~200 | Two standard containers with thin wrappers. Can't be tested meaningfully without a format reader and pipeline executor. |
| Execution Plan / Index Planner | ~300 | Bridge between `cxl` analysis output and pipeline execution. Inherently coupled to both sides. |
| External Merge Sort / Loser Tree | ~400 | Domain-specific sort comparator tied to `Record` ordering semantics and `MemoryBudget` thresholds. |
| Spill (NDJSON+LZ4) | ~200 | Implementation detail of memory management. Single consumer (sort/distinct). |
| Config parsing | ~500 | Every struct consumed by exactly one place in `clinker-core`. Extracting would require re-exporting everything. |

---

## 12. Build & Operational Configuration

### 12.1 Workspace Cargo.toml

```toml
[workspace]
resolver = "2"
members = [
    "crates/clinker-record",
    "crates/cxl",
    "crates/cxl-cli",
    "crates/clinker-format",
    "crates/clinker-core",
    "crates/clinker",
]

[workspace.dependencies]
# Shared dependency versions — each crate picks what it needs
clinker-record = { path = "crates/clinker-record" }
cxl = { path = "crates/cxl" }
clinker-format = { path = "crates/clinker-format" }
clinker-core = { path = "crates/clinker-core" }

serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde-saphyr = "2"
chrono = { version = "0.4", features = ["serde"] }
miette = { version = "7", features = ["fancy"] }
clap = { version = "4", features = ["derive"] }
regex = "1"
ahash = "0.8"
indexmap = "2"
tracing = "0.1"

[workspace.metadata.release]
# Ensure all crates are versioned in lockstep
shared-version = true

[profile.release]
lto = "fat"
codegen-units = 1
strip = "symbols"
panic = "abort"
```

### 12.2 Individual Crate Cargo.toml (dependency inheritance)

```toml
# crates/clinker-record/Cargo.toml
[package]
name = "clinker-record"
version.workspace = true
edition = "2024"
description = "Core data types (Value, Schema, Record) for the Clinker ETL engine"

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
ahash = { workspace = true }
indexmap = { workspace = true }
```

```toml
# crates/cxl/Cargo.toml
[package]
name = "cxl"
version.workspace = true
edition = "2024"
description = "CXL language parser, type checker, and evaluator"

[dependencies]
clinker-record = { workspace = true }
miette = { workspace = true }
chrono = { workspace = true }
regex = { workspace = true }
ahash = { workspace = true }
# No parser combinator crate — parser is hand-rolled (Pratt + recursive descent)
```

```toml
# crates/clinker-format/Cargo.toml
[package]
name = "clinker-format"
version.workspace = true
edition = "2024"
description = "Streaming CSV, XML, JSON, and fixed-width readers/writers for Clinker"

[dependencies]
clinker-record = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
csv = "1"
quick-xml = "0.37"
chrono = { workspace = true }
miette = { workspace = true }
```

```toml
# crates/clinker-core/Cargo.toml
[package]
name = "clinker-core"
version.workspace = true
edition = "2024"
description = "Pipeline orchestration, config parsing, and execution engine for Clinker"

[dependencies]
clinker-record = { workspace = true }
cxl = { workspace = true }
clinker-format = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
serde-saphyr = { workspace = true }
rayon = "1"
crossbeam-channel = "0.5"
chrono = { workspace = true }
regex = { workspace = true }
ahash = { workspace = true }
lz4_flex = "0.11"
tempfile = "3"
tracing = { workspace = true }
miette = { workspace = true }
glob = "0.3"
uuid = { version = "1", features = ["v7"] }
ctrlc = "3"
```

```toml
# crates/clinker/Cargo.toml
[package]
name = "clinker"
version.workspace = true
edition = "2024"
description = "CLI for the Clinker ETL engine"

[dependencies]
clinker-core = { workspace = true }
clap = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt", "ansi"] }
miette = { workspace = true }
```

```toml
# crates/cxl-cli/Cargo.toml
[package]
name = "cxl-cli"
version.workspace = true
edition = "2024"
description = "Standalone CXL language validator and REPL"

[dependencies]
cxl = { workspace = true }
clinker-record = { workspace = true }
clap = { workspace = true }
miette = { workspace = true }
serde_json = { workspace = true }
```

### 12.3 Binary size target

With `strip` and `panic = "abort"`, target ≤10MB release binary for the `clinker` crate.

### 12.4 CI checks

- `cargo clippy --workspace -- -D warnings`
- `cargo test --workspace` — full suite
- `cargo test -p clinker-record` — data model in isolation
- `cargo test -p cxl` — CXL language tests independent of pipeline
- `cargo test -p clinker-format` — format readers/writers against fixture files
- `cargo audit` (no known vulnerabilities)
- `cargo deny check` (verify no C dependencies in tree)
- Benchmark suite: process a 50MB XML file, a 20MB CSV, and a 10MB JSON. Track throughput (records/sec) and peak RSS across releases.

---

## 13. v1 Scope vs. v2 Deferral

### v1 (Build Now)

- `clinker-record`: Core data model (Value, Schema, Record, MinimalRecord)
- `cxl`: CXL parser, type checker, evaluator with full function library
- `cxl-cli`: Standalone `cxl check` / `cxl eval` validator
- `clinker-format`: CSV, XML, JSON, fixed-width readers/writers; multi-record dispatcher; schema loading + override resolution
- `clinker-core`: Config parsing with `serde-saphyr`, two-pass pipeline (Skinny Pass → Pointer Sort → Fat Pass → Projection), source-level and chunk-level parallelism, memory budget with spill-to-disk, DLQ, external merge sort for `sort_output`
- `clinker`: CLI binary with `--explain`, `--dry-run`, progress reporting, signal handling (graceful shutdown), exit codes

### v2 (Deferred)

- API Intercept phase (HTTP sources with auth, retry, circuit breaker)
- Database sources (sqlx)
- Stage-level parallelism (independent transform lanes)
- Pipeline parallelism (overlapping Phase 1 / Phase 2)
- Cloud secrets providers
- Watch mode (directory polling)
- OAuth2 token refresh
- `rust_decimal` for financial precision
- CXL user-defined functions
- CXL `match` / pattern matching expressions

---

## 14. Security

- **Path validation**: All file paths in YAML config are resolved relative to a configurable `--base-dir` (default: directory containing the YAML file). Paths containing `..` are rejected. Absolute paths are rejected unless `--allow-absolute-paths` is passed.
- **YAML DoS protection**: `serde-saphyr` budget constraints limit recursion depth, sequence length, and document size.
- **Environment variable injection**: Only `${VAR}` syntax is supported (not shell expansion). Variable names are validated against `[A-Z_][A-Z0-9_]*`.
- **Output overwrite protection**: By default, refuse to overwrite existing output files. Override with `--force` or `overwrite: true` in output config.
