# Key Concepts

This page covers the mental model behind Clinker pipelines. If you have
experience with other ETL tools, most of this will feel familiar -- but pay
attention to where Clinker diverges, especially around CXL, per-record
evaluation, and the memory budget.

## Batch jobs, not unbounded streams

A Clinker run is a **finite batch job**. Source nodes read their files until
EOF, the DAG drains, and the process exits. There are no watermarks against
wall-clock time, no infinite-source semantics, no exactly-once delivery across
restarts. If you have used Flink, Kafka Streams, or Beam in unbounded mode:
Clinker is not that.

The word "streaming" in Clinker's documentation always refers to **per-record
evaluation within a single batch run** -- records flow through the graph one
at a time rather than being materialized as a whole table -- not to
long-running stream-processor semantics. Internal identifiers in the codebase
(function names like `streaming_output_task`, config fields like
`strategy: streaming`, error messages, log lines) use the word in the same
row-by-row sense; if you see it in a stack trace, it is not Flink leaking
through.

## Pipelines are DAGs

A pipeline is a directed acyclic graph of nodes. Data flows from sources,
through processing nodes, to outputs. There are no cycles -- a node cannot
consume its own output, directly or indirectly.

You define the graph by setting `input:` on each consumer node, naming the
upstream node it reads from. Clinker resolves these references, validates that
the graph is acyclic, and determines execution order automatically.

## The `nodes:` list

Every pipeline has a single flat list of nodes. Each node has a `type:`
discriminator that determines its behavior. The eight node types are:

| Type          | Purpose                                                     |
|---------------|-------------------------------------------------------------|
| `source`      | Read data from a file (CSV, JSON, XML, fixed-width)         |
| `transform`   | Apply CXL logic to reshape, filter, or enrich records       |
| `aggregate`   | Group records and compute summary values (sum, count, etc.) |
| `route`       | Split a stream into named ports based on conditions          |
| `merge`       | Concatenate multiple streams that share a schema             |
| `combine`     | Join records across N inputs with cross-input predicates     |
| `output`      | Write data to a file                                         |
| `composition` | Embed a reusable sub-pipeline                                |

You can have as many nodes of each type as your pipeline requires. The only
constraint is that the resulting graph must be a valid DAG.

## CXL is not SQL

CXL is a per-record expression language. Each record flows through a CXL block
independently -- there is no table-level context, no `SELECT`, no `FROM`, no
`JOIN`. Think of it as a programmable row mapper.

The core statements:

- **`emit name = expr`** -- produce a field in the output record. Only emitted
  fields appear downstream. If you want to pass a field through unchanged, you
  must emit it explicitly: `emit id = id`.
- **`let name = expr`** -- bind a local variable for use in later expressions.
  Local variables do not appear in the output.
- **`filter condition`** -- discard the record if the condition is false. A
  filtered record produces no output and is not counted as an error.
- **`distinct`** / **`distinct by field`** -- deduplicate records. `distinct`
  deduplicates on all output fields; `distinct by field` deduplicates on a
  specific field.

CXL uses `and`, `or`, and `not` for boolean logic -- not `&&` or `||`. String
concatenation uses `+`. Conditional expressions use
`if ... then ... else ...` syntax.

System namespaces use a `$` prefix: `$pipeline.*`, `$window.*`, `$meta.*`.
These provide access to pipeline metadata, window function state, and record
metadata respectively.

## Per-record evaluation and the memory budget

Within a run, records flow through the pipeline **one at a time**. Clinker
does not load an entire file into memory before processing it. A source reads
one record, pushes it through the downstream nodes, and then reads the next.
This is what "streaming" means in Clinker -- row-by-row evaluation inside a
finite batch job, not Flink-style unbounded stream processing.

Per-record evaluation keeps **per-row** memory usage bounded for the
stateless parts of the graph (Transform, Route, Merge, most Combine
probe-side work, Output). The DAG executor itself materializes intermediate
buffers between non-fused stages, so the overall memory footprint scales with
the largest live intermediate stage's output -- not with total input size.
For a fused streaming path (Source → Transform → Output, no branching, no
fan-out), no intermediate buffer materializes and a 100 GB CSV passes through
with the same footprint as a 100 KB CSV. For general DAG shapes (diamonds,
Route fan-out, Merge fan-in, Composition bodies), the predecessor's output
stays in `node_buffers` until every consumer has read it -- and that buffer
size grows with input size unless an upstream blocking operator caps it.

**Stateful operators must accumulate.** Aggregate, sort, and grace-hash
Combine cannot emit until they have seen enough input -- sums need every
addend, a full sort needs the last row, a hash join needs the build side
complete. These operators run inside a configured RSS budget (default 512 MB)
and **degrade gracefully** under pressure rather than OOM:

- **Aggregate** uses hash aggregation by default and spills partitions to
  disk when soft/hard memory thresholds trip. When the input is already
  sorted by the group key, the planner picks streaming aggregation, which
  requires only constant memory.
- **Sort** spills runs to disk and merges them.
- **Combine** picks among in-memory hash join, grace hash join (spilled), and
  IEJoin / sort-merge depending on predicates and memory pressure.

The memory ceiling is a first-class promise. Clinker is designed to share a
server with JVM applications, databases, and other services without competing
for RAM.

## Input wiring

Consumer nodes reference their upstream via the `input:` field:

```yaml
- type: transform
  name: enrich
  input: customers    # reads from the node named "customers"
```

Route nodes produce named output ports. Downstream nodes reference a specific
port using dot notation:

```yaml
- type: route
  name: split_by_region
  input: customers
  config:
    routes:
      us: region == "US"
      eu: region == "EU"
    default: other

- type: output
  name: us_output
  input: split_by_region.us    # reads from the "us" port
```

Merge nodes accept multiple inputs using `inputs:` (plural):

```yaml
- type: merge
  name: combined
  inputs:
    - us_transform
    - eu_transform
```

## Schema declaration

Source nodes require an explicit `schema:` that declares every column's name
and type:

```yaml
config:
  schema:
    - { name: customer_id, type: int }
    - { name: email, type: string }
    - { name: balance, type: float }
    - { name: created_at, type: date }
```

Clinker uses these declarations to type-check CXL expressions at compile time,
before any data is read. If a CXL block references a field that does not exist
in the upstream schema, or applies an operation to an incompatible type, the
error is caught during validation -- not at row 5 million of a production run.

Supported types include `int`, `float`, `string`, `bool`, `date`, and
`datetime`.

## Error handling

Each node can specify an error handling strategy:

| Strategy      | Behavior                                                      |
|---------------|---------------------------------------------------------------|
| `fail_fast`   | Stop the pipeline on the first error (default)                |
| `continue`    | Route error records to a dead-letter queue file and continue  |
| `best_effort` | Log errors and continue without writing error records         |

When using `continue`, Clinker writes rejected records to a DLQ file alongside
the output. Each DLQ entry includes the original record, the error category,
the error message, and the node that rejected it. This makes diagnosing
production issues straightforward: check the DLQ, fix the data or the
pipeline, and rerun.
