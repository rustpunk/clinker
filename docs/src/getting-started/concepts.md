# Key Concepts

This page covers the mental model behind Clinker pipelines. If you have
experience with other ETL tools, most of this will feel familiar -- but pay
attention to where Clinker diverges, especially around CXL and streaming
execution.

## Pipelines are DAGs

A pipeline is a directed acyclic graph of nodes. Data flows from sources,
through processing nodes, to outputs. There are no cycles -- a node cannot
consume its own output, directly or indirectly.

You define the graph by setting `input:` on each consumer node, naming the
upstream node it reads from. Clinker resolves these references, validates that
the graph is acyclic, and determines execution order automatically.

## The `nodes:` list

Every pipeline has a single flat list of nodes. Each node has a `type:`
discriminator that determines its behavior. The seven node types are:

| Type          | Purpose                                                     |
|---------------|-------------------------------------------------------------|
| `source`      | Read data from a file (CSV, JSON, XML, fixed-width)         |
| `transform`   | Apply CXL logic to reshape, filter, or enrich records       |
| `aggregate`   | Group records and compute summary values (sum, count, etc.) |
| `route`       | Split a stream into named ports based on conditions          |
| `merge`       | Combine multiple streams into one                            |
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

## Streaming execution

Records flow through the pipeline one at a time. Clinker does not load an
entire file into memory before processing it. A source reads one record,
pushes it through the downstream nodes, and then reads the next.

This design keeps memory usage bounded regardless of file size. A 100 GB CSV
is processed with the same memory footprint as a 100 KB CSV.

The exception is aggregation. Aggregate nodes must accumulate state across
records (to compute sums, counts, averages, etc.). Clinker uses hash
aggregation by default and spills to disk when memory pressure exceeds
configured limits. When the input is already sorted by the group key, Clinker
can use streaming aggregation, which requires only constant memory.

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
