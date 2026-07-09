# Non-Goals

This page lists what Clinker is deliberately **not**. These are
architectural commitments — design surfaces Clinker will not grow into,
not just features that haven't been built yet.

If you arrived here because you were considering Clinker for one of the
scenarios below, the answer is "a different tool is the right fit." Each
non-goal is paired with the kind of tool that is the right fit.

## Not an unbounded stream processor

Clinker reads sources that have an end. A pipeline run is a **finite
job**: Sources read until EOF, the DAG drains, the process exits.

**Out of scope:**

- Kafka topics, Kinesis streams, Pub/Sub subscriptions (long-running
  consumers without a natural end).
- Server-Sent Events, WebSocket subscriptions, webhooks-as-input.
- `tail -f`-style file followers.
- Watermarking against wall-clock time.
- Exactly-once delivery across process restarts.
- Stateful infinite-stream windowing (tumbling / sliding / session
  windows over event time without a finite boundary).

**Right fit instead:** Apache Flink, Kafka Streams, Apache Beam in
unbounded mode, Vector with streaming sources, Benthos with streaming
inputs, Apache NiFi.

## Not a multi-process or distributed engine

One `clinker run` invocation is one operating-system process. Clinker
does not spawn worker processes, does not coordinate a cluster, and does
not shuffle data between machines.

**Out of scope:**

- Worker-process pools on a single machine.
- Multi-machine sharded execution.
- Network shuffle between executors.
- Cluster managers (Kubernetes operators, YARN, Mesos integrations).
- Distributed memory accounting.
- Partial-failure recovery across worker boundaries.

**Right fit instead:** Apache Spark, Trino / Presto, Apache Flink in
cluster mode, Apache Beam on Dataflow, Hadoop MapReduce.

**Scaling Clinker:** give the host more cores, more RAM, more disk — the
DuckDB / Polars / Kettle / Hop model. If a single host genuinely can't
fit the work, partition the input by file or by key and run multiple
`clinker` invocations from a shell script. That's a five-line script,
not an architectural addition.

## Not a long-running service

Clinker is a **CLI binary**, not a server. There is no daemon mode, no
HTTP control plane, no JDBC/ODBC listener, no UI server, no scheduled
job runner inside Clinker itself.

**Out of scope:**

- HTTP API exposing pipeline execution.
- Built-in cron / scheduler / orchestrator.
- Persistent connection pool living across pipeline runs.
- A long-lived process accepting new pipeline submissions over a socket.

**Right fit instead:**

- For scheduling: cron, systemd timers, Airflow, Dagster, Prefect,
  Temporal.
- For HTTP-fronted ETL: any of the above orchestrators wrapping
  `clinker run` invocations.
- For interactive queries against finite data: DuckDB, Polars, or any
  embedded query engine.

Orchestration by Temporal (and the others above) is supported via a
**shell-out contract** — the orchestrator runs `clinker run` as a child
process and reads its exit code, logs, and metrics. Clinker embeds no
Temporal client or worker; that coupling is a decided non-goal
([issue #622](https://github.com/rustpunk/clinker/issues/622)). See
[Running Under a Workflow Orchestrator](ops/orchestrator-contract.md) for
the exit-code, cancellation, and output-atomicity guarantees that contract
depends on.

## Not an OLAP / SQL query engine

Clinker is a **per-record expression engine** with explicit `nodes:` in
a DAG. It does not parse SQL, does not optimize joins via cost-based
optimization across the whole pipeline, and does not present a relational
table model.

**Out of scope:**

- SQL parsing (the CXL language is the surface; no `SELECT ... FROM` is
  accepted).
- Cost-based join reordering across more than the local Combine node.
- Materialized views or query caching.
- Interactive query latencies under a second.
- ANSI-SQL semantics for NULL, type coercion, or aggregate behavior.

**Right fit instead:** DuckDB, ClickHouse, DataFusion, Trino, Postgres,
or any RDBMS. If you want SQL-driven transformation over files, DuckDB
is the closest single-binary alternative to Clinker for the cases where
SQL is the right surface.

## Not a connector marketplace

Clinker ships with a deliberately small set of source and sink types:
CSV, JSON, XML, fixed-width files in the current release; finite-cursor
REST and SQL sources on the roadmap. There is no plugin registry, no
third-party connector store, no SaaS-API catalog.

**Out of scope:**

- Hundreds of pre-built SaaS integrations (Salesforce, HubSpot,
  Stripe, etc.).
- A central registry of community-maintained connectors.
- Schema discovery against arbitrary external APIs.
- Change-data-capture (CDC) sources.

**Right fit instead:** Airbyte, Fivetran, Stitch, Singer with its tap
ecosystem, dlt (data load tool).

## Not a streaming-CDC engine

Clinker treats each pipeline run as a **fresh, finite pass** over the
input. It does not maintain a persistent log of source changes, does
not replicate row-level changes from a database, and does not produce
an append-only stream of inserts / updates / deletes.

**Out of scope:**

- Postgres logical replication subscriptions.
- MySQL binlog tailing.
- Debezium-style CDC stream production.
- Maintaining a target database in continuous sync with a source.

**Right fit instead:** Debezium, Maxwell, AWS DMS, Striim, Estuary Flow,
or vendor-native CDC like Snowflake Streams.

## What Clinker **is**

For the positive framing, see the [Introduction](README.md) and
[Key Concepts](getting-started/concepts.md). The short version:

- A pure-Rust, single-binary, **bounded-memory batch DAG executor** for
  finite file and finite-cursor inputs.
- Per-record evaluation through a directed acyclic graph of Source,
  Transform, Aggregate, Route, Merge, Combine, Output, and Composition
  nodes.
- Pipelines declared in YAML, transformation logic written in CXL (a
  custom per-record expression language).
- One process, finite job, EOF-then-exit. Disk spill under memory
  pressure rather than OOM.
