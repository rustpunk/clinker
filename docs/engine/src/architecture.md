# Overview & Pillars

Clinker is a **bounded-memory batch DAG executor**. A pipeline run is a finite job over finite input: Source nodes read until EOF, the DAG drains, the process exits with a status code. It pairs a custom expression language (CXL) with YAML pipeline orchestration.

Within a run, stateless operators (Transform, Route, most Combine probe-side work, Output) evaluate records **one at a time** without per-record state accumulation. The DAG executor materializes intermediate buffers between non-fused stages, so memory scales with the largest live intermediate stage's output, not total input size; fused Source → Transform → Output paths skip materialization entirely. Blocking operators (Aggregate, sort, grace-hash Combine) accumulate state inside the configured RSS budget (default 512 MB) and spill to disk when soft/hard thresholds trip rather than OOM the process.

## The three pillars

Every design decision cascades from three commitments. They are permanent — an architectural proposal that violates any of them is rejected at design review, not implementation review.

1. **Finite inputs only.** Files (CSV / JSON / XML / fixed-width) and finite-cursor network sources (paginated REST, SQL `SELECT` cursors) — both reach EOF after exhausting their cursor. Unbounded sources (Kafka, Kinesis, SSE, webhooks, `tail -f`) are out of scope permanently.

2. **Finite jobs.** No daemon mode, no service surface, no infinite event loop. `clinker run` invokes, drains, exits.

3. **Single process forever.** One invocation = one OS process. Parallelism happens inside the process via `std::thread` and Rayon — no worker-process pools, no multi-machine sharding, no network shuffle, no cluster manager. Scale by adding cores / RAM / disk to one host (the DuckDB / Polars / Kettle model). If a host genuinely can't fit the work, partition the input by file or key and run multiple `clinker` invocations from a shell script.

These pillars are why the memory arbitrator is a single in-process component rather than a distributed scheduler, why there is no network shuffle in Combine, and why spill-to-local-disk is the universal pressure-relief valve.

## Crate dependency layers (bottom → top)

```
Applications:   clinker (CLI)  |  cxl-cli (REPL)
                     ↓                ↓
Orchestration:  clinker-core (DAG planner + executor)
                clinker-channel (workspace/channel mgmt)
                clinker-schema (source .schema.yaml validation)
                     ↓
Language/IO:    cxl (lexer → parser → typecheck → eval)
                clinker-format (CSV/JSON/XML/fixed-width readers/writers)
                     ↓
Foundation:     clinker-record (Value, Record, Schema, coercion)

Bench plumbing: clinker-bench-support (deterministic RecordFactory + payload generators)
                clinker-benchmarks (cross-crate benchmark harness)
```

The bench crates are siblings, not part of the runtime layer.

## The node taxonomy

Pipelines use a single flat `nodes:` list; each entry's `type:` discriminator selects a variant of one homogeneous DAG:

- **Source** — input reader bound to a `.schema.yaml`.
- **Transform** — record-level CXL projection / filter / lookup (1×1).
- **Aggregate** — grouped or windowed reduction.
- **Route** — predicate-based fan-out.
- **Merge** — streamwise concatenation of inputs.
- **Combine** — N-ary record combining with mixed predicates (equi + range + arbitrary CXL); distinct from Merge and Transform+lookup.
- **Reshape** — per-group mutate-and-synthesize.
- **Output** — sink writer.
- **Composition** — call-site node referencing a `.comp.yaml` reusable sub-pipeline, lowered at compile time.

The plan itself is a petgraph DAG (`ExecutionPlanDag`) of topologically-sorted nodes, each carrying a parallelism strategy and `NodeProperties` (ordering / partitioning provenance). CXL is typechecked at compile time into a `TypedProgram`, and schema is propagated across the DAG at plan time.

## Key engine decisions

- **Memory-aware aggregation.** Hash aggregation with disk spill; streaming aggregation when sort order permits; RSS tracking with soft/hard limits. The mechanism is documented in [Memory Arbitration & Scheduling](memory-arbitration.md).
- **Compile-time CXL typechecking.** Type inference produces a `TypedProgram`; see [Compiler Phases & Type Unification](cxl-internals.md).
- **Diagnostics.** All user-facing errors use `miette` for span-annotated reports. `Spanned<PipelineNode>` covers the YAML side, `cxl::Span` covers the expression side, and they compose into one report.
- **Pure Rust policy.** `deny.toml` bans cmake; no C build dependencies in clinker crates.
