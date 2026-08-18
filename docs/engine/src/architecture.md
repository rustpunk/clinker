# Overview & Pillars

Clinker is a **bounded-memory batch DAG executor**. A pipeline run is a finite job over finite input: Source nodes read until EOF, the DAG drains, the process exits with a status code. It pairs a custom expression language (CXL) with YAML pipeline orchestration.

Within a run, stateless operators (Transform, Route, most Combine probe-side work, Sink) evaluate records **one at a time** without per-record state accumulation. The DAG executor materializes intermediate buffers between non-fused stages, so memory scales with the largest live intermediate stage's output, not total input size; fused Source → Transform → Sink paths skip materialization entirely. Blocking operators (Aggregate, sort, grace-hash Combine) accumulate state inside the configured RSS budget (default 512 MB) and spill to disk when soft/hard thresholds trip rather than OOM the process.

## The three pillars

Every design decision cascades from three commitments. They are permanent — an architectural proposal that violates any of them is rejected at design review, not implementation review.

1. **Finite inputs only.** Files (CSV / JSON / XML / fixed-width / EDIFACT / X12 / HL7 v2 / SWIFT MT) and finite-cursor network sources (paginated REST with hard page/record caps) — both reach EOF after exhausting their cursor. Unbounded sources (Kafka, Kinesis, SSE, webhooks, `tail -f`) are out of scope permanently.

2. **Finite jobs.** No daemon mode, no service surface, no infinite event loop. `clinker run` invokes, drains, exits.

3. **Single process forever.** One invocation = one OS process. Parallelism happens inside the process via `std::thread` and Rayon — no worker-process pools, no multi-machine sharding, no network shuffle, no cluster manager. Scale by adding cores / RAM / disk to one host. If a host genuinely can't fit the work, partition the input by file or key and run multiple `clinker` invocations from a shell script.

These pillars are why the memory arbitrator is a single in-process component rather than a distributed scheduler, why there is no network shuffle in Combine, and why spill-to-local-disk is the universal pressure-relief valve.

## Crate dependency layers (top → bottom)

```
Applications:    clinker (CLI) | cxl-cli (CXL tool)
                      |
Edge services:   clinker-channel | clinker-net | clinker-schema | clinker-lineage
                      |
Execution:       clinker-exec (runtime operators, memory, spill, metrics)
                      |
Planning:        clinker-plan (YAML, validation, CXL binding, compiled DAG)
                      |
Language / IO:   cxl | clinker-format
                      |
Foundation:      clinker-record | clinker-core-types

Support:         clinker-scenarios | clinker-bench-support | clinker-benchmarks
```

The support crates are siblings, not part of the default runtime path. Some
edge crates depend on more than one lower layer; the repository's AI crate map
records the detailed dependency edges and their evidence.

## The node taxonomy

Pipelines use a single flat `nodes:` list; each entry's `type:` discriminator selects a variant of one homogeneous DAG:

- **Source** — finite input endpoint with an inline, generated, or external schema.
- **Transform** — record-level CXL projection / filter / lookup (1×1).
- **Aggregate** — grouped or windowed reduction.
- **Route** — predicate-based fan-out.
- **Merge** — streamwise concatenation of inputs.
- **Combine** — N-ary record combining with mixed predicates (equi + range + arbitrary CXL); distinct from Merge and Transform+lookup.
- **Reshape** — per-group mutate-and-synthesize.
- **Cull** — per-group rule evaluation with retained and removed output ports.
- **Envelope** — document-level consolidation or expansion at explicit DAG boundaries.
- **Sink** — terminal writer.
- **Composition** — call-site node referencing a `.comp.yaml` reusable sub-pipeline, lowered at compile time.

The plan itself is a petgraph DAG (`ExecutionPlanDag`) of topologically-sorted nodes, each carrying a parallelism strategy and `NodeProperties` (ordering / partitioning provenance). CXL is typechecked at compile time into a `TypedProgram`, and schema is propagated across the DAG at plan time.

## Planner/runtime handoff

`clinker-plan` is the execution-admission layer: canonical YAML parsing,
topology and path validation, schema binding, CXL typechecking, composition
binding, and lowering produce a `CompiledPlan`. Public executor entry points
accept `&CompiledPlan`, but the current implementation then calls
`plan.config()` and recompiles before dispatch. The stored plan is therefore a
typed public boundary today, but its stored DAG and other compiled artifacts
are not yet the artifacts the runtime dispatches directly.

The locked D-01 through D-11 contract corrects that mismatch in Phase 5 /
PERF-01: the supplied plan must remain authoritative and reusable for
sequential in-process runs, while only an enumerated run envelope may refresh.
Persistent cache identity, semantic comparison, integrity checks, and
source-map refresh are part of the same downstream contract; none of that work
is implemented by this chapter. See
[Stored-plan execution and cache identity](https://github.com/rustpunk/clinker/blob/main/docs/ai/15_PRODUCTION_CONTRACTS.md#stored-plan-execution-and-cache-identity)
and [Streaming vs. Blocking Stages](execution-model.md#plan-admission-and-runtime-entry).

## Terminal destination vocabulary

`PipelineNode::Sink`, `SinkConfig`, and YAML `type: sink` are the current
terminal-writer surface; planning lowers them to `PlanNode::Sink` and runtime
execution delegates to `executor/sink_dispatch.rs`. The retired
`type: output` spelling is rejected with the paste-ready correction
`type: sink`. Output ports, produced artifacts and paths, serialization
formats, stdout and machine output, writer results, and OpenLineage output
datasets remain distinct and valid output vocabulary. See [Sink
Nodes](https://github.com/rustpunk/clinker/blob/main/docs/user/src/nodes/sink.md),
[Sink Internals](sink-internals.md), and
[Terminal destination vocabulary](https://github.com/rustpunk/clinker/blob/main/docs/ai/15_PRODUCTION_CONTRACTS.md#terminal-destination-vocabulary).

## Key engine decisions

- **Memory-aware aggregation.** Hash aggregation with disk spill; streaming aggregation when sort order permits; RSS tracking with soft/hard limits. The mechanism is documented in [Memory Arbitration & Scheduling](memory-arbitration.md).
- **Compile-time CXL typechecking.** Type inference produces a `TypedProgram`; see [Compiler Phases & Type Unification](cxl-internals.md).
- **Diagnostics.** All user-facing errors use `miette` for span-annotated reports. `Spanned<PipelineNode>` covers the YAML side, `cxl::Span` covers the expression side, and they compose into one report.
- **Pure Rust policy.** No crate in the graph invokes a C compiler. The two places that could — TLS and content hashing — are held to Rust implementations: rustls with the graviola provider rather than ring or aws-lc-rs, and blake3's `pure` feature, which keeps the `std::arch` SIMD paths and gives up only the variants its build script would compile through `cc`. That second trade is not free: `pure` hashes at roughly 85% of the assembly build's throughput at the same instruction set, and roughly 57% on a CPU whose AVX-512 kernels it declines to use. The policy accepts that cost rather than a build-time C dependency. A CI job builds the workspace and all its targets with every C-compiler environment variable pointed at a failing program, and then requires a crate that does compile C to fail — so the guarantee is checked, and the check is checked, rather than either being asserted. `deny.toml` bans cmake alone, and deliberately does not attempt this: a name blocklist cannot separate a build script that runs `cc` from one that only declares it.

The boundaries available to engine extensions are described in
[Extension Seams](extension-seams.md).
