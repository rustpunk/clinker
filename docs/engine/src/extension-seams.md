# Extension Seams

Clinker has explicit places where new behavior joins the engine, but it does
not have a general-purpose plug-in system. Some extensions implement a typed
streaming contract; others require coordinated changes across the authoring,
planning, and runtime layers.

The distinction matters. A transport can join the common ingest path by
implementing `RecordSource`. A new YAML node cannot be dropped into a registry:
it changes the language of pipeline topology and must be understood by the
planner, executor, diagnostics, and plan consumers.

## The central boundary

```text
pipeline YAML + CXL
        |
        v
clinker-plan
  parse -> validate -> bind/typecheck -> lower/enrich
        |
        | CompiledPlan / ExecutionPlanDag
        v
clinker-exec
  ingest -> dispatch -> arbitrate memory -> write/report
```

The planner owns author input and proves as much as it can before any record
flows. It is the sole authority that admits a pipeline for execution. The
executor owns runtime effects and consumes compiled artifacts. Raw YAML does
not belong in operator code, and byte or network mechanics do not belong in
plan lowering. `clinker-schema` is an advisory discovery and warning tool: its
bounded scans and heuristic field extraction do not authorize execution or
override a planner rejection (D-17).

## File formats

`FormatReader` turns a byte source into records; `FormatWriter` turns records
into bytes. Both are `Send` but not `Sync` because one worker owns each stream.
Their less obvious hooks are part of the seam too: schema discovery,
multi-file source identity, document preparation and envelope events,
non-finalizing byte flushes, document framing, and byte counts.

A format becomes user-selectable only after its typed YAML options and central
reader/writer construction arms are wired. It must also state which schema,
multi-record, envelope, splitting, and document-cardinality features it can
represent. This is deliberate compile-time wiring, not dynamic discovery.

## Non-file transports

`RecordSource` is the transport-neutral ingest contract. File readers reach it
through an adapter; paginated REST implements it directly and enters execution
as `SourceInput::Records`. Once records cross this boundary, common ingest owns
schema coercion, provenance, document signals, watermarks, backpressure, and
handoff to the DAG.

Every source is finite. A cursor must end after a bounded result set; daemon
polling and unbounded streams do not fit this seam.

## Pipeline nodes

A pipeline node crosses three representations:

```text
PipelineNode (author shape) -> PlanNode (compiled shape) -> dispatch arm
```

Adding or changing one therefore requires an end-to-end review:

- strict span-aware YAML parsing, topology and configuration validation;
- schema propagation, CXL typing, lowering, and composition-body behavior;
- ordering, partitioning, correlation-key, streaming, scheduling, and
  cardinality properties;
- runtime ports, record/control-event flow, DLQ, metrics, cancellation,
  memory, spill, and cleanup;
- explain output, lineage, examples, documentation, and boundary tests.

The compiled DAG also contains synthetic nodes inserted by planning. Those are
runtime machinery, not automatically valid YAML node types.

## Clinker Expression Language (CXL)

The Clinker Expression Language's extension seam is its ordered compiler
pipeline:

```text
parse -> resolve -> typecheck -> analyze/extract -> evaluate
```

Each phase consumes stronger input than the previous phase. New syntax or an
AST form must keep node identifiers and recursive visitors coherent and must be
handled by every later phase that can receive it. CXL remains below planning
and execution: it knows records and expression semantics, not pipeline YAML or
operator scheduling. It is a per-record ETL expression language, not SQL, and
it does not decide whether a pipeline is executable.

## Diagnostics and runtime resources

Diagnostic codes are registered centrally, then emitted with source spans and,
where useful, typed payloads. The long-form `clinker explain --code` pages are a
second coordinated surface rather than an automatic result of registration.

Runtime resources are run-scoped rather than global. Memory consumers register
with one `MemoryArbitrator`; writers, window indexes, progress callbacks, spill
guards, and similar state are passed through explicit handles or registries.
A new stateful operator participates in shared memory, pause/spill, disk quota,
shutdown, and cleanup rules instead of creating an independent resource model.

## Composition resource slots

The current composition surface is only partly wired. `_compose.resources_schema`
parses resource declarations, and the declaration model currently has one
`file` kind. Ordinary composition call sites also parse `resources:` as
untyped JSON. Binding currently accepts every call-site resource because
`validate_resources` is a stub, and the executor does not consume those values.
Accepted input at this seam must not be mistaken for working runtime behavior.

D-12 through D-15 assign the replacement to AUTH-01: a bounded typed
kind registry, concrete named catalog, declared typed slots, fail-closed
call-site and overlay binding, attempted-versus-winning provenance, and
secret-safe preflight handles. Reusable content and compiled plans may retain
only non-secret semantics and secret references; credentials and live handles
are run-local and must be closed on failure. D-16 also requires rejection of
ordinary composition call-site `outputs` and `alias`, which currently
parse without providing the promised remapping or namespace. `_compose.outputs`,
the composition node name, and the distinct overlay insertion alias remain
valid concepts.

These are locked downstream contracts, not usable extension APIs today. See
the [composition resources and call-site surface contract](https://github.com/rustpunk/clinker/blob/main/docs/ai/15_PRODUCTION_CONTRACTS.md#composition-resources-and-call-site-surface).

## Approved transitional exceptions

Four narrow dependency and parser exceptions are recorded by D-20 through
D-23. Each approves only the boundary named here:

| Decision | Current permitted boundary | Forbidden expansion | Owner |
|---|---|---|---|
| D-20 | `clinker-format -> cxl` may import only logical type and document path/index vocabulary: `cxl::typecheck::Type` and `cxl::analyzer::doc_paths::{DocPath, DocIndex}`. | No parser, resolver, evaluator, planner, or other analyzer dependency. | Phase 1 contract; neutral extraction is deferred. |
| D-21 | `clinker-exec -> clinker-bench-support` may remain optional behind `bench-alloc`, outside default and release graphs. | No default-runtime edge or trusted allocation claim until forwarding, allocator identity, plausible measurements, and distortion are qualified. | Phase 5 / PERF-07. |
| D-22 | Direct `serde_saphyr::from_str*` calls belong only in `clinker-plan::yaml` and parser-specific tests. | No production or cross-module test bypass of `clinker_plan::yaml`. | Phase 2 repair; Phase 6 / EVID-03 qualification. |
| D-23 | A manifest dependency remains only after source, build, generated-code, feature, test, and supported-API use is proven. | No speculative dependency or async/runtime coupling. | CONT-05 bounded cleanup. |

D-20 is implemented as a transitional exception. D-21 and D-23 are only
partly implemented, and D-22's known executor-test bypass has not yet been
repaired. The full current evidence and compatibility rules live in the
[production contract register](https://github.com/rustpunk/clinker/blob/main/docs/ai/15_PRODUCTION_CONTRACTS.md#approved-exceptions-and-rejected-placeholders).

## Declarative and read-only edges

Channels alter only declared overlay surfaces and feed the effective result
back through normal compilation. Composition bodies are sealed except for
declared ports, config, scoped variables, and, after AUTH-01 lands, typed
resource slots. Unknown resource kinds, slots, or catalog names must fail
closed; channel reachability does not create a second admission authority.

Plan consumers should remain read-only edges. `clinker-lineage`, for example,
walks a `CompiledPlan` without depending on or invoking runtime operators; live
run facts are supplied by the CLI boundary.

For exact change paths, source anchors, focused tests, and unresolved-boundary
routing, see the
[implementer seam map](https://github.com/rustpunk/clinker/blob/main/docs/ai/35_EXTENSION_SEAMS.md).
