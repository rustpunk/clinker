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

### Allocation-aware CSV construction

CSV execution uses `CsvReader::from_reader_admitted` and
`MultiRecordReader::new_csv_admitted` with finite allocation resources. The
explicit legacy reader constructors remain available for non-executing
inspection and existing library callers; they do not establish runtime
admission. Document preparation returns `OwnedMap` through both `FormatReader`
and `RecordSource`, preserving child ownership across adapters. Legacy readers
can move-wrap an existing map without claiming that it was admitted.

Direct CSV output construction requires `WriterResources`:
`CsvEncoder::into_boxed_writer` returns a `FormatWriterHandle`. Wrappers that
allocate another writer use `FormatWriterHandle::try_new(value, &scope)` with
an `AllocationScope`; all writer hooks and byte counts forward through the
handle. `AsMut<dyn FormatWriter>` borrows the interface without exposing its
allocation owner. `FormatWriterHandle::from_legacy(Box<dyn FormatWriter>)` is
an explicit boundary for unchanged implementations, not a CSV fallback.

Split construction uses `WriterFactory::try_new(closure, &scope)` and calls
`create(destination, schema)` to obtain a `FormatWriterHandle` for each file.
The factory admits the concrete closure layout before type erasure; captured
allocations need separate owners. `WriterFactory::from_legacy` leaves an
unchanged codec factory outside CSV/JSON/XML ungoverned. `CountedFormatWriter` and
`SplittingWriter` retain writer handles, so wrapping and rotating a CSV writer
preserve its backing charge. See [memory ownership](memory-arbitration.md#exact-allocation-admission-for-prepared-output).

### Finite native writer construction

JSON and XML expose `JsonEncoder`/`XmlEncoder`, admitted shared
`JsonEncoderConfig`/`XmlEncoderConfig`, and borrowed options. Construct an
encoder with finite `WriterResources`, then wrap it in `PreparedWriter<W, E>` for a
borrowed or owned destination. For type erasure, `into_boxed_writer` admits the
concrete allocation and returns `FormatWriterHandle`. Pass explicit finite
`MemoryOnlyResources` for direct use or executor `WriterResources` at runtime;
there is no unlimited constructor and no raw `JsonWriter`/`XmlWriter` fallback.

`FormatEncoder::prepare` must leave committed state unchanged. Return pending
cache/framing/count state and commit it only after complete stage delivery.
Every wrapper forwards document hooks, `flush_bytes`, finalization, byte counts
and failures. Never add buffering outside the prepared writer that can retry
bytes from `Drop` or obscure accepted destination counts. `WriterFactory`
retains the admitted config owner across split rotations.

Schema cache identity is `SharedStorageIdentity<Schema>`, never a raw address
or an owned schema clone. The old cache and its prospective replacement remain
charged together until the operation commits or aborts. Preserve the legacy
weak-backing reservation and final deallocation order. See
[native ownership](memory-arbitration.md#native-jsonxml-configuration-and-schema-caches).

### Prepared storage extensions

`ResourceAuthority::create_stage` returns the sealed `OperationStage` owner.
Implement `StageStorage` and call `StorageStage::create(scope, storage)` rather
than implementing or boxing an operation-stage trait. The adapter admits the
concrete backend and its progress buffer before writes; `finish` consumes the
writable stage and moves the same owner into `PreparedBytes` without another
backend allocation.

`StageStorage` supplies read/write, `seal`, `complete` and inline failure
evidence. `seal` rewinds and establishes complete immutable bytes; `complete`
releases readback storage before encoder state commits. `resource_failed` must
not allocate or change the observed error. Recover resource evidence through
`failure`/`resource_error` at I/O boundaries, and never seal a failed prefix.
Providers receive no destination handle. The sealed wrapper keeps the backend
and its lease inseparable through delivery and deallocation; see
[prepared storage](storage-internals.md#prepared-output-storage) for partial
delivery and cleanup debt.

### Fixed-width repeating groups

Fixed-width repeating groups demonstrate the coordinated form of this seam.
The strict `Column` shape carries `fields`, `occurs`, and an optional physical
`count_field` through patching, overlay provenance, canonical identity, and
normal planner admission before either format constructor runs. Layout
resolution then proves a finite maximum width with checked arithmetic. The
reader retains one declared-length row, while the writer validates and encodes
one declared-length record before committing bytes. The group remains one
logical array-of-records column; a derived count cell is layout machinery and
never enters the logical schema or CXL namespace. Delimiter-packed scalar
cells remain a distinct `split_values` representation and cannot bypass the
positional-group checks. The executable byte and rejection matrix is in
`crates/clinker-format/tests/fixed_width_repeating_groups.rs`, with planner
admission coverage in
`crates/clinker-plan/src/plan/tests/multi_value_validation.rs`.

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

## Composition example corpus

Committed composition fragments are an executable authoring contract, not
parser fixtures. The corpus test follows this boundary:

```text
recursive inventory -> exact case set -> production loader -> generated pipeline -> clinker -> exact bytes
```

The inventory discovers every `.comp.yaml` recursively without a manual
allowlist and compares normalized, repository-relative paths with the case
manifest as exact sets. Duplicate manifest keys, path escapes, missing or empty
directories, missing cases, and extra cases are separate failures. Both
inventory paths and failure ordering are deterministic.

For each case, the harness materializes only the selected fragment in an
isolated workspace, calls the production composition scanner and compiler, and
then invokes the built `clinker` executable. A case succeeds only when the
process status, run counters, and output bytes match its committed expectation.
`clinker run --explain` remains a compilation check and cannot replace the
runtime byte comparison.

This seam adds no production retention or execution work of its own. Its fixed,
finite fixtures exercise the existing compiled-plan, runtime, telemetry, and
lineage paths; their existing memory and observability contracts remain the
authoritative ones.

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

`_compose.resources_schema` declares typed slots; the bounded workspace
catalog currently admits the `file` kind. A composition call binds a slot to
one logical catalog identity through strict scalar `resources:` values.
Binding rejects missing and undeclared slots, unknown identities, kind or
capability mismatches, inline descriptors, credential selectors, and the
ordinary call-site `outputs` and `alias` fields. Each winning logical binding
retains its complete attempted-versus-winning overlay provenance.

An authored body Source names its slot explicitly with `resource: <slot>` in
the Source config. The binder never infers a slot from a Source name or path.
It rejects a body Source that combines `resource:` with `path`, `glob`,
`regex`, or `paths`, and it rejects an authored body Source with no resource
link. Input ports remain separate synthetic Source roots seeded by the caller;
they do not receive activation entries. Top-level direct file Sources keep
their existing matcher surface and reject `resource:` because no top-level
catalog-binding surface exists.

Each bound call compiles its body Sources into scope-qualified
`CompiledSourceInstance` values. The retained requirement contains the slot,
logical binding and provenance, kind, finite capabilities, opener family,
run-local lifetime, and stable logical dataset identity. It deliberately drops
the catalog's physical path and cannot contain a credential choice, secret,
live handle, I/O state, or thread state. Separate calls to the same composition
therefore share immutable logical descriptor semantics but have distinct
Source identities at activation.

For a data run, the CLI resolves each credential-free `file` requirement back
to the admitted workspace catalog and captures its validated path inside an
opaque, single-use factory. The executor receives only the sealed activation
bundle, takes each complete group lease before opening any member, opens every
member before publishing a bounded Source channel, and retains the sessions
until that composition scope ends. A partial open, reader failure, shutdown,
or ordinary completion closes sessions, releases leases, and unregisters the
channels' memory consumers. Credential-bearing groups still fail preflight:
there is no credential-profile selection surface yet. See the
[composition resources and call-site surface contract](https://github.com/rustpunk/clinker/blob/main/docs/ai/15_PRODUCTION_CONTRACTS.md#composition-resources-and-call-site-surface).

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
