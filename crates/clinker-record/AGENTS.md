# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/clinker-record`.
Follow the root instructions first.

## Purpose

Foundational data-model crate for Clinker records: values, records, schemas,
document context, provenance, grouping keys, counters, accumulators, and
storage/resolver traits.

## Responsibilities

- Own shared row/value vocabulary used by CXL, format, plan, exec, CLI, channel, net, schema-adjacent code, and benchmarks.
- Keep records schema-indexed: positional values interpreted through `Arc<Schema>`.
- Provide low-level resolver/storage traits without depending on `clinker-exec`.
- Preserve spill-friendly wire payloads for values, records, accumulators, and document context.
- Preserve memory-sensitive string/value layout assumptions.
- Keep grouping, distinct, aggregate, and counter vocabulary centralized here when downstream crates need it.

## Important public APIs

- `Value`, `NULL`, and `FieldStr`.
- `Record`, `RecordPayload`, and `MinimalRecord`.
- `Schema`, `SchemaBuilder`, and `FieldMetadata`.
- Fixed-width formatting enums `Justify`, `LineSeparator`, and `TruncationPolicy` (`schema_def`). The typed source-schema vocabulary (`Column`, `SourceSchema`) lives in `clinker-format`, above this crate's `cxl` dependency boundary.
- `RecordStorage`, `RecordView`, `FieldResolver`, `HashMapResolver`, and `WindowContext`.
- `DocumentContext`, `DocumentId`, `DocumentGrain`, `EnvelopeRecord`, and `synthetic_document_context`.
- `RecordProvenance`, `GroupByKey`, `value_to_group_key`, `PipelineCounters`, and `RetractionCounters`.
- Accumulator APIs including `AccumulatorEnum`, `AccumulatorRow`, `AggregateType`, `Reversibility`, and `AccumulatorError`.
- Coercion helpers such as `coerce_to_int`, `coerce_to_float`, `coerce_to_date`, and `coerce_to_datetime`.

## Internal module map

- `value`: typed runtime values, custom serde wire form, map helpers, and heap-size accounting.
- `field_str`: 24-byte inline/shared/unique string representation for `Value::String`.
- `record`: schema-indexed records, record vars, document context attachment, and `RecordPayload`.
- `schema`: column ordering, field metadata, name lookup, and `SchemaBuilder`.
- `schema_def`: schema-file and inline schema structures.
- `storage`, `record_view`, `resolver`: zero-copy storage and field/window resolution contracts.
- `document_context`: `$doc` envelope records, document ids/grains, synthetic context, and spill codec.
- `provenance`, `group_key`, `accumulator`, `counters`, `coercion`, `minimal`: focused data-model helpers.

## Dependency rules

### Allowed dependencies

Current normal dependencies are intentional: `serde`, `serde_json`, `chrono`,
`ahash`, `indexmap`, and `smol_str`.

Current dev/bench dependencies are expected only for tests and benches:
`criterion`, `clinker-bench-support`, and `postcard`.

### Forbidden or suspicious dependencies

- Do not add normal dependencies on downstream workspace crates such as `cxl`, `clinker-format`, `clinker-plan`, `clinker-exec`, `clinker-channel`, `clinker-net`, `clinker`, `clinker-schema`, or benchmark harness crates.
- Be suspicious of async runtimes, network clients, filesystem/path-validation crates, YAML parsers, CLI/diagnostic frameworks, Rayon/crossbeam runtime machinery, format readers/writers, or executor memory/spill implementations.
- Do not add native toolchain dependencies, OpenSSL/native-tls, C build steps, or cargo-deny exceptions without approval.
- Keep `clinker-bench-support` dev/bench-only.

## Important invariants

- `Record` values are positional and must match `Schema` column order; unknown `Record::set` fields are a debug assertion and release no-op, not recovery.
- `RecordPayload` carries only `values`, `record_vars`, and `doc_id`; schema and full document context are side-table/interned spill data.
- `RecordStorage` is `Send + Sync` and returns borrowed `&Value` to keep window/evaluator paths zero-copy.
- `FieldStr` is a 24-byte storage optimization; storage arms must not affect equality, ordering, hashing, grouping, or serialized content.
- `Value` postcard/tagged wire form and `DocumentContext` section ordering are spill-sensitive.
- `DocumentContext` is shared per document through `Arc`; synthetic records use the process-wide synthetic context.
- `DocumentGrain` is the output-envelope frame identity, not always the innermost document id.
- `FieldMetadata` marks engine-stamped columns; default user-field iteration skips stamped columns.
- Group keys canonicalize default integers/floats together, reject NaN, and treat null as caller-controlled.
- Accumulator finalize must surface overflow via `AccumulatorError`, not wrapping casts.
- Crate-local guidance is most relevant for broad changes to `Value`, `Record`, schema, provenance, document context, storage, accumulator, or counter semantics.

## Common mistakes for AI agents to avoid

- Adding planner, executor, format, or CXL behavior into this crate.
- Treating `Value::Map` as universally writable by every downstream format.
- Serializing `FieldStr` storage-arm information or making storage arms semantically visible.
- Widening `FieldStr`, `Value`, `FieldMetadata`, `RecordPayload`, or accumulator state without checking tests, spill behavior, and benches.
- Hand-building schemas with mismatched metadata; prefer `SchemaBuilder`.
- Changing document-context equality or header semantics without envelope tests.
- Making `RecordStorage` clone values instead of borrowing them.
- Treating debug assertions as user-facing validation.

## Local commands

- **Inferred:** `cargo check -p clinker-record --locked --offline`
- **Inferred:** `cargo test -p clinker-record --locked --offline`
- **Inferred:** `cargo check --benches -p clinker-record --locked --offline`
- **Inferred, performance-sensitive only:** `cargo bench -p clinker-record --bench record_ops`

For Rust source changes, also run the relevant workspace gates from the root
`AGENTS.md` unless the user explicitly scopes validation narrower.

## Documentation updates

- `docs/ai/10_ARCHITECTURE.md`, `docs/ai/20_CRATE_MAP.md`, `docs/ai/40_COMMON_PATTERNS.md`, and `docs/ai/60_PERFORMANCE_NOTES.md` for crate role, dependency, invariant, or performance-sensitive changes.
- `docs/ai/80_OPEN_QUESTIONS.md` for unresolved data-model or counter semantics.
- `docs/engine/src/auto-widen-internals.md` and `docs/engine/src/output-internals.md` for auto-widen/map/output behavior changes.
- `docs/user/src/cxl/builtins-map.md` and related `docs/user/src/cxl/*.md` for public value/map behavior changes.
- Relevant user/engine envelope and document-context docs before copying older examples.

## Approval Gates

- Keep this file narrowly scoped to record/value/schema risks and avoid
  duplicating the root onboarding docs.
- `PipelineCounters::ok_count` currently deduplicates by source row number and can undercount multi-source row-number collisions; confirm the intended globally unique source-row stamp before changing counter semantics.

## Evidence

- `crates/clinker-record/Cargo.toml`
- `crates/clinker-record/src/lib.rs`
- `crates/clinker-record/src/value.rs`
- `crates/clinker-record/src/field_str.rs`
- `crates/clinker-record/src/record/mod.rs`
- `crates/clinker-record/src/schema.rs`
- `crates/clinker-record/src/storage.rs`
- `crates/clinker-record/src/resolver.rs`
- `crates/clinker-record/src/record_view.rs`
- `crates/clinker-record/src/document_context.rs`
- `crates/clinker-record/src/accumulator/`
- `crates/clinker-record/src/counters.rs`
- `crates/clinker-record/benches/record_ops.rs`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/40_COMMON_PATTERNS.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
