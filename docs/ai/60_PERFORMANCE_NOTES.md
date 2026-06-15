# AI Onboarding: Performance Notes

Purpose: Preserve verified performance assumptions, benchmark entry points, and memory-model notes for future AI agents.

Use the labels below carefully:

- **Verified hot path** means code comments, benchmark targets, or executor structure explicitly identify the path as per-record/per-stage/hot.
- **Likely hot path** means the path is on core ETL execution, IO, memory, or serialization surfaces, but this session did not find a direct "hot path" claim or run profiling.
- **Resource-sensitive** means correctness and throughput depend on memory, disk, cache, or synchronization behavior, but the path may not be CPU-hot.

## Source Evidence

Primary source evidence used:

- `Cargo.toml`
- `crates/clinker-record/src/field_str.rs`
- `crates/clinker-record/src/value.rs`
- `crates/cxl/src/eval/mod.rs`
- `crates/cxl/src/eval/compiled.rs`
- `crates/clinker-format/src/source.rs`
- `crates/clinker-format/src/csv/reader.rs`
- `crates/clinker-format/src/json/reader.rs`
- `crates/clinker-exec/src/executor/*`
- `crates/clinker-exec/src/pipeline/*`
- `crates/clinker-exec/src/aggregation/*`
- `crates/clinker-net/src/*`
- `crates/clinker-channel/src/staging_copy.rs`
- `crates/clinker-bench-support/src/{alloc,cache}.rs`
- `crates/*/benches/*.rs`
- `benches/pipelines/**/*.yaml`

## Verified Hot Paths

### Transform Dispatch And CXL Evaluation

1. **Area/module:** `clinker-exec::executor::transform_dispatch`, `cxl::eval`.
2. **Why performance-sensitive:** Transform dispatch executes CXL per record. The dispatch code calls the compiled program the "per-record evaluator for the transform hot loop" and iterates every input record.
3. **Existing optimization choices:** `ProgramEvaluator` compiles a typed program into closures on first use and caches one compiled program per `RecordStorage` `TypeId`. Scalar expressions used by aggregation are lowered once so regex cache and literal baking are paid once. Transform output uses `Vec::with_capacity(input_records.len())`, and shutdown is polled every 1024 records instead of every row.
4. **Avoid:** Do not reintroduce recursive AST walks, per-record regex compilation, per-record evaluator construction inside the loop, or per-row shutdown atomics without benchmark evidence.
5. **Benchmarks/tests available:** `cargo bench -p cxl --bench eval`, `cargo bench -p cxl --bench parse`, `cargo bench -p clinker-exec --bench pipeline`, `crates/clinker-exec/tests/transform_stream_fusion.rs`.
6. **Confidence:** High.
7. **Evidence:** `crates/clinker-exec/src/executor/transform_dispatch.rs:29`, `:106`, `:159`, `:161`; `crates/cxl/src/eval/mod.rs:5`, `:102`, `:172`, `:184`; `crates/cxl/benches/eval.rs:59`; `crates/cxl/benches/parse.rs:8`.

### Record And Value Storage

1. **Area/module:** `clinker-record::FieldStr`, `clinker-record::Value`, `Record` operations.
2. **Why performance-sensitive:** `Value::String` is documented as the dominant ETL payload shape, and `FieldStr` width controls the per-`Value` byte cost that drives RSS/spill thresholds.
3. **Existing optimization choices:** `FieldStr` uses a 24-byte union with inline strings up to 23 bytes, `Arc<str>` for shared long strings, and `Box<str>` for long unique strings. `Value` custom serialization keeps wire form as string bytes and does not serialize the storage arm. Size assertions pin `FieldStr` at 24 bytes and `Value` at the expected footprint.
4. **Avoid:** Do not widen `FieldStr`/`Value`, remove inline storage, make long shared clones deep-copy by default, or serialize the in-memory storage arm unless all memory/spill assumptions and record benchmarks are updated.
5. **Benchmarks/tests available:** `cargo bench -p clinker-record --bench record_ops`; size/clone/serde tests in `clinker-record`.
6. **Confidence:** High.
7. **Evidence:** `Cargo.toml:45`; `crates/clinker-record/src/field_str.rs:1`, `:27`, `:111`, `:300`; `crates/clinker-record/src/value.rs:13`, `:36`, `:126`; `crates/clinker-record/benches/record_ops.rs:27`, `:88`, `:130`.

### Memory Arbitration, Node Buffers, And Spill

1. **Area/module:** `clinker-exec::pipeline::memory`, `executor::node_buffer`, `pipeline::spill`.
2. **Why performance-sensitive:** A single `MemoryArbitrator` governs spill/abort decisions across Aggregate, sort, grace-hash, sort-merge join, IEJoin, and inter-stage `node_buffers`. `should_spill` is the shared poll point.
3. **Existing optimization choices:** Consumer bytes are mirrored through atomics; pause fast path is lock-free; consumer registry reads use `ArcSwap<Vec<_>>` snapshots; rare register/unregister operations clone-and-swap. Node buffers can be memory, spilled, or mixed, and spilled drains stream from disk. Spill files use a JSON schema header plus postcard frames, optional LZ4, document-context interning, and a max frame-size cap before allocation.
4. **Avoid:** Do not add locks to arbitration hot reads, clone spill-backed buffers for fan-out, remove spill frame caps, or let each operator create its own independent memory budget.
5. **Benchmarks/tests available:** `cargo bench -p clinker-exec --bench arbitration_poll`, `arena`, `spill_compression`; spill/memory tests such as `memory_backpressure.rs`, `storage_spill_dir.rs`, `route_fanout_soft_spill.rs`, `post_aggregate_window_spilled.rs`.
6. **Confidence:** High.
7. **Evidence:** `crates/clinker-exec/src/pipeline/memory.rs:17`, `:293`, `:358`, `:708`; `crates/clinker-exec/benches/arbitration_poll.rs:1`, `:12`; `crates/clinker-exec/src/executor/node_buffer.rs:1`, `:34`, `:173`, `:263`; `crates/clinker-exec/src/pipeline/spill.rs:1`, `:27`, `:82`, `:152`.

### Combine, Grace Hash, And IEJoin

1. **Area/module:** `clinker-exec::pipeline::{combine,grace_hash,iejoin}` and `executor::combine_dispatch`.
2. **Why performance-sensitive:** Combine materializes build-side records, hashes or range-joins against probe-side records, can spill, and has dedicated Criterion targets and test guards.
3. **Existing optimization choices:** Build-side hash table is immutable after construction and index-separated; hash keys are canonicalized; probe accepts pre-extracted keys so callers can reuse a `Vec<Value>` buffer, documented as eliminating one per-row heap allocation at 100K driver rows. IEJoin implements the VLDB Union Arrays variant with coarse bit-array skipping to avoid nested-loop blow-up. Match-collect has a cap of 10,000 matches per driver.
4. **Avoid:** Do not replace IEJoin with nested loops, remove NULL short-circuits, allocate probe key vectors per driver row, skip full-key collision verification, or change collect caps without combine benches and tests.
5. **Benchmarks/tests available:** `cargo bench -p clinker-exec --bench combine`, `combine_grace_hash`, `combine_iejoin`, `combine_nary_3input`; `crates/clinker-exec/tests/combine_test.rs` includes bench-scaffold and strategy/correctness gates.
6. **Confidence:** High.
7. **Evidence:** `crates/clinker-exec/src/pipeline/combine.rs:13`, `:44`, `:675`, `:715`; `crates/clinker-exec/src/pipeline/grace_hash/probe.rs:1`, `:59`, `:83`; `crates/clinker-exec/src/pipeline/iejoin.rs:1`, `:33`, `:69`, `:82`; `crates/clinker-exec/tests/combine_test.rs:44`, `:4531`.

### Hash Aggregation And Window Runtime

1. **Area/module:** `clinker-exec::aggregation`, `pipeline::arena`, `executor::window_runtime`, `pipeline::window_context`.
2. **Why performance-sensitive:** Aggregation keeps per-group hash tables and spill state; windows use projected arenas and secondary indexes that are evaluated from transform loops.
3. **Existing optimization choices:** Aggregation clones accumulator prototypes for new groups instead of dispatching factories per key. Group memory estimation precomputes spill thresholds. Pre-aggregation filters and binding args are compiled once. Window runtimes share `Arc<Arena>` and `Arc<SecondaryIndex>` across per-record eval and recompute paths instead of rebuilding.
4. **Avoid:** Do not allocate accumulator factories per group, rebuild window indexes during per-record evaluation, or route strict pipelines through relaxed/buffer-mode overhead.
5. **Benchmarks/tests available:** `cargo bench -p clinker-exec --bench window`; benchmark pipeline YAML under `benches/pipelines/execution_mode/*aggregate*` and `*window*`; aggregation/window integration tests under `crates/clinker-exec/tests/`.
6. **Confidence:** High for aggregation/window memory sensitivity; medium for exact CPU hotness without fresh profiles.
7. **Evidence:** `crates/clinker-exec/src/aggregation/mod.rs:1`, `:20`; `crates/clinker-exec/src/aggregation/hash.rs:33`, `:78`, `:103`, `:120`, `:143`; `crates/clinker-exec/src/pipeline/arena.rs:44`, `:143`, `:191`; `crates/clinker-exec/src/executor/window_runtime.rs:4`, `:26`; `crates/clinker-exec/benches/window.rs:29`.

## Likely Hot Or Resource-Sensitive Paths

### Source Ingest, Streaming Fusion, And Output

1. **Area/module:** `clinker-exec::executor::{ingest,streaming,dispatch,output_dispatch}`.
2. **Why performance-sensitive:** Every source feeds the same crossbeam ingest channel; streaming fusion bypasses full `node_buffers` for eligible producer-to-output paths and preserves back-pressure.
3. **Existing optimization choices:** Executor is synchronous: one `std::thread` per declared source, bounded crossbeam channels, and Rayon for CPU kernels. Streaming output drains a bounded channel on a writer thread and charges/discharges per batch/record. Output writer construction is lazy on first projected schema.
4. **Avoid:** Do not add an async runtime to the executor path, unbound channels, materialize full streaming outputs, or remove drain-on-fatal behavior that prevents bounded send deadlock.
5. **Benchmarks/tests available:** `cargo bench -p clinker-exec --bench parallel`, `pipeline`; tests `streaming_output.rs`, `streaming_ingest_fixes.rs`, `transform_stream_fusion.rs`, `merge_interleave.rs`.
6. **Confidence:** High for resource sensitivity and back-pressure; medium for CPU hotness.
7. **Evidence:** `crates/clinker-exec/src/executor/mod.rs:283`, `:779`, `:1284`, `:1300`; `crates/clinker-exec/src/executor/streaming.rs:28`, `:239`, `:307`; `crates/clinker-exec/src/executor/dispatch.rs:840`, `:879`, `:896`, `:925`.

### Format Readers, Writers, And Resource Loading

1. **Area/module:** `clinker-format::{source,csv,json,xml,...}`, executor reader/writer registry.
2. **Why performance-sensitive:** Format IO is on all pipeline edges. JSON/XML envelope paths can require multiple passes and document indexing.
3. **Existing optimization choices:** `ReopenableSource` reopens paths without whole-file buffering; one-shot pathless readers are consumed lazily for one-pass formats and buffered only when a multi-pass reader needs it. CSV reuses `csv::StringRecord` and strips BOM via a wrapper. JSON arrays/NDJSON stream record-by-record; envelope pre-scan retains only declared `$doc.*` sections and caps retained index bytes.
4. **Avoid:** Do not buffer file-backed inputs whole, collect JSON arrays into `Vec`, remove source-identity checks between passes, or make one-pass formats force `into_reopenable`.
5. **Benchmarks/tests available:** `cargo bench -p clinker-format --bench io_throughput`; format pipeline benchmarks under `benches/pipelines/format/`; tests under `crates/clinker-format/tests/` and many executor format tests.
6. **Confidence:** High for resource sensitivity; medium for exact hotness.
7. **Evidence:** `crates/clinker-format/src/source.rs:1`, `:9`, `:68`, `:99`, `:149`; `crates/clinker-format/src/csv/reader.rs:27`, `:42`, `:98`; `crates/clinker-format/src/json/reader.rs:1`, `:11`, `:86`, `:245`; `crates/clinker-format/benches/io_throughput.rs:13`, `:47`, `:77`, `:112`, `:147`.

### Serialization And Deserialization

1. **Area/module:** `clinker-record::Value`, `clinker-record::DocumentContext`, `clinker-exec::pipeline::spill`, metrics/reporting.
2. **Why performance-sensitive:** Serialization is used for spill, metrics, DLQ, benchmark reports, and tests. Spill serialization sits on memory-pressure paths.
3. **Existing optimization choices:** `Value` uses tagged serde compatible with postcard; production JSON output bypasses serde `Value` dispatch through format helpers. Spill record bodies use postcard, while the schema header stays JSON for inspectability. Document contexts are interned once per spill file and reloaded via `Arc<DocumentContext>`.
4. **Avoid:** Do not replace postcard spill frames with verbose JSON rows, inline document context per record, or remove deserialize frame-size caps.
5. **Benchmarks/tests available:** `spill_compression` bench, `record_ops` value heap-size/string benches, spill/storage tests.
6. **Confidence:** High for spill serialization; medium for other reporting paths.
7. **Evidence:** `crates/clinker-record/src/value.rs:36`, `:62`, `:95`; `crates/clinker-exec/src/pipeline/spill.rs:16`, `:27`, `:48`, `:82`, `:141`.

### Caches

1. **Area/module:** CXL compiled-program/regex caches, benchmark data cache, channel staging cache, schema/window runtime caches.
2. **Why performance-sensitive:** These caches prevent repeated compile, generation, copy, or rebuild work on loops and repeated runs.
3. **Existing optimization choices:** `ProgramEvaluator` caches compiled programs by storage `TypeId`; compiled CXL captures regexes once at lowering; benchmark data cache uses BLAKE3 hash sidecars and atomic temp-file rename; channel staging uses stable content-addressed `.staged`/manifest pairs and per-source OS locks; window runtime shares `Arc` arena/index handles.
4. **Avoid:** Do not compile regexes per record, regenerate benchmark data unconditionally, copy staged sources when reuse-if-fresh is valid, or remove manifest/lock semantics around staged files.
5. **Benchmarks/tests available:** CXL eval/parse benches; `crates/clinker-bench-support/src/cache.rs` tests; `crates/clinker-channel/tests/staging_reuse_concurrent.rs`; `cargo bench -p clinker-channel --bench channel_merge`.
6. **Confidence:** High for cache existence and purpose; medium for runtime impact except where comments identify hot use.
7. **Evidence:** `crates/cxl/src/eval/mod.rs:115`, `:184`; `crates/cxl/src/eval/compiled.rs:33`, `:1129`; `crates/clinker-bench-support/src/cache.rs:1`, `:84`, `:197`; `crates/clinker-channel/src/staging_copy.rs:28`, `:55`, `:101`; `crates/clinker-exec/src/executor/window_runtime.rs:4`.

### Async Boundaries And Threading

1. **Area/module:** workspace runtime model, `clinker-net`.
2. **Why performance-sensitive:** Changing sync/async boundaries can alter back-pressure, blocking behavior, and scheduler assumptions.
3. **Existing optimization choices:** Executor is explicitly synchronous: source ingest uses `std::thread`, CPU kernels use Rayon, and back-pressure uses bounded crossbeam channels. REST source transport is finite-pull and blocking via `ureq` on the source ingest thread. `tokio` is a workspace dependency, but `rg` found no `async fn`, `.await`, or Tokio usage in Rust sources in this session.
4. **Avoid:** Do not introduce async into executor/REST paths without redesigning back-pressure, lock, and shutdown behavior.
5. **Benchmarks/tests available:** REST tests under `crates/clinker-net/tests/`; executor streaming/back-pressure tests; `parallel` bench.
6. **Confidence:** High.
7. **Evidence:** `Cargo.toml:75`; `crates/clinker-exec/src/executor/mod.rs:283`; `crates/clinker-net/src/lib.rs:1`; `crates/clinker-net/src/rest.rs:1`, `:36`.

### Profiling Hooks And Metrics

1. **Area/module:** `clinker-exec::executor::stage_metrics`, `clinker-bench-support::alloc`, benchmark harnesses.
2. **Why performance-sensitive:** These are the supported observability hooks for timing, RSS, CPU, IO, heap deltas, and spill bytes.
3. **Existing optimization choices:** Stage timers collect elapsed time, RSS, CPU, IO, optional heap deltas, and per-stage spill bytes. RSS capture is documented as low overhead at stage granularity. `bench-alloc` wraps the global allocator with relaxed atomic counters and is explicitly feature-gated because it distorts parallel timings.
4. **Avoid:** Do not treat `bench-alloc` timing as production throughput, increase stage metrics to per-row sampling without overhead measurements, or remove per-stage spill attribution.
5. **Benchmarks/tests available:** `cargo check --features bench-alloc -p clinker-benchmarks`; `crates/clinker-bench-support/tests/bench_alloc.rs`; all Criterion targets.
6. **Confidence:** High.
7. **Evidence:** `crates/clinker-exec/src/executor/stage_metrics.rs:1`, `:13`, `:39`, `:147`, `:199`; `crates/clinker-bench-support/src/alloc.rs:1`, `:16`, `:27`, `:77`.

## Benchmark Suites

Criterion benchmark targets currently found:

- `cargo bench -p clinker-record --bench record_ops`
- `cargo bench -p cxl --bench eval`
- `cargo bench -p cxl --bench parse`
- `cargo bench -p clinker-format --bench io_throughput`
- `cargo bench -p clinker-exec --bench arbitration_poll`
- `cargo bench -p clinker-exec --bench arena`
- `cargo bench -p clinker-exec --bench combine`
- `cargo bench -p clinker-exec --bench combine_grace_hash`
- `cargo bench -p clinker-exec --bench combine_iejoin`
- `cargo bench -p clinker-exec --bench combine_nary_3input`
- `cargo bench -p clinker-exec --bench composition`
- `cargo bench -p clinker-exec --bench deferred_buffer_pruning`
- `cargo bench -p clinker-exec --bench parallel`
- `cargo bench -p clinker-exec --bench pipeline`
- `cargo bench -p clinker-exec --bench provenance`
- `cargo bench -p clinker-exec --bench sort`
- `cargo bench -p clinker-exec --bench spill_compression`
- `cargo bench -p clinker-exec --bench window`
- `cargo bench -p clinker-channel --bench channel_merge`
- `cargo bench -p clinker-benchmarks --bench e2e_matrix`
- `cargo bench -p clinker-benchmarks --features bench-xlarge --bench e2e_xlarge`

Benchmark pipeline configs live under `benches/pipelines/` and cover scale, realistic ETL, formats, CXL operations, combine, execution modes, and feature behavior.

## Unsafe Code Notes

Unsafe code found during this inspection:

- `clinker-record::FieldStr` uses a private union, raw `Arc`/`Box` pointer round-trips, unchecked UTF-8, and unsafe `Send`/`Sync` impls to preserve the 24-byte string layout. This is performance/memory motivated and heavily documented.
- `clinker-bench-support::alloc::AccountingAlloc` uses unsafe `GlobalAlloc` forwarding for benchmark allocation accounting.
- `clinker-bench-support` generators use `from_utf8_unchecked` for deterministic generated bytes.
- Platform memory/stat code uses FFI/syscalls for RSS and process stats.
- Some unsafe calls are test-only environment-variable mutations required by Rust 2024; these are not performance optimizations.

Do not edit unsafe performance code without reading the local safety comments, size assertions, and relevant benches first.

## Review Notes

- This page did not run Criterion measurements; it documents available benchmark entry points and source-level performance assumptions.
- Existing docs under `docs/user` and `docs/engine` contain useful context, but source comments, manifests, benches, and tests were treated as primary evidence.
- Future agents should profile before changing algorithms or memory models, especially in transform/CXL eval, record/value storage, combine joins, memory arbitration, spill, and format IO.
