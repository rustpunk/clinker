# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/clinker-bench-support`.
Follow the root instructions first.

## Purpose

`clinker-bench-support` is shared test and benchmark support. It provides
deterministic data generators, benchmark pipeline discovery, cached generated
input files, in-memory I/O helpers, combine fixtures, and optional allocation
accounting.

This is not runtime product code. Keep it out of default runtime paths.

## Responsibilities

- Generate deterministic benchmark/test data for CSV, JSON, NDJSON, XML, fixed-width, nested-envelope, records, CXL templates, and combine cases.
- Discover benchmark pipeline YAML under `benches/pipelines`, sorted deterministically and excluding `future/`.
- Cache generated benchmark data under `bench-data/` or `CLINKER_BENCH_DATA` with `.meta.json` sidecars and BLAKE3 invalidation.
- Provide generic I/O helpers such as `SharedBuffer`, `fast_reader`, `slow_reader`, and `DelayedRowReader`.
- Provide feature-gated heap allocation counters through `bench-alloc`.
- Stay below `clinker-exec`; return generic `Read` / `Write` objects instead of importing executor-only types.

## Important public APIs

- `workspace_root`.
- `discover_pipeline_configs` and `ConfigEntry`.
- `Scale`, `Scale::ALL`, `SMALL`, `MEDIUM`, `LARGE`, `XLARGE`.
- `FieldKind`, `FieldKind::default_layout`, and `write_field_value`.
- `RecordFactory` and `CxlComplexity`.
- `cache::{BenchDataCache, DataSpec, DataFormat, NestedWrapper}`.
- `combine::CombineDataGen`.
- `io::{SharedBuffer, DelayedRowReader, slow_reader, fast_reader}`.
- `alloc::{AccountingAlloc, ALLOC, Stats, Region}` behind `bench-alloc`.
- Crate-root compatibility re-exports for generated data helpers such as `CsvPayload`, `generate_ndjson`, `generate_json_array`, `generate_xml`, `generate_fixed_width`, and nested-envelope writers.

## Internal module map

- `src/lib.rs`: crate docs, workspace discovery, benchmark config discovery, scale constants, field kinds, `RecordFactory`, CXL templates, and compatibility re-exports.
- `src/cache.rs`: generated data cache, cache hashes, filenames, env overrides, temp-file writes, and nested JSON/XML wrapping.
- `src/combine.rs`: deterministic correlated build/probe record sets for combine benchmarks.
- `src/io.rs`: in-memory output capture and synthetic fast/slow byte readers.
- `src/alloc.rs`: feature-gated global allocator accounting.
- `src/generators/`: deterministic format generators for CSV, JSON, XML, fixed-width, and nested envelopes.

## Dependency rules

### Allowed dependencies

Existing dependencies are the baseline:

- Internal: `clinker-record`.
- Normal external/workspace dependencies: `fastrand`, `blake3`, `glob`, `serde`, `serde_json`, `chrono`.
- Dev-only dependencies: `quick-xml`, `tempfile`, `serial_test`.
- Feature-gated code: `bench-alloc` enables only local allocation-accounting code.

### Forbidden or suspicious dependencies

- Do not add `clinker-exec` here; `src/io.rs` documents that this would form a dependency cycle.
- Be suspicious of normal dependencies on `clinker-plan`, `clinker-format`, `cxl`, `clinker`, `clinker-net`, `clinker-channel`, or `clinker-schema`.
- Do not add async runtimes, network clients, native TLS/OpenSSL, C build steps, or cargo-deny exceptions without approval.
- Keep XML parsing dev-only unless a real non-test API needs it.

## Important invariants

- Same inputs and seeds should produce byte-identical generated data.
- Benchmark pipeline discovery must be sorted and must exclude `future/`.
- `Scale` labels are Criterion/report identifiers; changing them changes benchmark identity.
- `FieldKind` and `DataFormat` use stable discriminants for cache hashing.
- Cache hashes must include every generation parameter that affects bytes, including field types, tags, wrappers, and generator version.
- Cache writes use temp files and rename; deterministic content makes last-writer-wins acceptable.
- `CLINKER_BENCH_REGENERATE` tests must remain serial because env mutation is process-global.
- Nested-envelope generators should stream through `Write` with reused scratch buffers instead of accumulating full documents.
- `bench-alloc` must remain feature-gated; its own docs say it distorts parallel timing.
- Unsafe code is local and justified for allocator forwarding and generated ASCII string construction.

## Common mistakes for AI agents to avoid

- Turning this crate into a default runtime dependency path.
- Adding `clinker-exec` imports to build executor `FileSlot` values directly.
- Making benchmark data nondeterministic through wall-clock seeds or unordered discovery.
- Regenerating cached data unconditionally.
- Changing cache filenames or hash inputs without considering stale cache behavior and benchmark comparability.
- Running ignored large-data tests casually.
- Treating `bench-alloc` timings as production throughput.
- Removing `serial_test` from env-var mutation tests.
- Copying generator shortcuts into production parsers or writers.

## Local commands

- **Inferred:** `cargo check -p clinker-bench-support --locked --offline`
- **Inferred:** `cargo test -p clinker-bench-support --locked --offline`
- **Inferred:** `cargo test -p clinker-bench-support --features bench-alloc --locked --offline`
- **Inferred:** `cargo test -p clinker-bench-support --locked --offline -- --ignored`
- **Verified:** `git diff --check`

Run ignored tests only when explicitly needed; docs note at least one ignored XML generator test creates about 600 MB.

## Documentation updates

Update relevant docs when this crate changes public helpers, benchmark discovery, cache behavior, feature gates, unsafe allocation accounting, or benchmark data shape:

- `crates/clinker-bench-support/src/lib.rs`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/40_COMMON_PATTERNS.md`
- `docs/ai/50_TESTING_AND_COMMANDS.md`
- `docs/ai/60_PERFORMANCE_NOTES.md`
- `docs/ai/80_OPEN_QUESTIONS.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
- Relevant YAML under `benches/pipelines/` when discovery assumptions or benchmark coverage change.

## Unclear / ask human

Track unresolved questions only in `docs/ai/80_OPEN_QUESTIONS.md`. Relevant entries include benchmark helper runtime boundaries, benchmark identity/cache compatibility, and the long-term `bench-alloc` dependency edge.

## Evidence

- `crates/clinker-bench-support/Cargo.toml`
- `crates/clinker-bench-support/src/lib.rs`
- `crates/clinker-bench-support/src/cache.rs`
- `crates/clinker-bench-support/src/io.rs`
- `crates/clinker-bench-support/src/alloc.rs`
- `crates/clinker-bench-support/src/combine.rs`
- `crates/clinker-bench-support/src/generators/`
- `crates/clinker-bench-support/tests/bench_alloc.rs`
- `crates/clinker-benchmarks/src/lib.rs`
- `crates/clinker-benchmarks/src/runner.rs`
- `crates/clinker-exec/Cargo.toml`
- `benches/pipelines/`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/40_COMMON_PATTERNS.md`
- `docs/ai/50_TESTING_AND_COMMANDS.md`
- `docs/ai/80_OPEN_QUESTIONS.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
