# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/clinker-exec`.
Follow the root instructions first.

## Purpose

`clinker-exec` is the runtime execution engine for compiled Clinker pipeline
DAGs. It owns source ingestion, operator dispatch, bounded-memory coordination,
spill handling, DLQ behavior, metrics/reporting, and runtime I/O handoff.

## Responsibilities

- Execute validated/compiled pipeline plans from `clinker-plan`.
- Drive finite Source nodes through `RecordSource` / `SourceInput`.
- Dispatch runtime operators: source, transform, aggregate, combine, route, merge, reshape, cull, envelope, composition, output, and correlation commit.
- Preserve bounded-memory behavior through `MemoryArbitrator`, node buffers, streaming handoff, backpressure, and spill.
- Own runtime reports: counters, DLQ entries, stage metrics, watermarks, rollback cursors, spill totals, RSS peaks, and interrupted status.
- Keep runtime execution synchronous with `std::thread`, bounded `crossbeam_channel`, and Rayon CPU kernels.

## Important public APIs

- `executor::PipelineExecutor`.
- `PipelineExecutor::{run_plan_with_readers_writers, run_plan_with_readers_writers_in_context}`.
- `PipelineExecutor::{explain_plan, explain_plan_dag}`.
- `executor::{PipelineRunParams, ExecutionReport, WriterRegistry, SourceReaders}`.
- `executor::single_file_reader`.
- `source::{RecordSource, SourceInput}`.
- `executor::storage_validate::{validate_storage_config, ResolvedStorage, StorageValidationError, FreeSpaceWarning, CapHeadroomWarning}`.
- `pipeline::memory::{MemoryArbitrator, MemoryConsumer, ArbitrationPolicy, ConsumerHandle, build_policy}`.
- `pipeline::shutdown::{ShutdownToken, install_signal_handler}`.
- `StageCollector`, `StageMetrics`, `StageName`.

## Internal module map

- `executor/`: main run body, source ingest, DAG dispatch, operator dispatch arms, node buffers, streaming handoff, DLQ, output registry, storage validation, stage metrics, and spill purge.
- `pipeline/`: runtime kernels and support for arena/window storage, combine/hash/sort/IEJoin/grace-hash, memory arbitration, spill files, shutdown, and sysstats.
- `source/`: transport-agnostic row source contract and multi-file reader handoff.
- `aggregation/`: hash/streaming aggregation, aggregate errors, and spill state.
- `output/`: output opening and sidecar writing.
- `metrics.rs`, `dlq.rs`, `progress.rs`, `projection.rs`, `sketch/`, and `modules.rs`: supporting runtime surfaces.

## Dependency rules

### Allowed dependencies

Existing normal dependencies are intentional evidence:

- Workspace/internal: `clinker-core-types`, `clinker-plan`, `clinker-record`, `clinker-format`, `cxl`.
- Runtime/concurrency: `crossbeam-channel`, `rayon`, `arc-swap`, `fs4`, `ctrlc` on native targets.
- Data/error/support: `serde`, `serde_json`, `serde-saphyr`, `indexmap`, `hashbrown`, `ahash`, `chrono`, `regex`, `thiserror`, `miette`, `tracing`.
- Spill/storage/runtime utilities: `tempfile`, `lz4_flex`, `postcard`, `blake3`, `uuid`, `glob`, `walkdir`, `csv`, `petgraph`, `libc`, `windows-sys`.
- Test/bench only or feature-gated: `criterion`, `insta`, `proptest`, `serial_test`, `clinker-channel`, and `clinker-bench-support`.

### Forbidden or suspicious dependencies

- Do not add async runtimes or async executor assumptions to the core runtime without architecture review.
- Do not add OpenSSL, native TLS, C build toolchain dependencies, or cargo-deny exceptions without approval.
- Do not let `clinker-bench-support` leak into default runtime paths; keep it feature-gated or dev-only.
- Be suspicious of transport-specific runtime dispatch; transports should converge on `RecordSource`.
- Do not add executor dependencies back into lower layers such as `clinker-plan`, `cxl`, `clinker-format`, or `clinker-record`.

## Important invariants

- Public executor entry points consume `CompiledPlan`, not raw YAML config.
- Runtime remains finite-batch and synchronous; `RecordSource::next_record` is finite by contract.
- Source transports feed the same ingest path through `SourceInput`; dispatcher logic should not branch on file vs non-file transport.
- One run-scoped `MemoryArbitrator` governs spill, pause, abort, and memory attribution decisions.
- Spill directory lifetime and `.lock` handling are load-bearing; `SpillDir` must release the lock before removing the directory.
- Crash purge runs only for configured spill roots, not arbitrary OS temp dirs.
- Document-boundary punctuations flow inline with records and must not be dropped or sent on side channels.
- Runtime writes fields against schemas already widened/bound by planning.
- Use `PipelineError::Internal` for violated plan/runtime invariants instead of panics.
- Snapshot tests, fixture YAML, and proptests are design contracts.
- Hypothesis: `bench-alloc` is acceptable only while strictly feature-gated and absent from default runtime paths.

## Common mistakes for AI agents to avoid

- Accepting `PipelineConfig` in new public executor APIs.
- Moving config parsing, YAML validation, or schema binding into `clinker-exec`.
- Adding async/Tokio code to the core execution path.
- Creating independent memory limits or spill decisions outside `MemoryArbitrator`.
- Buffering unbounded source data or whole documents in streaming paths.
- Dropping `StreamEvent` punctuations in transform/route/merge/output paths.
- Special-casing REST or future transports in operator dispatch.
- Treating `pipeline.batch_size` as a semantic output knob; verify whether it should affect only handoff/memory behavior.
- Updating runtime behavior without matching tests and docs.

## Local commands

- **Verified:** `git diff --check`
- **Inferred:** `cargo check -p clinker-exec --locked --offline`
- **Inferred:** `cargo check -p clinker-exec --features bench-alloc --locked --offline`
- **Inferred:** `cargo check --benches -p clinker-exec --locked --offline`
- **Inferred:** `cargo test -p clinker-exec --locked --offline <test_name>`
- **Inferred:** `ulimit -n 4096 && cargo test -p clinker-exec --locked --offline`

For Rust source changes, also run the workspace gates from the root
`AGENTS.md` unless the user explicitly scopes validation narrower. Spill-heavy
tests may need the raised `ulimit`.

## Documentation updates

Update relevant docs/examples when this crate changes runtime behavior,
diagnostics, storage/spill/memory semantics, output behavior, metrics, or
executor APIs:

- `docs/user/src/ops/memory.md`
- `docs/user/src/ops/storage.md`
- `docs/user/src/ops/streaming-vs-blocking.md`
- `docs/user/src/ops/metrics.md`
- `docs/user/src/ops/exit-codes.md`
- `docs/user/src/pipelines/error-handling.md`
- Relevant `docs/user/src/nodes/*.md`
- `docs/engine/src/execution-model.md`
- `docs/engine/src/memory-arbitration.md`
- `docs/engine/src/storage-internals.md`
- Relevant engine internals docs such as `combine-internals.md`, `output-internals.md`, `correlation-lifecycle.md`, and `retraction-protocol.md`
- `docs/ai/*.md` when architecture, commands, crate boundaries, or invariants change
- Relevant examples and fixtures under `examples/pipelines/` and `crates/clinker-exec/tests/fixtures/`

## Unclear / ask human

Use `docs/ai/80_OPEN_QUESTIONS.md` as the consolidated open-question list.
Relevant entries for this crate include current `CompiledPlan` reuse semantics,
the feature-gated `bench-alloc` edge, and SQL cursor wording without current
implementation evidence.

## Evidence

- `crates/clinker-exec/Cargo.toml`
- `crates/clinker-exec/src/lib.rs`
- `crates/clinker-exec/src/executor/mod.rs`
- `crates/clinker-exec/src/executor/params.rs`
- `crates/clinker-exec/src/source/mod.rs`
- `crates/clinker-exec/src/pipeline/memory.rs`
- `crates/clinker-exec/src/pipeline/shutdown.rs`
- `crates/clinker-exec/src/executor/spill_purge.rs`
- `crates/clinker-exec/tests/`
- `crates/clinker-exec/tests/snapshots/`
- `crates/clinker-exec/proptest-regressions/pipeline/iejoin.txt`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
