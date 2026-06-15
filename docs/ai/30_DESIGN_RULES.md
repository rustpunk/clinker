# AI Onboarding: Design Rules

Purpose: Collect design constraints and review rules that AI agents must validate before changing behavior.

## Status

AI-generated initial draft requiring human review. Claims below were checked
against current manifests, CI, and source files, not only against older docs.
`CLAUDE.md` and `docs/engine/src/architecture.md` still mention an older
`clinker-core` crate layout; treat those portions as stale unless re-verified
against current `Cargo.toml` and `crates/*`.

## Source Evidence

Validate this page against:

- `CLAUDE.md`
- `deny.toml`
- `.github/workflows/ci.yml`
- `rg "Legacy|Internal|Block|serde\\(default\\)|ignore" crates docs`
- `rg "serde-saphyr|serde_yaml|serde_yml|cmake|native-tls|openssl" Cargo.toml Cargo.lock crates deny.toml`
- `cargo metadata --no-deps`

Existing files under `docs/*` may be stale. Treat them as secondary context only; any design-rule claim found in `docs/*` must be validated against code, manifests, tests, examples, or safe local commands before being copied here.

## Architectural Rules

- Clinker is a finite-job, single-process pipeline engine. The user-facing
  non-goals explicitly rule out unbounded stream processing, worker-process
  pools, distributed execution, daemon/service mode, SQL query-engine behavior,
  connector marketplaces, and CDC. Code evidence matches this: `RecordSource`
  says `next_record` is finite by contract, REST sources require page/record
  caps, and `PipelineExecutor` runs synchronously over OS threads, bounded
  channels, and Rayon rather than an async service loop.
- Preserve the current crate layering. `clinker-plan` parses config, resolves
  schemas, compiles CXL, validates, and produces a typed
  `ExecutionPlanDag`; it does not depend on runtime operators. `clinker-exec`
  consumes compiled plans and owns runtime dispatch. `clinker-record` and
  `clinker-core-types` are lower-level vocabulary crates. Edge crates
  (`clinker`, `cxl-cli`, `clinker-channel`, `clinker-net`, `clinker-schema`)
  should not become back-edges into lower layers without a deliberate design
  review.
- The executor public entry point is compiled-plan based. The
  `PipelineExecutor::run_plan_with_readers_writers` method accepts
  `&clinker_plan::plan::CompiledPlan` and includes a `compile_fail` doctest
  rejecting `&PipelineConfig`. New executor APIs should keep
  planning/validation ahead of execution rather than accepting raw config
  casually.
- The runtime path is synchronous. `PipelineExecutor` docs state no async
  runtime is required; source ingest uses one `std::thread` per declared source,
  live handoff uses bounded `crossbeam_channel`s, and CPU-bound kernels use one
  run-scoped Rayon pool sized by `pipeline.concurrency.threads`. `tokio` exists
  in workspace dependencies, but current core execution is not async.
- Bounded-memory behavior is load-bearing. `PipelineMeta.memory` defaults to a
  512 MiB limit at arbitrator construction, and `BackpressureKnob` selects
  `spill`, `pause`, or `both`. Runtime code creates one run-scoped spill
  directory, one run-scoped `MemoryArbitrator`, registers source/node-buffer
  consumers, and uses spill cleanup/locking. Changes to blocking operators,
  streaming eligibility, or buffer admission need memory tests or explain-output
  coverage.
- The unified `nodes:` topology is the only parsed pipeline shape. `PipelineConfig`
  uses `nodes: Vec<Spanned<PipelineNode>>`; comments say legacy top-level
  `inputs`/`outputs`/`transformations` are rejected by serde. Current
  `PipelineNode` variants include `Source`, `Transform`, `Aggregate`, `Route`,
  `Merge`, `Combine`, `Output`, `Reshape`, `Cull`, `Envelope`, and
  `Composition`.
- Channels and compositions are sealed at declared boundaries. `clinker-channel`
  documents that channels can override declared config parameters only, cannot
  patch mid-graph internals, and cannot reach sealed composition internals.
  Keep extension points explicit through schemas, declared params, ports, and
  resources.
- Path security uses proof tokens. `clinker-plan::security::ValidatedPath` has a
  private inner path and can only be obtained through `validate_path`, which
  rejects null bytes, traversal, encoded traversal, unauthorized absolute paths,
  and symlink escapes where filesystem state permits. APIs that need trusted
  paths should accept `ValidatedPath`, not raw `PathBuf`.

## Primary Code Evidence

- Workspace and layering: root `Cargo.toml`; `docs/ai/20_CRATE_MAP.md`;
  `crates/*/Cargo.toml`.
- Planning boundary: `crates/clinker-plan/src/lib.rs`;
  `crates/clinker-plan/src/config/pipeline.rs`;
  `crates/clinker-plan/src/config/pipeline_node.rs`.
- Runtime boundary: `crates/clinker-exec/src/executor/mod.rs`;
  `crates/clinker-exec/src/source/mod.rs`;
  `crates/clinker-exec/src/executor/source_stream.rs`;
  `crates/clinker-exec/src/executor/dispatch.rs`;
  `crates/clinker-exec/src/pipeline/memory.rs`.
- Finite network transport: `crates/clinker-net/src/lib.rs`;
  `crates/clinker-net/src/rest.rs`.
- Records and format APIs: `crates/clinker-record/src/lib.rs`;
  `crates/clinker-format/src/lib.rs`;
  `crates/clinker-format/src/error.rs`.
- Channel boundary: `crates/clinker-channel/src/lib.rs`;
  `crates/clinker-channel/src/binding.rs`;
  `crates/clinker-channel/src/overlay.rs`.
- Security: `crates/clinker-plan/src/security.rs`.
- CI/dependency gates: `.github/workflows/ci.yml`; `deny.toml`; root
  `Cargo.toml`.

## Refactoring Rules

- Do not add compatibility shims around retired config shapes or old module
  names. Current code contains explicit retire-gate tests in
  `clinker-plan/src/lib.rs` for the old `cxl_compile` module name and retired
  `E100` diagnostic code.
- Keep dead-code pressure intact. CI intentionally runs
  `cargo clippy --workspace -- -D warnings` without `--all-targets` before the
  all-targets pass so test-only `pub(crate)` items still fail as dead code.
  New public or crate-public items should have real non-test callers or a clear
  public API reason.
- Prefer typed state and proof types at subsystem boundaries. Existing examples:
  `ValidatedPath` for path security, `CompiledPlan` for executor entry,
  `RecordSource: Send` for source transport, `CxlSource` for source-spanned CXL,
  and `Spanned<PipelineNode>` for YAML-origin diagnostics.
- Preserve span-aware parsing paths. `PipelineNode` uses a custom serde visitor
  to dispatch on `type:` while retaining serde-saphyr spans; avoid replacing
  that with a `serde_json::Value` intermediate unless you can prove diagnostic
  spans and `deny_unknown_fields` behavior survive.
- Use `PipelineError::Internal` for plan-time invariant violations discovered
  at runtime, not `panic!`, `todo!`, or silent fallback. Several executor and
  plan graph utilities document this convention.
- Add focused tests where the boundary is architectural, not just functional.
  Existing tests pin node taxonomy, streaming/fusion behavior, memory
  backpressure, scheduling, spill handling, document DLQ, channel overlays, REST
  pagination, provenance, and snapshot explain output.

## Configuration And Format Rules

- YAML parsing must go through `clinker_plan::yaml`. The module states it is
  the single YAML parser chokepoint and the only place that should call
  `serde_saphyr::from_str*` or construct a serde-saphyr budget. It enforces a
  32 MiB pre-parse input limit, max depth 256, include depth 0, max nodes
  100,000, and alias/anchor ratio checks.
- Use `serde-saphyr`, not `serde_yaml` or `serde_yml`, for YAML. The workspace
  pins `serde-saphyr = "=0.0.23"` with `miette` support. Current source shows
  normal YAML config paths using `clinker_plan::yaml::from_str`; the direct
  `serde_saphyr::from_str` found in tests should be treated as test-local
  spike/validation code, not a new production pattern.
- Keep `#[serde(deny_unknown_fields)]` on user-facing config structs where it
  already exists. This is part of the config contract for top-level pipeline
  metadata, memory/concurrency/metrics blocks, and node variant payloads.
- Be conservative with `#[serde(default)]`. It is valid for optional or
  backward-compatible config surfaces already modeled that way, but do not use
  it to hide newly mandatory fields or preserve a retired shape. If adding a
  default, document the user-facing omitted-value behavior and add parse tests.
- Source and format behavior should stay transport-agnostic where possible.
  File readers implement `FormatReader`; executor ingest works through
  `RecordSource`; REST adapts into `RecordSource` and then flows through the
  same ingest channel as files. Avoid special-case runtime dispatch for a
  transport unless the shared contract is insufficient.
- TOML is used for storage/workspace configuration (`ClinkerToml`,
  `StorageConfig`) through the `toml` crate; do not route TOML through YAML
  helpers.
- JSON parsing with `serde_json` is normal for data-format readers, metrics,
  output sidecars, tests, and CXL CLI input. The YAML chokepoint rule does not
  apply to JSON payload parsing.

## Dependency Rules

- The workspace dependency policy is to avoid adding new native toolchain
  requirements without review. `deny.toml` bans `cmake`, and root `Cargo.toml`
  comments explain choices such as `blake3` with `pure` and `ureq` with
  `rustls` to avoid OpenSSL/native-tls and extra C build steps in Clinker
  crates.
- Do not introduce `openssl`, `native-tls`, `cmake`, new C build requirements,
  or equivalent transitive requirements without explicit architectural approval
  and corresponding `cargo deny` updates.
- `cargo deny` treats yanked crates as denied and all unmaintained advisories as
  findings. Existing ignored RustSec advisories are documented as Dioxus desktop
  transitive dependencies; current crate-map notes suggest Dioxus/Kiln
  references may be stale, so do not copy those exemptions into new design
  decisions without re-checking.
- Keep benchmark/test helper dependencies out of default runtime code.
  `clinker-exec` has an optional `clinker-bench-support` edge for
  `bench-alloc`; do not let benchmark helpers leak into default execution
  paths.
- Network transport currently uses blocking `ureq` over rustls. Adding async
  clients or a Tokio-driven runtime would be an architecture change, not a
  local connector tweak.

## Documentation Rules

- Treat AI onboarding docs as review scaffolds until human-reviewed. Validate
  claims against code, manifests, tests, examples, and safe local commands.
- Prefer evidence anchors over intent claims. Name files, modules, traits,
  functions, variants, tests, and commands that support a rule.
- Separate verified facts from hypotheses and stale references. Current example:
  older docs mention `clinker-core` and "11 workspace crates", while the current
  root workspace has 13 members and separate `clinker-plan` / `clinker-exec` /
  `clinker-core-types` crates.
- Do not write marketing copy in AI-facing docs. Keep rules actionable for a
  future engineer or coding agent deciding whether a change crosses an
  architectural boundary.
- When adding public Rust APIs, follow the repo's source-doc convention:
  public items generally have `///` summaries and `# Errors` / `# Panics` /
  `# Safety` sections where applicable. For documentation-only changes, do not
  edit Rust source to "fix" docs unless the task explicitly allows source edits.

## Testing And Review Rules

- The CI gate in `.github/workflows/ci.yml` currently runs:
  `cargo fmt --all --check`, two clippy passes, `cargo test --workspace`,
  `cargo check --benches --workspace`, `cargo check --features bench-alloc -p
  clinker-benchmarks`, `cargo test --benches -p clinker-benchmarks`, native
  Windows/macOS workspace tests, selected cross-target checks, and `cargo deny`.
- For docs-only edits, cargo tests are often unnecessary, but run targeted
  commands if the documentation generated from source or examples might be
  broken. At minimum inspect the rendered Markdown for broken headings and
  stale file names.
- For behavior changes, match tests to the boundary touched:
  config parsing/validation in `clinker-plan`, runtime execution in
  `clinker-exec/tests` or `src/executor/tests`, channel behavior in
  `clinker-channel/tests`, format reader/writer behavior in `clinker-format`
  tests, language changes in `cxl` tests and benches, and CLI behavior in
  `crates/clinker/tests`.
- Snapshot tests using `insta` and fixture YAML under `crates/clinker-exec/tests`
  are part of the design contract for explain output, node taxonomy, combine
  behavior, and user-visible diagnostics.

## Human Review Notes

- Confirm whether stale Dioxus/Kiln references in CI, root dependencies, and
  older docs should remain documented as exemptions or be removed.
- Confirm whether the direct `serde_saphyr::from_str` call in
  `crates/clinker-exec/tests/composition_binding_test.rs` is intentionally
  test-local, or whether a future cleanup should route even that through
  `clinker_plan::yaml::from_str`.
