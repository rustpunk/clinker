# AGENTS.md

Root `AGENTS.md` still applies. This file adds local guidance for `clinker-channel`.

## Purpose

`clinker-channel` owns channel binding, validation, overlay application, and source-file staging copy for Clinker launch flows.

## Responsibilities

- Parse `.channel.yaml` files into `ChannelBinding` values and compute raw-file BLAKE3 channel hashes.
- Validate `DottedPath` keys, channel var names, channel targets, and composition config keys.
- Apply `ChannelDefault` / `ChannelFixed` config layers to `CompiledPlan` provenance.
- Resolve channel `vars:` overrides/adds for `$vars`, `$pipeline`, `$source`, and `$record`.
- Stamp `ChannelIdentity` on compiled plans.
- Stage selected source files to local disk with stable cache paths, manifests, locks, reuse, cleanup, and crash purge.

## Important public APIs

- `ChannelBinding`, `ChannelTarget`, `DottedPath`
- `ChannelBinding::{from_yaml_bytes, load}`
- `scan_workspace_channels`, `validate_channel_bindings`
- `apply_channel_overlay`, `ChannelOverlayResult`
- `SourceStager`, `StagingPlanEntry`, `ReuseDecision`, `open_source_file`
- `ChannelError`, `StagingError`

## Internal module map

- `src/lib.rs`: channel authoring guide and crate-root re-exports.
- `src/binding.rs`: channel YAML parsing, target classification, dotted-path validation, workspace scan, and composition-target validation.
- `src/overlay.rs`: provenance overlay merge, channel identity stamping, scoped-var overlay validation/coercion.
- `src/staging_copy.rs`: source staging protocol, cache layout, manifests, advisory locks, reuse prediction, cleanup, crash purge, and platform open/fsync helpers.
- `src/error.rs`: channel parse and validation errors.

## Dependency rules

### Allowed dependencies

Current normal dependencies are intentional: `clinker-plan`, `clinker-core-types`, `clinker-record`, `serde-saphyr`, `serde`, `serde_json`, `chrono`, `blake3`, `indexmap`, `tracing`, `thiserror`, `walkdir`, `uuid`, `tempfile`, `fs4`, and Unix-only `nix` with `fs`. Current dev dependencies are `criterion` and `serial_test`.

### Forbidden or suspicious dependencies

- Do not add executor/runtime operator back-edges casually; this is an edge/integration crate.
- Do not add async runtimes, network clients, native TLS/OpenSSL, C build steps, or cargo-deny exceptions without approval.
- Do not add benchmark/test helpers to default runtime paths.
- Treat direct production YAML parsing outside `clinker_plan::yaml` as suspicious.
- Avoid new filesystem locking/copy/hash crates that duplicate `fs4`, `tempfile`, `blake3`, or existing staging policy types without review.

## Important invariants

- Channels override declared config/resource surfaces only; no mid-graph patching or sealed composition-internal access.
- `DottedPath` is strict: one or two segments, no empty/edge/consecutive dots, and only ASCII alphanumeric, underscore, or dot.
- Workspace channel scans are bounded and do not follow symlinks.
- `ChannelDefault` and `ChannelFixed` must preserve provenance precedence; fixed wins over default.
- Channel `vars:` on composition targets currently errors.
- Var overrides must match declared types and use planner scoped-var checking/coercion helpers.
- Source-scoped var overrides must name an existing source node.
- `ChannelIdentity` is based on raw channel file bytes.
- Staging uses stable content-addressed `<source_id>.staged` plus a manifest; the manifest is the commit marker.
- Staging copy is bounded-memory: one fixed chunk buffer, no whole-file accumulation.
- Per-source advisory locks protect copy/publish, readers, cleanup, overwrite, and crash purge.
- Crash purge must be liveness-aware and grace-gated; do not reap live sibling artifacts.
- `SourceStager` keeps shared read guards alive until cleanup so staged files cannot vanish before or during reads.
- Hypothesis: resource overlay fields are parsed but not fully applied by `apply_channel_overlay`; ask before documenting resources as implemented overlay behavior.

## Common mistakes for AI agents to avoid

- Treating channel overlays as arbitrary JSON patches.
- Letting channels reach into composition internals beyond declared schemas.
- Replacing `DottedPath` or var-name validation with raw strings.
- Applying a channel overlay without checking returned diagnostics for errors.
- Dropping `SourceStager` read guards before readers finish.
- Turning staging into per-run temp-only paths and breaking reuse.
- Removing the manifest commit-marker discipline.
- Making cleanup or purge delete files without lock/liveness checks.
- Assuming staging is a planner concern; policy parsing/validation is in `clinker-plan`, copy mechanics are here, and CLI orchestrates.

## Local commands

- Inferred: `cargo test -p clinker-channel --locked --offline`
- Inferred: `cargo check --benches -p clinker-channel --locked --offline`
- Inferred, performance-sensitive only: `cargo bench -p clinker-channel --bench channel_merge`
- Inferred, docs-only: `git diff --check`

## Documentation updates

Update these when changing related behavior:

- `docs/user/src/pipelines/channels.md`
- `docs/user/src/pipelines/variables.md`
- `docs/user/src/ops/storage.md`
- `docs/engine/src/storage-internals.md`
- `docs/explain/E105.md`, `docs/explain/E335.md`, `docs/explain/E336.md`, `docs/explain/E337.md`
- `examples/pipelines/channels/*/*.channel.yaml` and nearby pipeline examples
- `docs/ai/20_CRATE_MAP.md`, `docs/ai/30_DESIGN_RULES.md`, `docs/ai/40_COMMON_PATTERNS.md`, `docs/ai/80_OPEN_QUESTIONS.md`

## Evidence

- `crates/clinker-channel/src/lib.rs`
- `crates/clinker-channel/src/binding.rs`
- `crates/clinker-channel/src/overlay.rs`
- `crates/clinker-channel/src/staging_copy.rs`
- `crates/clinker-channel/src/error.rs`
- `crates/clinker-channel/Cargo.toml`
- `crates/clinker-channel/tests/channel_binding_test.rs`
- `crates/clinker-channel/tests/channel_merge_test.rs`
- `crates/clinker-channel/tests/channel_var_overlay_test.rs`
- `crates/clinker-channel/tests/staging_reuse_concurrent.rs`
- `crates/clinker-channel/benches/channel_merge.rs`
- `crates/clinker/src/main.rs`
- `crates/clinker-plan/src/config/storage.rs`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
