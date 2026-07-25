# AI Onboarding: Testing And Commands

Verified against origin/main cf6609b9 (2026-07-24).

Purpose: Give future Codex sessions a practical, current command guide for building, testing, linting, documenting, and validating Clinker changes.

Status labels:

- **Verified** means this Codex session ran the command successfully in this workspace.
- **Inferred** means the command is supported by repository config, CI, manifests, or docs, but this session did not run it successfully end to end.
- **Environment-dependent** means the command is valid but needs permissions, host tools, OS support, or higher resource limits than a restricted sandbox may provide.

## 1. Required Tools

- **Verified:** `rust-toolchain.toml` pins Rust `1.91` with `clippy` and `rustfmt`.
- **Verified:** `cargo`, `rustc`, and `rustfmt` were available from `~/.cargo/bin`.
- **Verified:** `cargo-deny` was available and `cargo deny check` passed outside the filesystem sandbox.
- **Verified:** `mdbook` was available and both mdBook projects built successfully.
- **Inferred:** There is no root `Makefile`, `justfile`, `package.json`, `trunk.toml`, Vite config, Netlify config, or Vercel config in the discovered workspace.

## 2. Basic Build Command

```bash
cargo build --workspace --locked --offline
```

Status: **Verified.**

Use `cargo build --workspace` when online dependency resolution is acceptable. The locked/offline variant is better for Codex sessions when `Cargo.lock` and the local cargo cache are already present.

## 3. Fast Check Command

```bash
cargo check --workspace --locked --offline
```

Status: **Verified.**

This is the best first compile signal after ordinary code edits. It does not compile benches or run tests.

For bench call-site compile coverage:

```bash
cargo check --benches --workspace --locked --offline
```

Status: **Verified.**

CI runs the non-locked online form:

```bash
cargo check --benches --workspace
```

Status: **Inferred from CI.**

## 4. Full Test Command

```bash
# Raise the file-descriptor soft limit to the 65536 floor the spill tests
# need. Raise-only by construction: the branch runs only when the current
# soft limit is below the floor, and both targets are >= the current soft
# limit — so an already-sufficient limit is never lowered. `-S` is
# load-bearing: a bare `ulimit -n N` sets the soft AND the hard limit, and
# an unprivileged process can never raise a hard limit back.
[ "$(ulimit -Sn)" != unlimited ] && [ "$(ulimit -Sn)" -lt 65536 ] \
  && { ulimit -S -n 65536 2>/dev/null || ulimit -S -n "$(ulimit -Hn)"; }

cargo test --workspace --locked --offline
```

Status: **Verified outside the sandbox.**

Why the fd floor matters: the spill path opens up to `MERGE_FAN_IN` = 64
readers per active k-way merge pass
(`crates/clinker-exec/src/pipeline/spill_merge.rs`), and grace-hash holds
one open probe-spill writer per on-disk partition
(`crates/clinker-exec/src/pipeline/grace_hash/mod.rs`, `PartitionState::OnDisk`).
`cargo test` multiplies that by the libtest thread count, which defaults to
the core count — so the process-wide peak scales with `nproc`, not with any
single test. Below the floor, spill tests fail with
`Too many open files (os error 24)`, and *which* tests fail varies run to
run: the victim is whichever test opens a file while the process is at the
ceiling.

Measured on a 32-core host (`nproc` = 32), sampling `/proc/<pid>/fd` of the
`clinker-exec` lib test binary about 16 000 times per run across five runs:
the peak concurrent descriptor count is **4165–4222**. Any floor at or below
`4096` is therefore below what the suite actually needs on a host this size.
The floor of `65536` gives roughly 15x headroom over the measured peak,
which absorbs higher core counts — demand scales with
`available_parallelism()`, so a 128-core host lands near ~17 000 and still
fits.

End-to-end confirmation on the same host, running
`cargo test -p clinker-exec --lib --locked --offline`:

| soft `ulimit -n` | result |
|---|---|
| 4096 | FAILED — 868 passed, 2 failed, both `Too many open files (os error 24)` |
| 4096 with `-- --test-threads=4` | FAILED — 868 passed, 2 failed |
| 65536 | ok — 870 passed, 0 failed |

Cutting the thread count does not rescue 4096: a single grace-hash or
cascaded-merge test can sit near the per-operator bound on its own.

Never write a bare `ulimit -n <n>` before the suite. On a host whose soft
limit already exceeds `<n>` that command *lowers* the limit into the failing
range — following the instruction becomes the cause of the failure it was
meant to prevent.

The `clinker-net` REST e2e tests separately need permission to bind local
sockets; they fail inside the restricted sandbox with
`Operation not permitted` and pass outside it.

CI runs:

```bash
cargo test --workspace
```

Status: **Inferred from CI.**

There is at least one intentionally ignored slow test:

```bash
cargo test -p clinker-bench-support -- --ignored
```

Status: **Inferred.** The ignored XML generator test says it generates about 600 MB.

## 5. Per-Crate Test Commands

Workspace packages from `cargo metadata --no-deps`:

```bash
cargo test -p clinker-record
cargo test -p clinker-bench-support
cargo test -p cxl
cargo test -p cxl-cli
cargo test -p clinker-format
cargo test -p clinker-core-types
cargo test -p clinker-plan
cargo test -p clinker-exec
cargo test -p clinker-channel
cargo test -p clinker-net
cargo test -p clinker
cargo test -p clinker-schema
cargo test -p clinker-lineage
cargo test -p clinker-benchmarks
cargo test -p clinker-scenarios
```

The scenario corpus gate is a `clinker` integration test, because
`CARGO_BIN_EXE_clinker` is only defined for that package:

```bash
cargo test -p clinker --test scenarios
```

It generates each scenario's input into a temporary directory, runs the real
CLI, and byte-compares every output against the committed goldens under
`examples/scenarios/*/expected/`. To re-bless after an intended change:

```bash
UPDATE_SCENARIO_GOLDENS=1 cargo test -p clinker --test scenarios -- --nocapture
```

`--nocapture` is required — libtest swallows stdout on a passing test, and a
re-bless run passes, so without it the input digest to paste back into the
harness's `GATES` table is never printed.

Status: **Inferred for the exact per-crate commands.** The full workspace test command above covered these packages successfully outside the sandbox at a soft fd limit of 65536 or higher.

Targeted examples:

```bash
cargo test -p cxl --locked --offline
cargo test -p clinker-exec --lib --locked --offline executor::tests::spill_dir_unavailable_midrun::unarmed_seam_lets_a_real_spilling_run_complete -- --exact
```

Status: **Verified.** The second command needs the section 4 fd floor: it
failed at `ulimit -n 1024` and passes at 65536. Apply the raise-only
snippet from section 4 first rather than prefixing a fixed `ulimit -n`.

## 6. Formatting Command

```bash
cargo fmt --all --check
```

Status: **Verified.**

To fix formatting locally:

```bash
cargo fmt --all
```

Status: **Inferred.** Do not run the fixing form unless formatting edits are in scope.

## 7. Linting Command

CI intentionally runs both clippy passes:

```bash
cargo clippy --workspace --locked --offline -- -D warnings
cargo clippy --workspace --all-targets --locked --offline -- -D warnings
```

Status: **Verified.**

The first pass omits `--all-targets` so dead code referenced only from tests still fails. The second pass adds tests, benches, and examples.

CI runs the online forms:

```bash
cargo clippy --workspace -- -D warnings
cargo clippy --workspace --all-targets -- -D warnings
```

Status: **Inferred from CI.**

Dependency/license/advisory audit:

```bash
cargo deny check
```

Status: **Verified outside the filesystem sandbox.** In the sandbox it failed to acquire `~/.cargo/advisory-dbs/db.lock` because that path was read-only. The successful run emitted warnings about duplicate allowed dependencies and stale ignore/allow config, but exited 0 with `advisories ok, bans ok, licenses ok, sources ok`.

## 8. Docs Generation Command

Rust API docs:

```bash
cargo doc --workspace --no-deps --locked --offline
```

Status: **Verified.**

This generated docs successfully but emitted rustdoc warnings for broken/private intra-doc links, invalid HTML tags, and bare URLs. Treat warnings as cleanup work if touching nearby docs, but this command currently exits 0.

User guide mdBook:

```bash
mdbook build docs/user -d /tmp/clinker-mdbook-user
```

Status: **Verified.**

Engine internals mdBook:

```bash
mdbook build docs/engine -d /tmp/clinker-mdbook-engine
```

Status: **Verified.**

## 9. Example/Demo Commands

CLI help:

```bash
cargo run --locked --offline -p clinker -- --help
cargo run --locked --offline -p cxl-cli -- --help
```

Status: **Verified.**

Common pipeline commands from `CLAUDE.md` and user docs:

```bash
cargo run -p clinker -- run examples/pipelines/customer_etl.yaml --explain
cargo run -p clinker -- run examples/pipelines/customer_etl.yaml --lineage -
cargo run -p clinker -- run examples/pipelines/customer_etl.yaml --dry-run -n 10
cargo run -p clinker -- run examples/pipelines/tumbling_clicks.yaml
cargo run -p clinker -- run examples/pipelines/hopping_sliding_5m_1h.yaml
cargo run -p clinker -- run examples/pipelines/scd_type2.yaml
cargo run -p clinker -- explain --code E103
cargo run -p cxl-cli -- check transform.cxl
cargo run -p cxl-cli -- eval -e 'emit result = 1 + 2'
cargo run -p cxl-cli -- fmt transform.cxl
```

Status: **Inferred.** This session did not execute pipeline examples because many write outputs or depend on specific fixture context. Prefer `--explain` or `--dry-run` first when validating examples.

## 10. Website/Docs Commands

The repo has two mdBook configs:

- `docs/user/book.toml`
- `docs/engine/book.toml`

Build commands are listed in section 8 and are **Verified**.

No website deployment config was discovered for Netlify, Vercel, Vite, Trunk, npm, or pnpm. A generated `docs/book/index.html` exists, but no deployment command was found in CI.

Status: **Inferred from file discovery.**

## 11. Benchmark/Performance Commands

CI bench gates (local `--locked --offline` variants; CI runs the plain online
forms shown in section 12):

```bash
cargo check --benches --workspace --locked --offline
cargo check --features bench-alloc -p clinker-benchmarks --locked --offline
cargo test --benches -p clinker-benchmarks --locked --offline
```

Status: **Verified.**

The `cargo test --benches -p clinker-benchmarks` command is not just a compile check; it runs a full e2e benchmark smoke matrix across many YAML configs and size tiers. It took several minutes in this session.

Benchmark targets from `cargo metadata`:

```bash
cargo bench -p clinker-record --bench record_ops
cargo bench -p cxl --bench eval
cargo bench -p cxl --bench parse
cargo bench -p clinker-format --bench io_throughput
cargo bench -p clinker-exec --bench arbitration_poll
cargo bench -p clinker-exec --bench arena
cargo bench -p clinker-exec --bench combine
cargo bench -p clinker-exec --bench combine_grace_hash
cargo bench -p clinker-exec --bench combine_iejoin
cargo bench -p clinker-exec --bench combine_nary_3input
cargo bench -p clinker-exec --bench composition
cargo bench -p clinker-exec --bench deferred_buffer_pruning
cargo bench -p clinker-exec --bench parallel
cargo bench -p clinker-exec --bench pipeline
cargo bench -p clinker-exec --bench provenance
cargo bench -p clinker-exec --bench sort
cargo bench -p clinker-exec --bench spill_compression
cargo bench -p clinker-exec --bench window
cargo bench -p clinker-channel --bench channel_merge
cargo bench -p clinker-benchmarks --bench e2e_matrix
cargo bench -p clinker-benchmarks --features bench-xlarge --bench e2e_xlarge
```

Status: **Inferred.** This session did not run Criterion benchmark measurements.

## 12. Commands Codex Should Run Before Claiming Success

For AI onboarding docs-only changes under `docs/ai/`:

```bash
git diff --check
```

Status: **Verified.** AI docs are not currently an mdBook, so this is the
smallest relevant gate unless the edit changes commands, links into generated
books, or user/engine docs.

For user or engine mdBook documentation changes, run the relevant book build:

```bash
mdbook build docs/user -d /tmp/clinker-mdbook-user
mdbook build docs/engine -d /tmp/clinker-mdbook-engine
```

Status: **Verified.** Run the mdBook command relevant to the docs changed; run
both if shared docs/theme or cross-book docs changed. `cargo fmt --all --check`
is not required for pure Markdown edits unless Rust source or generated Rust
docs are touched.

For Rust code changes:

```bash
cargo fmt --all --check
cargo check --workspace --locked --offline
cargo clippy --workspace --locked --offline -- -D warnings
cargo clippy --workspace --all-targets --locked --offline -- -D warnings
cargo test --workspace --locked --offline
```

Status: **Verified outside the sandbox for the full test command.** Apply
the raise-only fd snippet from section 4 before the test command; the
spill tests need a soft `ulimit -n` of at least 65536. If REST e2e tests
are involved, the full test command may need unsandboxed localhost socket
access.

For sprint-closing / CI parity:

```bash
cargo fmt --all --check
cargo clippy --workspace -- -D warnings
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check --benches --workspace
cargo check --features bench-alloc -p clinker-benchmarks
cargo test --benches -p clinker-benchmarks
cargo deny check
```

CI's `check` job additionally smoke-checks every example pipeline: it builds
the CLI (`cargo build --locked -p clinker`) and runs each
`examples/pipelines/*.yaml` through `clinker ... --explain`
(`.github/workflows/ci.yml`, "Smoke-check example pipelines" step). If a change
touches example pipelines, plan/config validation, or CLI explain behavior,
run that smoke pass locally before pushing.

Status: **Inferred from CI for the exact online forms.** Locked/offline variants of all Rust compile/test/bench commands above were verified where practical; `cargo deny check` was verified outside the filesystem sandbox.

## 13. Expensive, Flaky, Or Environment-Dependent Commands

- **Environment-dependent:** `cargo test --workspace` needs local socket permission for `clinker-net` REST e2e tests. The restricted sandbox produced `Operation not permitted`; the unsandboxed run passed.
- **Environment-dependent:** spill-heavy tests need a soft `ulimit -n` of at least 65536; demand scales with the libtest thread count, which defaults to the core count. At 1024 a `clinker-exec` spill test fails with `Too many open files (os error 24)`; at 4096 on a 32-core host several do, because the measured peak there is 4165-4222 descriptors. Raise the limit with the raise-only snippet in section 4 — a bare `ulimit -n <n>` lowers an already-higher limit into the failing range. CI is unaffected: `.github/workflows/ci.yml` sets no `ulimit`, so every job inherits the runner default.
- **Expensive:** `cargo test --benches -p clinker-benchmarks` runs the e2e benchmark smoke matrix and took several minutes.
- **Expensive:** `cargo bench ...` runs real Criterion measurements and should be reserved for performance-sensitive changes.
- **Expensive:** `cargo test -- --ignored` includes at least one XML generator test that reports generating about 600 MB.
- **Environment-dependent:** cross-target checks in CI require Rust targets `x86_64-pc-windows-msvc` and `aarch64-apple-darwin`; native Windows/macOS CI runs `cargo test --workspace`.

## 14. Troubleshooting Common Failures

- `Too many open files (os error 24)` in spill tests: check `ulimit -Sn`. It must be at least 65536; raise it with the raise-only snippet in section 4. Do not prefix a fixed `ulimit -n <n>` — that lowers an already-sufficient limit. If the hard limit (`ulimit -Hn`) is itself below the floor, raise the hard limit at the OS level or cut the parallelism with `cargo test -- --test-threads=<n>`; fewer threads reduces the demand but does not remove it, because a single grace-hash or cascaded-merge test can be near the per-operator bound on its own.
- A `proptest` failure caused by fd exhaustion writes a regression seed under `crates/clinker-exec/proptest-regressions/` and asks you to commit it. **Do not.** Replayed at a healthy fd limit those seeds pass — they are environment artifacts, not counterexamples, and committing one adds permanent noise implying a bug that does not exist. Delete the file, fix the fd limit, and re-run. The tracked regression files are `pipeline/iejoin.txt`, `pipeline/sort_key.txt`, and `pipeline/sort_merge_join.txt`; anything else appearing after a `Too many open files` run is an artifact.
- `Operation not permitted` in `crates/clinker-net/tests/rest_executor_e2e.rs`: the test likely cannot bind a local socket in the sandbox. Rerun outside the sandbox or in normal CI.
- `cargo deny check` cannot acquire `~/.cargo/advisory-dbs/db.lock`: the filesystem sandbox is read-only for that cargo advisory DB path. Rerun with permission to write/read the cargo advisory database.
- `cargo test --workspace` can run for a long time: the workspace has a large test suite with many integration tests. Use `cargo test -p <package>` or an exact test filter while iterating.
- `cargo test --benches -p clinker-benchmarks` runs many `Testing e2e/...` cases: this is expected. It is CI's benchmark smoke gate, not a quick compile check.
- Rustdoc warnings from `cargo doc --workspace --no-deps`: current docs build exits 0 with warnings for broken/private intra-doc links, invalid HTML tags, and bare URLs. Do not treat these warnings as a new failure unless your change introduced them or the command becomes warning-denied.
- A root `README.md` exists (project overview and pillars). For command evidence, still prefer `CLAUDE.md`, `Cargo.toml`, CI, crate manifests, `docs/user`, `docs/engine`, examples, tests, and benches over README prose.
