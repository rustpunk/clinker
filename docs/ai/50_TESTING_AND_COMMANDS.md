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
# need. Raise-only by construction: the body runs only when the current
# soft limit is below the floor, and both targets are >= the current soft
# limit — so an already-sufficient limit is never lowered. `-S` is
# load-bearing: a bare `ulimit -n N` sets the soft AND the hard limit, and
# an unprivileged process can never raise a hard limit back.
if [ "$(ulimit -Sn)" != unlimited ] && [ "$(ulimit -Sn)" -lt 65536 ]; then
  ulimit -S -n 65536 2>/dev/null || ulimit -S -n "$(ulimit -Hn)"
fi

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

On a host that is already at or above the floor the `if` is a no-op, not a
failure: it changes nothing and succeeds, so the block is safe to lift into
a setup script or chain with `&&` without stranding the command after it.

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

### Test artifact storage

Cargo compiles every file-level integration-test target as a separate linked
executable. That multiplies full debug information and incremental object
caches across Clinker's large integration matrix. The workspace therefore
uses this repository-wide test profile:

```toml
[profile.test]
debug = "line-tables-only"
incremental = false
```

`line-tables-only` preserves filenames and line numbers in backtraces while
omitting variable and parameter debug data. The setting changes only test
builds; ordinary development and release profiles keep their existing
behavior. When a targeted test needs an interactive debugger, opt back into
full debug information and incremental compilation for that invocation:

```bash
CARGO_PROFILE_TEST_DEBUG=full \
CARGO_PROFILE_TEST_INCREMENTAL=true \
cargo test -p <package> --test <test-target>
```

Keep the override targeted. Applying it to the full workspace recreates the
large artifact footprint the default profile avoids.

Related format-pipeline cases in `clinker-exec` share the `format_pipelines`
integration target instead of relinking the executor graph once per source
file. This reduced that package's Cargo integration targets from 136 to 124
while preserving all 103 format behavior cases as separately named Rust
modules. A topology test fails if a case file is not declared. Run
the complete suite or one case module with:

```bash
cargo test -p clinker-exec --test format_pipelines --locked --offline
cargo test -p clinker-exec --test format_pipelines --locked --offline \
  csv_charset::
```

Files directly under `crates/clinker-exec/tests/` remain automatically
discovered standalone targets. Cases intentionally grouped into the shared
harness live under `crates/clinker-exec/tests/format_pipelines/`; adding a new
file there also requires declaring its module in `format_pipelines.rs`.

In isolated cold targets on one Linux host, compiling only these thirteen
cases with `--no-run` used 2,319,033,679 bytes as separate targets and
944,084,479 bytes as the shared target, a 59.3% reduction for this slice. The
measurement includes the common dependency graph in both targets; it is disk
evidence, not a cross-host timing guarantee.

Package-only and workspace-wide commands can resolve different feature graphs,
so running both into one target directory may retain two hash families for the
same tests. In the same measurement session, a package-only `clinker-exec`
gate followed by the workspace gate left 125 superseded executables totaling
13,118,563,912 bytes. On a space-constrained machine, keep narrow iteration in
a disposable target and clean that target before starting the workspace gate:

```bash
CARGO_TARGET_DIR=target/package-iteration \
  cargo test -p clinker-exec --test format_pipelines --locked --offline
CARGO_TARGET_DIR=target/package-iteration cargo clean
cargo test --workspace --locked --offline
```

Use a task-specific target path and clean only that generated directory; do
not clean a shared target that another worktree or process is using.

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

The optional machine protocol and external child-process boundary have two
focused real-binary gates:

```bash
cargo test --locked --offline -p clinker --test machine_supervision
cargo test --locked --offline -p clinker --test machine_protocol_cli
```

Status: **Verified.** The first gate proves concurrent bounded pipe drains,
explicit deadline escalation, direct-child reaping, fail-closed terminal/status
reconciliation, unchanged finals after control loss, and fresh-process retry.
The second pins the schema, identity, lifecycle, typed failure, cancellation,
DLQ, and publication-event contract.

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

## 7.1. Dependency Policy

The shared-failure dependency gate is a detached Rust tool with its own locked
manifest under `tools/dependency-policy`. It is excluded from workspace membership
and does not add a runtime dependency to Clinker.

```bash
cargo fmt --manifest-path tools/dependency-policy/Cargo.toml --all -- --check
cargo clippy --manifest-path tools/dependency-policy/Cargo.toml --all-targets --locked --offline -- -D warnings
cargo test --manifest-path tools/dependency-policy/Cargo.toml --locked --offline
cargo run --manifest-path tools/dependency-policy/Cargo.toml --locked --offline -- --scope final --root .
```

Status: **Verified.** The checker uses `syn` and fails closed on unresolved or
unsupported production Rust rather than falling back to text parsing.

## 7.2. Release Policy

Release, workflow, filesystem, repository-control, and evidence policy live in
the detached Rust tool under `tools/release-policy`. The repository has no Python
implementation or fallback for these gates.

```bash
cargo fmt --manifest-path tools/release-policy/Cargo.toml --all -- --check
cargo clippy --manifest-path tools/release-policy/Cargo.toml --all-targets --locked --offline -- -D warnings
cargo test --manifest-path tools/release-policy/Cargo.toml --locked --offline
cargo test -p clinker --test attempt_publication --locked --offline
cargo test -p clinker --test output_containment --locked --offline
cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- workflow verify
cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- boundary audit --scope rust-only --root .
cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem self-test
```

Status: **Verified.** The Rust-only audit checks the real repository as well as
negative fixtures, and the compatibility scripts perform one unchanged Rust
delegation without carrying policy semantics.

`filesystem self-test` validates the direct two-profile hosted-runner topology,
including unconditional teardown followed by unconditional bounded evidence
upload. Local tests validate the strict
`clinker.filesystem-matrix-evidence/3` parser and reject legacy, unknown,
missing, or truncated lifecycle, stage, capacity, operator, recovery,
persistence, production admission-lock, and teardown proof. Actual
NFSv4.1/SMB3.1.1 interruption, independent-process count/byte contention, and
mounted `ENOSPC` certification run only in the privileged `filesystem-matrix`
job; an ordinary local run cannot create positive support evidence. Injected
`EDQUOT` remains non-qualifying seam coverage.

The tag workflow builds and smoke-tests `clinker` and `cxl` on each native
runner, then transfers only those exact executables to the Ubuntu assembly
job. The assembly job runs the detached policy tool to create and verify all
four deterministic archives, attest the final archive subjects, and stage the
private candidate. The policy tool is therefore not a Windows build
dependency; platform-specific release jobs do not duplicate its Unix process,
locking, or filesystem controls.

Candidate authorization deliberately precedes the build and contains only
values knowable before the protected tag exists. Validate that record, create
the tag, and reread the protected ref with the same immutable authorization:

```bash
cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- \
  decision validate \
  --authorization-schema scripts/release/release-candidate-authorization.schema.json \
  --authorization-record path/to/authorization.json \
  --require-authorization-id release-candidate-authorization \
  --require-authorized

cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- \
  publication create-candidate-tag \
  --repo rustpunk/clinker \
  --authorization-record path/to/authorization.json \
  --authorization-schema scripts/release/release-candidate-authorization.schema.json \
  --deadline-seconds 120

cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- \
  publication resolve-protected-ref \
  --repo rustpunk/clinker \
  --authorization-record path/to/authorization.json \
  --authorization-schema scripts/release/release-candidate-authorization.schema.json \
  --deadline-seconds 120
```

After the tag workflow has staged and freshly reread the private draft, derive
candidate evidence from the observed release ID, workflow run, checksums, and
archive bytes. A maintainer can then accept that exact evidence in the
candidate decision; neither the release ID nor artifact digests are guessed in
the pre-build authorization.

```bash
cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- \
  release verify \
  --repo rustpunk/clinker \
  --authorization-record path/to/authorization.json \
  --authorization-schema scripts/release/release-candidate-authorization.schema.json \
  --require-private \
  --fresh-download \
  --evidence-kind candidate \
  --evidence-schema scripts/release/release-evidence.schema.json \
  --evidence-manifest target/release-policy/candidate-evidence.json

cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- \
  decision validate \
  --schema scripts/release/release-decision.schema.json \
  --record path/to/candidate-decision.json \
  --authorization-schema scripts/release/release-candidate-authorization.schema.json \
  --authorization-record path/to/authorization.json \
  --candidate-evidence target/release-policy/candidate-evidence.json \
  --require-accepted
```

## 7.3. Phase 3 Recovery Matrix

Run the following commands separately, in order, and stop on the first nonzero
result. The command tokens are the recovery evidence contract; do not replace a
focused target with an alias, a broader suite, or a merged command. Before the
executor-heavy targets, apply the raise-only file-descriptor setup from section
4. The OTLP integration target binds local sockets and may require ordinary host
permissions outside a restricted sandbox.

1. Semantic plan identity is versioned separately from the byte-oriented
   pipeline hash and changes with effective typed semantics and admitted
   dependency content:

   ```bash
   cargo test --locked --offline -p clinker-plan semantic_fingerprint
   ```

2. Shared typed failures expose exact category and retry advice without parsing
   rendered diagnostics or exit status:

   ```bash
   cargo test --locked --offline -p clinker-core-types --test failure_classification
   ```

3. One explicitly selected machine protocol owns stdout, preserves standalone
   behavior, bounds its records, and reports typed lifecycle and publication
   truth:

   ```bash
   cargo test --locked --offline -p clinker --test machine_protocol_cli
   ```

4. The approved Linux direct-child capability delivers actual SIGTERM, keeps
   both pipes draining through a distinct grace interval, forces only after
   expiry, reaps the child, and retries in a fresh process:

   ```bash
   cargo test --locked --offline -p clinker --test machine_supervision
   ```

5. Attempt publication remains coupled to cancellation arbitration and the
   current attempt ledger, including retained failed-attempt and visible-final
   truth:

   ```bash
   cargo test --locked --offline -p clinker --test attempt_publication
   ```

6. The ordinary standalone `run` command remains the default surface and does
   not require machine supervision:

   ```bash
   cargo run --locked --offline -p clinker -- run --help
   ```

7. All twelve production-reachable dispatcher boundaries return the exact
   typed status before effects and retain D-15 failed-attempt evidence without
   changing intended finals:

   ```bash
   cargo test --locked -p clinker-exec --features test-utils --test invariant_errors -- --nocapture
   ```

8. Workspace observability accepts only strict secret-free raw configuration
   and finite telemetry and independent-lineage bounds; structured endpoint
   admission remains outside the planner:

   ```bash
   cargo test --locked -p clinker-plan --test observability_config
   ```

9. Transform-authored logs, metrics, and traces use bounded static event
   declarations, explicit requested fields, and no retired routing or message
   interpolation surface:

   ```bash
   cargo test --locked -p clinker-plan --test transform_observability
   ```

10. The sole structured endpoint boundary admits one HTTPS origin, derives only
    the fixed logs, metrics, and traces routes, and enforces finite OTLP/HTTP
    request, response, retry, and shutdown bounds:

    ```bash
    cargo test --locked -p clinker-net --test otlp_http
    ```

11. Phase 1 D-42 external lineage uses canonical or catalog collection identity
    with standard input/output subset and authorized symlinks facets, never an
    implicit worker path:

    ```bash
    cargo test --locked -p clinker-lineage --test logical_identity
    ```

12. Phase 1 D-41 lifecycle facts and external lineage delivery remain shared in
    identity but independent in byte capacity, worker, deadline, counters, and
    bounded or hung-sink outcome:

    ```bash
    cargo test --locked -p clinker-lineage --test lifecycle_delivery
    ```

13. Fixed executor telemetry produces real privacy-gated logs, metrics, and
    traces without changing ETL, DLQ, or publication authority:

    ```bash
    cargo test --locked -p clinker-exec --test observability_isolation
    ```

14. The CLI preserves Phase 1 D-42 identity and facet policy through preflight,
    correlation, static output, and bounded external lineage delivery:

    ```bash
    cargo test --locked -p clinker --test lineage_cli
    ```

15. The CLI cross-signal matrix keeps OTLP and OpenLineage failures independent
    while authoritative outputs, DLQ, status, machine truth, publication, and
    retained attempt evidence remain unchanged:

    ```bash
    cargo test --locked -p clinker --test observability_isolation
    ```

16. The final dependency policy permits only the approved shared-failure edges
    and consumer use of `FailureClassification`, `FailureCategory`, and
    `RetryAdvice`, with no product orchestrator runtime:

    ```bash
    cargo run --manifest-path tools/dependency-policy/Cargo.toml --locked --offline -- --scope final --root .
    ```

17. The repository-owned AI documentation structure, links, contract tables,
    and ownership references remain valid:

    ```bash
    bash scripts/check-ai-docs.sh
    ```

18. The user-facing operator and pipeline contract renders successfully:

    ```bash
    mdbook build docs/user
    ```

19. The engine contract renders successfully:

    ```bash
    mdbook build docs/engine
    ```

20. All tracked Rust remains formatted:

    ```bash
    cargo fmt --all --check
    ```

21. The complete working-tree diff has no whitespace errors:

    ```bash
    git diff --check
    ```

## 8. Docs Generation Command

Rust API docs:

```bash
cargo doc --workspace --no-deps --locked --offline
```

Status: **Verified.**

This generated docs successfully but emitted rustdoc warnings for broken/private intra-doc links, invalid HTML tags, and bare URLs. Treat warnings as cleanup work if touching nearby docs, but this command currently exits 0.

User guide mdBook:

```bash
mdbook build docs/user -d target/mdbook/user
```

Status: **Verified.**

Engine internals mdBook:

```bash
mdbook build docs/engine -d target/mdbook/engine
```

Status: **Verified.**

After building either book, validate every chapter-page local `href` target and
fragment in the generated HTML:

```bash
bash scripts/check-ai-docs.sh --check-rendered-links \
  target/mdbook/user target/mdbook/engine
```

Status: **Verified.** External URLs remain outside this offline post-build
check; they follow the scheduled, cached policy described in section 12. The
derived `print.html` concatenation is excluded because mdBook rewrites all
chapter links into one document, where duplicate heading IDs do not preserve
the chapter-local fragment model.

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
# Lineage commands require an explicit [observability.lineage] identity policy
# in the workspace clinker.toml; see docs/user/src/ops/lineage.md.
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

The `cargo test --benches -p clinker-benchmarks` command is not just a compile
check. It executes every discovered benchmark pipeline once at Small scale and
fails on planning or runtime errors. Real `cargo bench` invocations retain the
Small, Medium, and Large Criterion timing matrix.

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

For AI onboarding docs-only changes under `docs/ai/`, run the pinned
repository-owned offline gate and scope the whitespace check to the files the
change owns:

```bash
bash scripts/check-ai-docs.sh
git diff --check -- docs/ai/<changed-file>.md docs/ai/<other-changed-file>.md
```

Status: **Verified.** The same `bash scripts/check-ai-docs.sh` command blocks
CI. It recursively checks `docs/ai/**/*.md` offline for the selected GitHub
Flavored Markdown structure, local targets, GitHub heading fragments, and the
production-contract schema and coverage. It installs no package and makes no
network request. D-52's authoritative status and ownership are in the
[production-contract register](15_PRODUCTION_CONTRACTS.md). The scoped
`git diff --check -- ...` remains required for the exact owned files; avoid an
unscoped check when unrelated working-tree edits are present.

External URLs are intentionally outside the blocking offline gate. D-52 assigns
them to a scheduled, cached, non-blocking check so network availability cannot
become a merge requirement. No external-link package, third-party action, or
specific implementation is pre-approved by that boundary.

For user or engine mdBook documentation changes, run the relevant book build:

```bash
mdbook build docs/user -d target/mdbook/user
mdbook build docs/engine -d target/mdbook/engine
bash scripts/check-ai-docs.sh --check-rendered-links \
  target/mdbook/user target/mdbook/engine
```

Status: **Verified.** Run the mdBook command relevant to the docs changed; run
both if shared docs/theme or cross-book docs changed. `cargo fmt --all --check`
is not required for pure Markdown edits unless Rust source or generated Rust
docs are touched. Rust tests are likewise reserved for executable, generated,
or code-coupled documentation; ordinary prose-only AI or book edits use the
documentation gates above.

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
spill tests need a soft `ulimit -n` of at least the 65536 floor. If REST e2e tests
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
- **Environment-dependent:** spill-heavy tests need a soft `ulimit -n` of at least 65536; demand scales with the libtest thread count, which defaults to the core count. At 1024 a `clinker-exec` spill test fails with `Too many open files (os error 24)`; at 4096 on a 32-core host two do, because the measured peak there is 4165-4222 descriptors. Raise the limit with the raise-only snippet in section 4 — a bare `ulimit -n <n>` lowers an already-higher limit into the failing range. CI is unaffected: `.github/workflows/ci.yml` sets no `ulimit`, so every job inherits the runner default.
- **Broader than a compile check:** `cargo test --benches -p clinker-benchmarks`
  executes one Small-scale e2e preflight for every discovered benchmark
  pipeline. Real Criterion measurements remain substantially more expensive.
- **Expensive:** `cargo bench ...` runs real Criterion measurements and should be reserved for performance-sensitive changes.
- **Expensive:** `cargo test -- --ignored` includes at least one XML generator test that reports generating about 600 MB.
- **Environment-dependent:** cross-target checks in CI require Rust targets `x86_64-pc-windows-msvc` and `aarch64-apple-darwin`; native Windows/macOS CI runs `cargo test --workspace`.

## 14. Troubleshooting Common Failures

- `Too many open files (os error 24)` in spill tests: check `ulimit -Sn`. It must be at least the 65536 floor (see section 4 for the measurement behind it and the headroom it carries); raise it with the raise-only snippet there. Do not prefix a fixed `ulimit -n <n>` — that lowers an already-sufficient limit. If the hard limit (`ulimit -Hn`) is itself below the floor, raise the hard limit at the OS level or cut the parallelism with `cargo test -- --test-threads=<n>`; fewer threads reduces the demand but does not remove it, because a single grace-hash or cascaded-merge test can be near the per-operator bound on its own.
- A `proptest` failure caused by fd exhaustion writes a regression seed under `crates/clinker-exec/proptest-regressions/` and asks you to commit it. **Do not.** Replayed at a healthy fd limit those seeds pass — they are environment artifacts, not counterexamples, and committing one adds permanent noise implying a bug that does not exist. Delete the file, fix the fd limit, and re-run. The tracked regression files are `pipeline/iejoin.txt`, `pipeline/sort_key.txt`, and `pipeline/sort_merge_join.txt`; anything else appearing after a `Too many open files` run is an artifact.
- `Operation not permitted` in `crates/clinker-net/tests/rest_executor_e2e.rs`: the test likely cannot bind a local socket in the sandbox. Rerun outside the sandbox or in normal CI.
- `cargo deny check` cannot acquire `~/.cargo/advisory-dbs/db.lock`: the filesystem sandbox is read-only for that cargo advisory DB path. Rerun with permission to write/read the cargo advisory database.
- `cargo test --workspace` can run for a long time: the workspace has a large test suite with many integration tests. Use `cargo test -p <package>` or an exact test filter while iterating.
- `cargo test --benches -p clinker-benchmarks` prints one preflight line per
  discovered pipeline: this is expected. It is CI's executable benchmark
  smoke gate, not a compile-only check; it does not run the Medium or Large
  timing tiers.
- Rustdoc warnings from `cargo doc --workspace --no-deps`: current docs build exits 0 with warnings for broken/private intra-doc links, invalid HTML tags, and bare URLs. Do not treat these warnings as a new failure unless your change introduced them or the command becomes warning-denied.
- A root `README.md` exists (project overview and pillars). For command evidence, still prefer `CLAUDE.md`, `Cargo.toml`, CI, crate manifests, `docs/user`, `docs/engine`, examples, tests, and benches over README prose.
