# AI Onboarding: Design Rules

Verified against the working tree on 2026-08-07.

Purpose: Record the reviewed invariants that constrain Clinker changes. This
page is intentionally narrower than [common patterns](40_COMMON_PATTERNS.md):
repetition is not a rule. Approved exceptions belong in the
[extension-seam map](35_EXTENSION_SEAMS.md), and full current-versus-target
status belongs in the
[production-contract register](15_PRODUCTION_CONTRACTS.md).

The rule schema is fixed: every rule names its scope, rationale, evidence,
exceptions, and verification method. If one of those fields cannot be filled
from current source, manifests, tests, or an accepted contract, route the claim
to [open questions](80_OPEN_QUESTIONS.md) rather than promoting it here.

## Execution Model

### Finite synchronous jobs

- **Scope:** Core execution, sources, transports, scheduling, and embedding
  APIs.
- **Invariant:** Clinker executes finite jobs synchronously in one process.
  Sources have a provable finite bound; core execution uses OS threads,
  bounded channels, and a run-scoped Rayon pool. An unbounded stream, daemon,
  distributed worker model, or async runtime is an architecture change.
- **Rationale:** Termination, bounded-resource reasoning, cancellation, and
  operator scheduling all rely on finite input and synchronous ownership.
- **Evidence:** `RecordSource`'s finite contract in
  `crates/clinker-exec/src/source/mod.rs`, executor construction under
  `crates/clinker-exec/src/executor/`, bounded REST options in
  `crates/clinker-net/src/rest.rs`, and the workspace dependency graph.
- **Exceptions:** None in the core runtime. External orchestrators may launch
  and supervise the synchronous CLI; see D-25 through D-33 in the
  [contract register](15_PRODUCTION_CONTRACTS.md).
- **Verification:** Inspect source construction and run the relevant executor
  or transport tests from [the command guide](50_TESTING_AND_COMMANDS.md).

### External supervision stays at the process edge

- **Scope:** Optional machine mode, workflow adapters, scheduling, retry,
  heartbeat, deadlines, cancellation escalation, and process-tree lifetime.
- **Invariant:** Clinker may emit one opt-in bounded lifecycle stream for one
  finite process, but it does not own a supervisor, worker, daemon, scheduler,
  workflow SDK, POSIX process group, or Windows Job Object. An external parent
  drains both pipes concurrently, heartbeats independently, enforces an overall
  deadline, delivers the actual platform graceful signal, keeps draining for a
  separate bounded grace interval, forces exactly once only after expiry,
  reaps before joining drains, and launches a fresh process for every retry.
- **Rationale:** Keeping durable workflow and platform job control external
  preserves the finite synchronous execution model and prevents advisory
  progress from becoming accidental resume or exactly-once state.
- **Evidence:** `crates/clinker/src/machine.rs`,
  `crates/clinker/tests/machine_protocol_cli.rs`,
  `crates/clinker/tests/machine_supervision.rs`, and the
  [supervision contract](../user/src/ops/orchestrator-contract.md).
- **Exceptions:** None. A future shipped adapter or process-tree owner requires
  an explicit architecture and dependency decision. The Linux evidence is a
  direct-child proof; it does not claim process-group or descendant ownership.
- **Verification:** Run both focused machine integration tests from the command
  guide and confirm process-launching helper code remains under `tests/support`.

### Compiled topology is authoritative

- **Scope:** Planning, executor entry points, topology, plan consumers, and
  runtime dispatch.
- **Invariant:** `clinker-plan` owns author-input admission and produces a
  compiled `ExecutionPlanDag`. That DAG is the authoritative topology and
  runtime-dispatch surface; `clinker-exec` consumes compiled planning artifacts
  rather than accepting raw YAML or `PipelineConfig` as an execution input.
- **Rationale:** One typed topology keeps validation, enrichment, scheduling,
  explain output, and exhaustive dispatch aligned and prevents runtime code
  from inventing a second graph interpretation.
- **Evidence:** `PipelineConfig::compile` and `ExecutionPlanDag` in
  `crates/clinker-plan/src/config/pipeline.rs` and
  `crates/clinker-plan/src/plan/execution/`, plus the compiled-plan executor
  entry point and compile-fail boundary in
  `crates/clinker-exec/src/executor/mod.rs`.
- **Exceptions:** A runtime envelope may refresh only the fields enumerated by
  D-01 through D-11. Persistent reuse and cache behavior remain owned by
  Phase 5 / PERF-01; this rule does not claim that work has landed.
- **Verification:** Run the compiled-plan boundary doctest and the smallest
  planner/executor tests named in the command guide; inspect explain output
  when topology or properties change.

## Layering And Typed Boundaries

### Preserve dependency direction

- **Scope:** Workspace manifests, crate APIs, and cross-crate imports.
- **Invariant:** Record and core vocabulary remain below Clinker Expression
  Language (CXL), format, and planning; `clinker-exec` consumes compiled plans;
  CLI and integration crates remain at the edge. New back-edges require a
  reviewed contract rather than local convenience. The Phase 3 shared-failure
  decision permits exactly `clinker-net -> clinker-core-types` and
  `clinker-lineage -> clinker-core-types`, with consumers using only
  `FailureClassification`, `FailureCategory`, and `RetryAdvice`.
- **Rationale:** Lower layers must not acquire planner or runtime policy, and
  edge integrations must not become alternate admission authorities.
- **Evidence:** Root and crate `Cargo.toml` files, crate responsibilities in
  [the crate map](20_CRATE_MAP.md), and current imports.
- **Exceptions:** Only the bounded D-20 and D-21 edges recorded in the
  [extension-seam map](35_EXTENSION_SEAMS.md) are approved; their presence does
  not authorize adjacent imports. The two Phase 3 failure edges add no
  re-export, feature, serialization policy, package, identity, or other shared
  type.
- **Verification:** Review `cargo metadata --no-deps`, relevant `cargo tree`
  output, and source imports before and after a dependency change.

### Use typed handoffs and proof tokens

- **Scope:** Planner/runtime, record/format, author-source, path-security, and
  transport boundaries.
- **Invariant:** Preserve established typed boundaries such as `CompiledPlan`,
  `ExecutionPlanDag`, `Spanned<T>`, `CxlSource`, `RecordSource`,
  `FormatReader`, `FormatWriter`, and `ValidatedPath`; do not replace them with
  raw YAML, unspanned strings, unchecked paths, or untyped maps.
- **Rationale:** These types carry validation, source locations, finiteness,
  ownership, or trust proofs that raw substitutes would erase.
- **Evidence:** `crates/clinker-plan/src/security.rs`,
  `crates/clinker-plan/src/yaml.rs`, `crates/clinker-plan/src/plan/`,
  `crates/clinker-format/src/traits.rs`, and
  `crates/clinker-exec/src/source/mod.rs`.
- **Exceptions:** TOML workspace/storage configuration and JSON data parsing
  have separate typed paths; they are not pipeline-YAML bypasses.
- **Verification:** Run boundary-specific parse, security, format, source, or
  executor tests and confirm public signatures retain the proof-bearing type.

### Admit an OTLP endpoint exactly once

- **Scope:** Workspace observability config, Collector endpoint admission,
  request authentication, fixed signal routing, and CLI runtime setup.
- **Invariant:** `clinker-plan` owns only strict secret-free raw endpoint/auth
  intent and numeric telemetry/lineage bounds. `clinker-net` alone parses and
  normalizes that text with `ureq::http::Uri`, returning the opaque
  `AdmittedOtlpEndpoint` proof and deriving only `/v1/logs`, `/v1/metrics`, and
  `/v1/traces`. The CLI composes that proof with the bounds before effects; it
  does not add another URI parser, raw-string overload, admitted type, or route.
- **Rationale:** One capability transition prevents divergent security checks,
  credential-bearing origins, and route confusion while keeping raw config and
  network authority in their owning crates.
- **Evidence:** `crates/clinker-plan/src/config/observability.rs`,
  `crates/clinker-net/src/otlp.rs`, `crates/clinker/src/observability.rs`, and
  `crates/clinker-net/tests/otlp_http.rs`.
- **Exceptions:** Credential-free HTTPS sends no headers. Referenced auth stays
  secret-free until Phase 4 D-13/D-15 and AUTH-01 provide a borrowed run-local
  applicator after endpoint admission; it may not change origin or route.
- **Verification:** Run the endpoint admission and successful-post tests plus
  the CLI pre-effect observability partition.

### Keep declared extension boundaries sealed

- **Scope:** Channels, compositions, overlays, ports, parameters, and resource
  bindings.
- **Invariant:** Extensions may affect only declared schemas, ports,
  parameters, variables, and typed resource slots. They may not patch sealed
  composition internals or introduce an alternate plan-admission path.
- **Rationale:** Explicit boundaries preserve validation, provenance,
  reproducibility, and bounded reasoning across reusable graphs.
- **Evidence:** `crates/clinker-channel/src/resolve.rs`,
  `crates/clinker-channel/src/overlay.rs`, and composition binding under
  `crates/clinker-plan/src/config/composition/`.
- **Exceptions:** D-12 through D-16 define the locked typed resource-catalog
  target and reject ordinary call-site `outputs:` and `alias:`; current and
  target status is in the contract register.
- **Verification:** Run focused channel/composition parse, provenance, binding,
  and execution tests for every changed declared surface.

## Correctness And Fail-Closed Admission

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
  findings. Do not add new ignore entries without documenting the reason and
  confirming the affected dependency remains in the active workspace graph.
- Keep benchmark/test helper dependencies out of default runtime code.
  `clinker-exec` has an optional `clinker-bench-support` edge for
  `bench-alloc`; do not let benchmark helpers leak into default execution
  paths.
- Network transport currently uses blocking `ureq` over rustls. Adding async
  clients or a Tokio-driven runtime would be an architecture change, not a
  local connector tweak.
- The dependency gate is scoped to the capability, not to the manifest diff, and
  it covers development, test, benchmark, and release tooling as well as runtime
  crates. Hand-rolling a capability an established crate provides — a parser,
  lexer, serializer, or encoder — is a dependency decision taken without review,
  as is implementing it in another language, vendoring third-party source, or
  calling an undeclared external binary. `AGENTS.md` carries the normative rule.
- Adding a non-Rust language to the build, test, or release path is an
  architectural decision requiring approval. The committed tree is Rust plus
  documentation assets: `git ls-files` matches no `.py`, `.sh`, or `.rb`
  sources, and the only committed JavaScript is the vendored mdBook theme under
  `docs/theme/`.
- Repeated adversarial repair of a hand-written substitute is evidence that the
  dependency decision was wrong, not a reason to keep patching. Reopen the
  approval question instead of growing the substitute.

### Canonical span-aware YAML parsing

- **Scope:** Production and test parsing of pipeline, composition, channel, and
  related YAML outside parser-specific tests.
- **Invariant:** YAML enters through `clinker_plan::yaml`; span-aware
  `serde-saphyr` parsing, strict budgets, custom node dispatch, and
  `Spanned<T>` diagnostics are load-bearing.
- **Rationale:** A direct parser call can bypass input budgets, source spans,
  or the single admission behavior used by production.
- **Evidence:** `crates/clinker-plan/src/yaml.rs`, the custom visitor in
  `crates/clinker-plan/src/config/pipeline_node.rs`, and strict config tests.
- **Exceptions:** The parser module's own parser-specific tests may call the
  underlying parser. D-22 records the current executor-test violation and its
  downstream repair owner; tests receive no general bypass.
- **Verification:** Search Rust sources for `serde_saphyr::from_str` outside
  `clinker-plan::yaml`, then run the affected parse and diagnostic tests.

### Strict user-facing configuration

- **Scope:** YAML fields, node bodies, defaults, retired shapes, CLI/config
  diagnostics, and typed overlays.
- **Invariant:** Preserve `deny_unknown_fields` where established, reject
  retired or unsupported surfaces, and require a source-located diagnostic
  that names the bad input, violated rule, and a paste-ready correction.
- **Rationale:** Silent fallback and successful no-ops turn author mistakes
  into incorrect ETL results.
- **Evidence:** Strict config structs and retire-gate tests in
  `crates/clinker-plan`, plus diagnostic types in
  `crates/clinker-core-types/src/diagnostic.rs`.
- **Exceptions:** Existing documented optional fields may use deliberate
  defaults. A new default must define omission behavior and add parse tests; it
  may not preserve a retired shape or hide a mandatory value.
- **Verification:** Add positive and negative boundary tests covering unknown,
  omitted, wrong-type, retired, and corrected inputs with expected spans.

### Structured invariant failures

- **Scope:** Runtime discovery of states that planning should have made
  unreachable.
- **Invariant:** Return the owning subsystem error, normally
  `PipelineError::Internal`, rather than panicking, silently falling back, or
  reporting success. At all twelve production-reachable specialized dispatcher
  boundaries, a wrong node kind returns bounded
  `PipelineError::DispatchMismatch` with
  `runtime.invariant.dispatch_mismatch`,
  `FailureCategory::InternalInvariant`, and `RetryAdvice::PolicyRequired`
  before mutable operator or publication effects.
- **Rationale:** A malformed or mismatched compiled artifact must fail closed
  without aborting the host process or corrupting downstream output.
- **Evidence:** `crates/clinker-plan/src/error.rs`, dispatcher entry guards
  under `crates/clinker-exec/src/executor/`, and
  `crates/clinker-exec/tests/invariant_errors.rs`.
- **Exceptions:** This SECU-03 runtime-invariant rule is separate from the
  numbered production contracts. Process aborts caused by unrecoverable
  platform behavior are not converted by documentation, and locally proven
  internal algorithm and Output assertions remain assertions.
- **Verification:** Run the exhaustive twelve-dispatcher matrix and confirm
  each row finishes, cleans up, leaves intended finals unchanged, and retains
  bounded failed-attempt evidence.

### Keep external lineage identity logical and stable

- **Scope:** External OpenLineage dataset identity, concrete partitions,
  aliases, CLI preflight, and local diagnostic compatibility.
- **Invariant:** Every external dataset uses a canonical datasource or exact
  catalog namespace/name. The collection name stays stable; authorized
  concrete input/output subsets use the standard subset facet and authorized
  aliases use the standard symlinks facet. No worker path, temporary path,
  attempt, drive letter, process context, or path hash may supply external
  identity. Path-derived identity is confined to exact
  `local_diagnostic_paths` mode.
- **Rationale:** Catalog identity must be independently reconstructible across
  hosts and attempts without collapsing a collection into one physical subset.
- **Evidence:** `crates/clinker-lineage/src/logical_identity.rs`,
  `crates/clinker-lineage/src/openlineage/facet.rs`,
  `crates/clinker-lineage/tests/logical_identity.rs`, and
  `crates/clinker/tests/lineage_cli.rs`.
- **Exceptions:** The current resolved config has no subset or symlink author
  fields; absence remains absence. Local diagnostic mode is synchronous,
  visibly labeled, and cannot enter external delivery.
- **Verification:** Run the logical-identity and CLI lineage suites, including
  relocation, missing-binding, facet-shape, and pre-effect rejection cases.

### Trusted paths use validated capabilities

- **Scope:** Input, output, include, spill, workspace, and staging paths that
  cross a trust boundary.
- **Invariant:** Code requiring a trusted path accepts `ValidatedPath` or an
  equivalent proven capability produced by canonical validation, not an
  unchecked `PathBuf` or string.
- **Rationale:** Lexical validation alone does not prevent traversal, encoded
  traversal, absolute-path policy violations, or symlink/junction escape.
- **Evidence:** `ValidatedPath` and `validate_path` in
  `crates/clinker-plan/src/security.rs` and their platform-specific tests.
- **Exceptions:** Internal paths derived entirely beneath an already validated
  root still require explicit containment reasoning; no public author input
  bypass exists.
- **Verification:** Run security tests for present and missing leaves on each
  supported native platform affected by the change.

## Bounded Resources

### Share lifecycle facts, not optional-delivery authority

- **Scope:** Machine events, OTLP logs/metrics/traces, OpenLineage, privacy,
  queue capacity, deadlines, workers, terminal facts, and publication truth.
- **Invariant:** One CLI-owned immutable lifecycle source records batch ID,
  execution ID, semantic fingerprint, and terminal facts once. OTLP and
  OpenLineage copy those facts and match machine correlation/terminal truth,
  while retaining independent capacities, deadlines, workers, counters, and
  typed outcomes. Privacy is enforced before telemetry queue admission, and no
  optional delivery result can change ETL, DLQ, process, machine, publication,
  visible-final, or retained-attempt truth.
- **Rationale:** Shared correlation prevents identity drift; independent
  bulkheads prevent one optional signal from stalling or redefining another or
  the finite job.
- **Evidence:** `crates/clinker/src/lifecycle.rs`,
  `crates/clinker/src/observability.rs`,
  `crates/clinker-lineage/src/delivery.rs`, and
  `crates/clinker/tests/observability_isolation.rs`.
- **Exceptions:** Guaranteed business or compliance events are ordinary
  outputs, not best-effort observability. Metrics spool, human diagnostics, and
  machine control remain separate paths.
- **Verification:** Compare each injected optional-delivery outcome against the
  no-fault authoritative artifact oracle and verify only its typed outcome and
  bounded counters differ.

### Share one run-scoped memory authority

- **Scope:** Ingest, node buffers, blocking/stateful operators, fan-out,
  telemetry arenas, spill, pause/resume, and cleanup.
- **Invariant:** Retained runtime state participates in the run-scoped
  `MemoryArbitrator`; spill-capable or pausable consumers account for all
  retained structures, poll at bounded intervals, and clean up on success,
  error, and interruption.
- **Rationale:** Independent budgets and invisible buffers defeat the process's
  cooperative bounded-memory guarantee and can deadlock backpressure.
- **Evidence:** `crates/clinker-exec/src/pipeline/memory.rs`, runtime consumer
  registration, node-buffer code, and spill integration tests.
- **Exceptions:** Small fixed-size state may be accounted as fixed admission
  overhead when the owning contract says so. The configured limit is a soft
  cooperative control, not a strict OS RSS reservation.
- **Verification:** Run focused memory, overshoot, pause/resume, spill, cleanup,
  and file-descriptor tests for the affected consumer.

### Keep test and benchmark support out of default runtime paths

- **Scope:** Dependency features, allocator instrumentation, benchmark helpers,
  release graphs, and performance claims.
- **Invariant:** Test/benchmark support is absent from default and release
  runtime graphs, and performance evidence is trusted only when its feature,
  allocator identity, plausibility, and distortion contract is verified.
- **Rationale:** Instrumentation must not change ordinary production behavior
  or lend authority to invalid measurements.
- **Evidence:** `bench-alloc` feature declarations in Cargo manifests,
  `crates/clinker-exec/src/executor/stage_metrics.rs`, and D-21 in the
  [extension-seam map](35_EXTENSION_SEAMS.md).
- **Exceptions:** The conditional D-21 edge is permitted only after its full
  repair contract passes; current allocation measurements remain untrusted.
- **Verification:** Compare default/release and feature-enabled `cargo tree`
  output and run the allocation plausibility/identity checks owned by Phase 5.

## Stable Contracts And Review

### Compatibility is curated, not inferred from visibility

- **Scope:** Public Rust re-exports, features, CLI/YAML surfaces, diagnostics,
  serialized tooling artifacts, and user-facing behavior.
- **Invariant:** A symbol or spelling is a compatibility promise only when its
  curated contract says so. Breaking a supported surface requires an explicit
  decision, migration note, documentation, and boundary tests.
- **Rationale:** Rust `pub` reachability, workspace reuse, or repeated prose do
  not by themselves establish support.
- **Evidence:** D-18 and D-19 in the contract register, root re-exports, feature
  gates, user docs, and current compatibility tests.
- **Exceptions:** Workspace-internal exposed API, test support, deprecated
  routes, and cleanup debt remain usable only within their named class.
- **Verification:** Review the compatibility matrix and run the public API,
  CLI/config, diagnostic, or serialization gates appropriate to the changed
  surface.

### Behavior and evidence change together

- **Scope:** Public behavior, configuration, diagnostics, commands,
  architecture, examples, documentation, and performance claims.
- **Invariant:** Update source-aligned docs and focused tests in the same change
  as behavior; label measurements by evidence status and never expose record
  values, credentials, machine-local paths, or local identities in examples.
- **Rationale:** Stale instructions and unsafe examples are correctness and
  information-disclosure failures, not editorial polish.
- **Evidence:** [AGENTS.md](../../AGENTS.md), the repository documentation gate,
  mdBook configuration, and D-34 through D-42 and D-51 through D-52.
- **Exceptions:** Pure prose corrections do not require Rust tests unless they
  touch generated, executable, or code-coupled documentation.
- **Verification:** Run `bash scripts/check-ai-docs.sh`, scoped
  `git diff --check`, the affected mdBook build, and focused executable tests
  when the changed surface is code-coupled.

## Review Checklist

Before changing behavior:

1. Identify the rule, its scope, and any explicitly approved exception.
2. Recheck the named source/manifests/tests rather than copying older prose.
3. Confirm the change preserves `ExecutionPlanDag` authority, dependency
   direction, typed trust boundaries, fail-closed admission, and run-scoped
   resource accounting where relevant.
4. Run the smallest verification named by the applicable rule, then broaden
   only across the dependency or user-facing surface that changed.
5. Update the contract register or open-question ledger when evidence changes;
   do not silently invent a new invariant.
