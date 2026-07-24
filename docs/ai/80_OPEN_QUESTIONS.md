# AI Onboarding: Open Questions

Verified against origin/main cf6609b9 (2026-07-24).

Purpose: Track unresolved documentation, architecture, API-stability, and
testing questions before future agents treat them as facts.

Dating convention: questions 1-27 were filed 2026-06-15 with the initial
registry; entries filed later carry an explicit `Filed:` line, and every new
question must include one. Question numbers are stable and never reused;
resolved or merged entries move to the Resolved Archive at the end of this
file, keeping their numbers.

## Source Evidence

Validate this page against:

- `docs/ai/*.md`
- `AGENTS.md`
- `crates/*/AGENTS.md`
- `Cargo.toml`
- `crates/*/Cargo.toml`
- Current Rust source, tests, examples, CI, and user/engine docs before acting
  on any item below.

Existing files under `docs/*`, `notebooklm-sources/*`, and older planning docs
may be stale. Do not copy their claims into code or docs without source
evidence.

## High Priority

### 1. Should executor runs consume the stored `CompiledPlan` DAG directly, or is re-entering compilation through `CompiledPlan::config()` intentional?

- Question: Is `PipelineExecutor::run_plan_with_readers_writers` expected to
  execute the stored `CompiledPlan` artifacts directly, or is the current
  delegation through `plan.config()` part of the intended runtime contract?
- Why it matters: The public boundary says executor APIs accept compiled plans,
  not raw config. If runtime recompilation is accidental, plan reuse,
  diagnostics, provenance, and tests may be weaker than the API suggests. If it
  is intentional, docs should explain why.
- Files/modules involved: `docs/ai/10_ARCHITECTURE.md`,
  `docs/ai/30_DESIGN_RULES.md`, `crates/clinker-exec/AGENTS.md`,
  `crates/clinker-plan/AGENTS.md`, `crates/clinker-exec/src/executor/mod.rs`,
  `crates/clinker-plan/src/plan/compiled.rs`,
  `crates/clinker-plan/src/config/pipeline.rs`.
- Suggested way to resolve it: Trace the executor run path and decide whether
  direct DAG execution is the target. Add regression tests around plan reuse or
  document the deliberate recompile step with its invariants.
- Priority: High

### 2. Should the folder overlay gain a `resources:` surface, and how broad should composition resources become?

(Absorbs former question 9, which asked how far composition resource kinds
should extend beyond file resources — the same declaration-only subsystem.
The per-value `fixed`-lock half of this entry is resolved; see the archive.)

- Open (resources fork): A `resources:` overlay surface is still absent, and
  restoring it is a genuine scope fork. The retired file-based `resources:` split
  was parsed into `ChannelBinding.resources_default` / `resources_fixed` but
  **never applied** — it took effect in no version. The broader composition
  resource subsystem is declaration-only: `_compose.resources_schema` and the
  call-site node `resources:` field parse, but `validate_resources`
  (`bind_schema.rs`) is a stub, resources get no `ProvenanceDb` entry,
  `Resource::File` is constructed at parse (`config/composition/raw.rs`) but
  never consumed at runtime. So a `resources:` overlay that merely round-trips
  would be an inert authoring surface, while making it "take effect" means
  building a new supply→resolution→consumption subsystem — a scope decision,
  not a wiring task.
- Why it matters: Deciding requires choosing between (a) deferring resources to a
  dedicated issue that designs the resource runtime, (b) restoring a parse-only /
  validated surface that does not affect execution, or (c) building the full
  subsystem now. These have materially different scope. The same decision should
  fix the intended resource-kind breadth (file-only versus a typed model) —
  agents must not invent new resource kinds by inference in the meantime.
- Files/modules involved: `crates/clinker-channel/src/manifest.rs`,
  `crates/clinker-channel/src/resolve.rs`,
  `crates/clinker-plan/src/config/composition/resource.rs`,
  `crates/clinker-plan/src/config/composition/raw.rs`,
  `crates/clinker-plan/src/plan/bind_schema.rs` (`validate_resources` stub),
  `docs/user/src/pipelines/compositions.md`.
- Suggested way to resolve it: Route the `resources:` surface and resource-kind
  breadth through one Decision Gate.
- Priority: Medium
- Filed: 2026-06-15; updated 2026-07-03 (fixed-lock half landed) and 2026-07-24
  (merged question 9).

### 3. Should pipeline-target channel config keys be validated before overlay application?

- Question: Is deferred validation for pipeline-targeting channel config keys
  intentional, or should pipeline targets get validation comparable to
  composition targets?
- Why it matters: Composition-target channel keys are checked against a symbol
  table, but pipeline-targeting fixtures are described as deferred. Weak
  validation can hide misspelled overrides until later or allow inconsistent
  provenance.
- Files/modules involved: `crates/clinker-channel/AGENTS.md`,
  `crates/clinker-channel/src/resolve.rs`,
  `crates/clinker-channel/src/overlay.rs`,
  `crates/clinker-channel/src/manifest.rs`,
  `crates/clinker-channel/tests/overlay_resolution_test.rs`,
  `docs/user/src/pipelines/channels.md`.
- Suggested way to resolve it: Define the intended pipeline-target validation
  surface. Add tests for valid keys, unknown keys, precedence, diagnostics, and
  the exact point where invalid overlays fail.
- Priority: High

### 4. What is the intended boundary between `clinker-schema` and `clinker-plan`, and how complete should its validation be?

(Absorbs former question 5 — discovery/validation depth is one half of the same
strengthen-or-keep-advisory decision.)

- Question: Should `clinker-schema` remain an advisory edge/authoring crate, or
  should schema discovery and validation move into the planner's compile-time
  boundary? If it stays advisory, how complete should `extract_schema_refs`,
  include/exclude glob behavior, format matching, and CXL field validation be?
- Why it matters: `clinker-schema` currently depends on `clinker-plan`, returns
  warnings, and uses lighter validation than the planner: glob matching is
  simple filename wildcards, `schema:` extraction is line-oriented, CXL field
  extraction is heuristic, and some warning variants appear unused. Agents need
  to know whether to strengthen this crate or keep it separate from canonical
  planning; docs and tests should not imply full workspace validation unless it
  is implemented.
- Files/modules involved: `docs/ai/20_CRATE_MAP.md`,
  `docs/ai/90_CRATE_AGENT_PLAN.md`, `crates/clinker-schema/AGENTS.md`,
  `crates/clinker-schema/src/lib.rs`, `crates/clinker-schema/src/validate.rs`,
  `crates/clinker-schema/src/discovery.rs`, `crates/clinker-plan/src/schema/`,
  `crates/clinker-plan/src/config/pipeline.rs`.
- Suggested way to resolve it: Maintainers should choose the boundary and
  document it in crate docs and AI docs. If `clinker-schema` stays advisory,
  user docs should avoid compiler-grade claims and tests should pin the current
  heuristic limitations; if it moves toward planning, replace heuristics with
  structured YAML/CXL parsing, expand warning coverage, and add planner tests.
- Priority: High
- Filed: 2026-06-15; merged question 5 on 2026-07-24.

### 6. Should user-facing docs be updated to the unified `nodes:` shape and all current node types?

(Absorbs former question 20 — choosing canonical envelope/document-context
docs and marking stale envelope examples historical is part of the same
stale-user-docs sweep.)

- Question: Which older user/engine docs still describe retired
  `inputs:` / `outputs:` / `transformations:` shapes or "eight node types",
  which envelope/document-context examples use retired shapes, and should they
  be modernized now?
- Why it matters: Current planning code accepts a unified `nodes:` list with
  eleven node variants. Stale docs can cause agents to revive retired config
  shapes, omit active nodes such as `reshape`, `cull`, and `envelope`, or copy
  stale envelope snippets into tests and examples.
- Files/modules involved: `docs/ai/70_GLOSSARY.md`,
  `crates/clinker-plan/AGENTS.md`,
  `crates/clinker/AGENTS.md`, `docs/user/src/getting-started/concepts.md`,
  `docs/user/src/pipelines/structure.md`, `docs/user/src/pipelines/envelope-and-doc-context.md`,
  `docs/user/src/nodes/envelope.md`,
  `crates/clinker-plan/src/config/pipeline_node.rs`,
  `crates/clinker-exec/src/executor/envelope_dispatch.rs`.
- Suggested way to resolve it: Audit user and engine docs for retired shapes,
  update examples to unified `nodes:`, pick canonical envelope examples, and
  add doc/example checks where feasible so future changes do not drift.
- Priority: High
- Filed: 2026-06-15; merged question 20 on 2026-07-24.

## Medium Priority

### 8. Is `clinker-format -> cxl` a permanent layering rule?

- Question: Should `clinker-format` continue depending on `cxl` for
  doc-path-aware envelope/extraction behavior, or should CXL-aware logic move
  into planning or execution?
- Why it matters: The dependency is current and working, but it makes the
  format crate more than a pure serialization leaf. Future format or envelope
  changes need to know whether this coupling is architectural or transitional.
- Files/modules involved: `docs/ai/10_ARCHITECTURE.md`,
  `docs/ai/20_CRATE_MAP.md`, `crates/clinker-format/AGENTS.md`,
  `crates/clinker-format/Cargo.toml`, `crates/clinker-format/src/envelope.rs`,
  `crates/clinker-format/src/doc_index.rs`, `crates/cxl/`.
- Suggested way to resolve it: Ask maintainers to classify the edge as
  intentional or debt. If intentional, document the allowed CXL use in
  `clinker-format`; if not, plan a refactor boundary and tests.
- Priority: Medium

### 10. Which planner and CXL public APIs are stable user-facing API versus internal exposed surface?

- Question: Should public symbols such as `cxl::resolve::HashMapResolver`,
  `cxl::typecheck::Row`, and legacy `clinker_plan::config::route::RouteConfig`
  / `RouteBranch` be documented as stable, test/support, or cleanup debt?
- Why it matters: AI docs list public APIs so future agents recognize them, but
  public visibility does not always imply a compatibility promise. Extending or
  documenting test doubles and legacy config structs as stable would freeze
  accidental surface area.
- Files/modules involved:
  `crates/cxl/AGENTS.md`, `crates/cxl/src/resolve/mod.rs`,
  `crates/cxl/src/resolve/test_double.rs`,
  `crates/cxl/src/typecheck/row.rs`,
  `crates/clinker-plan/src/config/route.rs`,
  `crates/clinker-plan/src/config/pipeline_node.rs`.
- Suggested way to resolve it: Add crate-level public API policy. Deprecate,
  hide, or document each public symbol based on intended downstream use, and add
  tests only for the promised surface.
- Priority: Medium

### 11. Are parsed CLI flags that appear weakly wired intentional placeholders or documentation drift?

- Question: Which `clinker run` flags are fully implemented runtime behavior,
  and which are parse-only, test-only, or future placeholders?
- Why it matters: The CLI is public. Docs should not promise behavior for flags
  that are not used in the run path, and agents should not wire them casually in
  core crates without tests.
- Files/modules involved: `crates/clinker/AGENTS.md`,
  `crates/clinker/src/main.rs`,
  `docs/user/src/ops/cli-reference.md`,
  `crates/clinker/tests/`.
- Suggested way to resolve it: Audit `RunArgs` fields from parsing through
  execution. For each flag, add behavior tests, document it, or mark/remove it
  if intentionally reserved.
- Priority: Medium

### 14. Should `PipelineCounters::ok_count` use a globally unique source-row identity?

- Question: Should successful-record deduplication use source plus row identity
  instead of `row_num` alone?
- Why it matters: Row-number collisions across sources can undercount distinct
  inputs; `counters.rs` now documents this as a known limitation but still keys
  by `row_num`. Counter semantics are visible through metrics and `$pipeline`
  CXL counters.
- Files/modules involved: `crates/clinker-record/AGENTS.md`,
  `crates/clinker-record/src/counters.rs`,
  `crates/clinker-exec/src/executor/params.rs`,
  `docs/user/src/ops/metrics.md`,
  `docs/user/src/pipelines/variables.md`.
- Suggested way to resolve it: Define the row identity model, update counters
  and serialization if needed, and add multi-source tests that pin expected
  counts.
- Priority: Medium

### 15. Is the optional `clinker-exec -> clinker-bench-support` `bench-alloc` edge acceptable long term?

- Question: Should allocation instrumentation remain as a feature-gated normal
  dependency edge from executor to benchmark support, or move elsewhere?
- Why it matters: The edge is feature-gated but still crosses from runtime into
  benchmark support. Future agents need to know whether this is a blessed
  exception or debt.
- Files/modules involved: `docs/ai/10_ARCHITECTURE.md`,
  `docs/ai/20_CRATE_MAP.md`, `docs/ai/40_COMMON_PATTERNS.md`,
  `docs/ai/60_PERFORMANCE_NOTES.md`, `crates/clinker-exec/AGENTS.md`,
  `crates/clinker-bench-support/AGENTS.md`,
  `crates/clinker-exec/Cargo.toml`,
  `crates/clinker-exec/src/executor/stage_metrics.rs`,
  `crates/clinker-bench-support/src/alloc.rs`.
- Suggested way to resolve it: Confirm the feature-gated edge policy. If kept,
  document it as an explicit exception and add dependency tests/gates if
  practical. If not, move allocation hooks into a lower or benchmark-only crate.
- Priority: Medium

### 16. Which benchmark identifiers, cache keys, generated formats, and CI JSON fields are compatibility surfaces?

- Question: Are `Scale` labels, `FieldKind`/`DataFormat` discriminants,
  `BenchDataCache` hash inputs, unsupported generated formats, and
  `target/bench-results/summary.json` fields stable enough to preserve?
- Why it matters: Changing these can invalidate benchmark comparisons, cached
  data, and downstream CI/report tooling. Adding unsupported benchmark YAML
  without generator support can create false coverage.
- Files/modules involved: `crates/clinker-bench-support/AGENTS.md`,
  `crates/clinker-benchmarks/AGENTS.md`,
  `crates/clinker-bench-support/src/lib.rs`,
  `crates/clinker-bench-support/src/cache.rs`,
  `crates/clinker-benchmarks/src/format_mapping.rs`,
  `crates/clinker-benchmarks/src/report.rs`,
  `benches/pipelines/`.
- Suggested way to resolve it: Write a benchmark compatibility note covering
  stable identifiers, cache invalidation policy, unsupported generated formats,
  and report schema expectations. Add tests for report JSON if downstream tools
  depend on it.
- Priority: Medium

### 17. Should direct `serde_saphyr::from_str` calls in tests be allowed exceptions?

- Question: Is direct use of `serde_saphyr::from_str` in tests an approved
  test-local exception to the YAML chokepoint rule, or should all tests use
  `clinker_plan::yaml::from_str` too?
- Why it matters: The production rule is strict because spans, parser budgets,
  and `Spanned<T>` behavior are load-bearing. Tests can accidentally become
  copy-paste sources for production code.
- Files/modules involved: `docs/ai/30_DESIGN_RULES.md`,
  `docs/ai/40_COMMON_PATTERNS.md`, `crates/clinker-plan/AGENTS.md`,
  `crates/clinker-plan/src/yaml.rs`,
  `crates/clinker-exec/tests/composition_binding_test.rs`.
- Suggested way to resolve it: Decide whether tests may bypass the chokepoint.
  If allowed, document the narrow exception. If not, update tests and add an
  `rg`-based check or review rule.
- Priority: Medium

### 18. Should diagnostic code pages exist for channel variable overlay errors?

- Question: Should `E109`, `E110`, and `E111` get dedicated `docs/explain/`
  pages and lookup coverage?
- Why it matters: Channel var overlay failures are user-visible diagnostics.
  Diagnostic-code lookup should be consistent for errors emitted by channel and
  planning layers.
- Files/modules involved:
  `crates/clinker-channel/src/overlay.rs`,
  `docs/user/src/pipelines/channels.md`,
  `docs/user/src/pipelines/variables.md`,
  `crates/clinker-plan/src/plan/explain_provenance.rs`,
  `docs/explain/`.
- Suggested way to resolve it: Decide which channel codes are public. Add
  explain pages and tests for `clinker explain --code`, or document why the
  codes are intentionally not lookup-backed.
- Priority: Medium

### 19. What charset and non-UTF-8 support is actually promised by format docs?

- Question: Should only X12 document ISO-8859-1 behavior, while HL7/SWIFT/other
  formats remain UTF-8 only, or is broader charset support planned?
- Why it matters: Encoding support is user-visible and format-specific. Docs
  should not imply non-UTF-8 support where readers reject it.
- Files/modules involved: `docs/user/src/formats/x12.md`,
  `docs/user/src/formats/hl7.md`, `docs/user/src/formats/swift.md`,
  `crates/clinker-format/AGENTS.md`,
  `crates/clinker-format/src/x12/`,
  `crates/clinker-format/src/hl7/tokenizer.rs`,
  `crates/clinker-format/src/swift/`.
- Suggested way to resolve it: Audit format readers and docs by format. Add
  acceptance/rejection tests for non-UTF-8 cases and update docs to match.
- Priority: Medium

### 21. Should `CROSS_RECORD_TRANSFORMS_PLAN.md` be marked historical or active?

- Question: Is `CROSS_RECORD_TRANSFORMS_PLAN.md` obsolete planning history, or
  does it still contain active design guidance?
- Why it matters: The file references old paths such as
  `crates/clinker-core/...`, while related concepts now exist as `reshape` and
  `cull` under current plan/exec crates. Agents may mistake it for current
  architecture.
- Files/modules involved: `CROSS_RECORD_TRANSFORMS_PLAN.md`,
  `crates/clinker-plan/src/config/pipeline_node.rs`,
  `crates/clinker-exec/src/executor/reshape_dispatch.rs`,
  `crates/clinker-exec/src/executor/cull_dispatch.rs`,
  `docs/user/src/nodes/reshape.md`, `docs/user/src/nodes/cull.md`.
- Suggested way to resolve it: Add an explicit status note to the plan file:
  historical, superseded, or active. If active, update paths and open work
  items to current crate names.
- Priority: Medium

## Low Priority

### 22. What is the intended expansion of "CXL"?

- Question: Should docs expand the acronym "CXL", or should they define it
  only by behavior as Clinker's expression language?
- Why it matters: AI docs can safely describe CXL behavior, but inventing an
  acronym expansion would create false terminology.
- Files/modules involved: `docs/ai/70_GLOSSARY.md`,
  `docs/user/src/getting-started/concepts.md`,
  `docs/engine/src/cxl-internals.md`, `crates/cxl/Cargo.toml`,
  `crates/cxl/src/lib.rs`.
- Suggested way to resolve it: Ask maintainers for the canonical expansion. If
  none exists, add a note that CXL is a name, not an expanded acronym.
- Priority: Low

### 24. Are weakly used declared dependencies intentional compatibility hooks or cleanup debt?

- Question: Should declared dependencies with weak current source use, such as
  `miette` in `clinker-core-types` / `cxl-cli` and some `clinker-schema`
  manifest entries, be kept intentionally?
- Why it matters: Agents may remove dependencies as cleanup or cite them as
  important architecture without understanding whether they are reserved for
  compatibility, diagnostics, or stale.
- Files/modules involved: `crates/clinker-core-types/AGENTS.md`,
  `crates/cxl-cli/AGENTS.md`, `crates/clinker-schema/AGENTS.md`,
  `crates/clinker-core-types/Cargo.toml`, `crates/cxl-cli/Cargo.toml`,
  `crates/clinker-schema/Cargo.toml`.
- Suggested way to resolve it: Run a source-use audit, ask maintainers for
  intent, and either remove unused dependencies or document why they are kept.
- Priority: Low

### 25. Is `tokio` reserved dependency surface or stale workspace debt?

- Question: Why does the workspace include `tokio` when AI docs say current
  core execution and REST transport are synchronous and source search found no
  async runtime use?
- Why it matters: Future agents might treat `tokio` as permission to add async
  execution or connectors, which would be an architectural change.
- Files/modules involved: `docs/ai/10_ARCHITECTURE.md`,
  `docs/ai/30_DESIGN_RULES.md`, `docs/ai/60_PERFORMANCE_NOTES.md`,
  `Cargo.toml`, `crates/clinker-exec/AGENTS.md`,
  `crates/clinker-net/AGENTS.md`.
- Suggested way to resolve it: Ask maintainers whether `tokio` is reserved,
  stale, or for external tooling. Document the answer near dependency rules and
  async-runtime guidance.
- Priority: Low

### 26. Which weakly inferred AI common-pattern notes should become project rules?

- Question: Should notes such as "DAG dispatcher, not ECS" and project-wide
  `SchemaBuilder` preference be promoted from observed patterns to reviewed
  design rules?
- Why it matters: These rules are useful but partly wording- and convention-
  based. Agents should know whether they are enforceable review criteria or
  local heuristics.
- Files/modules involved: `docs/ai/40_COMMON_PATTERNS.md`,
  `docs/ai/30_DESIGN_RULES.md`,
  `crates/clinker-record/src/schema.rs`,
  `crates/clinker-plan/src/plan/execution/mod.rs`,
  `crates/clinker-exec/src/executor/dispatch.rs`.
- Suggested way to resolve it: Maintainers should review the human-review notes
  in `40_COMMON_PATTERNS.md` and either move approved items into
  `30_DESIGN_RULES.md` or label them explicitly as local patterns.
- Priority: Low

### 27. Should docs-only AI changes have a renderer or link-check gate beyond `git diff --check`?

- Question: Is `git diff --check` sufficient for AI docs, or should docs-only
  changes also run mdBook builds, Markdown rendering checks, or link checks?
- Why it matters: AI docs are not currently an mdBook, but large tables and
  links can still break readability. Future agents need a consistent smallest
  relevant gate.
- Files/modules involved: `docs/ai/50_TESTING_AND_COMMANDS.md`, `AGENTS.md`,
  `docs/ai/*.md`.
- Suggested way to resolve it: Decide the docs-only validation policy for
  `docs/ai`. Update `50_TESTING_AND_COMMANDS.md` and root `AGENTS.md` if a
  renderer or link checker becomes required.
- Priority: Low

### 29. Should `ProvenanceDb` be keyed by `PlanNodeId` despite its name-addressed query contract?

- Question: After the `PlanNodeId` identity rip, `CompileArtifacts.provenance`
  (`ProvenanceDb`) is the one per-node compile facility still keyed by the bare
  `(node_name, param_name)`. A top-level node and a composition-body node that
  share a name and both carry provenance-tracked config params collide in the
  flat map. It was left name-keyed because its query surface is user-facing.
- Why it matters: `explain --provenance` resolves a dotted `node_name.param_name`
  path, and `ProvenanceDb`'s public API (`get`, `params_for_node`,
  `node_names`) is name-addressed by contract — so re-keying the internal store
  to `PlanNodeId` would require a `PlanNodeId → name` reverse map at every query
  site and changes the externally-observable lookup model. The residual
  collision is narrow (same name across scopes, both with tracked params) but
  real.
- Files/modules involved:
  `crates/clinker-plan/src/plan/explain_provenance.rs` (`ProvenanceDb`),
  `crates/clinker-plan/src/plan/bind_schema.rs` (`bind_composition` provenance
  population), the `explain --provenance` CLI path.
- Suggested way to resolve it: Decide whether cross-scope node-name reuse is
  supported for provenance-tracked compositions. If so, key the internal store
  by `PlanNodeId` and resolve the user-facing dotted path to an id at query
  time (preserving the external contract); add a nested-composition regression
  test. If not, add a bind-time diagnostic rejecting the same-name collision, or
  document that provenance is attributed by scoped path.
- Priority: Low
- Filed: 2026-06-24 (residual of resolved question 28).

### 30. Analytic windows inside a nested composition body are never resolved

- Question: `resolve_composition_body_windows`
  (`crates/clinker-plan/src/plan/execution/composition.rs`) resolves each
  composition call site only against the TOP-LEVEL DAG's `id_to_index` bridge.
  A composition node whose call site lives inside an enclosing body (a
  composition-of-composition) has no entry in the top-level bridge, so its
  `composition_idx` stays `None` and the inner body's analytic-window
  `IndexSpec`s are never built/backfilled. Should body-window resolution walk
  nested scopes?
- Why it matters: A window builtin over a nested composition body sees an
  unpopulated `window_index` and silently emits `Null`/incorrect window results
  at runtime instead of a diagnostic. This is a pre-existing limitation — the
  prior name-map scheme had the same top-level-only reach — surfaced (but not
  changed) by the move to the `PlanNodeId -> NodeIndex` bridge.
- Files/modules involved:
  `crates/clinker-plan/src/plan/execution/composition.rs`
  (`resolve_composition_body_windows`),
  `crates/clinker-plan/src/plan/composition_body.rs`
  (`body_indices_to_build`).
- Suggested way to resolve it: Decide whether analytic windows are supported in
  nested composition bodies. If so, resolve each body's call site against the
  enclosing scope's bridge (recurse through `composition_bodies`), not only the
  top-level DAG; add a nested-composition window regression test. If not, emit a
  bind-time diagnostic rejecting `analytic_window:` in a body reachable only
  through a nested composition.
- Priority: Low
- Filed: 2026-06-24.

### 31. Authoring-time `numeric -> int|float` inference (`clinker guess`) is unbuilt

(The schema-resolver unification this question originally tracked has landed;
see the Resolved Archive. This entry keeps the one remaining follow-on.)

- Question: The `numeric` type is an inference-only union that never survives a
  resolved schema (compile rejects unresolved `type: numeric` with E158). The
  planned authoring-time path that concretizes it — a `clinker guess` flow that
  infers `int` versus `float` from sample data — is unbuilt. Should it be built,
  and with what sampling/precision rules?
- Why it matters: Until an inference path exists, users must hand-resolve
  `numeric` columns; docs and examples must keep declaring concrete
  `int`/`float`, and agents must not treat `numeric` as a runtime type.
- Files/modules involved: `crates/clinker-plan/src/schema/mod.rs`,
  `crates/cxl/src/typecheck/types.rs`, `crates/clinker/src/main.rs`,
  `docs/user/src/nodes/source.md`.
- Suggested way to resolve it: Design the `clinker guess` sampling and
  concretization rules, then implement it as an authoring-time CLI flow that
  rewrites the schema declaration; keep runtime strictness unchanged.
- Priority: Medium
- Filed: 2026-07-02; narrowed to the `clinker guess` follow-on 2026-07-24.

## Resolved Archive

Numbers are never reused. One line per entry: the answer and its evidence.

- **2 (partial, `fixed` lock; resolved 2026-07-03):** The folder overlay carries
  a `fixed:` block beside `config:` on every layer file; a lower layer locks a
  value against every higher layer via `ResolvedValue::apply_layer_fixed`, and
  `$config` folding plus `channels resolve` honor it (issue #772;
  `docs/user/src/pipelines/channels.md`, "Locking a value: `fixed`"). The
  `resources:` fork remains open as question 2.
- **5 (merged 2026-07-24):** Folded into question 4 — discovery/validation depth
  is one half of the strengthen-or-keep-advisory decision.
- **7 (resolved by documentation):** Current transports are file and finite
  REST; SQL-cursor wording is roadmap-only, stated in
  `crates/clinker-net/AGENTS.md`. Only HTTP pagination cursors exist
  (`crates/clinker-net/src/rest.rs`).
- **9 (merged 2026-07-24):** Folded into question 2 — resource-kind breadth and
  the `resources:` overlay surface are one scope decision.
- **12 (resolved):** `Command::Eval.expr` stays a single `Option<String>`; user
  docs show multiple CXL statements inside one `-e` value; the `cxl-cli`
  package description says validator/evaluator/formatter
  (`crates/cxl-cli/src/main.rs`; `docs/user/src/cxl/cxl-cli.md`).
- **13 (resolved):** `json_to_value` recursively maps JSON objects to
  `Value::Map`; docs list `{object}` as `Map`; unit coverage pins nested
  objects inside maps and arrays (`crates/cxl-cli/src/main.rs`).
- **20 (merged 2026-07-24):** Folded into question 6 — canonical
  envelope/document-context docs are part of the stale-user-docs sweep.
- **23 (resolved):** The `reserve/` package is a crates.io name-reservation
  placeholder only — its README states the role and "pre-release placeholder"
  status. It is intentionally untracked (absent from clones); do not cite its
  files as repository evidence.
- **28 (filed 2026-06-22, resolved 2026-06-24):** Per-node CXL artifact tables
  in `CompileArtifacts` are keyed by a dense `PlanNodeId` minted at graph
  construction (stronger than `(scope, name)`; `ScopedNodeId`/`NodeScope`
  deleted), eliminating the cross-scope last-writer-wins collision
  (`crates/clinker-plan/src/plan/bind_schema.rs`). The one name-keyed facility
  left by design is `ProvenanceDb` — open question 29.
- **31 (partial, schema-resolver unification; resolved 2026-07-03):** The two
  per-source schema representations collapsed into one `Column`/`SourceSchema`;
  the format-layer `FieldType`/`FieldDef` vocabulary and the `decimal` type
  token are retired (decimal = `float` + `precision`/`scale`); `patch_schema`
  (E230-E235) is the single schema-override path; `CompiledPlan` carries
  `bound_schemas` + `SchemaProvenanceDb`; unresolved `type: numeric` is
  rejected at compile (E158) and `generated` on a non-EDI format is rejected
  (E159). The `clinker guess` inference follow-on remains open as question 31.
