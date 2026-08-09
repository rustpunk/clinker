# AI Onboarding: Open Questions

Verified against working tree fb07dd7c (2026-07-26).

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
- Status: Resolved
- Decision: D-01 through D-11
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` records the authoritative stored-plan and runtime-envelope contract; `crates/clinker-exec/src/executor/mod.rs` and `crates/clinker-plan/src/plan/compiled.rs` show the current recompile path and stored artifacts.
- Implementation owner: Phase 5 / PERF-01
- Verified: 2026-07-29

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
- Documentation status: The user guide now labels composition resource
  declarations and call-site bindings as reserved and non-operational. This
  warning does not resolve the runtime or overlay design choice.
- Status: Resolved
- Decision: D-12 through D-15
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` defines the typed catalog-and-slot model; `crates/clinker-plan/src/plan/bind_schema.rs` still contains the current `validate_resources` stub.
- Implementation owner: Phase 4 / AUTH-01
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-43
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` requires pre-fold validation of every overlay layer; `crates/clinker-channel/src/resolve.rs` and `crates/clinker-channel/src/overlay.rs` are the current resolution boundary.
- Implementation owner: Phase 2 / CORR-06
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-17
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` makes `clinker-plan` the sole execution-admission authority and bounds `clinker-schema` as advisory; `crates/clinker-schema/src/validate.rs` contains the current advisory checks.
- Implementation owner: Phase 4 / AUTH-02
- Verified: 2026-07-29

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
- Status 2026-07-26: The live user and engine books now describe all eleven
  node variants, node-specific wiring, the `_compose:` plus `nodes:` definition
  shape, and current analytic-window nesting. The committed
  `examples/pipelines/compositions/*.comp.yaml` corpus still contains retired
  `transformations:` definitions, so this question remains open until those
  examples are modernized or explicitly marked historical.
- Status: Resolved
- Decision: D-44
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` classifies the composition examples as executable documentation and requires `_compose:` plus `nodes:`; `examples/pipelines/compositions/` is the owned migration corpus.
- Implementation owner: Phase 4 / AUTH-04
- Verified: 2026-07-29

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
  `crates/clinker-format/Cargo.toml`, `crates/clinker-format/src/schema.rs`,
  `crates/clinker-format/src/doc_index.rs`, `crates/cxl/`.
- Suggested way to resolve it: Ask maintainers to classify the edge as
  intentional or debt. If intentional, document the allowed CXL use in
  `clinker-format`; if not, plan a refactor boundary and tests.
- Priority: Medium
- Status: Resolved
- Decision: D-20
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` permits only logical-type and document-path/index use; `crates/clinker-format/Cargo.toml`, `crates/clinker-format/src/schema.rs`, and `crates/clinker-format/src/doc_index.rs` show the current dependency and use.
- Implementation owner: Phase 1 / CONT-05
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-18 and D-19
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` defines the curated compatibility facade and classifies every seeded symbol; current exports live in `crates/cxl/src/resolve/mod.rs`, `crates/cxl/src/typecheck/row.rs`, and `crates/clinker-plan/src/config/route.rs`.
- Implementation owner: Phase 1 / CONT-04
- Verified: 2026-07-29

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
- Documentation status: The known `--channel` omission and the incorrect claim
  that `-n/--dry-run-n` implies `--dry-run` were corrected on 2026-07-26. The
  broader field-by-field wiring audit remains open.
- Status: Resolved
- Decision: D-45
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` prohibits accepted parse-only options and names the first audit set; `crates/clinker/src/main.rs` is the current `RunArgs` and run-path authority.
- Implementation owner: Phase 4 / AUTH-05
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-46
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` defines `SourceRowId { source: PlanNodeId, ordinal: u64 }`; `crates/clinker-record/src/counters.rs` documents the current row-number-only limitation.
- Implementation owner: Phase 2 / CORR-02
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-21
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` permits only repaired feature-gated instrumentation and marks current measurements untrusted; `crates/clinker-exec/Cargo.toml` carries the `bench-alloc` edge.
- Implementation owner: Phase 1 / CONT-05
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-47
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` defines versioned machine IDs, manifests, caches, and report envelopes; `crates/clinker-bench-support/src/cache.rs` and `crates/clinker-benchmarks/src/report.rs` expose the current surfaces.
- Implementation owner: Phase 5 / PERF-07
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-22
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` rejects test-local parser bypasses; `crates/clinker-plan/src/yaml.rs` is the canonical wrapper and `crates/clinker-exec/tests/composition_binding_test.rs` contains the current exception to remove.
- Implementation owner: Phase 6 / EVID-03
- Verified: 2026-07-29

### 18. Should the remaining E109 and E111 conditions get explain pages?

- Question: Should `E109` and `E111` get dedicated `docs/explain/` pages and
  lookup coverage?
- Why it matters: E109 is a user-visible channel overlay failure and E111 is a
  composition-body validation failure. E110 now has a dedicated extraction
  page, and the former channel meanings split to E116-E118 with their own
  pages; these two older conditions remain without lookup coverage.
- Files/modules involved:
  `crates/clinker-channel/src/overlay.rs`,
  `docs/user/src/pipelines/channels.md`,
  `docs/user/src/pipelines/variables.md`,
  `crates/clinker-plan/src/plan/explain_provenance.rs`,
  `crates/clinker/src/main.rs`, `docs/explain/`,
  `docs/user/src/ops/explain.md`.
- Suggested way to resolve it: Decide whether these conditions warrant public
  long-form pages, then add the pages and lookup tests or document why they are
  intentionally registry-only.
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
- Status: Resolved
- Decision: D-49
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` locks the per-format charset matrix; the current boundaries are implemented under `crates/clinker-format/src/` and described in `docs/user/src/formats/`.
- Implementation owner: Phase 4 / AUTH-06
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-50
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` establishes Clinker Expression Language and per-record ETL framing; `docs/ai/70_GLOSSARY.md` applies the canonical expansion.
- Implementation owner: Phase 1 / CONT-08
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-23
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` requires source-unused declarations to be removed after a complete source/feature/API audit; current declarations are visible in the named crate manifests.
- Implementation owner: Phase 4 / CONT-05
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-23
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` classifies unused workspace Tokio as removable source-unused debt; `Cargo.toml` records the current declaration while production source remains synchronous.
- Implementation owner: Phase 4 / CONT-05
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-51
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` defines the promotion rule; `docs/ai/30_DESIGN_RULES.md` and `docs/ai/40_COMMON_PATTERNS.md` now separate reviewed invariants from observed, preferred, and local patterns.
- Implementation owner: Phase 1 / CONT-08
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-52
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` selects the pinned offline documentation check plus scoped whitespace validation; `scripts/check-ai-docs.sh` implements the repository-owned gate.
- Implementation owner: Phase 1 / CONT-08
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-53
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` selects `PlanNodeId`-keyed provenance with canonical escaped addresses and ambiguity errors; `crates/clinker-plan/src/plan/explain_provenance.rs` shows the current name-keyed query contract.
- Implementation owner: Phase 2 / CORR-04
- Verified: 2026-07-29

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
- Documentation status: The window-function guide now warns authors not to use
  analytic windows inside nested composition calls until this question is
  resolved.
- Status: Resolved
- Decision: D-54
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` requires scope-local windows at every otherwise valid nesting depth; `crates/clinker-plan/src/plan/execution/composition.rs` contains the current top-level-only resolution path.
- Implementation owner: Phase 2 / CORR-03
- Verified: 2026-07-29

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
- Status: Resolved
- Decision: D-55
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` locks bounded preview, exhaustive evidence, and guarded rewrite semantics; `crates/clinker-plan/src/schema/mod.rs` retains strict E158 runtime admission and `crates/clinker/src/main.rs` has no current `guess` command.
- Implementation owner: Phase 4 / AUTH-03
- Verified: 2026-07-29

### 32. Should reserved composition call-site `outputs:` and `alias:` be removed or implemented?

- Question: The composition node parser accepts call-site `outputs:` and
  `alias:`, but binding computes output rows exclusively from
  `_compose.outputs` and does not consume the call-site output map, while the
  stored alias does not namespace body nodes. Should these fields be rejected,
  assigned concrete semantics, or retained as explicitly reserved syntax?
- Why it matters: These are strict, user-authored YAML fields. Accepting them
  without an effect invites authors to believe output remapping or collision
  avoidance occurred when it did not.
- Files/modules involved:
  `crates/clinker-plan/src/config/pipeline_node.rs`,
  `crates/clinker-plan/src/plan/bind_schema.rs` (`compute_output_rows`),
  `docs/user/src/pipelines/compositions.md`.
- Suggested way to resolve it: Decide the intended call-site output and
  namespacing model. Remove the fields with a diagnostic if no second spelling
  is needed, or implement and test their exact binding, collision, provenance,
  and downstream-port behavior.
- Priority: Medium
- Filed: 2026-07-26. The user guide currently marks both fields reserved.
- Status: Resolved
- Decision: D-16
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` rejects ordinary call-site `outputs:` and `alias:` while preserving `_compose.outputs` and overlay insertion aliases; `crates/clinker-plan/src/config/pipeline_node.rs` shows the currently parsed fields.
- Implementation owner: Phase 4 / AUTH-01
- Verified: 2026-07-29

### 33. Should parsed `PipelineMeta` specification stubs remain accepted YAML?

- Question: `date_locale`, `log_rules`, and `include_provenance` are declared as
  specification stubs on `PipelineMeta`, and source-wide use is limited to
  parsing/holding those values. Should Clinker reject them until implemented,
  remove them, or define and wire their runtime behavior?
- Why it matters: Pipeline metadata is a strict public authoring surface.
  Accepting a setting that does nothing can produce a successful run whose
  formatting, logging, or provenance behavior differs from the author's
  expectation.
- Files/modules involved: `crates/clinker-plan/src/config/pipeline.rs`,
  `docs/user/src/pipelines/structure.md`, planner and executor consumers chosen
  by the eventual behavior.
- Suggested way to resolve it: Choose reject, remove, or implement for each
  field. If a field remains reserved, emit an explicit validation diagnostic
  instead of silently accepting it; if implemented, add behavior tests and
  user documentation in the same change.
- Priority: Medium
- Filed: 2026-07-26. The user guide currently warns that `date_locale` and
  `include_provenance` have no runtime effect; `log_rules` remains undocumented.
- Status: Resolved
- Decision: D-24
- Evidence: `docs/ai/15_PRODUCTION_CONTRACTS.md` requires rejection until each field has end-to-end behavior; `crates/clinker-plan/src/config/pipeline.rs` contains the current accepted specification stubs.
- Implementation owner: Phase 2 / CORR-05
- Verified: 2026-07-29

### 32. Output permissions are secure but not configurable for shared drop zones

- Question: Unix output files are created with owner-only mode `0600`. Should
  Clinker expose a validated output-permission policy for destinations whose
  intended consumer is a different service account or group?
- Why it matters: `0600` is a safe default and prevents accidental disclosure,
  but it requires an external ACL/ownership policy for common ETL drop-zone
  workflows. Silently honoring process umask would make the effective access
  harder to review and would weaken the current default.
- Files/modules involved:
  `crates/clinker-exec/src/output/containment.rs`, output configuration in
  `clinker-plan`, and `docs/user/src/nodes/output.md`.
- Suggested way to resolve it: Research group-owned drop-zone practices on
  Unix, NFS, and SMB; decide whether the surface should be an explicit mode,
  an ACL-oriented policy, or remain external. Preserve `0600` as the default
  and reject unsafe or unsupported values rather than inheriting ambient umask.
- Priority: Medium
- Filed: 2026-08-02.

### 33. Corporate TLS roots are not configurable for REST sources

- Question: Should REST sources trust the platform certificate store, accept an
  explicit CA bundle path, or support both for TLS-inspecting corporate
  proxies?
- Why it matters: Proxy routing already follows the standard proxy environment,
  but the current `ureq` Rustls feature uses bundled public Web PKI roots. A
  private corporate CA installed on the host is therefore not enough to make an
  intercepted vendor connection trusted.
- Files/modules involved: root `Cargo.toml`, `crates/clinker-net/src/rest.rs`,
  and `docs/user/src/formats/source-network.md`.
- Suggested way to resolve it: Compare Rustls platform verification with an
  explicit PEM bundle surface across Linux, macOS, and Windows; threat-model CA
  path substitution and diagnostics; obtain dependency approval before adding
  a verifier or certificate-loading crate. Never add an insecure skip-verify
  option.
- Priority: High
- Filed: 2026-08-02.

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
- **21 (resolved by maintainer decision, 2026-07-24):** The cross-record
  document-level transforms design is a historical record, not active
  guidance — it shipped as the `Reshape` and `Cull` nodes
  (`crates/clinker-exec/src/executor/reshape_dispatch.rs`,
  `crates/clinker-exec/src/executor/cull_dispatch.rs`) — and its plan file has
  been retired from the tracked tree to local working notes. Treat any
  surviving copy as history; where it disagrees with source, source wins.
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
  the format-layer `FieldType`/`FieldDef` vocabulary is retired, while the
  unified CXL type now supports first-class exact `decimal` columns with
  `precision`/`scale` attributes; `patch_schema` (E230-E245) is the single
  schema-override path; `CompiledPlan` carries `bound_schemas` plus
  `SchemaProvenanceDb`; unresolved `type: numeric` is rejected at compile
  (E158), and `generated` on a non-EDI format is rejected (E159). The
  `clinker guess` inference follow-on remains open as question 31. This current
  wording supersedes the earlier archive claim that the decimal token was
  retired.
- **32 (filed 2026-08-08):** `send_otlp_json` re-parses each payload into a
  `serde_json::Value` tree to count the items it contains
  (`crates/clinker-net/src/otlp.rs`, `validate_and_count_payload`), although
  the producer already counted them while encoding and passes the count to
  `DeliveryBackend::deliver`. The second parse is not redundant as written:
  this is a public entry point taking arbitrary bytes, and the walk also
  checks the payload matches the selected signal envelope. Removing it means
  either trusting a caller-supplied count at a validated boundary, or
  deserializing into shape-only types whose elements are `IgnoredAny` so the
  count survives without a DOM. The second is the better answer and is a
  change of its own; the cost today is one extra parse and an allocation of
  the payload's order on the exporter thread, per request.
- **33 (filed 2026-08-08):** Twelve dispatchers each carry their own
  `<Kind>DispatchContext` enum, `From` impl, and
  `dispatch_<kind>_mismatch_for_testing` helper (for example
  `crates/clinker-exec/src/executor/cull_dispatch.rs`), differing in nothing
  but the operator name. One generic carrier in `dispatch.rs` would serve all
  of them. Left as is for now because collapsing them touches every operator
  entry point at once; the risk is that a thirteenth operator copies the block
  again, or that one copy is edited inconsistently and quietly loses its
  mismatch guard.
- **34 (filed 2026-08-08, needs a maintainer decision):** RFC 8288 `Link`
  header parsing in `crates/clinker-net/src/rest/continuation.rs`
  (`parse_link_field`, `split_link_values`, `unquote`) is hand-rolled — roughly
  a hundred lines of quoted-string, comma, and semicolon tokenizing, including
  the `rel` token-list and escaping corners. AGENTS.md is explicit that
  hand-rolling a parser a vetted crate provides is a dependency decision taken
  without review, so this needs approving as either a dependency or a
  deliberate exception rather than being grown further. Each mis-handled corner
  of the grammar is a pagination failure against a real server, and this file
  has now been repaired twice for exactly that class of defect. No behavior
  change is proposed here; the decision is which way to close it.
- **35 (filed 2026-08-09, needs a maintainer decision):** When the machine
  protocol's liveness worker gives up on a sink that has refused records for
  the whole patience window, the only report is a `tracing` warning on stderr.
  A supervisor consuming the protocol stream alone still sees an ordinary
  successful terminal and cannot learn the liveness channel died. The two
  obvious in-band answers are both blocked: the stream itself is the thing
  that failed, so a record announcing it may not arrive either, and the
  bulkhead rule says a delivery outcome never determines process status, so
  the exit code must not change. Closing this properly means a schema-2 field
  on the terminal record stating whether the liveness channel survived, which
  is a protocol change rather than a repair.
- **36 (filed 2026-08-09, needs a maintainer decision):** The recovery-matrix
  release gate in `tools/release-policy/src/recovery.rs` is keyed on internal
  planning identifiers, held as load-bearing constants rather than comments.
  The project's comment rule keeps such labels out of Rust source, and they
  are worse as constants: a reader cannot tell what they name, and the gate
  stops matching the moment that planning artifact moves on. The gate is
  validating a real receipt, so the identifiers cannot simply be deleted --
  closing this means deciding what the receipt should be keyed on instead
  (a content hash, a named capability set, or the command registry itself)
  and reissuing the receipt against it.
- **37 (filed 2026-08-09):** `LogDispatcher::emit` allocates one
  `Vec<SignalField>` per emitted record on the per-record transform path
  (`crates/clinker-exec/src/log_dispatch.rs`). A reusable scratch buffer is
  not available: `SignalField<'a>` borrows both the field name and the record
  value, so the vector cannot outlive the record it describes without unsafe
  lifetime reuse. Directives with no `fields:` already allocate nothing, since
  the vector is built with zero capacity. Removing the remaining cost means
  either an inline-capacity vector (a new dependency) or an arena, both of
  which are approval-gated decisions rather than repairs.
- **38 (filed 2026-08-09, resolved as designed):** A machine-mode run that
  cannot install its signal handler exits 4 having written nothing to the
  protocol stream, so a supervisor reading only the stream sees a process exit
  without a word. Reviewed and kept: the refusal is deliberately pre-effect --
  `signal_handler_installation_failure_is_preeffect` asserts the filesystem is
  untouched and the stream unopened -- and a `started` record would announce a
  run that never began, which is a worse thing for a stream to say than
  nothing. The distinct exit status is what carries the fact. Reopening this
  means deciding whether the protocol should gain a record for "refused before
  starting", which is a schema change and shares its shape with question 35.
