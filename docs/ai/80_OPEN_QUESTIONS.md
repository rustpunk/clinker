# AI Onboarding: Open Questions

Purpose: Track unresolved documentation, architecture, API-stability, and
testing questions before future agents treat them as facts.

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

### 2. Should channel `resources.default` and `resources.fixed` be implemented, rejected, or documented as reserved?

- Question: What is the intended behavior for parsed channel resource overlay
  fields when `apply_channel_overlay` currently applies config and var layers
  but not resource overlays?
- Why it matters: Channels are documented as overriding declared config/resource
  surfaces only. Parsed-but-underwired resource fields can mislead future agents
  into documenting behavior that is not implemented or adding ad hoc overlay
  logic without preserving provenance.
- Files/modules involved: `docs/ai/80_OPEN_QUESTIONS.md`,
  `crates/clinker-channel/AGENTS.md`,
  `crates/clinker-channel/src/binding.rs`,
  `crates/clinker-channel/src/overlay.rs`,
  `crates/clinker-plan/src/config/composition/resource.rs`,
  `docs/user/src/pipelines/channels.md`,
  `examples/pipelines/channels/`.
- Suggested way to resolve it: Decide whether resource overlays are active
  scope. If active, implement them with diagnostics and tests for precedence,
  provenance, and composition boundaries. If future-only, reject or warn on the
  fields and document them as reserved.
- Priority: High

### 3. Should pipeline-target channel config keys be validated before overlay application?

- Question: Is deferred validation for pipeline-targeting channel config keys
  intentional, or should pipeline targets get validation comparable to
  composition targets?
- Why it matters: Composition-target channel keys are checked against a symbol
  table, but pipeline-targeting fixtures are described as deferred. Weak
  validation can hide misspelled overrides until later or allow inconsistent
  provenance.
- Files/modules involved: `crates/clinker-channel/AGENTS.md`,
  `crates/clinker-channel/src/binding.rs`,
  `crates/clinker-channel/src/overlay.rs`,
  `crates/clinker-channel/tests/channel_binding_test.rs`,
  `docs/user/src/pipelines/channels.md`.
- Suggested way to resolve it: Define the intended pipeline-target validation
  surface. Add tests for valid keys, unknown keys, precedence, diagnostics, and
  the exact point where invalid overlays fail.
- Priority: High

### 4. What is the intended boundary between `clinker-schema` and `clinker-plan`?

- Question: Should `clinker-schema` remain an advisory edge/authoring crate, or
  should schema discovery and validation move into the planner's compile-time
  boundary?
- Why it matters: `clinker-schema` currently depends on `clinker-plan`, returns
  warnings, and uses lighter validation than the planner. Agents need to know
  whether to strengthen this crate or keep it separate from canonical planning.
- Files/modules involved: `docs/ai/20_CRATE_MAP.md`,
  `docs/ai/90_CRATE_AGENT_PLAN.md`, `crates/clinker-schema/AGENTS.md`,
  `crates/clinker-schema/src/lib.rs`, `crates/clinker-schema/src/validate.rs`,
  `crates/clinker-schema/src/discovery.rs`, `crates/clinker-plan/src/schema/`,
  `crates/clinker-plan/src/config/pipeline.rs`.
- Suggested way to resolve it: Maintainers should choose the boundary and
  document it in crate docs and AI docs. If `clinker-schema` stays advisory,
  user docs should avoid compiler-grade claims. If it moves toward planning,
  add parser-backed validation and planner tests.
- Priority: High

### 5. How complete should `clinker-schema` discovery and validation be?

- Question: Should `extract_schema_refs`, include/exclude glob behavior, format
  matching, and CXL field validation become YAML/parser-backed, or is the
  current heuristic authoring support enough?
- Why it matters: Current guidance says `exclude_globs` is not applied,
  `schema:` extraction is line-oriented, CXL field extraction is heuristic, and
  some warning variants appear unused. Docs and tests should not imply full
  workspace validation unless that is implemented.
- Files/modules involved: `crates/clinker-schema/AGENTS.md`,
  `crates/clinker-schema/src/discovery.rs`,
  `crates/clinker-schema/src/validate.rs`,
  `crates/clinker-schema/src/model.rs`,
  `examples/pipelines/retract-demo/*.schema.yaml`.
- Suggested way to resolve it: Decide supported validation depth. Either
  document current limitations clearly and add tests that pin them, or replace
  heuristics with structured YAML/CXL parsing and expand warning coverage.
- Priority: High

### 6. Should user-facing docs be updated to the unified `nodes:` shape and all current node types?

- Question: Which older user/engine docs still describe retired
  `inputs:` / `outputs:` / `transformations:` shapes or "eight node types",
  and should they be modernized now?
- Why it matters: Current planning code accepts a unified `nodes:` list with
  eleven node variants. Stale docs can cause agents to revive retired config
  shapes or omit active nodes such as `reshape`, `cull`, and `envelope`.
- Files/modules involved: `docs/ai/70_GLOSSARY.md`,
  `docs/ai/80_OPEN_QUESTIONS.md`, `crates/clinker-plan/AGENTS.md`,
  `crates/clinker/AGENTS.md`, `docs/user/src/getting-started/concepts.md`,
  `docs/user/src/pipelines/structure.md`, `docs/user/src/pipelines/envelope-and-doc-context.md`,
  `crates/clinker-plan/src/config/pipeline_node.rs`.
- Suggested way to resolve it: Audit user and engine docs for retired shapes,
  update examples to unified `nodes:`, and add doc/example checks where
  feasible so future changes do not drift.
- Priority: High

### 7. Should source-node docs and transport docs mention REST as implemented, and SQL cursors as roadmap only?

- Question: Should `source` documentation say current transports are file and
  finite REST, while SQL cursor wording is future or unsupported?
- Why it matters: `clinker-net` implements REST only, but docs and manifests
  mention SQL cursors. Agents should not infer SQL support or document file as
  the only current transport.
- Files/modules involved: `crates/clinker-net/AGENTS.md`,
  `docs/ai/20_CRATE_MAP.md`, `docs/user/src/nodes/source.md`,
  `docs/user/src/formats/source-network.md`,
  `crates/clinker-net/Cargo.toml`, `crates/clinker-net/src/lib.rs`,
  `crates/clinker-net/src/rest.rs`,
  `crates/clinker-exec/tests/transport_validation.rs`.
- Suggested way to resolve it: Align manifest descriptions, user docs, and AI
  docs with implemented REST behavior. Mark SQL cursors explicitly as roadmap
  or remove the wording until implementation exists.
- Priority: High

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

### 9. How broad should composition resource support become beyond file resources?

- Question: Are composition resources intended to stay file-only for now, or is
  a broader typed resource model planned?
- Why it matters: The planning layer has resource declaration and payload types,
  but AI docs say evidence currently shows only file resources and validation is
  partly stubbed. Agents should not invent new resource kinds by inference.
- Files/modules involved: `docs/ai/40_COMMON_PATTERNS.md`,
  `crates/clinker-plan/AGENTS.md`,
  `crates/clinker-plan/src/config/composition/resource.rs`,
  `crates/clinker-plan/src/config/composition/raw.rs`,
  `crates/clinker-plan/src/config/composition/tests.rs`,
  `docs/user/src/pipelines/compositions.md`.
- Suggested way to resolve it: Document current supported resource kinds and add
  tests that reject unsupported kinds. If expanding support, design the resource
  model first and update channel/resource overlay semantics at the same time.
- Priority: Medium

### 10. Which planner and CXL public APIs are stable user-facing API versus internal exposed surface?

- Question: Should public symbols such as `cxl::resolve::HashMapResolver`,
  `cxl::typecheck::Row`, and legacy `clinker_plan::config::route::RouteConfig`
  / `RouteBranch` be documented as stable, test/support, or cleanup debt?
- Why it matters: AI docs list public APIs so future agents recognize them, but
  public visibility does not always imply a compatibility promise. Extending or
  documenting test doubles and legacy config structs as stable would freeze
  accidental surface area.
- Files/modules involved: `docs/ai/80_OPEN_QUESTIONS.md`,
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
  `docs/ai/80_OPEN_QUESTIONS.md`, `crates/clinker/src/main.rs`,
  `docs/user/src/ops/cli-reference.md`,
  `crates/clinker/tests/`.
- Suggested way to resolve it: Audit `RunArgs` fields from parsing through
  execution. For each flag, add behavior tests, document it, or mark/remove it
  if intentionally reserved.
- Priority: Medium

### 12. Should `cxl-cli` docs, manifest description, and CLI behavior be aligned?

- Question: Are user docs showing multiple `-e` flags correct, or should the
  docs be corrected to match `Command::Eval.expr: Option<String>`? Should the
  `cxl-cli` manifest description continue saying "REPL" when source currently
  exposes only `check`, `eval`, and `fmt` subcommands?
- Why it matters: Repeated expression examples and REPL wording can mislead
  users and future tests. If repeated `-e` or REPL behavior is intended, the CLI
  type and evaluation/output semantics need to change deliberately.
- Files/modules involved: `crates/cxl-cli/AGENTS.md`,
  `crates/cxl-cli/Cargo.toml`, `crates/cxl-cli/src/main.rs`,
  `docs/user/src/cxl/cxl-cli.md`, `docs/ai/20_CRATE_MAP.md`.
- Suggested way to resolve it: Decide the intended CLI behavior. Either update
  docs/manifest wording to one expression and no REPL, or change the Clap field
  and CLI implementation to support the promised behavior with tests.
- Priority: Medium

### 13. Should `cxl-cli --record` preserve nested JSON objects as `Value::Map`?

- Question: Is mapping nested JSON objects to `Value::Null` in `json_to_value`
  intentional v1 scope, or should objects become `Value::Map`?
- Why it matters: `clinker-record::Value` has a `Map` variant, and silently
  nulling objects affects evaluation results. Docs need to state the behavior
  if it remains intentional.
- Files/modules involved: `crates/cxl-cli/AGENTS.md`,
  `crates/cxl-cli/src/main.rs`, `crates/clinker-record/src/value.rs`,
  `docs/user/src/cxl/cxl-cli.md`.
- Suggested way to resolve it: Confirm the intended object mapping. Add unit
  tests for nested objects and update CXL CLI docs to match the chosen behavior.
- Priority: Medium

### 14. Should `PipelineCounters::ok_count` use a globally unique source-row identity?

- Question: Should successful-record deduplication use source plus row identity
  instead of `row_num` alone?
- Why it matters: Current crate guidance says row-number collisions across
  sources can undercount distinct inputs. Counter semantics are visible through
  metrics and `$pipeline` CXL counters.
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
- Files/modules involved: `docs/ai/80_OPEN_QUESTIONS.md`,
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

### 20. Which envelope and document-context docs are canonical?

- Question: Should older envelope/document examples using retired shapes be
  modernized or explicitly marked historical?
- Why it matters: Envelope and document context behavior is complex and used by
  source readers, stream events, output framing, and docs. Future agents should
  not copy stale snippets into tests or examples.
- Files/modules involved: `docs/user/src/pipelines/envelope-and-doc-context.md`,
  `docs/user/src/nodes/envelope.md`, `docs/ai/70_GLOSSARY.md`,
  `crates/clinker-format/AGENTS.md`, `crates/clinker-exec/AGENTS.md`,
  `crates/clinker-plan/src/config/pipeline_node.rs`,
  `crates/clinker-exec/src/executor/envelope_dispatch.rs`.
- Suggested way to resolve it: Pick canonical examples, rewrite stale snippets
  to unified `nodes:` syntax, and add example or docs tests if practical.
- Priority: Medium

### 21. Should `CROSS_RECORD_TRANSFORMS_PLAN.md` be marked historical or active?

- Question: Is `CROSS_RECORD_TRANSFORMS_PLAN.md` obsolete planning history, or
  does it still contain active design guidance?
- Why it matters: The file references old paths such as
  `crates/clinker-core/...`, while related concepts now exist as `reshape` and
  `cull` under current plan/exec crates. Agents may mistake it for current
  architecture.
- Files/modules involved: `CROSS_RECORD_TRANSFORMS_PLAN.md`,
  `docs/ai/80_OPEN_QUESTIONS.md`,
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

### 23. Is `reserve/` definitively only a crates.io name-reservation package?

- Question: Does `reserve/` have any active release workflow or maintenance
  role beyond reserving the `clinker` crate name?
- Why it matters: AI docs currently mark it as inferred from local files. Agents
  should not modify or document it as runtime code.
- Files/modules involved: `docs/ai/10_ARCHITECTURE.md`,
  `docs/ai/20_CRATE_MAP.md`, `docs/ai/70_GLOSSARY.md`,
  `docs/ai/90_CRATE_AGENT_PLAN.md`, `reserve/Cargo.toml`,
  `reserve/src/lib.rs`, `reserve/README.md`.
- Suggested way to resolve it: Ask maintainers to confirm the package role.
  Add a short status note in `reserve/README.md` or AI docs.
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
