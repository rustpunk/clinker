# AI Changelog

Verified against origin/main cf6609b9 (2026-07-24).

Purpose: Track architectural decisions, history, and evidence that future AI
agents should know before changing Clinker.

This file is not a substitute for source review. It records what was documented
or inferred at a point in time, links to evidence, and calls out uncertainty.
Do not treat an entry as a human decision unless it explicitly says so and links
to supporting evidence.

The initial 2026-06-15 entries were written from the working tree without git
history; later entries cite the merged PRs and commits they describe.

## Source Evidence

Primary documents for this initial entry:

- [docs/ai/00_READ_THIS_FIRST.md](00_READ_THIS_FIRST.md)
- [docs/ai/10_ARCHITECTURE.md](10_ARCHITECTURE.md)
- [docs/ai/20_CRATE_MAP.md](20_CRATE_MAP.md)
- [docs/ai/30_DESIGN_RULES.md](30_DESIGN_RULES.md)
- [docs/ai/50_TESTING_AND_COMMANDS.md](50_TESTING_AND_COMMANDS.md)
- [docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md)
- [AGENTS.md](../../AGENTS.md)

Representative code and manifest evidence cited by those docs includes:

- [Cargo.toml](../../Cargo.toml)
- [crates/clinker-plan/src/lib.rs](../../crates/clinker-plan/src/lib.rs)
- [crates/clinker-plan/src/config/pipeline.rs](../../crates/clinker-plan/src/config/pipeline.rs)
- [crates/clinker-plan/src/config/pipeline_node.rs](../../crates/clinker-plan/src/config/pipeline_node.rs)
- [crates/clinker-exec/src/executor/mod.rs](../../crates/clinker-exec/src/executor/mod.rs)
- [crates/clinker-exec/src/source/mod.rs](../../crates/clinker-exec/src/source/mod.rs)
- [crates/clinker-format/src/lib.rs](../../crates/clinker-format/src/lib.rs)
- [crates/clinker-record/src/lib.rs](../../crates/clinker-record/src/lib.rs)
- [crates/cxl/src/lib.rs](../../crates/cxl/src/lib.rs)

## Entries

### 2026-07-25: `record_path` Has One Grammar, Enforced at Both Layers

Type: Source-backed fact (issue #949).

Summary: A source's `record_path` is parsed through a single grammar rather than
split ad hoc by each reader. XML takes a slash-separated path of XML element
names rooted at the document element; JSON takes a dot-separated path of object
keys from the document root. XPath and JSONPath shapes (`//product`, `$.data`, a
leading `/`, empty segments) are rejected — an XPath-shaped value used to split
into a segment no element name can equal, so the run emitted zero records and
reported success. Rejection happens at plan time with E363 (over the call-site
pipeline's nodes and each composition body's nodes) and again at reader
construction, which covers callers that build a reader config directly.
Evidence: `crates/clinker-format/src/record_path.rs`;
`crates/clinker-plan/src/config/record_path.rs`; `docs/explain/E363.md`.

### 2026-07-24: Multi-Value Fields Replace `array_paths`

Type: Source-backed fact (PRs #925, #930-#932, #934, #935, #938, #939,
#942/#943/#946; 2026-07-22 through 2026-07-24).

Summary: Columns can now declare `multiple: true`, holding zero or more values
per record. CSV reads/writes them as delimited cells; XML writes repeated child
elements. Undeclared repeated input fails loud instead of dropping values, and
the old `array_paths:` config key is retired (rejected with E360; E359-E362
family covers declaration/format mismatches). `clinker config --resolved`
prints a pipeline config in canonical form with multi-value shorthand
expanded. Evidence: `crates/clinker-plan/src/config/multi_value.rs`;
`crates/clinker/src/main.rs` (`ConfigArgs`).

### 2026-07-20: Batch Merge — Format Fidelity, CXL Soundness, Port-Aware Node Buffers

Type: Source-backed fact (PRs #884, #885, #889-#907, #909/#910; merged
2026-07-20).

Summary: Twenty PRs landed in one batch: CXL now rejects type-mismatched
operands, unknown method calls, and oversized int literals (#884); node-buffer
keys are port-aware so multi-output fan-in keeps every port (#885); the
overloaded E200 diagnostic split into distinct parse/resolve/type codes
(#898); DateTime spill payloads widened to i128 nanoseconds (#890); EDI
round-trip fidelity fixes (EDIFACT UNH, chunked UNA, SWIFT block-id interning,
#896); sub-second datetime precision in default CSV output (#903); and a root
`README.md` (#891). CI gained an example-pipeline `--explain` smoke gate and a
manually startable gauntlet (#895, #910).

### 2026-07-18: Memory-Budget Milestone Ratified

Type: Human-ratified decision plus source-backed facts (PRs #182, #671/#358,
#868; merged 2026-07-18).

Summary: Startup memory-budget validation is hard-limit-only with a clearer
E312 (#671/#358 — the soft threshold is derived, not user-set); the CLI
`--memory-limit` flag overrides `pipeline.memory.limit` per invocation (#182);
and wide multi-run spill merges are bounded to a fixed k-way fan-in via a
cascaded multi-pass loser-tree merge (#868,
`crates/clinker-exec/src/pipeline/loser_tree.rs`).

### 2026-07-09 to 2026-07-20: Bounded-Memory Join Campaign

Type: Source-backed fact (PRs #845-#885 spanning 2026-07-09 through
2026-07-20).

Summary: Every combine strategy is now bounded on both the input and output
axes. Pure-range IEJoin became a block-band external join (#856, output axis
#846/#860, equi+range input axis #871; `pipeline/iejoin/block.rs`); sort-merge
join gained complete multi-run external sort (#849); unmatched-driver
residency is bounded (#852); `match: collect` gained an output backstop
(#848/#853) and `match: first` unified on post-match-projection semantics
(#855); range-join float keys use order-preserving encoding (#845) and
datetime range/sort/join keys reduce to one exact i128-nanosecond order
(#877/#880). The memory-arbitration resume controller landed 2026-07-09 with a
liveness guarantee and `resume_threshold` config (#822).

### 2026-07-04 to 2026-07-05: Correctness Batch Campaign

Type: Source-backed fact (batch PRs #796-#822 spanning 2026-07-04 through
2026-07-09).

Summary: Cross-cutting correctness batches: spill-disk-cap parity across
aggregate/sort/sort-merge/grace-hash writers (#817), format-writer hot-path
de-allocation across JSON/XML/CSV/fixed-width (#818), node-buffer accounting
(spill-request servicing, metered drains, admission gating, #819), plan
diagnostics and composition-body validation parity (#820), lossless
auto-widen and single-byte pad contract (#821), and the resume controller
(#822).

### 2026-07-02 to 2026-07-03: Overlay Epic, Schema Unification, Exact Decimal, Live Lineage

Type: Source-backed facts (merged 2026-07-02/03).

Summary: The channel/group folder overlay replaced the legacy channel-file
path in a full rip (#773) and gained a per-value `fixed:` lock (#777). Source
schemas unified onto one `Column` type — format layout and CXL type merged,
`decimal` type token retired in favor of `float` + `precision`/`scale`
(#774, #762), with the standalone exact fixed-point decimal for monetary
columns landing the same window (#780). `clinker-lineage` gained live
run-lifecycle OpenLineage events over the file transport (#779).

### 2026-06-25: clinker-lineage Crate Added

Type: Source-backed fact (crate skeleton #667; column lineage, composition
tracing, and `$doc`-envelope DIRECT lineage in follow-ups #659-#664, #730,
#734 through early July).

Summary: A fourteenth workspace crate, `clinker-lineage`, emits OpenLineage
column-level lineage (core spec 2-0-2, `ColumnLineageDatasetFacet` 1-2-0) from
compiled plans as NDJSON, consumed by the CLI `run --lineage` /
`run --lineage-events` flags. The engine core has no lineage dependency; run
lifecycle is orchestrated at the CLI edge.

### 2026-06-15: GitHub Issue Agent Workflow Defined

Type: AI workflow documentation update.

Summary: Split the GitHub issue workflow into a short routing entry point plus
focused workflow slices for milestone planning, readiness reviews, decision
gates, implementation, pull request review/merge, operations, WIP limits, and
failure controls.

What this entry can say factually:

- Agent implementation is now documented as gated on scoped Agent Task issues
  marked `agent-ready`.
- Vague, stale, broad, or under-specified work routes through Readiness Review
  before implementation.
- Product, architecture, dependency, public API, schema, auth, security, memory,
  or compatibility ambiguity routes through Decision Gate before
  implementation.
- Agent-authored PRs are documented as evidence for one Agent Task.
- Agents must not merge PRs by default; in this public-contribution repository,
  maintainers review and merge unless they explicitly instruct otherwise.
- Sequence dependencies are tracked primarily with native GitHub `blocked by` /
  `blocking` relationships, plus project status and project ordering. Body
  blocks/comments are fallback mirrors for external blockers or tool permission
  gaps. Blocked issues are excluded from `Agent Ready`.
- GitHub Projects are documented as flow boards only; stale projects should be
  archived or rebuilt, and the active Project should expose focused queue,
  grounding, decision, review, milestone, and decision-register views.
- Workflow-relevant issues and PRs are expected to be added to the active
  Project and default to `Status = Intake` until routed.
- Closed issues tied to active umbrellas or dependency chains should remain in
  the Project as `Status = Done`; only confirmed unrelated closed items should
  be archived.
- Rejected, superseded, or intentionally abandoned items that remain useful
  board context should use `Status = Won't Do`.
- GitHub Projects saved views are documented as a manual web UI setup step
  because current `gh project` commands do not manage saved views and the
  available GraphQL schema exposes Project v2 views for reading, not creating
  or updating.
- Implementation agents now have a post-implementation follow-up check to
  compare acceptance criteria against the diff, scan for shortcut/scope-creep
  signatures, and capture deferred findings as comments or follow-up issues.
- Implementation agents are pointed at the implementation and review slices
  instead of the full planning workflow.

Changed files:

- [AGENTS.md](../../AGENTS.md)
- [docs/ai/GITHUB_ISSUE_AGENT_WORKFLOW.md](GITHUB_ISSUE_AGENT_WORKFLOW.md)
- [docs/ai/github-workflow/PLANNING.md](github-workflow/PLANNING.md)
- [docs/ai/github-workflow/GROUNDING.md](github-workflow/GROUNDING.md)
- [docs/ai/github-workflow/DECISIONS.md](github-workflow/DECISIONS.md)
- [docs/ai/github-workflow/IMPLEMENTATION.md](github-workflow/IMPLEMENTATION.md)
- [docs/ai/github-workflow/REVIEW.md](github-workflow/REVIEW.md)
- [docs/ai/github-workflow/OPERATIONS.md](github-workflow/OPERATIONS.md)
- [docs/ai/AI_CHANGELOG.md](AI_CHANGELOG.md)

### 2026-06-15: Initial AI Documentation Set Recorded

Type: AI documentation creation.

Summary: Added this lightweight changelog as the initial architectural
decision/history file for future AI agents.

What this entry can say factually:

- The AI onboarding docs currently describe Clinker as a bounded-memory,
  single-process, finite-batch DAG executor for ETL-style jobs.
- The docs separate verified facts, hypotheses, stale references, and open
  questions.

What this entry does not claim:

- It does not claim to know the original author's intent.
- It does not claim any historical decision that is not documented in the
  current files.
- It does not claim that the AI docs are authoritative over source code.

Changed file:

- [docs/ai/AI_CHANGELOG.md](AI_CHANGELOG.md)

## Major Architecture Facts Discovered

The following are documented facts or source-backed inferences from the current
working tree. Future agents should re-check them before making architecture
changes.

- Clinker is organized as a Rust workspace with planning, execution, record,
  core-types, CXL, format, channel, network, schema, lineage, CLI (`clinker`
  and the standalone `cxl-cli`), and benchmark/support crates.
- Pipelines use YAML and the current documented shape is a unified top-level
  `nodes:` list.
- `clinker-plan` owns config loading, validation, schema binding, CXL
  compilation/typechecking, DAG construction, and `CompiledPlan` creation.
- `clinker-exec` owns runtime execution, source ingestion, operator dispatch,
  bounded channels, memory arbitration, spill handling, metrics, DLQ, and
  shutdown.
- Executor APIs are documented as taking validated/compiled plans rather than
  raw YAML config.
- The runtime is documented as finite and synchronous: OS threads,
  `crossbeam_channel`, and Rayon are used; an async runtime is not currently
  part of core pipeline execution.
- Bounded-memory behavior is architectural, not incidental. Memory
  arbitration, backpressure, spill behavior, and node buffers are load-bearing.
- `clinker-record` and `clinker-core-types` are lower-level vocabulary crates;
  `cxl` sits above records; `clinker-plan` sits below execution; CLI,
  channel, network, and schema crates are edge/integration surfaces.
- `clinker-format` currently depends on `cxl`, which is a documented layering
  edge to review rather than something this file should explain away.
- `clinker-net` currently adapts finite REST sources into executor
  `RecordSource` inputs. This means network transport is integrated at the
  executor-source boundary, not isolated as a pure low-level IO crate.

## Open Question Routing

Current unresolved questions are tracked in `docs/ai/80_OPEN_QUESTIONS.md`. Keep this changelog focused on dated evidence, resolved uncertainty, and factual documentation maintenance history.
## Instructions For Future Agents

- Update this file when making architectural changes, changing subsystem
  boundaries, adding/removing public config, changing executor/planner
  contracts, adding transports, changing memory/spill behavior, or resolving an
  open question.
- Record rejected approaches when they explain why a boundary or behavior was
  chosen. Keep these short and factual.
- Link to changed files, tests, examples, docs, and relevant issue/decision
  notes when available.
- Distinguish human decisions from AI inferences. Use labels such as
  "Human decision", "Source-backed fact", "AI inference", and "Open question".
- Do not invent past decisions. If the evidence is missing, say that it is
  missing and add or link an open question.
- Do not claim the original author intended something unless that intent is
  documented.
- If source code and this file disagree, source code and current tests win.
  Update this file after validating the new state.
- Keep entries lightweight. Prefer precise links and short explanations over
  broad architecture essays.
