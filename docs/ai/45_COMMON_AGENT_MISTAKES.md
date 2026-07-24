# AI Onboarding: Common Agent Mistakes

Verified against origin/main cf6609b9 (2026-07-24).

Purpose: Centralize recurring mistakes that AI coding agents make when changing
Clinker. Use this as a review aid alongside
[docs/ai/40_COMMON_PATTERNS.md](40_COMMON_PATTERNS.md) and before editing
arm-heavy surfaces such as DAG nodes, formats, transports, CLI commands,
diagnostics, or test fixtures.

This file complements the common-pattern guide: that guide says what to reuse;
this guide says where agents commonly drift. Source code, manifests, tests, and
examples still win when they disagree with this file.

## Source Evidence

Validate this page against:

- [AGENTS.md](../../AGENTS.md)
- [docs/ai/00_READ_THIS_FIRST.md](00_READ_THIS_FIRST.md)
- [docs/ai/10_ARCHITECTURE.md](10_ARCHITECTURE.md)
- [docs/ai/20_CRATE_MAP.md](20_CRATE_MAP.md)
- [docs/ai/30_DESIGN_RULES.md](30_DESIGN_RULES.md)
- [docs/ai/40_COMMON_PATTERNS.md](40_COMMON_PATTERNS.md)
- [docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md)
- `crates/clinker-plan/src/config/pipeline_node.rs`
- `crates/clinker-plan/src/plan/execution/mod.rs`
- `crates/clinker-exec/src/executor/dispatch.rs`
- `crates/clinker-exec/src/executor/*_dispatch.rs`
- `crates/clinker-plan/src/config/format.rs`
- `crates/clinker-plan/src/config/source.rs`
- `crates/clinker-plan/src/config/composition/resource.rs`
- `crates/clinker-plan/src/plan/explain_provenance.rs`
- `crates/clinker-core-types/src/diagnostic.rs`
- `crates/clinker-core-types/src/dlq.rs`
- `crates/clinker-format/src/lib.rs`
- `crates/clinker-format/src/*/reader.rs`
- `crates/clinker-format/src/*/writer.rs`
- `crates/clinker-exec/src/source/mod.rs`
- `crates/clinker-exec/src/executor/stream_event.rs`
- `crates/clinker-channel/src/resolve.rs`
- `crates/clinker-channel/src/overlay.rs`
- `crates/clinker-schema/src/model.rs`
- `crates/clinker-schema/src/validate.rs`
- `crates/clinker-bench-support/src/cache.rs`
- `crates/clinker-bench-support/src/lib.rs`
- `crates/clinker/src/main.rs`
- `docs/explain/*.md`
- Relevant integration tests under `crates/clinker-exec/tests/`,
  `crates/clinker-format/tests/`, `crates/clinker-plan/src/plan/tests/`,
  and crate-local `tests` modules.

## High-Risk Drift Surfaces

These surfaces have parallel arms, layered representations, or repeated
documentation/test families. They need sibling comparison before a change is
done.

| Surface | Compare before editing | Common drift |
|---|---|---|
| DAG node taxonomy | `PipelineNode`, `PlanNode`, plan lowering, schema binding, compile artifacts, `dispatch_plan_node`, `*_dispatch.rs`, node docs, examples, tests | One node gets different CXL artifact, schema, DLQ, metrics, or memory handling than peer nodes |
| Format families | `InputFormat`, `OutputFormat`, format readers/writers, structured-output guard, format docs, format tests, schema validation, benchmark format data | A reader, writer, docs page, schema matcher, or benchmark generator supports a different format set |
| Source matching and transports | `SourceConfig` matchers, `SourceTransport`, discovery, staging, `SourceInput::Files` / `Records`, `RecordSource`, `clinker-net` REST builders | File and REST paths diverge on schema, document context, cancellation, fan-out identity, or staging behavior |
| Channel targets and overlays | `OverlayKind::Pipeline` / `Composition`, folder-overlay derivation layers, config default/fixed, resource default/fixed, vars scopes, validation, overlay, channel docs/tests | Pipeline and composition targets validate different things without documenting whether the asymmetry is intentional |
| Composition config/resources | `ParamType`, `ResourceKind`, `Resource`, composition signatures, channel resource fields, open questions | A new resource kind is parsed or documented without real validation, overlay, runtime payload, and tests |
| Diagnostics and explain lookup | diagnostic registry, emission sites, `docs/explain/<code>.md`, `explain_code`, CLI help, snapshots/tests | A code is emitted without a page, a page exists without lookup, or help text lists stale code groups |
| DLQ/error handling | `ErrorStrategy`, `DlqGranularity`, `DlqErrorCategory`, per-record/document/correlation paths, rollback cursors, rate thresholds | One operator routes a recoverable error differently from sibling operators or bypasses DLQ accounting |
| Stream/document boundaries | `StreamEvent`, `Punctuation`, structural rejects, document DLQ, envelope nodes, merge/combine/aggregate/output punctuation discipline | An operator drops or side-channels document boundaries instead of preserving/reconciling inline events |
| Explain output formats | `ExplainFormat::Text` / `Json` / `Dot`, storage summaries, provenance explain, CLI preambles | JSON/DOT output gets human text or misses data added to text explain |
| CLI commands and flags | `Commands`, `RunArgs`, `MetricsCommands`, user CLI docs, CLI tests, actual run path | A flag is parsed and documented but weakly wired, or one command gets different exit/diagnostic behavior |
| Bench data and reports | `DataFormat`, `FieldKind`, `Scale`, cache hashes, generator modules, benchmark format mapping, report JSON, benchmark YAML | A new generated format or field kind misses cache key, extension, generator, report, or CI smoke coverage |
| Schema advisory validation | `SourceFormat`, `FormatCategory`, `InputFormat` matcher, `.schema.yaml` docs/examples, open questions | Schema docs imply compiler-grade support while validation remains advisory or format mapping is partial |
| Storage, staging, and spill | storage config, staging copy, source staging policy, spill cap/free-space validation, E330-E337 pages | Staging and spill errors collapse into generic I/O or one path gets cap/free-space checks the other lacks |
| Value and type conversions | `Value`, coercion, format readers/writers, CXL evaluator, CXL CLI JSON conversion, serde/spill payloads | A new value shape works in one converter but serializes, spills, compares, or evaluates differently elsewhere |
| Public docs/examples/fixtures | `docs/user/src/nodes`, `docs/user/src/formats`, `examples/pipelines`, `benches/pipelines`, snapshots | Code behavior changes but only one doc/example family is updated, leaving agents to copy stale shapes |

## 1. Letting Sibling Arms Drift

Mistake: treating one enum, dispatch, format, or command arm as an isolated
implementation. A change lands in `transform`, `route`, or `aggregate` with a
different standard for CXL artifacts, schema handling, diagnostics, DLQ,
metrics, memory behavior, or tests. The same risk appears in input/output
formats, transports, channel targets, CLI commands, and benchmark/data-format
arms.

Avoid it:

- Before editing one arm, list its siblings and compare the full path from
  config to runtime and tests.
- If the behavior is generic, put the rule at the shared owner instead of
  copying similar logic into one arm.
- If the behavior is genuinely arm-specific, document the reason in code,
  tests, or the relevant docs.
- Prefer the existing naming, payload, error, and test style used by sibling
  arms unless there is a clear reason to diverge.

Evidence to check:

- DAG nodes: `PipelineNode`, `PlanNode`, compile artifacts, plan lowering,
  node-property passes, `dispatch_plan_node`, and `*_dispatch.rs`.
- Formats: `InputFormat`, `OutputFormat`, reader/writer traits, envelope
  hooks, schema inference, document context, and format integration tests.

## 2. Updating Only One Layer Of A Node

Mistake: adding or changing a node behavior in YAML config but missing one of
the downstream layers: validation, schema binding, CXL typing, plan lowering,
runtime dispatch, streaming/memory classification, explain/provenance output,
diagnostics, examples, or tests.

Avoid it:

- Treat node changes as a path, not a file edit. Review config, plan, exec,
  docs, and tests explicitly.
- Not every node uses every subsystem, but every skipped subsystem should be an
  intentional non-participation, not an omission.
- Add the smallest test that would fail if the new behavior exists only in the
  parser or only in the executor.

Good review question: "If this arm were `route`, `aggregate`, `combine`, or a
format reader instead, what extra file or test would I have checked?"

## 3. Creating A New Abstraction For One Local Problem

Mistake: introducing a trait, registry, macro, global map, wrapper type, or
public helper because the current arm is inconvenient, without proving the
pattern repeats across a real subsystem boundary.

Avoid it:

- Start from the existing local pattern: typed enums, `CompiledPlan`,
  `ExecutionPlanDag`, run-scoped registries, `FormatReader` / `FormatWriter`,
  `RecordSource`, `ValidatedPath`, `Spanned<T>`, and subsystem error enums.
- Add an abstraction only when it removes real duplication across sibling arms
  or encodes a boundary the code already has.
- Keep executor internals private or `pub(crate)` unless there is a stable
  caller-facing API reason.

## 4. Crossing The Plan/Runtime Boundary

Mistake: putting runtime operator behavior in `clinker-plan`, moving raw YAML
config into `clinker-exec`, or letting executor APIs accept unvalidated config
when a compiled or validated artifact is the established boundary.

Avoid it:

- Keep YAML parsing, validation, schema binding, CXL compilation, and DAG
  construction in `clinker-plan`.
- Keep source ingest, operator execution, memory/spill behavior, output
  writing, DLQ, metrics, and shutdown in `clinker-exec`.
- Use `PipelineError::Internal` for runtime detection of a plan-time invariant
  violation instead of panicking or silently falling back.

## 5. Bypassing Span-Aware Parsing And Strict Config

Mistake: parsing production YAML through `serde_json::Value`, direct
`serde_saphyr::from_str`, or ad hoc string handling; adding `serde(default)` or
dropping `deny_unknown_fields` to make a fixture parse; losing `Spanned<T>`
diagnostics while simplifying dispatch.

Avoid it:

- Route production YAML through `clinker_plan::yaml`.
- Preserve `Spanned<T>` where user diagnostics need source locations.
- Keep `deny_unknown_fields` on user-facing config structs where it already
  exists.
- Add parse/diagnostic tests when changing config shape.

## 6. Treating Old Docs Or Current Chat As Source Truth

Mistake: copying a claim from older docs, a prior session, or an issue summary
without checking current source, manifests, examples, or tests. This is how
retired config shapes, stale crate names, or unsupported features get revived.

Avoid it:

- Use docs as routing and context, then verify against source evidence.
- If source and docs disagree, update docs or record the mismatch in
  [docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md).
- Do not strengthen a weak claim. Downgrade it, remove it, or make the
  uncertainty explicit.

## 7. Special-Casing Transports Or Formats In The Wrong Layer

Mistake: adding a file, REST, or format-specific branch deep in generic
dispatch code when the shared contracts already exist.

Avoid it:

- File formats should normally implement `FormatReader` and/or `FormatWriter`.
- Non-file finite sources should normally implement `RecordSource` and enter
  execution as `SourceInput::Records`.
- Shared source ingest, document context, envelope events, counting, and
  schema coercion should stay shared unless the common contract is genuinely
  insufficient.

## 8. Forgetting Finite-Batch And Bounded-Memory Assumptions

Mistake: adding unbounded polling, daemon loops, async-runtime assumptions,
whole-file buffering, independent memory budgets, or operator-local spill
policy that bypasses the run-scoped memory model.

Avoid it:

- Keep sources finite by contract.
- Preserve bounded channels, node buffers, `MemoryArbitrator`, backpressure,
  spill, and cleanup semantics.
- Profile or add targeted tests before changing hot paths, blocking operators,
  streaming eligibility, or memory admission behavior.

## 9. Widening Public API, Dependencies, Or Test Helpers For Convenience

Mistake: making crate-private code public for an integration test, adding a
dependency to avoid a small local implementation, enabling benchmark helpers in
default runtime code, or creating a cargo-deny exception before checking the
existing dependency policy.

Avoid it:

- Prefer module tests for private behavior and public integration tests for
  user-visible behavior.
- Ask before adding dependencies, native toolchain requirements, async
  runtimes, C build steps, OpenSSL/native-tls, or cargo-deny exceptions.
- Keep benchmark and test support out of default runtime paths.

## 10. Testing Only The Happy Path Or Only The Edited Arm

Mistake: proving the local case works while leaving sibling-arm parity,
negative diagnostics, snapshot output, examples, or fixture compatibility
untested.

Avoid it:

- Pick the smallest test at the boundary changed: config/plan, runtime,
  format, language, channel, CLI, or docs.
- Add cross-arm tests when a shared rule should apply to multiple node or
  format arms.
- Do not update snapshots or fixtures as mechanical churn; explain the
  behavior change they pin.
- For docs-only AI onboarding edits, run `git diff --check`.

## 11. Adding A Diagnostic Without Its Whole Surface

Mistake: emitting a new diagnostic code, DLQ category, or error variant in one
module and stopping there. In Clinker, diagnostic behavior is often spread
across the code registry, emission sites, CLI `explain --code`, `docs/explain`,
snapshots, exit-code behavior, and user docs.

Avoid it:

- Add or update the diagnostic registry entry when adding a new code.
- Add a `docs/explain/<code>.md` page and wire `explain_code` when the code
  is public lookup surface, or document why it intentionally is not lookup
  backed.
- Keep CLI help text and user docs in sync with the current code groups.
- Add tests or snapshots that prove the code, payload, and lookup behavior.

## 12. Updating A Cross-Cutting Policy In Only One Path

Mistake: changing a policy such as DLQ behavior, document-boundary handling,
storage/staging validation, channel overlay precedence, or structured-output
cardinality in one path while sibling paths keep the old behavior.

Avoid it:

- Find the shared funnel first: examples include `push_dlq`,
  `DlqErrorCategory`, `StreamEvent`, `OutputFormat::is_single_document`,
  storage validation helpers, and channel overlay/validation entry points.
- Prefer changing the funnel plus focused exceptions over editing every arm.
- If a sibling path stays different, add a comment or test naming the reason.

## 13. Confusing Parallel But Non-Equivalent Families

Mistake: assuming two parallel families are interchangeable because their names
look similar. Examples: `InputFormat` versus `SourceFormat`, file format versus
source transport, schema validation versus planner validation, composition
resources versus channel resources, record-level DLQ versus document-level DLQ,
and text explain versus JSON/DOT explain.

Avoid it:

- Identify the owning layer and authority for each family before copying code
  or docs.
- Treat advisory crates and user-facing docs as secondary until checked
  against planner/executor behavior.
- Preserve intentional asymmetry where an open question says the boundary is
  unresolved.

## 14. Treating Open Questions As Permission

Mistake: using an unresolved question as justification to implement one answer
locally, document it as fact, or hide the uncertainty in a crate-local guide.

Avoid it:

- Check [docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md) before touching
  areas called out there.
- If the work requires a product, architecture, dependency, public API, schema,
  auth, security, memory, or compatibility decision, stop and get that decision
  rather than encoding a guess.
- Keep unresolved uncertainty in the central open-questions file.

## 15. Publishing Process Artifacts Instead Of Engineering Results

Mistake: putting local machine paths, private notes, agent-tool names, AI
attribution, or implementation-process narration into public issues, PRs,
comments, commits, docs, or examples.

Avoid it:

- Use repo-relative paths.
- Describe changed behavior, files, validation, and remaining risk.
- Keep local-only artifacts and personal workflow details out of checked-in
  docs and GitHub text.

## Arm-Heavy Change Checklist

Before finishing a change to any arm-heavy surface, answer these questions:

1. Which sibling arms did I compare?
2. What is the single source of truth for this behavior?
3. Which layers changed: config, validation, binding, plan, runtime, docs,
   examples, tests, diagnostics, metrics, memory, or explain output?
4. Which layers did I intentionally leave unchanged, and why?
5. What existing pattern did I reuse?
6. Which targeted command or test proves the boundary I touched?
7. Did I check open questions for this subsystem?
