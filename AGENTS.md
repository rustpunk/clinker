# AGENTS.md

## Project Summary

Clinker is a bounded-memory, single-process batch DAG executor for finite ETL-style jobs. Pipelines are YAML, expressions are CXL, planning lives separately from runtime execution, and the main CLI is the `clinker` crate.

Do not invent architecture; update docs when changing behavior.

## Read First

Start here, then verify claims against source, manifests, tests, and examples:

- [docs/ai/00_READ_THIS_FIRST.md](docs/ai/00_READ_THIS_FIRST.md)
- [docs/ai/10_ARCHITECTURE.md](docs/ai/10_ARCHITECTURE.md)
- [docs/ai/20_CRATE_MAP.md](docs/ai/20_CRATE_MAP.md)
- [docs/ai/30_DESIGN_RULES.md](docs/ai/30_DESIGN_RULES.md)
- [docs/ai/50_TESTING_AND_COMMANDS.md](docs/ai/50_TESTING_AND_COMMANDS.md)
- [docs/ai/80_OPEN_QUESTIONS.md](docs/ai/80_OPEN_QUESTIONS.md)

Treat older docs as secondary context when they conflict with current code.

## Workspace Layout

- `crates/clinker-record`: records, values, schemas, provenance, document context.
- `crates/cxl`: CXL parser, resolver, typechecker, analyzer, evaluator.
- `crates/clinker-format`: streaming readers/writers, formats, envelopes.
- `crates/clinker-plan`: YAML config, validation, schema binding, CXL compile, DAGs.
- `crates/clinker-exec`: runtime executor, operators, memory, spill, metrics, DLQ.
- `crates/clinker`: main CLI.
- `crates/clinker-channel`, `clinker-net`, `clinker-schema`: integration crates.
- `crates/clinker-bench-support`, `clinker-benchmarks`: test/benchmark support.
- `crates/clinker-scenarios`: deterministic generator for the `examples/scenarios` corpus.
- `docs/user`, `docs/engine`, `docs/ai`: user docs, internals docs, AI onboarding.
- `examples/pipelines`, `benches/pipelines`: runnable and benchmark pipeline YAML.
- `examples/scenarios`: end-to-end scenarios executed against committed goldens.

## Architecture Rules

- Keep layering intact: record/core vocabulary below CXL/format/plan; `clinker-exec` consumes compiled plans; CLI/integration crates stay at the edge.
- Executor APIs should take validated/compiled plans, not raw YAML config.
- Clinker is finite-batch and synchronous; do not add unbounded stream, daemon, distributed, or async-runtime assumptions casually.
- Preserve bounded-memory behavior: memory arbitration, backpressure, spill, and node-buffer semantics are load-bearing.
- Reuse typed boundaries: `CompiledPlan`, `ValidatedPath`, `Spanned<T>`, `RecordSource`, `FormatReader`, `FormatWriter`.

## Commands

Before claiming success, run the smallest relevant gate from
[docs/ai/50_TESTING_AND_COMMANDS.md](docs/ai/50_TESTING_AND_COMMANDS.md).
Use targeted `cargo test -p <crate>` while iterating, then broaden validation
when the change crosses crate boundaries.

For docs-only AI onboarding edits, the current smallest gate is
`git diff --check`. See the command guide for mdBook, rustdoc, cargo-deny,
bench, socket, and file-descriptor caveats.

## Safety Rules

- During documentation-only tasks, do not modify Rust source.
- Do not revert or overwrite user changes. Check `git status --short` before editing.
- Keep edits scoped. Avoid unrelated refactors, destructive commands, and metadata churn.
- Never push to main directly; push only feature branches created for an explicitly requested task.
- If a term, architecture rule, or behavior is unclear, record it in [docs/ai/80_OPEN_QUESTIONS.md](docs/ai/80_OPEN_QUESTIONS.md) instead of guessing.

## Correctness Posture

The project is greenfield and has no deployed users, so the cost of getting a design right is a rewrite and the cost of getting it wrong is permanent. That asymmetry decides the calls below.

- Prefer the breaking change. A compatibility shim preserves a shape nobody depends on at the price of carrying it forever.
- Complexity and effort are not reasons to defer a correct refactor. "Large" is an estimate, not an objection.
- When the choice is between a correct hard option and an expedient easy one, take the correct one. This holds at implementation time as strongly as at planning time: a correctness deferral invented while coding is the same defect as one written into the plan, and it arrives without the review a plan gets.
- Every surface named in an agreed design is a requirement of that landing, not a candidate for a follow-up. Dropping one needs the maintainer's explicit agreement, not a note in the PR.
- A component must not report a state it has not established. Prefer arrangements where the wrong report is unrepresentable — one shared function, one derived count — over two places that agree today and are documented to stay in step.

## Domain Facts

Recurring ground truths. Each has been got wrong more than once; treat a design that contradicts one as wrong until the contradiction is explained.

- **CXL is not SQL.** It is an expression language for record-at-a-time ETL. Boolean operators are `and` / `or` / `not`. When researching prior art, weight non-SQL ETL tools (Vector, Benthos, Embulk, NiFi, Logstash, Jolt, Singer) at least as heavily as query engines — a design imported from a SQL planner usually assumes a set-at-a-time model this engine does not have.
- **Clinker is row-based, not columnar.** Record-at-a-time, bounded memory. Columnar-engine techniques rarely transfer, and the ones that do need their assumptions restated first.
- **System namespaces carry a `$` sigil** — `$pipeline.*`, `$window.*`, `$meta.*`, `$doc.*`, `$source.*`.
- **Engine-stamped columns never reach author vocabulary.** Frozen-identity shadow columns (`$ck.*`) and other `$`-namespaced schema columns are stripped from writer output by default and surface only on an explicit Output-node opt-in. The engine routes identity through joins and aggregates without the author ever naming a shadow column.
- **`$doc.*` section names are author-defined** in pipeline YAML. `head`, `foot`, `header`, `footer` are examples, never built-ins — do not hardcode them in plans, designs, examples, or docs.
- **Never infer workload frequency from fixture data.** Example and test corpora are written to exercise a path, not to describe production. Optimize for every shape the surface admits rather than the shape a fixture happens to have.

## Verification

A gate that was not run, or whose result was read from the wrong place, is a gate that did not happen.

- `--explain` proves a pipeline compiles. It never proves the output is right. Execute examples against real data and compare bytes before claiming a behavior works.
- Run the full check-job gauntlet before pushing — `cargo fmt --all --check` included. A targeted `-p` clippy pass is not the gate; formatting and cross-crate lints fail independently of it.
- Before declaring a sprint done, run `cargo test --benches -p clinker-benchmarks`, not just `cargo check --benches`. The bench targets run end-to-end pipeline pre-flights that a compile check cannot fail on.
- Read a gate's status from the gate. A command piped into another reports the last command's exit status, so a failing suite can return success; redirect output to a file and read the file.
- Merge only when the review is resolved, rebases have settled, and CI is green on every platform including Windows and macOS.

## Repository Hygiene

- The git identity for this repository is `rustpunk`, which is not the machine default. Check it before the first commit of a session.
- Stage by explicit path. `git add -A` sweeps scratch and probe files that tooling leaves in the tree; review `git status` and `git show --stat` before pushing.
- Never use `git add -f`. Ignored paths — `docs/internal/`, `notes/`, local settings — are ignored deliberately.
- Local progress and tracking files stay out of git.
- Do not name specific prior-art tools or vendors in issues, PRs, or comments. Make the argument on its merits; the comparison belongs in internal notes.

## User-Facing Surface

Anything a pipeline author writes by hand — a YAML key, a CXL construct, a CLI flag, an option value — is a user interface. Changing it is a design decision, not an implementation detail.

- Ground surface decisions in patterns config authors have already met and that demonstrably work in tools of this shape, rather than in a spelling that only makes sense from inside the engine. Where an established convention exists, follow it; where this project departs from one, say so in the PR and give the reason.
- One concept, one spelling. A second syntax for something the surface already expresses is a defect, not a convenience.
- The common case is the short case; the general form stays reachable without rewriting the simple one.
- Engine vocabulary stays out of author vocabulary — internal identifiers and namespaced machinery are not things a user should have to type.
- Errors are part of the surface: a diagnostic names the offending input, the rule it broke, and a corrected form the author can paste.
- User documentation ships in the same PR as the surface change.

## Rust Conventions

- Prefer existing local patterns over new abstractions.
- Use structured parsers/APIs rather than ad hoc string handling.
- Preserve span-aware YAML parsing through `clinker_plan::yaml` and `Spanned<T>`.
- Keep user-facing config strict where `deny_unknown_fields` is established.
- Use subsystem error enums and `PipelineError::Internal` for invariant violations; avoid panic-based runtime behavior.
- Add focused tests at the boundary touched: plan/config in `clinker-plan`, runtime in `clinker-exec`, format in `clinker-format`, language in `cxl`, channel in `clinker-channel`, CLI in `clinker`.
- Doc comments on public items state what the signature cannot: whether the item streams or blocks, what it holds live, and which invariant its caller must already have established.

## Dependencies And Approval

Approval is gated on the capability, not on the manifest diff. It covers development, test, benchmark, CI, and release tooling as well as the shipped runtime: being "only tooling" changes which manifest an edge belongs in and how it is verified, never whether approval is needed.

- Do not add dependencies, native toolchain requirements, async runtimes, C build steps, OpenSSL/native-tls, or cargo-deny exceptions without approval.
- Keep benchmark/test helpers out of default runtime paths.
- If a command needs network, writes outside the workspace, or needs elevated permissions, ask for approval through the tool flow and explain why.
- When a task needs a capability an established crate already provides, raise it for approval. Review covers architectural fit and the crate's maintenance status, so asking is the cheap path; work done to avoid the conversation is not a saving.
- Do not substitute any of these to keep the manifest unchanged: hand-rolling the capability (parsers, lexers, tokenizers, serializers, encoders, date/time arithmetic, cryptography); implementing it outside Rust; vendoring or copying third-party source into the tree; or shelling out to an undeclared external binary.
- Hand-rolling what a vetted crate provides is a dependency decision taken without review. Raise it rather than growing it, and treat an implementation that needs repeated adversarial repair as evidence the decision was wrong rather than as a reason to keep patching.
- Adding a non-Rust language to the build, test, or release path is an approval-gated architectural decision in its own right. Committed tooling is Rust; the only committed non-Rust sources are the vendored mdBook theme assets under `docs/theme/`.
- Not asking is not approval. The absence of a request is not evidence that a dependency was considered and rejected.
- Check that a crate is still maintained before proposing it. Assistants reliably reach for names that were popular years ago and are now archived or superseded; recency, release cadence, and advisories are part of the proposal, not a detail to confirm afterwards.
- If approval is refused or deferred, reduce scope or record the gap in [docs/ai/80_OPEN_QUESTIONS.md](docs/ai/80_OPEN_QUESTIONS.md); do not ship a hand-rolled substitute instead.

## Documentation Rules

- Update user/engine/AI docs when changing behavior, config, diagnostics, commands, or architecture.
- Prefer links to detailed docs over long explanations here.
- Mark stale or contradictory docs; do not silently copy stale claims.
- For public-facing behavior changes, update examples or explain-code docs when relevant.

## GitHub Issue Workflow

For creating, triaging, splitting, or closing GitHub issues, follow [docs/ai/GITHUB_ISSUE_AGENT_WORKFLOW.md](docs/ai/GITHUB_ISSUE_AGENT_WORKFLOW.md).

Use that workflow only when the task involves GitHub issues, milestones, labels, sub-issues, or autonomous issue closure.

Agent workflow policy:

- Milestones are planning containers, not implementation issues.
- Agents may implement only scoped Agent Task issues marked `agent-ready`.
- Route vague, stale, broad, or under-specified work through a Readiness Review.
- Route unresolved product, architecture, dependency, public API, schema, auth, security, memory, or compatibility choices through a Decision Gate.
- A PR closes a coherent group of Agent Tasks — grouped by shared subsystem, dependency chain, or review context; a single-task PR is the degenerate case when no coherent grouping exists. Never split one Agent Task across multiple PRs.
- Agents must not merge PRs by default; leave PRs for maintainer review and merge unless a maintainer explicitly instructs otherwise. A maintainer-approved delivery plan that specifies merge-on-green for a set of PRs counts as explicit instruction for those PRs.

## Definition Of Done

- Code/docs match the requested scope.
- Relevant tests/checks were run, or skipped with a clear reason.
- `git diff --check` passes.
- Behavior changes have matching docs and tests.
- Open questions are captured in `docs/ai/80_OPEN_QUESTIONS.md`.
- Final response summarizes changed files, validation, and any remaining risks.
