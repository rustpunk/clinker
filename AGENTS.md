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
- `docs/user`, `docs/engine`, `docs/ai`: user docs, internals docs, AI onboarding.
- `examples/pipelines`, `benches/pipelines`: runnable and benchmark pipeline YAML.

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

- Do not modify Rust source unless the user explicitly asks.
- Do not revert or overwrite user changes. Check `git status --short` before editing.
- Keep edits scoped. Avoid unrelated refactors, destructive commands, and metadata churn.
- Never push.
- If a term, architecture rule, or behavior is unclear, record it in [docs/ai/80_OPEN_QUESTIONS.md](docs/ai/80_OPEN_QUESTIONS.md) instead of guessing.

## Rust Conventions

- Prefer existing local patterns over new abstractions.
- Use structured parsers/APIs rather than ad hoc string handling.
- Preserve span-aware YAML parsing through `clinker_plan::yaml` and `Spanned<T>`.
- Keep user-facing config strict where `deny_unknown_fields` is established.
- Use subsystem error enums and `PipelineError::Internal` for invariant violations; avoid panic-based runtime behavior.
- Add focused tests at the boundary touched: plan/config in `clinker-plan`, runtime in `clinker-exec`, format in `clinker-format`, language in `cxl`, channel in `clinker-channel`, CLI in `clinker`.

## Dependencies And Approval

- Do not add dependencies, native toolchain requirements, async runtimes, C build steps, OpenSSL/native-tls, or cargo-deny exceptions without approval.
- Keep benchmark/test helpers out of default runtime paths.
- If a command needs network, writes outside the workspace, or needs elevated permissions, ask for approval through the tool flow and explain why.

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
- One Agent Task should normally produce one PR, and one PR should close one Agent Task.
- Agents must not merge PRs by default; leave PRs for maintainer review and merge unless a maintainer explicitly instructs otherwise.

## Definition Of Done

- Code/docs match the requested scope.
- Relevant tests/checks were run, or skipped with a clear reason.
- `git diff --check` passes.
- Behavior changes have matching docs and tests.
- Open questions are captured in `docs/ai/80_OPEN_QUESTIONS.md`.
- Final response summarizes changed files, validation, and any remaining risks.
