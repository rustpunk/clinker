# 00 — Read This First

## Purpose

This file is the entry point for AI agents and senior Rust contributors using
the Clinker AI onboarding docs. Start here to understand which docs to read,
how to classify evidence, what rules must be preserved, and what to verify
before claiming success.

These docs help future sessions regain repository context without relying on
old chat history. They do not replace source review.

Use these evidence labels consistently:

- **Verified:** Checked against current source, manifests, tests, examples, CI,
  or a local command in this workspace.
- **Strong inference:** Not directly stated as a rule, but strongly supported
  by current code structure, tests, manifests, and repeated local patterns.
- **Needs grounding:** Plausible but not yet confirmed. Do not build architecture or
  user-facing claims on it; move it to open questions or validate it against code.
- **Open question:** Unresolved uncertainty that should be tracked in
  [docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md), not guessed around.

## How to use these docs

Recommended reading order by task:

1. **New AI session / general orientation:** read
   [AGENTS.md](../../AGENTS.md), this file,
   [docs/ai/20_CRATE_MAP.md](20_CRATE_MAP.md),
   [docs/ai/30_DESIGN_RULES.md](30_DESIGN_RULES.md), and
   [docs/ai/50_TESTING_AND_COMMANDS.md](50_TESTING_AND_COMMANDS.md).
2. **Feature implementation:** add
   [docs/ai/10_ARCHITECTURE.md](10_ARCHITECTURE.md),
   [docs/ai/40_COMMON_PATTERNS.md](40_COMMON_PATTERNS.md), the relevant
   crate-level `AGENTS.md`, and user/engine docs for the touched behavior.
3. **Bug fixing:** read the crate map, design rules, testing commands, the
   nearest crate-level `AGENTS.md`, and tests or fixtures near the bug. Use
   [docs/ai/70_GLOSSARY.md](70_GLOSSARY.md) when terminology affects behavior.
4. **Cross-crate refactor:** read architecture, crate map, design rules,
   common patterns, open questions, and
   [docs/ai/AI_CHANGELOG.md](AI_CHANGELOG.md) before editing.
5. **Performance work:** read
   [docs/ai/60_PERFORMANCE_NOTES.md](60_PERFORMANCE_NOTES.md), testing
   commands, architecture, and the relevant benchmarks/tests.
6. **Test/documentation work:** read
   [docs/ai/50_TESTING_AND_COMMANDS.md](50_TESTING_AND_COMMANDS.md), the
   affected user/engine/AI docs, and
   [docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md) for known stale areas.
7. **Crate-level work:** read
   [docs/ai/90_CRATE_AGENT_PLAN.md](90_CRATE_AGENT_PLAN.md) and the nearest
   crate-level `AGENTS.md` if present.

## Minimum reading checklist for AI agents

Before editing code, an AI agent must read:

- [AGENTS.md](../../AGENTS.md)
- [docs/ai/00_READ_THIS_FIRST.md](00_READ_THIS_FIRST.md)
- [docs/ai/20_CRATE_MAP.md](20_CRATE_MAP.md)
- [docs/ai/30_DESIGN_RULES.md](30_DESIGN_RULES.md)
- [docs/ai/50_TESTING_AND_COMMANDS.md](50_TESTING_AND_COMMANDS.md)
- The nearest crate-level `AGENTS.md`, if present.

Also read architecture, common-pattern, performance, or glossary docs when the
task touches those areas. For docs-only tasks, still check
`git status --short` before editing and avoid changing source files unless the
user explicitly asks.

## Repository memory model

Repository docs are durable memory for future agents. Previous chat or session
context is not durable and should not override checked-in docs or current
source evidence.

When durable docs exist, future agents should use them first, then verify
claims against current code, manifests, tests, examples, and safe local
commands. Any important architectural discovery should be recorded in the
appropriate `docs/ai/` file, and unresolved uncertainty should be recorded in
[docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md).

## Rules for future AI agents

- Do not invent architecture.
- Prefer source evidence over assumptions.
- Mark uncertainty explicitly.
- Respect crate boundaries.
- Keep root `AGENTS.md` concise.
- Put detailed explanations in `docs/ai/`.
- Update docs when changing behavior, APIs, architecture, tests, or commands.
- Do not modify unrelated files.
- Do not make broad refactors without human approval.
- Do not add dependencies without checking design rules and asking when
  uncertain.

## Definition of done

Before claiming success, an agent should:

1. Identify touched crates and boundaries.
2. Read relevant local `AGENTS.md` files.
3. Run appropriate commands from
   [docs/ai/50_TESTING_AND_COMMANDS.md](50_TESTING_AND_COMMANDS.md).
4. Update affected user, engine, or AI docs.
5. Report files changed.
6. Report verification commands run.
7. Report remaining uncertainty or skipped checks.

For docs-only AI onboarding edits, the smallest relevant gate is usually:

```bash
git diff --check
```

## Documentation map

- [docs/ai/10_ARCHITECTURE.md](10_ARCHITECTURE.md): source-backed overview of
  Clinker's runtime model, planning/execution boundary, subsystem roles, and
  architectural invariants.
- [docs/ai/20_CRATE_MAP.md](20_CRATE_MAP.md): workspace crate list, dependency
  direction, crate responsibilities, known coupling, tests, benches, examples,
  and stale references.
- [docs/ai/30_DESIGN_RULES.md](30_DESIGN_RULES.md): design constraints for
  finite batch execution, crate layering, YAML parsing, dependency policy,
  bounded memory, config strictness, and review gates.
- [docs/ai/40_COMMON_PATTERNS.md](40_COMMON_PATTERNS.md): repeated local
  implementation patterns such as streaming traits, proof tokens, registries,
  span-aware YAML, error enums, and test organization.
- [docs/ai/50_TESTING_AND_COMMANDS.md](50_TESTING_AND_COMMANDS.md): practical
  build, test, lint, docs, benchmark, and troubleshooting command guide.
- [docs/ai/60_PERFORMANCE_NOTES.md](60_PERFORMANCE_NOTES.md): performance- and
  memory-sensitive paths, benchmark entry points, cache notes, and unsafe
  performance boundaries.
- [docs/ai/70_GLOSSARY.md](70_GLOSSARY.md): repository-specific terms for
  product, crates, pipeline nodes, CXL, execution, formats, and storage.
- [docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md): unresolved
  architecture, documentation, API, and testing questions.
- [docs/ai/90_CRATE_AGENT_PLAN.md](90_CRATE_AGENT_PLAN.md): planning notes for
  existing crate-level `AGENTS.md` coverage, remaining gaps, and guidance
  ownership.
- [docs/ai/GITHUB_ISSUE_AGENT_WORKFLOW.md](GITHUB_ISSUE_AGENT_WORKFLOW.md):
  entry point for GitHub-issue-driven agent work, with links to focused
  workflow slices under `docs/ai/github-workflow/`.
- [docs/ai/AI_CHANGELOG.md](AI_CHANGELOG.md): lightweight history of AI
  documentation, source-backed architecture facts, and decisions or inferences
  future agents should know.

## Missing or incomplete docs

No expected `docs/ai/` file from this index was missing during this pass.
[docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md) tracks areas where source,
docs, or intended behavior need clarification.

## When to update which doc

| Change type | Docs to update |
|---|---|
| New crate | [docs/ai/20_CRATE_MAP.md](20_CRATE_MAP.md), root [AGENTS.md](../../AGENTS.md) if repo-wide guidance changes |
| Architecture change | [docs/ai/10_ARCHITECTURE.md](10_ARCHITECTURE.md), [docs/ai/AI_CHANGELOG.md](AI_CHANGELOG.md), [docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md) if uncertainty remains |
| New design rule | [docs/ai/30_DESIGN_RULES.md](30_DESIGN_RULES.md) |
| New repeated pattern | [docs/ai/40_COMMON_PATTERNS.md](40_COMMON_PATTERNS.md) |
| New command/test workflow | [docs/ai/50_TESTING_AND_COMMANDS.md](50_TESTING_AND_COMMANDS.md) |
| Performance-sensitive change | [docs/ai/60_PERFORMANCE_NOTES.md](60_PERFORMANCE_NOTES.md), benchmark docs or tests when relevant |
| New domain term | [docs/ai/70_GLOSSARY.md](70_GLOSSARY.md) |
| Unresolved uncertainty | [docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md) |
| New crate-level guidance | The crate-local `AGENTS.md`, and [docs/ai/90_CRATE_AGENT_PLAN.md](90_CRATE_AGENT_PLAN.md) if the plan changes |
| Public behavior, config, or diagnostic change | User docs, examples or explain-code docs, plus the relevant `docs/ai/` file |
| Test fixture, snapshot, or benchmark policy change | [docs/ai/50_TESTING_AND_COMMANDS.md](50_TESTING_AND_COMMANDS.md), [docs/ai/40_COMMON_PATTERNS.md](40_COMMON_PATTERNS.md), or [docs/ai/60_PERFORMANCE_NOTES.md](60_PERFORMANCE_NOTES.md) as applicable |

## Current known limitations

The full list is in
[docs/ai/80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md). The most important
current uncertainties are:

- `PipelineExecutor::run_plan_with_readers_writers` accepts `CompiledPlan`, but
  the intended relationship between the stored DAG and runtime recompilation
  needs confirmation.
- Channel resource overlays and pipeline-target channel config validation need
  clearer intended behavior.
- The long-term boundary and validation depth of `clinker-schema` versus
  `clinker-plan` remain unresolved.
- Some user-facing docs may still describe retired config shapes, an outdated
  node count, or unclear envelope/document-context examples.
- Transport docs need alignment around implemented file and finite REST support
  versus SQL cursor wording.
- The `clinker-format -> cxl` dependency edge is current, but whether it is a
  permanent layering rule remains open.
- Some public planner/CXL symbols may be exposed without a clear stability
  policy.

## First prompt for a new Codex session

Copy/paste this prompt when starting a new Codex session:

> Before doing any work, read AGENTS.md and docs/ai/00_READ_THIS_FIRST.md. Then identify the crate boundaries, docs, and verification commands relevant to this task. Do not edit files until you summarize your plan.
