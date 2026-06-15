# AI Agent Workspace

Purpose: Explain how future Codex and Claude agents should use the AI onboarding documentation.

## Status

This is an AI-generated initial draft requiring human review.

## How Agents Should Use This Folder

- Start with `docs/ai/00_READ_THIS_FIRST.md`.
- Treat every `docs/ai/*.md` file as a review scaffold until a human marks it as reviewed.
- Validate claims from the listed source evidence before relying on them.
- Treat existing `docs/*` files as secondary context only. They may be stale, and any claim from `docs/*` must be validated against code, manifests, tests, examples, or safe local commands before use.
- Keep AI-facing onboarding updates in `docs/ai/`.
- Use the root `AGENTS.md` for concise repo-wide operating instructions.
- Do not modify Rust source files when the task is documentation-only.

## Source Evidence

Validate this page against:

- `AGENTS.md`
- `docs/ai/*.md`
- `CLAUDE.md`
- `Cargo.toml`
- `crates/*/Cargo.toml`
- `git status --short`

## Future Agent Checklist

- Read `AGENTS.md` and `docs/ai/00_READ_THIS_FIRST.md`.
- Check `git status --short` before editing.
- Validate architecture claims against source, manifests, tests, and examples.
- Update `docs/ai/80_OPEN_QUESTIONS.md` rather than guessing when a term or rule is unclear.

## Index

- `00_READ_THIS_FIRST.md`: entry point, reading order, evidence labels, and documentation update map.
- `10_ARCHITECTURE.md`: architecture overview and subsystem boundaries.
- `20_CRATE_MAP.md`: current workspace crate roles and dependency direction.
- `30_DESIGN_RULES.md`: architecture, dependency, config, and review rules.
- `40_COMMON_PATTERNS.md`: repeated implementation patterns and local conventions.
- `50_TESTING_AND_COMMANDS.md`: canonical command guide.
- `60_PERFORMANCE_NOTES.md`: hot paths, memory-sensitive code, and benchmark entry points.
- `70_GLOSSARY.md`: project-specific terms.
- `80_OPEN_QUESTIONS.md`: unresolved uncertainty for human review.
- `90_CRATE_AGENT_PLAN.md`: crate-level `AGENTS.md` inventory and ownership map.
- `GITHUB_ISSUE_AGENT_WORKFLOW.md`: issue-driven agent workflow.
- `AI_CHANGELOG.md`: lightweight AI documentation history and decision notes.

## Human Review Notes

- Confirm whether this README should stay human-review-scaffolded or become the reviewed index for `docs/ai/`.
