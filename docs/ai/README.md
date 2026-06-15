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

## Human Review Notes

- Confirm whether this README should stay human-review-scaffolded or become the reviewed index for `docs/ai/`.
