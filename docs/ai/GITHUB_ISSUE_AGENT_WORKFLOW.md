# GitHub Issue Agent Workflow

Purpose: Define how autonomous coding agents create, triage, split, work, and close GitHub issues for this repository.

Use this workflow only when the task involves GitHub issues, milestones, labels, sub-issues, or autonomous issue closure.

## Agent-Ready Issues

Issues labeled or written for agents are executable work contracts for Claude Code, Codex, or another coding agent. Human review is not assumed in the normal loop.

An agent-ready issue must have:

- one primary outcome
- clear scope and out-of-scope boundaries
- relevant context or source pointers
- acceptance criteria
- verification commands
- a close condition the agent can verify without asking

Agents may implement, test, document, create follow-up issues, and close agent-ready issues when the close criteria are satisfied.

One agent should normally work one implementation issue at a time. A second issue is acceptable only when it is docs-only, tests-only, or touches clearly non-overlapping files.

## Labels

Use these workflow labels when available:

- `agent-ready`, `agent-plan-first`, `agent-investigate`
- `needs-context`, `needs-decision`, `needs-splitting`, `not-agent-ready`, `blocked`
- `agent-size:S`, `agent-size:M`, `agent-size:L`, `agent-size:XL`
- `agent-mode:investigate`, `agent-mode:implement`, `agent-mode:fix-ci`, `agent-mode:review`, `agent-mode:docs-tests`, `agent-mode:split`, `agent-mode:decision`
- `risk:low`, `risk:medium`, `risk:high`

Prefer existing `crate:*` labels over generic area labels. Generic frontend/backend labels are less useful in this repository than crate ownership.

## Sizing

- `agent-size:S`: direct fix, docs task, or focused test task.
- `agent-size:M`: normal autonomous implementation issue; one bounded PR or commit set.
- `agent-size:L`: one outcome but complex; plan first before editing.
- `agent-size:XL`: not agent-ready; split into atomic issues or convert into a milestone/epic tracker.

## Vague Issues

When an issue lacks clear scope, acceptance criteria, or verification, the agent should first perform an Issue Readiness Review as a comment or issue.

After the review, the agent may:

- proceed if missing details can be safely inferred from code, tests, docs, and architecture rules
- split the issue into smaller linked issues
- convert the issue into a milestone/epic tracker
- create a decision/blocker issue if implementation would cross an architecture, dependency, security, credential, or product-behavior boundary

The agent must not implement speculative behavior when the expected behavior is not derivable from repository evidence.

## Split Protocol

If an issue is too broad, split it before implementation.

Close the parent only when it has been converted into linked atomic issues, or leave it open as an epic/milestone tracker.

Use GitHub's native sub-issues API for umbrella/sub-issue relationships instead of markdown task-list checkboxes in parent issue bodies.

## Block Protocol

Create or link a decision/blocker issue instead of implementing when completion requires:

- new dependencies or cargo-deny exceptions not already approved
- async runtimes, daemon/service behavior, distributed execution, or unbounded streams
- credentials or external services not available to the agent
- security-sensitive access or policy choices
- product behavior that cannot be derived from repository evidence
- architecture changes that conflict with `AGENTS.md` or `docs/ai/30_DESIGN_RULES.md`

## Decision Gates

For bounded architecture or design questions, use the Agent Decision Gate issue form.

The agent may choose among options if the choice preserves:

- existing architecture rules
- bounded-memory behavior
- crate layering
- public docs/tests consistency
- dependency policy

If no option satisfies those constraints, create or link a blocker and do not implement.

## Close Protocol

Before closing an agent issue, leave a final comment with:

- summary of behavior changed
- files/crates touched
- tests/checks run
- skipped checks and why
- follow-up issues created

Close the issue only when all acceptance criteria and verification requirements are satisfied.
