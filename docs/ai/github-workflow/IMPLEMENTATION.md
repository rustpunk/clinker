# GitHub Workflow: Implementation

Purpose: Define what an implementation agent needs to know to work an Agent
Task issue.

Implementation agents usually need only this file, the root
`docs/ai/GITHUB_ISSUE_AGENT_WORKFLOW.md`, and
[REVIEW.md](REVIEW.md). They do not need milestone planning details unless the
issue asks for planning or splitting.

## Agent-Ready Requirements

Only `Agent Task` issues in `Agent Ready` should be assigned for
implementation.

An agent-ready issue must have:

- one primary outcome
- size `agent-size:S`, `agent-size:M`, or carefully bounded `agent-size:L`
- clear scope and out-of-scope boundaries
- relevant context or source pointers
- observable acceptance criteria
- verification commands
- blockers absent or resolved
- decision gates closed or not needed
- a close condition the agent can verify

`agent-size:XL` is not implementable. Split it or convert it into a milestone
or parent issue.

## Choosing Work

Agents should choose work from the curated ready queue, not by scanning all open
issues.

Pick the first issue that satisfies all of these:

- Project `Status` is `Agent Ready`.
- It has `agent-ready` or otherwise explicit agent authorization.
- It is not labeled `blocked`, `needs-context`, `needs-decision`,
  `needs-splitting`, or `not-agent-ready`.
- Its `Blocked by:` list is empty, closed, merged, or explicitly waived.
- It fits the agent's mode, size, risk, and touched area.
- It does not conflict with an active PR or another agent's active issue.

If no implementation issue satisfies those rules, switch to the next useful
non-implementation lane: grounding, decision evidence, tests, docs, reproducing
bugs, or PR review.

## Workflow

```text
Agent Ready
  -> assign one agent
  -> agent reads issue, comments, linked issues/PRs, AGENTS.md, and referenced files
  -> plan first for L or high-risk work
  -> implement the smallest change satisfying acceptance criteria
  -> add/update tests and docs as required
  -> run verification
  -> run post-implementation follow-up check
  -> open PR linked to the issue
  -> post final issue comment with evidence
```

## Rules

- One Agent Task should produce one PR.
- One PR should close one Agent Task unless explicitly approved.
- Use `Closes #...` only for the exact implementation issue, not a parent
  milestone or epic.
- Keep changes minimal and within scope.
- Prefer existing project patterns over new abstractions.
- Add or update tests when behavior changes.
- Update docs/examples if public behavior, config, diagnostics, commands, or
  architecture change.
- Do not weaken tests to make a change pass.
- Do not introduce dependencies, public APIs, migrations, async runtimes, or
  security-sensitive behavior without approval.
- Create follow-up issues instead of expanding scope.

## Post-Implementation Follow-Up

Before requesting review, run a short follow-up check so session discoveries do
not disappear:

- Compare every acceptance criterion against the diff and mark it delivered,
  partial, or missing with file/line evidence where possible.
- Scan for scope creep: public API or config surface added but not described in
  the issue or PR.
- Scan for shortcut signatures: new lint suppressions, ignored/skipped tests,
  weakened assertions, legacy/old/v1 parallel paths, mandatory fields made
  default-on-missing, tombstone TODO/FIXME comments, or deferred work mentioned
  without a tracking issue.
- Capture bugs, smells, architectural findings, and future-work ideas noticed
  during the session.
- Fix now only when the edit is tiny, obvious, in scope for the current issue,
  and can be verified with the listed checks.
- Otherwise add a PR comment, issue comment, or follow-up issue. Do not fold
  unrelated cleanup or new feature work into the PR.

GitHub issue and PR comments must be understandable without knowing the agent
session, skill name, or prompt that produced them.

## Stop Conditions

Stop implementation and route the issue when:

- the issue is blocked by an open prerequisite issue, decision, PR, or external
  condition
- acceptance criteria cannot be satisfied as written
- verification cannot be run and no substitute evidence is possible
- the issue requires an unapproved dependency
- the issue requires a public API, schema, auth, security, product, memory, or
  architecture decision
- the issue contains multiple independent outcomes
- tests fail for reasons unrelated to the change
- the implementation would require broad unrelated refactoring

When a blocker is found, comment with the blocker, update or request status and
labels, and do not partially implement around it.

## Required PR Description

```markdown
## Summary
...

## Issue
Closes #...

## Changes
- ...

## Acceptance criteria
- [x] ...

## Verification
Commands run:
- ...

Results:
- ...

Skipped checks:
- None / explain

## Risk / rollback
...

## Follow-up issues
- ...

## Post-implementation check
- Acceptance criteria checked against diff:
- Shortcut/scope-creep scan:
- Follow-up issues/comments:
```
