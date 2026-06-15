# GitHub Workflow: Review

Purpose: Define review and closeout expectations for agent-authored PRs.

Agent PRs are proposals with evidence. In this public-contribution repository,
agents must not merge PRs by default. Leave PRs for maintainer review and merge
unless a maintainer explicitly instructs otherwise.

## Review Flow

```text
Agent self-check
  -> CI / automated checks
  -> AI review
  -> Maintainer review
  -> Maintainer merge decision
```

## Agent Merge Boundary

An agent should stop at PR readiness by default. Before requesting maintainer
review, confirm:

- The issue is an Agent Task or otherwise clearly agent-authorized.
- The PR closes the exact implementation issue, not a parent milestone or epic.
- Acceptance criteria are satisfied.
- Required verification passed or skipped checks are justified in the PR.
- Required decision gates are closed or not needed.
- No unapproved dependency, public API, schema, auth, security, memory, or
  architecture change is hidden in the PR.

Agents must not run merge commands or manually close implementation issues when
the linked PR should close them on maintainer merge, unless a maintainer
explicitly instructs otherwise.

## Review Checklist

- PR maps to exactly one issue or one coherent sub-issue.
- No unrelated cleanup.
- No hidden public API, schema, dependency, auth, security, memory, or
  architecture change.
- Acceptance criteria are satisfied.
- Edge cases and error behavior are intentional.
- Backward compatibility is considered.
- No acceptance criterion is partial or missing without an explicit follow-up
  and maintainer-visible explanation.
- Regression tests exist for bug fixes.
- Verification commands were run or skipped with a reason.
- Tests were not weakened to pass.
- Existing project patterns are followed.
- Follow-up work is captured as issues, not hidden TODO drift.
- PR description includes the post-implementation check.
- CI is green.
- AI review was considered.
- Maintainer approval is complete.
- Linked issue will close correctly.

## Close Protocol

Before closing an agent issue, leave a final comment with:

- summary of behavior changed
- files/crates touched
- acceptance criteria status
- tests/checks run
- skipped checks and why
- risks or rollback notes
- follow-up issues created

Close the issue only when all acceptance criteria, verification requirements,
and maintainer review/merge requirements are satisfied. If a linked PR closes
the issue on merge, do not manually close the issue first.
