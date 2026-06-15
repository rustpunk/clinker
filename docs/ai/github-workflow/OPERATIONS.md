# GitHub Workflow: Operations

Purpose: Define project statuses, labels, sizing, WIP limits, and common
failure controls for GitHub-issue-driven agent work.

## GitHub Project Use

Use one current GitHub Project as the live flow board for agent work. The
Project is for queueing and visibility; issues, PRs, milestones, labels, and
comments remain the durable source of truth.

If an old Project is stale or wildly out of date, do not use its statuses as
evidence. Either archive it and create a fresh Project, or rename it as
archived/stale and rebuild a new active board from current open issues and
PRs.

Recommended active Project name:

```text
Agent Delivery Board
```

Recommended fields:

- `Status`: current workflow state.
- `Milestone`: larger goal container.
- `Issue Type`: Milestone tracker, Readiness Review, Decision Gate, Agent Task,
  PR, follow-up.
- `Agent Mode`: investigate, implement, fix-ci, review, docs-tests, split,
  decision.
- `Agent Size`: S, M, L, XL.
- `Risk`: low, medium, high.
- `Area`: crate or subsystem.
- `Decision State`: not needed, needed, proposed, decided, blocked.
- `Owner`: human owner or maintainer.
- `Agent`: assigned agent/session if relevant.
- `Verification`: required, running, passed, skipped with reason.
- `Last Grounded`: date the issue was last checked against current repo state.
- `PR Link`: linked pull request.

Recommended views:

- `Agent Queue`: `Status = Agent Ready`; exclude blocked and maintainer-review
  labels; sort by milestone order, risk, then updated date.
- `Needs Grounding`: `Status = Needs Grounding` or `needs-context`.
- `Needs Decision`: `Status = Needs Decision` or `needs-decision`.
- `Needs Maintainer`: `Status = Maintainer Review`, `Changes Requested`,
  `Blocked`, or `needs-maintainer-review`.
- `Review Bottleneck`: `Status = PR Open`, `AI Review`, `Maintainer Review`,
  or `Changes Requested`; sort by oldest updated.
- `Milestone Plan`: group by milestone and sort manually by dependency order.
- `Decision Register`: Decision Gate issues and decided/blocked decisions.

Use built-in automations lightly:

- Added to project -> `Intake`.
- Issue closed -> `Done`.
- PR opened -> `PR Open`.
- PR merged -> `Done`.

Do not automate `Agent Ready`. Readiness depends on scope, blockers,
acceptance criteria, verification, and decision gates.

## What Goes In The Project

Add every open issue and PR that participates in delivery flow to the active
Project:

- Agent Task issues.
- Readiness Review issues.
- Decision Gate issues.
- Parent or milestone-tracker issues.
- Follow-up issues created from implementation, review, grounding, or decision
  work.
- Pull requests linked to Agent Tasks.
- Blockers that affect issue sequencing, even when they are not implementation
  work.

Do not add unrelated discussion, support, or external-only tracking items unless
they block repository work.

Default new Project items to `Intake`. Move them only after a routing pass sets
scope, blockers, issue type, size, risk, and verification expectations.

Agents that create or discover a workflow issue should add it to the active
Project. If the agent cannot update Projects because of permissions or token
scope, it must leave a comment saying the issue needs Project triage.

## Project Status

Use labels for durable metadata. Use the GitHub Project `Status` field for
workflow state.

Recommended statuses:

```text
Intake
Needs Grounding
Needs Decision
Needs Splitting
Agent Ready
Agent Running
PR Open
AI Review
Maintainer Review
Changes Requested
Ready to Merge
Done
Blocked
```

Normal flow:

```text
Intake
  -> Needs Grounding
  -> Needs Decision
  -> Agent Ready
  -> Agent Running
  -> PR Open
  -> AI Review
  -> Maintainer Review
  -> Ready to Merge
  -> Done
```

`Needs Decision`, `Needs Splitting`, `Changes Requested`, and `Blocked` are
explicit routing states, not failure states.

Automate only obvious state changes such as added-to-project -> `Intake`,
closed issue -> `Done`, and merged PR -> `Done`. Do not fully automate
`Agent Ready`; readiness is a semantic judgment.

## Dependency Tracking

Sequence dependencies are tracked in three places:

- Issue links and body fields for durable evidence: `Blocked by:` and
  `Unblocks:`.
- Project `Status` for workflow state: `Blocked`, `Needs Decision`,
  `Needs Grounding`, `Needs Splitting`, or `Agent Ready`.
- Project ordering for the next executable queue.

Use this issue dependency block when sequence matters:

```markdown
Blocked by:
- #...

Unblocks:
- #...
```

Rules:

- If `Blocked by:` contains an open issue, unmerged PR, unresolved Decision
  Gate, missing credential, or external condition, the issue is not
  `Agent Ready`.
- A dependent issue can stay in the milestone backlog, but it must be excluded
  from the agent queue until blockers clear.
- When a blocker closes or merges, update the dependent issue status and labels
  during the next planning, grounding, or operations pass.
- If a blocker is waived, leave a comment explaining who waived it and why.
- Do not rely on title prefixes or issue numbers as the only dependency signal.
- Keep the `Milestone Plan` project view manually ordered so the next
  unblocked issue in each chain is visible.

## Agent Pickup Rule

Agents choose from the smallest curated queue that is ready now:

1. Project view: `Status = Agent Ready`.
2. Labels exclude: `blocked`, `needs-context`, `needs-decision`,
   `needs-splitting`, `not-agent-ready`, `needs-maintainer-review`.
3. `Blocked by:` is empty, closed, merged, or explicitly waived.
4. WIP limits allow starting work.
5. The issue size, risk, and crate/area labels match the agent's assignment.

If the ready queue is empty, agents should not pull blocked implementation work.
They should move to grounding, decision evidence, tests, docs, reproduction, or
PR review.

## Labels

Use these workflow labels when available:

- `agent-ready`, `agent-plan-first`, `agent-investigate`
- `needs-context`, `needs-decision`, `needs-splitting`, `not-agent-ready`,
  `blocked`, `needs-maintainer-review`
- `agent-size:S`, `agent-size:M`, `agent-size:L`, `agent-size:XL`
- `agent-mode:investigate`, `agent-mode:implement`, `agent-mode:fix-ci`,
  `agent-mode:review`, `agent-mode:docs-tests`, `agent-mode:split`,
  `agent-mode:decision`
- `risk:low`, `risk:medium`, `risk:high`

Prefer existing `crate:*` labels over generic area labels. Generic
frontend/backend labels are less useful in this repository than crate
ownership.

If the repository contains `scripts/sync-agent-labels.sh`, use it to create or
update workflow labels:

```bash
scripts/sync-agent-labels.sh --repo rustpunk/clinker
```

Preview changes without writing to GitHub:

```bash
scripts/sync-agent-labels.sh --repo rustpunk/clinker --dry-run
```

## Sizing

- `agent-size:S`: direct fix, docs task, or focused test task.
- `agent-size:M`: normal autonomous implementation issue; one bounded PR.
- `agent-size:L`: one outcome but complex; plan first before editing.
- `agent-size:XL`: not agent-ready; split into atomic issues or convert into a
  milestone/epic tracker.

## WIP Limits

Start with these limits and adjust based on review capacity:

```text
Needs Decision: max 3 open decision gates per milestone
Agent Running: max 1 implementation issue per agent
PR Open awaiting review: max 3 total
High-risk PRs: max 1 active at a time
Changes Requested: max 2 before starting new implementation
XL implementation issues: 0
```

When the PR queue is full, agents should switch to:

- readiness reviews
- test coverage
- documentation
- reproducing bugs
- reviewing existing PRs
- creating follow-up issues

## Failure Controls

| Failure mode | Control |
|---|---|
| Vague issue goes straight to implementation | No implementation unless `agent-ready`; route through Readiness Review. |
| Milestone treated as an issue | Milestone is never assigned to an agent; only Agent Task issues are assigned. |
| Hidden decision inside PR | Decision Gate required for public API, schema, dependency, auth, security, memory, or architecture changes. |
| Review bottleneck | WIP limit on PRs; stop starting implementation when review queue is full. |
| Large AI PR | One issue = one PR; split by behavior. |
| Test laundering | Reviewer checks test diff; require regression tests for bug fixes. |
| False confidence from green CI | Acceptance criteria must be behavior-level and include verification evidence. |
| Agent conflict | One active implementation issue per agent; use crate/file labels and worktrees when parallel work is needed. |
| Scope creep | Capture follow-up issues instead of widening the PR; mention any intentional surface widening in the PR. |
| Stale issue | Route through Readiness Review before implementation. |
| Agent starts blocked dependent work | Blocked issues are excluded from `Agent Ready`; use `Blocked by:` and project status. |
| Session findings vanish | Implementation agents run a post-implementation follow-up check and file/comment follow-ups. |
| Shortcut implementation slips through | Post-implementation and review checks scan for suppressions, ignored tests, weak assertions, legacy paths, and untracked deferrals. |
| Decision churn | Close Decision Gates with final decisions and record durable architecture decisions in docs. |
| Agent merges public-contribution PR | Agents must not merge by default; maintainers review and merge unless they explicitly instruct otherwise. |
| Permission drift | Use sandboxed, scoped commands and request approval for sensitive operations. |
| Auto-close wrong issue | Use `Closes #...` only for the exact Agent Task issue. |
| Agent fixes unrelated failing tests | Document unrelated failures and create a separate issue. |
