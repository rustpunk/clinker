# GitHub Milestone Orchestration

Verified against origin/main cf6609b9 (2026-07-24).

## Purpose

Milestone orchestration is the coordinator workflow for driving a project
outcome through planning, queue curation, implementation PRs, review, closeout,
and final milestone verification. It sits above the focused per-phase workflow
docs in this directory; it does not replace them.

## State Model

Use the GitHub Project `Status` field as the primary workflow state. Labels are
mirrored routing signals where the repository has matching labels.

Canonical flow:

```text
Intake
  -> Needs Grounding / Needs Decision / Needs Splitting / Blocked / Umbrella
  -> Agent Ready
  -> Agent Running
  -> PR Open
  -> Ready to Merge
  -> Done / Won't Do
```

State invariants:

- A milestone is a project-outcome boundary, not the queue itself.
- `Agent Ready` requires one coherent outcome, clear scope, acceptance criteria,
  verification, and no unresolved blocker or decision.
- `Agent Running` means exactly one implementation agent owns that issue.
- `PR Open` means implementation moved to PR review and CI.
- `Ready to Merge` is a maintainer gate, not agent permission to merge.
- `Done` requires the issue's own acceptance criteria and required children to
  be complete, cancelled, or explicitly out of scope.
- `Umbrella` parents stay open while required children remain open.
- `Needs Decision` must include decision-gate content, not just labels/status.
- `Blocked` must name the concrete blocker.

## Coordinator Ownership

Run one coordinator per milestone. The coordinator owns:

- milestone snapshot and exit criteria
- active queue size and pull order
- issue claiming and Project/label state transitions
- parent/sub-issue/dependency consistency
- cross-issue blockers, follow-ups, and milestone notes
- closeout reconciliation after PR merge

Implementation agents own only their target issue, branch, PR, and directly
linked follow-up comments.

## Active Queue

Keep a small active queue, normally 3-5 issues. Before dispatch:

1. Refresh a queue snapshot for the milestone (via the repository's snapshot
   tooling when present, otherwise `gh` issue/project queries).
2. Check `Agent Ready`, `Agent Running`, `PR Open`, and review states.
3. Confirm no selected issue has an unresolved native dependency, parent/child
   blocker, decision gate, or stale route label.
4. Avoid parallel issues that touch the same file/module scope unless the
   maintainer accepts the conflict risk.

Run a queue-curation pass when the next 3-5 issues are not obvious.

## Claim Protocol

Claiming prevents duplicate implementation agents.

1. Re-read the issue snapshot immediately before claiming.
2. Prepare one batched update that moves selected issues to
   `Status = Agent Running`.
3. Remove `agent-ready` from claimed issues when the Project status is changed so
   label-only queue scans do not redispatch them.
4. Preflight the update as a dry run (via the repository's snapshot tooling when
   present, otherwise `gh`/GraphQL) and inspect the result.
5. Apply only if every preflight operation is valid.
6. Record the claim in the orchestration tracker before dispatch.

If the issue changed state between snapshot and claim, stop and requeue.

## Dispatch Prompt Shape

Each implementation dispatch should include:

```markdown
Implement Agent Task issue #NNN in OWNER/REPO, following the implementation
workflow (IMPLEMENTATION.md).

Coordinator run: #TRACKER
Milestone: <name>
Ownership: this agent owns only issue #NNN, its branch, and its PR.
Parallel context: other agents may be working on different issues; preserve their changes and stop if scopes overlap.
Do not modify milestone notes, parent status, or unrelated issues.
Stop and route if acceptance criteria, verification, blockers, or decisions are unclear.
Open one PR; do not merge.
```

For review and closeout:

```markdown
Review PR #NNN against its linked Agent Task, following the review workflow
(REVIEW.md).
```

```markdown
Run the close protocol for PR #NNN after merge (REVIEW.md, Close Protocol).
```

## Orchestration Tracker

Use a milestone orchestration issue or comment as persistent run state. Keep it
short and operational:

```markdown
## Milestone Orchestration State

Milestone:
Coordinator:
Last snapshot:
Exit criteria:

### Active slots
| Issue | Owner | Status | Branch | PR | Scope | Next action |
| --- | --- | --- | --- | --- | --- | --- |

### Queue
| Pull order | Issue | Reason ready | Risk | Verification |
| --- | --- | --- | --- | --- |

### Blockers and routes
| Issue | Route | Reason | Owner |
| --- | --- | --- | --- |

### Closeout
| PR | Issue | Merge state | Closeout state | Follow-ups |
| --- | --- | --- | --- | --- |
```

## Stop Conditions

Stop the affected lane and route instead when:

- scope, acceptance criteria, or verification is missing
- a decision, dependency bump, public API change, security choice, schema change,
  or expensive-to-revert behavior choice is required
- the issue is multiple independent outcomes
- parent/sub-issue/dependency state is unclear
- Project fields are not visible or update preflight fails
- a concurrent update moved the issue out of the expected state
- verification cannot run or unrelated failures block confidence
- implementation requires broad unrelated refactoring
- a PR is ready to merge and no maintainer has explicitly approved merge

## Milestone Exit Gate

The milestone is complete only when:

- all required milestone issues are closed, cancelled, or explicitly out of scope
- required child/sub-issues are closed or cancelled
- Project statuses and labels match final state
- verification is recorded for completed implementation issues
- follow-up issues are linked and marked blocking or non-blocking
- parent/umbrella issues are not closed prematurely
- milestone notes describe final state and remaining non-goals
