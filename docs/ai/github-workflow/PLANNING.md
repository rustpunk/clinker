# GitHub Workflow: Planning

Purpose: Define milestone and backlog planning for agent-executable work.

Read this file only when creating or updating milestones, ordering issue
backlogs, splitting broad work, or choosing the next agent queue.

## Milestone Planning

Use milestones for goal tracking, not direct execution. Do not assign a whole
milestone to an agent. Select a small queue of ready issues from the milestone
instead.

Run a planning pass at the start of a milestone and a smaller recurring
refinement pass while it is active.

Planning outputs:

- milestone goal, non-goals, and exit criteria
- ordered issue list
- active Project view updated for the milestone
- dependency chain for blocked issues
- next 3-5 agent-ready issues
- Readiness Review issues for vague work
- Decision Gate issues for unresolved choices
- explicit blockers and non-goals
- WIP limits for the next execution window

Planning checklist:

- Confirm the user/developer outcome for the milestone.
- Confirm what is out of scope.
- Review open issues, stale issues, failed tests, known bugs, prior PR review
  comments, and open decision gates.
- Split XL or multi-outcome work before implementation.
- Put decisions before implementation.
- Put grounding before implementation.
- Put prerequisite test/fixture/docs issues before dependent behavior changes
  when that reduces risk.
- Prefer low-risk unlockers before broad changes.
- Stop starting new implementation when review is the bottleneck.

Planning should update both the milestone description and the active GitHub
Project. The milestone explains the goal and dependency chain; the Project
shows current queue state and the next issues agents may pull.

## Sequence Dependencies

Track dependencies on the dependent issue, not only in milestone notes.

Each blocked issue should include:

```markdown
Blocked by:
- #123 Decision Gate: ...
- #124 Agent Task: ...
- #125 PR: ...

Unblocks:
- #130 ...
```

Use project status and labels consistently:

- A blocked issue is `Blocked`, `Needs Decision`, `Needs Grounding`, or
  `Needs Splitting` in `Status`; it is not `Agent Ready`.
- Add `blocked`, `needs-decision`, `needs-context`, or `needs-splitting` as
  durable metadata.
- Remove blocker labels and move to `Agent Ready` only after prerequisites are
  closed, merged, or explicitly waived.

For a chain of work, make the next unblocked issue obvious in the milestone
description and project order. Do not rely on agents inferring sequence from
issue numbers.

## Milestone Description

Milestone descriptions should include:

```markdown
## Goal
...

## Non-goals
- ...

## Exit criteria
- ...

## Decision gates
- #...

## Agent-ready queue
1. #...

## Dependency chain
- #A blocks #B
- #B blocks #C
```

Use GitHub native sub-issues for parent/child relationships instead of markdown
task-list checkboxes in parent issue bodies.

## Split Protocol

If an issue is too broad, split it before implementation.

Close the parent only when it has been converted into linked atomic issues, or
leave it open as an epic/milestone tracker.

Split by independently reviewable behavior, not by agent convenience.
