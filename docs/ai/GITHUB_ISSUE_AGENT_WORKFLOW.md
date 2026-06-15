# GitHub Issue Agent Workflow

Purpose: Define the entry point and routing rules for GitHub-issue-driven
agent work in this repository.

Use this workflow when a task involves GitHub issues, milestones, labels,
sub-issues, pull requests, project status, or autonomous issue closure.

The core rule is that implementation is not the default workflow. Planning,
grounding, decisions, implementation, and review are separate modes with
different permissions and exit criteria.

## Which File To Read

Read only the workflow slice that matches the work:

- Planning or milestone setup:
  [github-workflow/PLANNING.md](github-workflow/PLANNING.md)
- Vague, stale, broad, or under-specified issues:
  [github-workflow/GROUNDING.md](github-workflow/GROUNDING.md)
- Product, architecture, dependency, public API, schema, auth, security, memory,
  or compatibility choices:
  [github-workflow/DECISIONS.md](github-workflow/DECISIONS.md)
- Agent implementation work:
  [github-workflow/IMPLEMENTATION.md](github-workflow/IMPLEMENTATION.md)
- PR review, merge readiness, or review bottlenecks:
  [github-workflow/REVIEW.md](github-workflow/REVIEW.md)
- Labels, project status, sizing, WIP limits, and failure controls:
  [github-workflow/OPERATIONS.md](github-workflow/OPERATIONS.md)

An implementation agent usually needs this file,
[github-workflow/IMPLEMENTATION.md](github-workflow/IMPLEMENTATION.md), and
[github-workflow/REVIEW.md](github-workflow/REVIEW.md). It does not need
milestone planning details unless the issue asks for planning or splitting.

## Operating Model

Use this hierarchy:

```text
Milestone
  -> planning and scope container

Readiness Review issue
  -> turns vague work into agent-ready work, a decision gate, a split, or a blocker

Decision Gate issue
  -> resolves a bounded architecture/product choice before implementation

Agent Task issue
  -> one autonomous implementation packet

Pull Request
  -> evidence that the Agent Task is complete
```

Agents own bounded work packets through implementation, verification, and PR
evidence. In this public-contribution repository, agents must not merge PRs by
default; leave PRs for maintainer review and merge unless a maintainer
explicitly instructs otherwise.

## Work Item Types

- **Milestone:** larger goal or release container with goal, non-goals, exit
  criteria, linked decision gates, and a short ready queue.
- **Readiness Review:** investigation-only issue used when the work is vague,
  stale, broad, or missing acceptance criteria.
- **Decision Gate:** bounded decision issue used before implementation when a
  product, architecture, dependency, API, security, schema, memory, or
  compatibility choice is unresolved.
- **Agent Task:** implementation-ready work packet with one outcome, clear
  boundaries, observable acceptance criteria, and verification commands.
- **Pull Request:** proposed evidence that one Agent Task is complete.

## Routing Rules

- Do not assign a whole milestone to an agent. Select ready issues from the
  milestone instead.
- Agents may implement only `Agent Task` issues with `Status = Agent Ready`.
- Route vague, stale, broad, or under-specified work through Readiness Review.
- Route unresolved product, architecture, dependency, public API, schema, auth,
  security, memory, or compatibility choices through Decision Gate.
- Split `agent-size:XL` or multi-outcome work before implementation.
- Track sequence dependencies in issue links and the project board; blocked
  issues are not agent-ready.
- Add workflow-relevant issues and PRs to the active GitHub Project, defaulting
  new items to `Intake` until routed.
- One Agent Task should normally produce one PR.
- One PR should normally close one Agent Task.
- Agents must not merge PRs by default; leave PRs for maintainer review and
  merge unless a maintainer explicitly instructs otherwise.

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
