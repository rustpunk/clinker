# GitHub Workflow: Grounding

Purpose: Convert vague, stale, or broad issues into agent-ready work, decision
gates, splits, milestones, or blockers.

Grounding is investigation only. Grounding agents may inspect repository state,
docs, tests, examples, linked issues, and PRs. They must not implement.

## When To Use

Use the Issue Readiness Review template when an issue:

- is vague, stale, or broad
- lacks acceptance criteria
- lacks verification steps
- may touch multiple subsystems
- says "improve", "clean up", "support", "fix weirdness", or "make better"
  without specifics
- contains product, architecture, dependency, API, security, schema, memory, or
  compatibility ambiguity

## Grounding Result Format

```markdown
## Grounding Result

### My understanding
...

### Evidence inspected
- Source:
- Tests:
- Docs:
- Related issues/PRs:

### Current behavior
...

### Desired behavior
...

### Proposed split
- #A Agent Task: ...
- #B Agent Task: ...
- #C Decision Gate: ...

### Recommended route
- [ ] Ready to implement
- [ ] Needs decision
- [ ] Needs splitting
- [ ] Should become milestone
- [ ] Blocked

### Why
...
```

## Routing Outcomes

After grounding, route to one of:

- `Agent Ready`
- `Needs Decision`
- `Needs Splitting`
- milestone/epic tracker
- `Blocked`

The agent must not implement speculative behavior when expected behavior is not
derivable from repository evidence.
