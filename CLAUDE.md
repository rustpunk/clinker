# Clinker

## Pre-commit checks

Before any git commit, run in this order:

1. `cargo fmt --all`
2. `cargo clippy --workspace -- -D warnings`

Fix any issues before committing.

## Plan deviation policy

Stop and ask ONLY for material deviations:
- Swapping crates or major dependencies
- Changing public type signatures, traits, or module paths named in the plan
- Skipping, merging, or reordering tasks
- Adding scope beyond the phase
- Picking a fundamentally different approach than the plan specifies

Do NOT stop for:
- Downstream edits that are logical consequences of a planned change
  (if the plan says "delete X", fixing everything that referenced X is
  part of the task, not a deviation)
- Local implementation choices inside a task
- Obvious bug fixes encountered along the way
- Renames, formatting, import reorganization
- Test adjustments needed to match planned behavior changes

When in doubt, prefer action over interruption. The user would rather
review a completed change than approve every micro-decision.

Surface genuine blockers in this format and wait:

```
DEVIATION REQUIRED

Task [N.X]: [name]
Obstacle: [what]
Plan specifies: [what was planned]
Why it fails: [specific error or constraint]

Options:
A) [alternative] -- [tradeoffs]
B) [alternative] -- [tradeoffs]
```
