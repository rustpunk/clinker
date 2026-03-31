# Clinker

## Pre-commit checks

Before any git commit, run in this order:

1. `cargo fmt --all`
2. `cargo clippy --workspace -- -D warnings`

Fix any issues before committing.

## Plan deviation policy

During plan execution, stop and ask before: swapping crates, changing
type signatures/traits/module paths, skipping/merging tasks, using a
different approach, or adding scope.

Surface blockers in this format and wait:

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
