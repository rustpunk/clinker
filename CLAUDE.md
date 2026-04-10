# Clinker

## Pre-commit checks

Before any git commit, run in this order:

1. `cargo fmt --all`
2. `cargo clippy --workspace -- -D warnings`

Fix any issues before committing.
