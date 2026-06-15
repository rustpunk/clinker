# AI Onboarding: Design Rules

Purpose: Collect design constraints and review rules that AI agents must validate before changing behavior.

## Status

This is an AI-generated initial draft requiring human review.

## Source Evidence

Validate this page against:

- `CLAUDE.md`
- `deny.toml`
- `.github/workflows/ci.yml`
- `rg "Legacy|Internal|Block|serde\\(default\\)|ignore" crates docs`
- `rg "serde-saphyr|serde_yaml|serde_yml|cmake|native-tls|openssl" Cargo.toml Cargo.lock crates deny.toml`
- `cargo metadata --no-deps`

Existing files under `docs/*` may be stale. Treat them as secondary context only; any design-rule claim found in `docs/*` must be validated against code, manifests, tests, examples, or safe local commands before being copied here.

## Architectural Rules

Placeholder.

## Primary Code Evidence

Placeholder.

## Refactoring Rules

Placeholder.

## Configuration And Format Rules

Placeholder.

## Dependency Rules

Placeholder.

## Documentation Rules

Placeholder.

## Human Review Notes

Placeholder.
