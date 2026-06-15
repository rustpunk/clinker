# AI Onboarding: Performance Notes

Purpose: Preserve future verified performance assumptions, benchmark entry points, and memory-model notes for AI agents.

## Status

This is an AI-generated initial draft requiring human review.

## Source Evidence

Validate this page against:

- `CLAUDE.md`
- `Cargo.toml`
- `crates`
- `benches/pipelines/**/*.yaml`
- `rg "spill|memory|buffer|arena|parallel|rayon|alloc|bench" crates benches`
- `cargo bench -p cxl`
- `cargo bench -p clinker-benchmarks`
- `cargo check --benches --workspace`

Existing files under `docs/*` may be stale. Treat them as secondary context only; any performance claim found in `docs/*` must be validated against code, manifests, benchmarks, tests, examples, or safe local commands before being copied here.

## Memory Model

Placeholder.

## Primary Code Evidence

Placeholder.

## Execution Hot Paths

Placeholder.

## Benchmark Suites

Placeholder.

## Allocation Notes

Placeholder.

## Spill And Storage Notes

Placeholder.

## Human Review Notes

Placeholder.
