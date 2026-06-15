# Crate-Level AGENTS.md Inventory

Purpose: Track the current crate-level `AGENTS.md` coverage, guidance
ownership, and remaining local-agent-doc gaps.

This file is not a historical decision record. It should describe the current
state of crate-level guidance and point to open questions when intent is
unclear.

## Current Coverage

Every current workspace crate has a crate-level `AGENTS.md` file:

| Crate | Local guide | Primary focus |
|---|---|---|
| `clinker-record` | `crates/clinker-record/AGENTS.md` | Record/value/schema invariants, storage, provenance, counters, performance-sensitive data model choices |
| `clinker-core-types` | `crates/clinker-core-types/AGENTS.md` | Leaf vocabulary boundary for spans, diagnostics, graph helpers, and DLQ categories |
| `cxl` | `crates/cxl/AGENTS.md` | CXL parser/resolver/typechecker/analyzer/evaluator phase order and language semantics |
| `cxl-cli` | `crates/cxl-cli/AGENTS.md` | Standalone `cxl` CLI behavior, JSON stdout, exit codes, and delegation to `cxl` |
| `clinker-format` | `crates/clinker-format/AGENTS.md` | Streaming readers/writers, format contracts, document/envelope behavior, source reopening |
| `clinker-plan` | `crates/clinker-plan/AGENTS.md` | YAML parsing, validation, schema/CXL binding, security proof types, compiled plans |
| `clinker-exec` | `crates/clinker-exec/AGENTS.md` | Runtime dispatch, bounded memory, spill, DLQ, metrics, streaming, scheduling |
| `clinker-channel` | `crates/clinker-channel/AGENTS.md` | Channel binding, overlay provenance, declared override boundaries, source staging |
| `clinker-net` | `crates/clinker-net/AGENTS.md` | Finite REST transport, `RecordSource` adaptation, blocking HTTP, source error handling |
| `clinker` | `crates/clinker/AGENTS.md` | CLI orchestration, diagnostics, exit codes, output promotion, metrics, channels, sources |
| `clinker-schema` | `crates/clinker-schema/AGENTS.md` | `.schema.yaml` parsing, discovery, advisory validation, planner boundary uncertainty |
| `clinker-bench-support` | `crates/clinker-bench-support/AGENTS.md` | Deterministic generated data, benchmark discovery/cache, feature-gated allocation accounting |
| `clinker-benchmarks` | `crates/clinker-benchmarks/AGENTS.md` | End-to-end benchmark harness, report output, feature-gated large benchmarks |

## How To Use Local Guides

- Read the root `AGENTS.md` first.
- Read this inventory only to find the relevant local guide and ownership
  boundaries.
- Read the crate-local `AGENTS.md` before editing files under that crate.
- If a change touches multiple crates, read every touched crate's local guide
  plus `docs/ai/10_ARCHITECTURE.md`, `docs/ai/20_CRATE_MAP.md`, and
  `docs/ai/30_DESIGN_RULES.md`.
- Keep command details centralized in
  `docs/ai/50_TESTING_AND_COMMANDS.md`; local guides may list focused commands
  but should not become the canonical command matrix.

## Guidance Ownership

- Root `AGENTS.md`: concise repo-wide rules, safety constraints, and links to
  detailed AI docs.
- `docs/ai/00_READ_THIS_FIRST.md`: onboarding order, evidence labels,
  repository memory model, and doc-update map.
- `docs/ai/20_CRATE_MAP.md`: workspace role/dependency map.
- `docs/ai/30_DESIGN_RULES.md`: architecture and dependency rules.
- `docs/ai/50_TESTING_AND_COMMANDS.md`: canonical command guide.
- Crate-level `AGENTS.md`: local invariants, common mistakes, touched docs,
  and focused validation commands for that crate.

## Uncertainty Affecting Local Guides

Keep unresolved uncertainty in `docs/ai/80_OPEN_QUESTIONS.md`, not in vague
local guidance. Current high-impact questions affecting crate-local docs
include:

- Whether executor runs should consume the stored `CompiledPlan` DAG directly
  or whether re-entering compilation through `CompiledPlan::config()` is
  intentional.
- Whether channel resource overlays are implemented behavior, reserved config,
  or should be rejected.
- The intended long-term boundary between `clinker-schema` and
  `clinker-plan`.
- Whether `clinker-format -> cxl` is permanent layering or transitional
  coupling.
- Whether `clinker-net` manifest wording around SQL cursors is roadmap text or
  should be removed until implementation exists.
- Which public planner/CXL APIs are stable user-facing API versus exposed
  internal surface.

## Maintenance Rules

- Update this file when adding, removing, renaming, or substantially changing a
  crate-level `AGENTS.md`.
- Do not use this file to bless architecture changes. Put architecture rules in
  `docs/ai/30_DESIGN_RULES.md` and history/decision notes in
  `docs/ai/AI_CHANGELOG.md`.
- Prefer deleting stale planning language over preserving historical
  recommendations that no longer match the workspace.
- If a local guide conflicts with source or manifests, source and current tests
  win; update the guide and, if needed, add an open question.
