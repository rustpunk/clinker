# Clinker

## Conventions

Cross-phase rules live in `docs/plans/cxl-engine/CONVENTIONS.md`. Consult
before authoring a new phase plan or touching AST/types, record iteration
order, golden fixtures, or error categorization.

**Locked architectural decisions** live in
`docs/plans/cxl-engine/LOCKED-DECISIONS.md`. Read it before any
`Cargo.toml` edit, new public type introduction, or module deletion.

## Pre-commit checks

Before any git commit, run in this order:

1. `cargo fmt --all`
2. `cargo clippy --workspace -- -D warnings`

Fix any issues before committing.

## Deviation policy

A **material deviation** is a change that contradicts one of:

1. An entry in `docs/plans/cxl-engine/LOCKED-DECISIONS.md`.
2. The `## Rip declarations` section of the active phase plan.
3. A crate, public type signature, trait, or module path explicitly named
   in the current phase plan.

**Everything else is not a deviation. Do not stop for it.** This includes:
- Downstream edits that are logical consequences of a planned change.
  If the plan says "delete X", fixing every site that referenced X is
  part of the task, not a separate deviation.
- Local implementation choices inside a task.
- Obvious bug fixes encountered along the way.
- Renames, formatting, import reorganization.
- Test adjustments needed to match planned behavior changes.

**When in doubt, prefer action over interruption.** The user would rather
review a completed change than approve every micro-decision.

### Batch before interrupting

When you hit one material blocker, **do not stop and ask immediately**.
First, keep investigating: read the surrounding code, probe the logical
downstream work, and surface every related blocker you can identify in
the same pass. Then interrupt once, with all of them together, in a
single DEVIATION REQUIRED block. One consolidated interruption per
investigation is vastly preferable to a back-and-forth drip.

The only reason to interrupt early is if continuing would require
destructive or irreversible work (e.g. a `git reset --hard`, schema
migration, or a change that would break the build for the next batch
of investigation).

### DEVIATION REQUIRED format

```
DEVIATION REQUIRED

Phase: [N]
Investigation summary: [one sentence on what you tried to do]

Blockers found (batched):

1. Task [N.X]: [name]
   Obstacle: [what]
   Plan / LD specifies: [LD-NNN or phase-plan section]
   Why it fails: [specific error or constraint]
   Options: A) ... B) ...

2. Task [N.Y]: [name]
   ...

Awaiting decision.
```

### Greenfield rip-and-replace

Clinker has zero users. When any proposal surfaces two paths — one that
reaches the correct target architecture and one that preserves the
existing legacy shape — choose the correct-architecture path. This rule
is **semantic, not positional**: it holds regardless of how the options
are labeled (A/B, 1/2, "clean"/"pragmatic", "bold"/"minimal"). See
LD-011 in `LOCKED-DECISIONS.md` for the forbidden shapes list.

Library constraints (serde-saphyr span limits, runtime-only schema from
file readers) are constraints, not shortcuts. Document them as LD
entries.

### Before claiming a deviation — sanity check

Before writing a DEVIATION REQUIRED block, run this three-question test:

1. **Does what I'm about to flag actually contradict an LD entry or the
   phase plan's explicitly-named crate/type/trait/module/rip-target?**
   If no — it's not a deviation. Proceed.
2. **Am I flagging this because it's new, or because it contradicts
   something settled?** New work inside a task's scope is normal work.
3. **Would a batch-before-interrupt pass surface more related blockers?**
   If yes — investigate before interrupting.
