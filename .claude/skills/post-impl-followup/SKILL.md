---
name: post-impl-followup
description: "Resurfaces the ephemeral findings a coding session generates — bugs noticed in passing, ideas from exploration, scope deviations decided mid-flight, architectural a-ha moments — that would otherwise vanish when context turns over. Reads the session transcript first for these items, then audits each landed PR's diff against its closing issue and scans for architectural-shortcut signatures (suppressed lints, ignored tests, tombstones, legacy renames, parallel old/new paths) as a secondary check. Triages each finding: FIX-NOW (small doc / comment edits applied this session before pushing) or DEFER (filed as a GH issue or comment, milestone-mapped) — never silently discarded. Per-action confirmation throughout. Self-contained — no other skill or policy file required. Use at sprint close before git push or gh pr create, after a PR merges, or retroactively across a milestone. Triggers: post-impl followup, session retrospective, audit before push, sprint-close gate, what came up this session, surface follow-ups, capture deviations, don't lose what we found, audit recent PRs, milestone-N audit."
allowed-tools: Bash, Read, Edit, Write, Glob, Grep, AskUserQuestion, Agent
---

# Post-Implementation Follow-up

A coding session generates a stream of ephemeral findings — bugs spotted
in passing, ideas surfaced during exploration, scope deviations decided
mid-flight, architectural a-ha moments. Without an explicit capture
step, they get referenced in conversation and then silently discarded
when context turns over.

This skill is that capture step. Reflect on the session, resurface what
came up, triage each item: trivial fixes land in this commit, real
findings become issues / comments / milestone suggestions. Nothing
qualifying gets dropped on the floor. A PR-diff audit and shortcut
scan run alongside as a secondary cross-check.

## Scope

One of:

- (no arg) — working tree vs `origin/main` (the live session)
- `<base>..HEAD` — explicit commit range
- `#<N>` — a single PR
- `milestone <N>` — every closed PR in a milestone

## Central judgement: FIX-NOW vs DEFER

For every finding: can I deliver this in this session, in under a few
minutes, without breaking anything? Yes → fix in place. No → defer.

**FIX-NOW** (apply before any GH writes, per-edit confirmation):

- Typo / wording / formatting in user-facing docs or doc-comments
- Missing doc-comment summary on a public item the sprint added
- Single-file doc update reflecting a small shipped surface change
  (e.g. a docs page lists 7 diagnostic codes, sprint added an 8th)
- Stale link, outdated example, broken in-tree reference
- Comment violating the repo's comment policy (stale internal-process
  references, paths into gitignored planning docs)
- Trivially-dead `use` / import the sprint introduced

**DEFER** (always file or comment):

- Anything changing test behaviour or coverage
- Code change crossing module boundaries
- Anything requiring an unmade design decision
- Pre-existing housekeeping unrelated to this sprint
- New feature work, however small
- Anything below ~90% confidence — when in doubt, defer

## Finding categories

Session-derived (the primary catch — these only exist in the
transcript and vanish if not captured):

| Category | Default disposition |
|---|---|
| **Bug / smell noticed in passing** — warning, brittle assertion, dead code spotted en route | NEW chore ISSUE (do NOT fold into closing commit) |
| **Idea / future work** — "we could also do Z" raised during exploration but not acted on | NEW ISSUE, milestone-mapped (or MILESTONE SUGGEST if multiple cluster) |
| **Architectural a-ha** — "X actually behaves like Y" insight from testing or debugging | COMMENT on related issue, or NEW ISSUE |
| **Process learning** — methodology insight worth preserving | FIX-NOW (update contributing guide / CLAUDE.md) or NEW chore ISSUE |
| **Convention drift** — code not following its own stated conventions | FIX-NOW if trivial; else NEW ISSUE |
| **Sizing / spec deviation decided mid-session** — acceptance said X, landed Y for a reason | COMMENT on the closed issue to record the decision |

PR-derived (secondary cross-check, caught by reading the diff):

| Category | Default disposition |
|---|---|
| **Silent acceptance gap** — issue spec required X, PR landed X − Y | COMMENT on closed issue with file:line |
| **Silent scope creep** — surface widening not in PR description | FIX-NOW if it's just an in-tree doc update; else COMMENT |
| **Untracked deferral** — "we'll do X later" with no issue filed | NEW ISSUE, milestone-mapped |
| **Doc / comment hygiene** — missing summary, stale ref, docs out of sync with shipped surface | FIX-NOW |
| **New theme** — 3+ findings (from either source) cluster on an unaddressed area | MILESTONE SUGGEST + linked issues |

## Process

Seven steps. Step 2 is Claude-native; Steps 3 + 4 fan out in parallel;
5–7 are sequential.

### 1. Preflight + scope

```bash
command -v gh >/dev/null || echo "MISSING: gh"
gh auth status >/dev/null 2>&1 || echo "MISSING: gh auth"
git rev-parse --git-dir >/dev/null 2>&1 || echo "MISSING: not in a git repo"
```

Resolve scope:

- (no arg) → `git log origin/main..HEAD --pretty=oneline` + `git status --short`
- `<base>..HEAD` → `git log <range> --pretty=oneline`
- `#<N>` → `gh pr view <N> --json mergeCommit,closingIssuesReferences`
- `milestone <N>` → `gh pr list --state merged --search "milestone:<N>" --json number,mergeCommit,closingIssuesReferences,title`

### 2. Session review — the primary capture (Claude native)

Read the conversation transcript end-to-end. Extract every finding
that exists *only* in the conversation, not in any diff:

1. **Bugs / smells noticed in passing** — warnings, brittle assertions,
   dead code, naming friction spotted while doing something else.
2. **Ideas / future work** — "we could also do Z" raised during
   exploration that didn't make it into scope.
3. **Architectural a-ha moments** — "X actually behaves like Y" insights
   from testing, debugging, or reading code. Mental-model corrections
   that the next session will lack.
4. **Sizing / spec deviations the session decided on** — acceptance said
   X, landed Y for a justifiable reason.
5. **Process learnings** — methodology insights worth preserving.
6. **Convention drift** — code not following its own conventions.

Per item: what it is (1 sentence), where in the session it surfaced,
whether already logged elsewhere, fix-now feasibility.

If no transcript is available (cold invocation), skip and flag the
limitation — the skill loses its primary purpose and becomes a
PR-audit-only run.

### 3. Per-PR audit — the secondary cross-check (parallel agents)

One `Explore` agent per PR, cap 5 in parallel (waves of 5 for larger
scopes). Each agent compares the diff to the closing issue's acceptance,
scans for shortcut signatures, surfaces undeclared widenings and
unfiled deferrals.

→ Full prompt template and shortcut-signature list:
[references/audit-prompt.md](references/audit-prompt.md)

### 4. Cross-reference open GH issues

For each non-FIX-NOW candidate:

```bash
gh issue list --repo <owner>/<repo> --state open --search "<keyword>" \
  --json number,title,milestone,labels --limit 20
```

Classify: **already covered** (no-op or context-add comment),
**partially covered** (comment with new context), or **novel** (NEW
ISSUE).

For novel items, list milestones via
`gh api repos/<owner>/<repo>/milestones --jq '.[] | "\(.number): \(.title) — \(.description)"'`
and map by theme. Suggest a **new milestone** only when 3+ findings
cluster on an unaddressed theme.

### 5. Triage

Tag each candidate: `FIX-NOW`, `COMMENT #N`, `NEW ISSUE`,
`MILESTONE SUGGEST`, or `NO-OP`. Apply the FIX-NOW criteria above —
small, obvious, tests-still-green, in-scope-for-this-sprint.

### 6. Synthesize action list

Four sections, **fix-in-place first** (so trivial edits land in the
closing commit, not as fragmented follow-ups).

→ Action-list template, command reference, edge cases, anti-patterns:
[references/execution.md](references/execution.md)

### 7. Confirm and execute

One `AskUserQuestion` per section, multi-select. Execute sequentially:
fix-in-place edits first (with a narrowly-scoped sanity check
appropriate to the file type), then GH writes. Revert + demote to NEW
ISSUE if a sanity check fails.

Do **not** commit fix-in-place edits inside this skill — leave them in
the working tree; the closing-commit step folds them in. This keeps the
skill read-only on git history.

## Output discipline

- Open with a 1-paragraph executive summary: PRs audited, candidate
  findings, in-place fixes vs deferred actions.
- Then the structured action list (fix-in-place first).
- Then per-PR detail with file:line for every claim.
- Close with the confirmation question for fix-in-place.

**No skill names, slash-commands, or session jargon in any GH issue
body or comment.** Issues are public; external readers arriving via
search must understand them cold. Apply to every body and comment, not
just the first.

## Example invocations

```
/post-impl-followup                  # working tree vs origin/main
/post-impl-followup #152              # single PR retroactively
/post-impl-followup milestone 1       # retrospective milestone sweep
```
