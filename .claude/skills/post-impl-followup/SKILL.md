---
name: post-impl-followup
description: "Post-implementation retrospective that surfaces deviations, silent scope changes, ideas, and stray observations that came up during an implementation session but are not yet logged. Compares each landed PR's diff against the closing issue's acceptance criteria, scans for architectural-shortcut signatures (suppressed lints, ignored tests, tombstone comments, legacy-rename patterns, parallel old/new path coexistence), and reads the current session's conversation for process-meta items (sizing surprises, pre-existing issues noticed in passing, architectural insights, follow-up ideas). For each finding, decides whether to fix it in-place this session (small code or doc changes) or defer it. Cross-references deferred findings against open GitHub issues and milestones, then proposes a structured action list: in-place fixes to apply now, new issues to file, existing issues to comment on, and milestone mapping (including suggesting a new milestone when a theme emerges). Writes to code only for in-place fixes (with confirmation) and to GitHub via gh after explicit user confirmation. Self-contained — does not depend on any other skill, policy file, or agent. Triggers on: post-impl followup, post-implementation followup, session retrospective, what came up during this session, did anything come up worth logging, surface follow-ups, what should we file, capture session deviations, log silent issues, milestone-1 audit, audit recent PRs."
argument-hint: "optional: commit range (default: <merge-base>..HEAD), single PR number, or 'milestone N' to scope to all closed PRs in a milestone"
allowed-tools: Bash, Read, Edit, Write, Glob, Grep, AskUserQuestion, Agent
---

# Post-Implementation Follow-up

Captures the items that naturally surface during an implementation session
but don't make it into the PR description, the closing issue, or any
existing GH issue. For each finding, decides whether to fix it
**right now in this session** (small code or doc changes) or defer it to
a GH issue / comment.

**Writes to code only for in-place fixes**, each gated by explicit user
confirmation. Writes to GitHub via `gh` for deferred items, also gated
by per-action confirmation.

## When to invoke

- **At sprint close, before any `git push` or PR opening.** The skill
  catches silent deviations and small doc gaps that CI checks don't
  surface, and fixes them in place so the closing commit is genuinely
  clean.
- At the end of an implementation session, after a PR is merged or
  about to be merged.
- Retroactively on a milestone's recently-closed PRs to catch silent
  deviations or untracked deferrals that accumulated.

This skill is a *session retrospective + in-place fix + GH bookkeeping*
layer. If you also run a separate code-quality audit (correctness,
coverage, coherence, debt), run it first; this skill captures the
*process-meta* items — sizing decisions, doc gaps, untracked deferrals,
convention drift — that a pure code audit misses.

## Fix-now vs defer: the central judgement

For every finding the skill surfaces, ask: **can I deliver this in the
current session, in under a few minutes, with confidence that I won't
break anything?** If yes, fix it in place. If no, file or comment.

**FIX-NOW candidates** (handle immediately, before any GH writes):

- Typo, wording, or formatting fix in user-facing docs (`docs/`,
  `README`, generated doc-site pages like mdbook / Sphinx / Docusaurus /
  VitePress) or in-tree doc-comments (`///`, `//!`, `/**`, docstrings).
- Missing doc-comment summary on a public item the sprint added.
- Single-file doc update to reflect a small surface change the PR
  shipped (e.g. a docs page lists 7 diagnostic codes, the sprint added
  an 8th, the page needs the new row).
- Broken in-tree link, stale path, outdated example snippet.
- Trivially-dead import or `use` line spotted in passing (the sprint's
  own additions only — do not chase old dead code unless explicitly in
  scope).
- A decision-record / ADR / locked-decision entry that needs a
  one-line amendment to reflect what actually landed (when the doc is
  in-tree).
- Comment that violates the repo's comment policy — typically references
  to ephemeral process artifacts (phase labels, internal task codes,
  paths into gitignored planning docs) that won't make sense to an
  external reader on GitHub. Strip or rewrite.

**DEFER candidates** (always file or comment, never fix-now):

- Anything that changes test behavior or coverage materially.
- Any code change crossing module boundaries.
- Any change requiring a design decision the user hasn't already made.
- Anything blocked on research or agent fan-out.
- Pre-existing issues unrelated to the sprint (file as a chore issue;
  do not fold into the sprint's closing commit).
- New feature work, however small.
- Anything that would require touching tests to stay green.
- Anything where you are not >90% confident the fix is correct.

**When in doubt, defer.** A new issue is cheap; a regression in the
closing commit is expensive.

## What this skill captures

Seven categories of finding, each mapped to a disposition:

| Category | Example | Default disposition |
|---|---|---|
| **Silent acceptance gap** | Issue spec required X, PR landed X minus an explain-subcommand wire-up | Comment on closed issue with file:line evidence; consider re-opening or filing follow-up |
| **Silent scope creep** | PR introduced a new diagnostic code beyond spec, undocumented elsewhere | If the doc gap is the only loose end and the docs live in-tree, FIX-NOW (update the docs page). Otherwise comment on closed issue |
| **Untracked deferral** | PR mentions "we'll do X later" but no follow-up issue exists | New issue, milestone-mapped |
| **Architectural observation** | Surfaced during testing; not a bug, but worth a comment or follow-up | Comment on related open issue, or new issue |
| **Pre-existing housekeeping** | Warnings, brittle assertions, naming friction noticed in passing | New chore issue, no milestone or housekeeping milestone (do NOT fold into closing commit) |
| **Doc / comment hygiene** | Missing doc-comment summary, comment with stale internal-process reference, docs out of sync with shipped surface | FIX-NOW |
| **New idea / theme** | Multiple findings cluster around a topic not covered by any milestone | Suggest a **new milestone** with proposed scope |

The defaults are starting points; classify per finding in Step 5.

## Inputs

The skill takes an **audit scope** as argument:

- No arg → working tree changes vs `origin/main` (the just-finished session).
- `<base>..HEAD` → an explicit commit range.
- `#<N>` (e.g. `#152`) → a single PR.
- `milestone <N>` → all closed PRs in a milestone (for retrospective sweeps).

Plus the **current session's conversation context** — the skill reads its
own caller transcript for process-meta items. If the conversation history
is unavailable (cold invocation), it proceeds with PR audit only and notes
the limitation in the report.

## Process

Seven steps. Steps 2–4 fan out in parallel; steps 5–7 are sequential.

### Step 1: Preflight + scope detection

One Bash call:

```bash
command -v gh >/dev/null || echo "MISSING: gh (https://cli.github.com/)"
gh auth status >/dev/null 2>&1 || echo "MISSING: gh auth (run 'gh auth login')"
git rev-parse --git-dir >/dev/null 2>&1 || echo "MISSING: not in a git repo"
```

Resolve the scope:

- **No arg**: `git log origin/main..HEAD --pretty=oneline` → list of commits;
  `git status --short` → uncommitted changes if any.
- **`<base>..HEAD`**: `git log <range> --pretty=oneline`.
- **`#<N>`**: `gh pr view <N> --json mergeCommit,closingIssuesReferences`.
- **`milestone <N>`**: `gh pr list --state merged --search "milestone:<N>" --json number,mergeCommit,closingIssuesReferences,title`.

Print scope summary and proceed.

### Step 2: Per-PR acceptance audit (parallel agents)

For each PR in scope, spawn one `Explore` agent with this prompt template:

> Audit PR #N (commit `<SHA>`) against the issue it closes. Repo at `<cwd>`,
> currently on `<branch>` at `<HEAD>`.
>
> 1. `gh pr view N --json title,body,closingIssuesReferences` → identify the closed issue.
> 2. `gh issue view <issue-N> --json title,body` → read the acceptance section.
> 3. `git show --stat <SHA>` → file list. `git show <SHA> -- <path>` for spot checks.
>
> For each acceptance bullet, mark **DELIVERED**, **MISSING**, or **PARTIAL** with file:line evidence.
>
> Scan the diff for architectural-shortcut signatures. Use the repo's own policy file if one exists (look for `ARCHITECTURE.md`, `CONTRIBUTING.md`, `CONVENTIONS.md`, or a similarly-named top-level conventions doc); otherwise scan for this generic list:
> - Suppression attributes introduced in this diff (`#[allow(...)]`, `#[ignore]`, `#[deprecated]`, `// eslint-disable`, `# noqa`, `# type: ignore`, etc.)
> - Rename-instead-of-delete naming (`Legacy*` / `Internal*` / `*Block` / `*Old` / `*V1` carrying behaviour rather than replacing it)
> - `#[serde(default)]` (or language-equivalent default-on-missing) attached to fields that are mandatory after a rename
> - Tombstone comments (TODO / FIXME / XXX / "removed X because Y" left in source instead of in the commit message)
> - Parallel new+old path coexistence — both code paths surviving the closing commit
> - Tests reduced to weaker assertions (e.g. `assert!(true)`, `expect(true).toBe(true)`) instead of being deleted or fixed
> - Ignored / skipped tests that were verifying a cutover
>
> Identify:
> - **Surface widenings not in PR description** — new public items, exported symbols, fields, type aliases, enum variants, trait/interface methods the PR body doesn't mention.
> - **Deferred work mentioned in PR/commit but not filed as follow-up** — grep for "follow-up", "next sub-issue", "later", "deferred", "for now", "TODO". For each, check if a tracking issue exists via `gh issue list --search`.
> - **Doc updates promised in acceptance but not made** (user-facing docs, code comments, decision records). For each missing doc update, identify the exact file:line that would need editing — this lets the caller decide whether to FIX-NOW or defer.
>
> Report under 600 words in this structured format:
> ```
> ACCEPTANCE
> - [bullet]: DELIVERED|MISSING|PARTIAL — evidence
> SHORTCUTS
> - (none) OR file:line — pattern — excerpt
> SURFACE WIDENINGS NOT IN PR
> - (none) OR file:line — symbol — why notable
> DEFERRED-WITHOUT-FOLLOWUP
> - (none) OR description (existing issue # if tracked)
> DOC GAPS
> - (none) OR file path — what's missing — fix-now feasible? (yes/no with one-line reason)
> OVERALL
> One sentence.
> ```
>
> Cite file:line for every claim.

Cap: 5 agents in parallel. For scopes >5 PRs, run in waves of 5.

### Step 3: Session conversation review (Claude native)

Read the **current session's conversation transcript** for process-meta
items that wouldn't appear in any PR diff. Six categories:

1. **Sizing/spec deviations the session decided on** — anything where the
   acceptance text said X and the work landed Y for a justifiable reason
   (test sizing math, schema-width adjustments, etc.).
2. **Pre-existing issues noticed in passing** — warnings the CI suite
   surfaced, brittle assertions, naming smells, dead code spotted en
   route. These pre-date the current PR and aren't its responsibility,
   but they're now logged in Claude's context and risk being lost.
3. **Architectural observations** — "we discovered that X behaves like
   Y" insights that surfaced during testing or debugging. Often the
   user's mental model differed from code reality, and the divergence
   itself is worth documenting.
4. **Ideas / future work** — "we could also do Z" comments that came up
   but weren't acted on.
5. **Process learnings** — methodology insights worth preserving in a
   playbook, contributing guide, or AI-assistant context file (e.g.
   "always dry-run sizing before committing test fixtures"). These
   don't always need a GH issue; sometimes they belong in repo docs.
6. **Convention drift** — places where the session noticed the codebase
   doesn't follow its own stated conventions.

For each item, capture:
- What it is (one sentence)
- Where it came from (which part of the session — file, test run, agent report)
- Whether it's already in the PR body, an existing issue, or unlogged
- **Fix-now feasibility** — could this be resolved with a single small
  edit in this session, or does it need scoping/design/test work?

If the conversation transcript is unavailable (e.g. skill invoked fresh
without prior session context), skip this step and note the limitation.

### Step 4: Cross-reference open GH issues

For every candidate finding from steps 2 + 3 that is **not** already
clearly FIX-NOW (skip GH search for trivial in-tree doc edits — they
won't have an issue):

```bash
# Search by keyword (per finding)
gh issue list --repo <owner>/<repo> --state open --search "<keyword>" --json number,title,milestone,labels --limit 20
```

Classify each finding:

- **Already covered** — an open issue's acceptance text already covers it.
  Action: none (or, if the finding adds context, comment on that issue).
- **Partially covered** — an open issue is in the right area but doesn't
  capture this specific case. Action: post a comment with the new context.
- **Novel** — no existing issue covers it. Action: propose a new issue
  *unless* it qualifies as FIX-NOW (see Step 5).

For each "novel" candidate, also determine **milestone mapping**:

```bash
gh api repos/<owner>/<repo>/milestones --jq '.[] | "\(.number): \(.title) — \(.description)"'
```

Match by theme:
- Existing milestone whose title/description aligns → propose under it.
- No existing milestone matches → either propose under no-milestone
  (chore label) or, if **multiple novel findings cluster around a single
  unaddressed theme**, suggest a **new milestone** with a draft title +
  scope blurb.

Pattern for suggesting a new milestone: three or more findings sharing a
theme not covered by any existing milestone is the trigger. Examples:
"observability: tracing + structured logs", "concurrency: thread-pool
sizing + back-pressure tuning".

### Step 5: Triage each finding — FIX-NOW vs defer

For every candidate from Steps 2 + 3, apply the criteria in the **Fix-now
vs defer** section above:

- Is it small (single file, <20 LOC, doc-only or trivially-safe)?
- Is the correct fix obvious (no design decision required)?
- Will it leave tests green without modification?
- Is it in-scope for the current sprint (versus pre-existing housekeeping)?

If all four are yes → **FIX-NOW**. Otherwise → defer (file, comment, or
suggest milestone).

Tag each finding with one disposition: `FIX-NOW`, `COMMENT #<N>`,
`NEW ISSUE`, `MILESTONE SUGGEST`, or `NO-OP` (already covered).

### Step 6: Synthesize action list

Produce a single structured proposal with four sections, in this order:

```markdown
## Proposed actions

### Fix in place this session (J items)
- **docs/user-guide/errors.md**: add row for new diagnostic E319 — 1-line table entry.
  Evidence: PR #152 introduced E319 in src/diagnostics.rs:212; the docs page lists E101–E318.
  Diff preview: `| E319 | explain | new diagnostic ... |`
- **src/lib.rs:88**: strip stale internal-task reference from doc comment.
  Diff preview: `- /// Wave 7c additions: ...` → `- /// Public lexer + parser entry points.`
- ...

### Comments to post (M items)
- **#125**: clarification on how Source-vs-Route admission works, surfaced during testing. [paragraph]
- ...

### New issues to file (N items)
- **Title**: chore: remove unused 'primary' parameters in test helpers
  **Milestone**: core: module decomposition (#3)
  **Labels**: area:core, enhancement, P2
  **Body preview**: [first 200 chars]
- ...

### Milestone suggestions (K items)
- **Proposed**: observability: structured tracing + metrics surface
  **Rationale**: 3 findings during the audit clustered around tracing
    gaps in dispatch, the lack of structured spans across operators,
    and the absence of metric integration tests.
  **Candidate scope**: [3-4 bullets]
- ...
```

The **Fix in place** section comes first because those edits should land
in the sprint's closing commit (or a small follow-on commit on the same
branch) before the GH writes happen. Deferring small fixes to issues
fragments work that could have closed cleanly in one commit.

### Step 7: Confirm and execute

Present the action list with `AskUserQuestion`. Use one question per
section so the user can accept/reject each section independently:

1. **Fix-in-place edits** — multi-select; user can opt out of individual
   edits. Default: accept all. On accept, apply each edit via `Edit` (or
   `Write` for new files), then run a minimal sanity check appropriate
   to what was touched:
   - Markdown / plain-text doc edited → no build needed.
   - Source code edited → run a fast, narrowly-scoped build / type-check
     for the touched module only (e.g. `cargo check -p <crate>`,
     `tsc --noEmit -p <project>`, `pyright <file>`); avoid full-suite
     reruns inside the skill.
   - Comment-only edits → no check.
   Report each applied edit by path.
2. **Comments to post** — multi-select. On accept, execute via `gh issue comment`.
3. **New issues to file** — multi-select; allow per-item edits to title /
   labels / milestone. On accept, execute via `gh issue create`.
4. **Milestone suggestions** — confirm individually. On accept, create
   the milestone, then file the clustered issues under it.

Execute each section sequentially. Fix-in-place first (so the sprint's
closing commit can include them), then GH writes.

```bash
# New issue
gh issue create --repo <owner>/<repo> --title "<title>" --milestone "<title>" --label "<label>" --label "<label>" --body "$(cat <<'EOF'
<body>
EOF
)"

# Comment
gh issue comment <N> --repo <owner>/<repo> --body "$(cat <<'EOF'
<comment>
EOF
)"

# New milestone (only when proposed and accepted)
gh api repos/<owner>/<repo>/milestones --method POST --field title="<title>" --field description="<description>"
```

**Use `--milestone <title>` not `<number>`** — `gh issue create` accepts
the title but errors on numeric IDs in some versions.

**Do not commit fix-in-place edits inside this skill.** Leave the edited
files in the working tree; the caller (or the sprint-close commit step)
folds them in. This keeps the skill read-only on git history.

Report each applied edit and each created issue/comment URL back to the
user.

## Output discipline

The skill's user-facing output is structured for scan-readability:

- Open with a one-paragraph **executive summary**: how many PRs audited,
  how many candidate findings, how many proposed in-place fixes vs
  deferred actions.
- Then the structured action list (fix-in-place first).
- Then the per-PR findings as collapsible / scannable detail (cite
  file:line throughout).
- Close with the confirmation question for fix-in-place.

**No skill names, no slash-command references, no internal session
jargon in any GH issue body or comment.** PR comments and issues are
public — they must read self-contained for external visitors who arrive
via a search-engine link with no prior context. Apply this to every
issue body and comment the skill writes, not just the first one.

## Edge cases

- **No closing issue on a PR** — the PR didn't have `Closes #N` or
  `Fixes #N`. Skip acceptance comparison for that PR; still scan for
  shortcuts and surface widenings. Note in the report.
- **Closed issue with no acceptance section** — fall back to the
  "Proposed change" or "Motivation" sections as the de-facto spec.
- **Milestone not in numbered format** — `gh api repos/.../milestones`
  returns by title; pass titles to `--milestone`, not numbers.
- **No in-repo shortcut-signature policy file** — use the generic
  signature list in the Step 2 prompt above.
- **Conversation transcript unavailable** — skip step 3, note the
  limitation in the report, proceed with steps 2 + 4–7 only. This is
  the cold-invocation mode (retrospective audits with no live session).
- **User declines all actions** — print the would-be actions as a
  digest the user can paste elsewhere later, then exit cleanly. Don't
  delete the analysis just because no writes happened.
- **Fix-in-place edit fails its sanity check** (the scoped build /
  type-check errors out) — revert the edit, demote the finding to NEW
  ISSUE, surface the failure to the user. Do not push past a failing
  check.
- **Working tree has unrelated dirty files at invocation** — proceed,
  but warn the user that fix-in-place edits will land on top of the
  existing dirty state and may need to be committed separately.

## Anti-patterns

- **Don't fold pre-existing housekeeping into fix-in-place.** A typo in
  a doc page is fair game; a dead module that's been rotting for six
  months is a chore issue, not a sprint-close edit.
- **Don't bundle distinct findings into one mega-issue.** Each
  architectural concern, each housekeeping item, each spec deviation
  gets its own issue. Three small chore issues > one omnibus chore.
- **Don't propose a new milestone for a single finding.** Three or more
  clustered findings is the bar.
- **Don't write GH issue bodies that reference this skill, the session
  that surfaced the finding, or any slash command.** External readers
  must understand the issue cold.
- **Don't auto-execute** — every fix-in-place edit and every GH write
  goes through user confirmation. This skill makes changes; it does
  not silently make them.
- **Don't re-litigate the PR after the audit.** If a finding turns out
  to be a misread (the PR did deliver X, you missed it), drop it from
  the action list; don't argue the point.
- **Don't expand a FIX-NOW edit beyond its scope.** If a typo fix
  reveals that the surrounding paragraph is also wrong, file a follow-up
  rather than rewriting the section inside this skill.

## Example invocation

```
/post-impl-followup
```

→ Audits the current session's changes (working tree vs `origin/main`),
reads the conversation transcript, proposes in-place fixes and deferred
actions.

```
/post-impl-followup #152
```

→ Audits a single PR retroactively. Fix-in-place edits are still
candidates (e.g. a missed doc row) even on a merged PR.

```
/post-impl-followup milestone 1
```

→ Audits every closed PR in milestone 1 (the retrospective sweep mode).
