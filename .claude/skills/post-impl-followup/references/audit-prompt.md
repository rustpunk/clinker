# Per-PR audit prompt (Step 3)

Spawn one `Explore` agent per PR with this template:

> Audit PR #N (commit `<SHA>`) against the issue it closes. Repo at
> `<cwd>`, currently on `<branch>` at `<HEAD>`.
>
> 1. `gh pr view N --json title,body,closingIssuesReferences` → identify the closed issue.
> 2. `gh issue view <issue-N> --json title,body` → read the acceptance section.
> 3. `git show --stat <SHA>` → file list. `git show <SHA> -- <path>` for spot checks.
>
> For each acceptance bullet, mark **DELIVERED**, **MISSING**, or
> **PARTIAL** with file:line evidence.
>
> Scan the diff for **architectural-shortcut signatures**. Use the
> repo's own policy file if one exists (look for `ARCHITECTURE.md`,
> `CONTRIBUTING.md`, `CONVENTIONS.md`, or a similarly-named top-level
> conventions doc); otherwise scan for this generic list:
>
> - Suppression attributes introduced in this diff: `#[allow(...)]`,
>   `#[ignore]`, `#[deprecated]`, `// eslint-disable`, `# noqa`,
>   `# type: ignore`, `@SuppressWarnings`, equivalents.
> - Rename-instead-of-delete naming: `Legacy*` / `Internal*` /
>   `*Block` / `*Old` / `*V1` carrying behaviour rather than
>   replacing it.
> - `#[serde(default)]` (or language-equivalent default-on-missing)
>   attached to fields that are mandatory after a rename.
> - Tombstone comments: TODO / FIXME / XXX / "removed X because Y"
>   left in source instead of the commit message.
> - Parallel new+old path coexistence — both surviving the closing
>   commit.
> - Tests reduced to weaker assertions (`assert!(true)`,
>   `expect(true).toBe(true)`, `assert True`) instead of being deleted
>   or fixed.
> - Ignored / skipped tests that were verifying a cutover.
>
> Identify additionally:
>
> - **Surface widenings not in PR description** — new public items,
>   exported symbols, fields, type aliases, enum variants,
>   trait/interface methods the PR body doesn't mention.
> - **Deferred work mentioned in PR/commit but not filed as a
>   follow-up** — grep for "follow-up", "next sub-issue", "later",
>   "deferred", "for now", "TODO". For each match, check `gh issue
>   list --search` for a tracking issue.
> - **Doc updates promised in acceptance but not made** — for each
>   missing update, identify the exact file:line that would need
>   editing. This lets the caller decide FIX-NOW vs defer.
>
> Report under 600 words in this structured format:
>
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
> - (none) OR file path — what's missing — fix-now feasible? (y/n with one-line reason)
> OVERALL
> One sentence.
> ```
>
> Cite file:line for every claim.

For scopes >5 PRs, run in waves of 5 agents.
