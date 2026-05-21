# Synthesize and execute (Steps 6–7)

## Contents

- Action-list template
- Confirmation flow
- Command reference
- Edge cases
- Anti-patterns

## Action-list template

```markdown
## Proposed actions

### Fix in place this session (J items)
- **path:line**: what changes — 1-line evidence.
  Diff preview: `<before>` → `<after>`
- ...

### Comments to post (M items)
- **#N**: short summary. [paragraph body]
- ...

### New issues to file (N items)
- **Title**: ...
  **Milestone**: ...
  **Labels**: ...
  **Body preview**: [first 200 chars]
- ...

### Milestone suggestions (K items)
- **Proposed**: <title>
  **Rationale**: 3+ findings clustered on this unaddressed theme.
  **Candidate scope**: [3-4 bullets]
- ...
```

Fix-in-place comes first so trivial edits land in the closing commit
rather than as fragmented follow-ups.

## Confirmation flow

One `AskUserQuestion` per section. Multi-select; user can opt out per
item. Execute sequentially:

### 1. Fix-in-place edits

On accept, apply each via `Edit` (or `Write` for new files), then run a
narrowly-scoped sanity check:

- Markdown / plain-text doc → no check.
- Source code → fast scoped type-check for the touched module only:
  `cargo check -p <crate>` / `tsc --noEmit -p <project>` /
  `pyright <file>` / `go build ./<pkg>`. Avoid full-suite reruns inside
  the skill.
- Comment-only edits → no check.

Report each applied edit by path.

### 2. Comments to post

```bash
gh issue comment <N> --repo <owner>/<repo> --body "$(cat <<'EOF'
<comment>
EOF
)"
```

### 3. New issues to file

```bash
gh issue create --repo <owner>/<repo> \
  --title "<title>" \
  --milestone "<title>" \
  --label "<label>" --label "<label>" \
  --body "$(cat <<'EOF'
<body>
EOF
)"
```

Pass milestone **titles**, not numeric IDs — `gh issue create` errors
on numbers in some versions. Per-item edits to title / labels /
milestone are permitted in the confirmation.

### 4. Milestone suggestions

Confirm individually. On accept:

```bash
gh api repos/<owner>/<repo>/milestones --method POST \
  --field title="<title>" \
  --field description="<description>"
```

Then file the clustered issues under it.

## Edge cases

- **No closing issue on a PR** — skip acceptance comparison for that PR;
  still scan for shortcuts and surface widenings. Note in the report.
- **Closed issue with no acceptance section** — fall back to the
  "Proposed change" or "Motivation" sections as the de-facto spec.
- **Milestone not in numbered format** — pass title to `--milestone`,
  not the number.
- **No in-repo shortcut-signature policy file** — use the generic list
  in `audit-prompt.md`.
- **Conversation transcript unavailable** (cold invocation) — skip
  Step 2, note the limitation, proceed with Steps 3 + 4–7 only. This
  loses the skill's primary purpose; the run becomes a PR-audit-only
  pass.
- **Fix-in-place edit fails its sanity check** — revert the edit,
  demote the finding to NEW ISSUE, surface the failure. Don't push past
  a failing check.
- **Working tree dirty at invocation** — proceed but warn that
  fix-in-place edits will land on top of the existing dirty state and
  may need to be committed separately.
- **User declines all actions** — print the would-be actions as a
  digest the user can paste later, exit cleanly. Don't drop the
  analysis just because no writes happened.

## Anti-patterns

- **Don't fold pre-existing housekeeping into fix-in-place.** A typo in
  a doc page is fair game; a dead module that's been rotting for six
  months is a chore issue, not a sprint-close edit.
- **Don't bundle distinct findings into a mega-issue.** One issue per
  concern. Three small chore issues beat one omnibus chore.
- **Don't propose a new milestone for a single finding.** Three or more
  clustered findings is the bar.
- **Don't write GH bodies that reference this skill, the session, or
  any slash command.** External readers must understand the issue cold.
- **Don't auto-execute.** Every fix-in-place edit and every GH write
  goes through confirmation.
- **Don't re-litigate.** If a finding turns out to be a misread (the
  PR did deliver X, you missed it), drop it; don't argue.
- **Don't expand a FIX-NOW edit beyond its scope.** If a typo fix
  reveals the surrounding paragraph is also wrong, file a follow-up
  rather than rewriting the section inline.
- **Don't commit fix-in-place edits inside the skill.** Leave them in
  the working tree; the closing-commit step folds them in.
