#!/usr/bin/env bash
#
# Delete local branches whose pull request has already merged.
#
# Why this exists rather than `git branch --merged`: the repository allows only
# squash merges, so a branch that has fully landed shares no commit with main
# and every ancestry-based check reports it as unmerged. `git branch --merged`
# lists nothing here no matter how much has landed, which is why stale branches
# accumulate unnoticed. Pull request state is the usable ground truth, so that
# is what this script reads.
#
# Dry run by default. Nothing is deleted without --apply.

set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd -P)
APPLY=0

usage() {
  printf 'usage: %s [--apply]\n' "${0##*/}" >&2
  printf '  --apply   delete the branches; without it, only report them\n' >&2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --apply)
      APPLY=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      usage
      exit 2
      ;;
  esac
done

command -v gh >/dev/null 2>&1 || {
  echo "prune-landed-branches: gh is required to read pull request state" >&2
  exit 1
}

cd "$REPO_ROOT" || exit 1

CURRENT=$(git rev-parse --abbrev-ref HEAD)

# One API call for every pull request, then a branch -> state lookup. Querying
# per branch would issue one request per branch and rate-limit on a large repo.
PR_STATE=$(gh pr list --state all --limit 1000 \
  --json headRefName,number,state \
  --jq '.[] | "\(.headRefName)\t\(.state)\t\(.number)"') || {
  echo "prune-landed-branches: could not read pull requests" >&2
  exit 1
}

landed=0
kept=0

while IFS= read -r branch; do
  [ -n "$branch" ] || continue
  [ "$branch" = "main" ] && continue

  # A branch checked out in any worktree cannot be deleted; remove the worktree
  # first. Reporting it as kept is more useful than failing the whole run.
  if [ "$branch" = "$CURRENT" ]; then
    kept=$((kept + 1))
    continue
  fi
  if git worktree list --porcelain | grep -qx "branch refs/heads/$branch"; then
    printf 'keep    %-55s checked out in a worktree\n' "$branch"
    kept=$((kept + 1))
    continue
  fi

  states=$(printf '%s\n' "$PR_STATE" | awk -F'\t' -v b="$branch" '$1 == b { print $2 }')

  if [ -z "$states" ]; then
    printf 'keep    %-55s no pull request\n' "$branch"
    kept=$((kept + 1))
    continue
  fi
  # An open pull request outranks a merged one: the same head branch can carry a
  # follow-up after an earlier one merged, and that work is still in flight.
  if printf '%s\n' "$states" | grep -qx OPEN; then
    printf 'keep    %-55s pull request still open\n' "$branch"
    kept=$((kept + 1))
    continue
  fi
  if ! printf '%s\n' "$states" | grep -qx MERGED; then
    printf 'keep    %-55s pull request closed without merging\n' "$branch"
    kept=$((kept + 1))
    continue
  fi

  num=$(printf '%s\n' "$PR_STATE" | awk -F'\t' -v b="$branch" '$1 == b && $2 == "MERGED" { print $3; exit }')
  landed=$((landed + 1))
  if [ "$APPLY" -eq 1 ]; then
    git branch -D "$branch" >/dev/null && printf 'deleted %-55s #%s\n' "$branch" "$num"
  else
    printf 'landed  %-55s #%s\n' "$branch" "$num"
  fi
done < <(git for-each-ref --format='%(refname:short)' refs/heads)

echo
if [ "$APPLY" -eq 1 ]; then
  git worktree prune
  printf 'deleted %d branch(es), kept %d\n' "$landed" "$kept"
else
  printf '%d branch(es) have merged and can be deleted, %d kept — rerun with --apply\n' \
    "$landed" "$kept"
fi
