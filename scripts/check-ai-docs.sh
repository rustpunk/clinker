#!/usr/bin/env bash

set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd -P)
SUBJECT_ROOT=$REPO_ROOT
MODE=scan
RENDERED_ROOTS=()

usage() {
  printf 'usage: %s [--self-test | --check-open-questions | --check-rendered-links DIR...]\n' "${0##*/}" >&2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --self-test)
      MODE=self-test
      shift
      ;;
    --check-open-questions)
      MODE=open-questions
      shift
      ;;
    --check-rendered-links)
      MODE=rendered-links
      shift
      while [ "$#" -gt 0 ]; do
        RENDERED_ROOTS+=("$1")
        shift
      done
      if [ "${#RENDERED_ROOTS[@]}" -eq 0 ]; then
        usage
        exit 2
      fi
      ;;
    --subject-root)
      [ "$#" -ge 2 ] || {
        usage
        exit 2
      }
      SUBJECT_ROOT=$2
      shift 2
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

failure_count=0
SELF_TEST_ROOT=
SELF_TEST_OUTSIDE_ROOT=

cleanup_self_test() {
  [ -z "$SELF_TEST_ROOT" ] || rm -rf -- "$SELF_TEST_ROOT"
  [ -z "$SELF_TEST_OUTSIDE_ROOT" ] || rm -rf -- "$SELF_TEST_OUTSIDE_ROOT"
}

trap cleanup_self_test EXIT

report_failure() {
  local file=$1
  local rule=$2
  local correction=$3
  printf 'FAIL: %s: %s. Fix: %s\n' "$file" "$rule" "$correction" >&2
  failure_count=$((failure_count + 1))
}

repo_relative() {
  local path=$1
  case "$path" in
    "$SUBJECT_ROOT") printf '.\n' ;;
    "$SUBJECT_ROOT"/*) printf '%s\n' "${path#"$SUBJECT_ROOT"/}" ;;
    *) printf '<outside-repository>\n' ;;
  esac
}

resolve_portable_path() {
  local input=$1
  local remaining
  local component
  local resolved=/
  local candidate
  local link_target
  local link_count=0

  case "$input" in
    /*) remaining=${input#/} ;;
    *) remaining=${PWD#/}/$input ;;
  esac

  while [ -n "$remaining" ]; do
    component=${remaining%%/*}
    if [ "$remaining" = "$component" ]; then
      remaining=
    else
      remaining=${remaining#*/}
    fi

    case "$component" in
      ''|.) ;;
      ..)
        if [ "$resolved" != / ]; then
          resolved=${resolved%/*}
          [ -n "$resolved" ] || resolved=/
        fi
        ;;
      *)
        if [ "$resolved" = / ]; then
          candidate=/$component
        else
          candidate=$resolved/$component
        fi

        if [ -L "$candidate" ]; then
          link_count=$((link_count + 1))
          [ "$link_count" -le 40 ] || return 1
          link_target=$(readlink "$candidate") || return 1
          if [ -n "$remaining" ]; then
            remaining=$link_target/$remaining
          else
            remaining=$link_target
          fi
          case "$link_target" in
            /*)
              resolved=/
              remaining=${remaining#/}
              ;;
          esac
        else
          resolved=$candidate
        fi
        ;;
    esac
  done

  if [ -d "$resolved" ]; then
    (CDPATH= cd -P -- "$resolved" && pwd -P)
  else
    printf '%s\n' "$resolved"
  fi
}

github_slug() {
  sed -E \
    -e 's/<[^>]*>//g' \
    -e 's/[`*_~]//g' \
    -e 's/[[:space:]]+#+[[:space:]]*$//' \
    -e 's/[^[:alnum:] _-]//g' \
    -e 's/[[:space:]]+/-/g' \
    | tr '[:upper:]' '[:lower:]'
}

fragment_exists() {
  local file=$1
  local expected=$2
  local in_fence=0
  local line
  local heading_marks
  local heading_text
  local base_slug
  local candidate
  local count
  local item
  local -a base_slugs=()
  local -a base_counts=()

  expected=${expected#\#}
  expected=${expected//%20/-}

  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      \`\`\`*|~~~*)
        if [ "$in_fence" -eq 0 ]; then
          in_fence=1
        else
          in_fence=0
        fi
        continue
        ;;
    esac
    [ "$in_fence" -eq 0 ] || continue

    if [[ "$line" == *"id=\"$expected\""* || "$line" == *"id='$expected'"* ]]; then
      return 0
    fi

    if [[ "$line" =~ ^(#{1,6})[[:space:]]+(.+)$ ]]; then
      heading_marks=${BASH_REMATCH[1]}
      heading_text=${BASH_REMATCH[2]}
      base_slug=$(printf '%s\n' "$heading_text" | github_slug)
      [ -n "$base_slug" ] || continue

      count=0
      for item in "${!base_slugs[@]}"; do
        if [ "${base_slugs[$item]}" = "$base_slug" ]; then
          count=${base_counts[$item]}
          base_counts[$item]=$((count + 1))
          break
        fi
      done
      if [ "$count" -eq 0 ]; then
        candidate=$base_slug
        if [ "${#base_slugs[@]}" -eq 0 ] || ! printf '%s\n' "${base_slugs[@]}" | grep -Fxq -- "$base_slug"; then
          base_slugs+=("$base_slug")
          base_counts+=(1)
        fi
      else
        candidate="$base_slug-$count"
      fi

      if [ "$candidate" = "$expected" ]; then
        return 0
      fi
    fi
  done < "$file"

  return 1
}

check_local_reference() {
  local source_file=$1
  local relative=$2
  local line_number=$3
  local target=$4
  local target_path
  local fragment=
  local resolved

  case "$target" in
    http://*|https://*|mailto:*|tel:*|ftp://*) return ;;
  esac

  if [[ "$target" == *#* ]]; then
    fragment=${target#*#}
  fi
  target_path=${target%%#*}
  target_path=${target_path%%\?*}

  if [ -z "$target_path" ]; then
    resolved=$source_file
  elif [[ "$target_path" = /* ]]; then
    resolved=$(resolve_portable_path "$SUBJECT_ROOT/${target_path#/}")
  else
    resolved=$(resolve_portable_path "$(dirname -- "$source_file")/$target_path")
  fi

  case "$resolved" in
    "$SUBJECT_ROOT"|"$SUBJECT_ROOT"/*) ;;
    *)
      report_failure "$relative:$line_number" \
        "local reference '$target' escapes the repository" \
        "use a repository-relative target"
      return
      ;;
  esac

  if [ ! -e "$resolved" ]; then
    report_failure "$relative:$line_number" \
      "local reference '$target' has a missing target" \
      "create '$(repo_relative "$resolved")' or correct the link"
    return
  fi

  if [ -n "$fragment" ] && [ -f "$resolved" ] && ! fragment_exists "$resolved" "$fragment"; then
    report_failure "$relative:$line_number" \
      "local reference '$target' has a missing GitHub heading fragment '$fragment'" \
      "use a heading fragment that exists in '$(repo_relative "$resolved")'"
  fi
}

rendered_fragment_exists() {
  local file=$1
  local fragment=$2

  grep -Fq -- "id=\"$fragment\"" "$file" \
    || grep -Fq -- "id='$fragment'" "$file" \
    || grep -Fq -- "name=\"$fragment\"" "$file" \
    || grep -Fq -- "name='$fragment'" "$file"
}

check_rendered_reference() {
  local rendered_root=$1
  local source_file=$2
  local relative=$3
  local line_number=$4
  local target=$5
  local target_path
  local fragment=
  local resolved

  case "$target" in
    http://*|https://*|mailto:*|tel:*|ftp://*|data:*|javascript:*|//* ) return ;;
  esac

  if [[ "$target" == *#* ]]; then
    fragment=${target#*#}
    fragment=${fragment%%\?*}
  fi
  target_path=${target%%#*}
  target_path=${target_path%%\?*}
  target_path=${target_path//%20/ }

  if [ -z "$target_path" ]; then
    resolved=$source_file
  elif [[ "$target_path" = /* ]]; then
    resolved=$(resolve_portable_path "$rendered_root/${target_path#/}")
  else
    resolved=$(resolve_portable_path "$(dirname -- "$source_file")/$target_path")
  fi

  case "$resolved" in
    "$rendered_root"|"$rendered_root"/*) ;;
    *)
      report_failure "$relative:$line_number" \
        "rendered local reference '$target' escapes the book output" \
        "use a target inside the rendered book or a published absolute URL"
      return
      ;;
  esac

  if [ -d "$resolved" ]; then
    resolved=$resolved/index.html
  fi
  if [ ! -f "$resolved" ]; then
    report_failure "$relative:$line_number" \
      "rendered local reference '$target' has a missing target" \
      "correct the source link or include the target in the rendered book"
    return
  fi

  if [ -n "$fragment" ] && ! rendered_fragment_exists "$resolved" "$fragment"; then
    report_failure "$relative:$line_number" \
      "rendered local reference '$target' has a missing HTML fragment '$fragment'" \
      "use an id or named anchor emitted in the rendered target"
  fi
}

scan_rendered_html_file() {
  local rendered_root=$1
  local file=$2
  local relative
  local line
  local line_number=0
  local rest
  local target
  local double_href='href="([^"]+)"'
  local single_href="href='([^']+)'"

  relative="${rendered_root##*/}/${file#"$rendered_root"/}"
  while IFS= read -r line || [ -n "$line" ]; do
    line_number=$((line_number + 1))

    rest=$line
    while [[ "$rest" =~ $double_href ]]; do
      target=${BASH_REMATCH[1]}
      rest=${rest#*"${BASH_REMATCH[0]}"}
      check_rendered_reference "$rendered_root" "$file" "$relative" "$line_number" "$target"
    done

    rest=$line
    while [[ "$rest" =~ $single_href ]]; do
      target=${BASH_REMATCH[1]}
      rest=${rest#*"${BASH_REMATCH[0]}"}
      check_rendered_reference "$rendered_root" "$file" "$relative" "$line_number" "$target"
    done
  done < "$file"
}

scan_markdown_file() {
  local file=$1
  local relative
  local in_fence=0
  local h1_count=0
  local previous_heading=0
  local line
  local line_number=0
  local heading_marks
  local heading_level
  local rest
  local target

  relative=$(repo_relative "$file")

  while IFS= read -r line || [ -n "$line" ]; do
    line_number=$((line_number + 1))

    case "$line" in
      \`\`\`*|~~~*)
        if [ "$in_fence" -eq 0 ]; then
          in_fence=1
        else
          in_fence=0
        fi
        continue
        ;;
    esac

    [ "$in_fence" -eq 0 ] || continue

    if [[ "$line" =~ ^(#{1,6})[[:space:]]+[^#[:space:]].*$ ]]; then
      heading_marks=${BASH_REMATCH[1]}
      heading_level=${#heading_marks}
      if [ "$heading_level" -eq 1 ]; then
        h1_count=$((h1_count + 1))
      fi
      if [ "$previous_heading" -gt 0 ] && [ "$heading_level" -gt $((previous_heading + 1)) ]; then
        report_failure "$relative:$line_number" \
          "heading level jumps from H$previous_heading to H$heading_level" \
          "insert the missing intermediate heading level"
      fi
      previous_heading=$heading_level
    fi

    rest=$line
    while [[ "$rest" =~ \[[^][]*\]\(([^()[:space:]]+)\) ]]; do
      target=${BASH_REMATCH[1]}
      rest=${rest#*"${BASH_REMATCH[0]}"}
      check_local_reference "$file" "$relative" "$line_number" "$target"
    done

    if [[ "$line" =~ ^[[:space:]]{0,3}\[[^][]+\]:[[:space:]]*(\<[^\>]+\>|[^[:space:]]+) ]]; then
      target=${BASH_REMATCH[1]}
      target=${target#<}
      target=${target%>}
      check_local_reference "$file" "$relative" "$line_number" "$target"
    fi
  done < "$file"

  if [ "$in_fence" -ne 0 ]; then
    report_failure "$relative" "contains an unclosed fenced code block" \
      "add the matching closing fence"
  fi
  if [ "$h1_count" -ne 1 ]; then
    report_failure "$relative" "contains $h1_count top-level headings; expected exactly one" \
      "keep one '# Title' heading and demote or add headings as needed"
  fi
}

validate_contract_evidence_value() {
  local line_number=$1
  local evidence=$2
  local remaining=$evidence
  local token
  local lower_token
  local resolved
  local formatted
  local valid_locator_count=0

  while [[ "$remaining" =~ \`([^\`]*)\` ]]; do
    token=${BASH_REMATCH[1]}
    remaining=${remaining#*"${BASH_REMATCH[0]}"}
    token=$(printf '%s\n' "$token" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')
    lower_token=$(printf '%s\n' "$token" | tr '[:upper:]' '[:lower:]')

    case "$lower_token" in
      ''|tbd|todo|none|n/a|unknown)
        report_failure "docs/ai/15_PRODUCTION_CONTRACTS.md:$line_number" \
          "contract row Evidence contains placeholder '$token'" \
          "replace the placeholder with a repository locator, validation command, or named source/test surface"
        return
        ;;
    esac

    case "$token" in
      /*|~*|file:*|[[:alpha:]]:[\\/]*)
        report_failure "docs/ai/15_PRODUCTION_CONTRACTS.md:$line_number" \
          "contract row Evidence locator '$token' is not repository-relative" \
          "use a repository-relative locator without a drive, scheme, home prefix, or leading slash"
        return
        ;;
    esac

    case "/$token/" in
      */../*)
        report_failure "docs/ai/15_PRODUCTION_CONTRACTS.md:$line_number" \
          "contract row Evidence locator '$token' contains parent traversal" \
          "use a direct repository-relative locator without '..' components"
        return
        ;;
    esac

    if [[ "$token" != *[[:space:]]* && ( "$token" == */* || "$token" == *.* ) ]]; then
      if [[ "$token" == *\\* ]]; then
        report_failure "docs/ai/15_PRODUCTION_CONTRACTS.md:$line_number" \
          "contract row Evidence locator '$token' is malformed" \
          "use forward-slash repository-relative locator syntax"
        return
      fi
      resolved=$(resolve_portable_path "$SUBJECT_ROOT/$token") || resolved=
      case "$resolved" in
        "$SUBJECT_ROOT"|"$SUBJECT_ROOT"/*) ;;
        *)
          report_failure "docs/ai/15_PRODUCTION_CONTRACTS.md:$line_number" \
            "contract row Evidence locator '$token' escapes the repository" \
            "use an existing target beneath the repository root"
          return
          ;;
      esac
      if [ ! -e "$resolved" ]; then
        report_failure "docs/ai/15_PRODUCTION_CONTRACTS.md:$line_number" \
          "contract row Evidence locator '$token' has a missing repository target" \
          "correct the locator or add the cited repository evidence"
        return
      fi
      valid_locator_count=$((valid_locator_count + 1))
    fi
  done

  case "$remaining" in
    *\`*)
      report_failure "docs/ai/15_PRODUCTION_CONTRACTS.md:$line_number" \
        "contract row Evidence contains unmatched backticks" \
        "close every Markdown code span"
      return
      ;;
  esac

  if [ "$valid_locator_count" -gt 0 ]; then
    return
  fi

  formatted=${evidence//\`/}
  formatted=$(printf '%s\n' "$formatted" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')
  if printf '%s\n' "$formatted" \
    | grep -Eq '^(cargo|bash|mdbook|git|rg|gh)[[:space:]]+[^[:space:]]'; then
    return
  fi
  if [[ "$formatted" == *[[:space:]]* ]] \
    && printf '%s\n' "$formatted" \
      | grep -Eqi '(code|config|corpus|dispatch|fields|graph|integration|manifests?|paths?|reference|registry|rules|search|seams?|source|staging|state|tests?|checks|docs|arenas?|D-[0-9][0-9])$'; then
    return
  fi

  report_failure "docs/ai/15_PRODUCTION_CONTRACTS.md:$line_number" \
    "contract row Evidence is not a repository locator, validation command, or named source/test surface" \
    "use an existing backticked repository-relative target or a specific multiword command or surface name"
}

check_contract_register() {
  local register=$SUBJECT_ROOT/docs/ai/15_PRODUCTION_CONTRACTS.md
  local relative=docs/ai/15_PRODUCTION_CONTRACTS.md
  local line_number
  local rule
  local correction
  local validation

  [ -f "$register" ] || return

  if ! grep -Fqx \
    '| Contract | Audience | Status | Observed now | Locked target | Requirement | Owner | Compatibility / reversibility | Evidence | Last verified |' \
    "$register"; then
    report_failure "$relative" \
      "contract table is missing the required status-aware schema" \
      "use columns: Contract, Audience, Status, Observed now, Locked target, Requirement, Owner, Compatibility / reversibility, Evidence, Last verified"
  fi

  if ! validation=$(awk -F'|' '
    function trim(value) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      return value
    }

    function emit(line_number, rule, correction) {
      printf "%d\t%s\t%s\n", line_number, rule, correction
    }

    function valid_separator(value) {
      value = trim(value)
      return value ~ /^:?-{3,}:?$/
    }

    function valid_status(value) {
      return value == "implemented" \
        || value == "partially-implemented" \
        || value == "locked-not-implemented" \
        || value == "deferred" \
        || value == "external-mutable"
    }

    function valid_owner(value) {
      return value ~ /(^|[^[:alnum:]])[A-Z][A-Z0-9]*-[0-9][0-9]([^[:alnum:]]|$)/ \
        || value ~ /(^|[^[:alnum:]])v[0-9]+([^[:alnum:]]|$)/
    }

    BEGIN {
      header = "| Contract | Audience | Status | Observed now | Locked target | Requirement | Owner | Compatibility / reversibility | Evidence | Last verified |"
      in_contract_table = 0
      expect_separator = 0
      unique_decisions = 0
    }

    $0 == header {
      in_contract_table = 1
      expect_separator = 1
      next
    }

    in_contract_table {
      if (expect_separator) {
        expect_separator = 0
        if (NF != 12) {
          emit(NR, "contract table separator does not have 10 columns", "add exactly 10 Markdown separator cells below the header")
        } else {
          invalid_separator = 0
          for (field = 2; field <= 11; field++) {
            if (!valid_separator($field)) {
              invalid_separator = 1
            }
          }
          if (invalid_separator) {
            emit(NR, "contract table separator contains an invalid cell", "use at least three hyphens, with optional edge colons, in each of the 10 separator cells")
          }
        }
        next
      }

      if ($0 !~ /^\|/) {
        in_contract_table = 0
        next
      }

      if (NF != 12) {
        emit(NR, "contract row does not match the 10-column schema", "supply exactly the 10 required contract fields")
        next
      }

      missing_field = 0
      for (field = 2; field <= 11; field++) {
        if (trim($field) == "") {
          missing_field = 1
        }
      }
      if (missing_field) {
        emit(NR, "contract row has a missing field", "supply every status, audience, observed, target, requirement, owner, compatibility, evidence, and verification field")
      }

      contract = trim($2)
      status = trim($4)
      requirement = trim($7)
      owner = trim($8)
      evidence = trim($10)
      verified = trim($11)

      if (!valid_status(status)) {
        emit(NR, "contract row has invalid Status " status, "use implemented, partially-implemented, locked-not-implemented, deferred, or external-mutable")
      }
      if (!valid_owner(owner)) {
        emit(NR, "contract row Owner does not name a requirement or version boundary", "name at least one owning requirement or version boundary")
      }
      if (verified !~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$/) {
        emit(NR, "contract row Last verified is not YYYY-MM-DD", "use an ISO-shaped date such as 2026-07-29")
      }

      if (contract !~ /^D-[0-9][0-9]$/) {
        emit(NR, "contract row first cell is not exactly one D-NN identifier: " contract, "use one identifier from D-00 through D-56")
        next
      }

      decision_number = substr(contract, 3) + 0
      if (decision_number > 56) {
        emit(NR, "contract row uses unknown decision identifier " contract, "use one identifier from D-00 through D-56")
        next
      }

      decision_count[contract]++
      if (decision_count[contract] == 1) {
        first_line[contract] = NR
        unique_decisions++
      }

      for (idx = 1; idx <= 8; idx++) {
        expected = sprintf("CONT-%02d", idx)
        pattern = "(^|[^[:alnum:]-])" expected "([^[:alnum:]-]|$)"
        if (requirement ~ pattern) {
          requirement_seen[expected] = 1
        }
      }
    }

    END {
      for (idx = 0; idx <= 56; idx++) {
        expected = sprintf("D-%02d", idx)
        if (decision_count[expected] == 0) {
          emit(0, "is missing decision row " expected, "add one complete contract-table row whose first cell is exactly " expected)
        } else if (decision_count[expected] > 1) {
          emit(first_line[expected], "decision row " expected " appears " decision_count[expected] " times", "keep exactly one complete row for " expected)
        }
      }

      if (unique_decisions != 57) {
        emit(0, "contains " unique_decisions " unique decision rows; expected exactly 57", "provide one row for every identifier from D-00 through D-56")
      }

      for (idx = 1; idx <= 8; idx++) {
        expected = sprintf("CONT-%02d", idx)
        if (!requirement_seen[expected]) {
          emit(0, "is missing " expected " coverage in the contract Requirement column", "add " expected " to the Requirement cell of its owning decision row")
        }
      }
    }
  ' "$register"); then
    report_failure "$relative" \
      "contract-table parser could not read the register" \
      "correct the checker or register syntax before accepting the documentation"
    return
  fi

  while IFS=$'\t' read -r line_number rule correction; do
    [ -n "$rule" ] || continue
    if [ "$line_number" -eq 0 ]; then
      report_failure "$relative" "$rule" "$correction"
    else
      report_failure "$relative:$line_number" "$rule" "$correction"
    fi
  done <<< "$validation"

  while IFS=$'\t' read -r line_number evidence; do
    [ -n "$evidence" ] || continue
    validate_contract_evidence_value "$line_number" "$evidence"
  done < <(awk -F'|' '
    function trim(value) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      return value
    }

    BEGIN {
      header = "| Contract | Audience | Status | Observed now | Locked target | Requirement | Owner | Compatibility / reversibility | Evidence | Last verified |"
      in_contract_table = 0
      expect_separator = 0
    }

    $0 == header {
      in_contract_table = 1
      expect_separator = 1
      next
    }

    in_contract_table {
      if (expect_separator) {
        expect_separator = 0
        next
      }
      if ($0 !~ /^\|/) {
        in_contract_table = 0
        next
      }
      if (NF == 12 && trim($10) != "") {
        printf "%d\t%s\n", NR, trim($10)
      }
    }
  ' "$register")
}

validate_ledger_entry() {
  local relative=$1
  local id=$2
  local body=$3
  local terminal_count
  local status

  terminal_count=$(printf '%s\n' "$body" \
    | grep -Ec '^[[:space:]]*-[[:space:]]+\**Status:\**[[:space:]]*(Resolved|Deferred)([^[:alpha:]]|$)' \
    || true)
  if [ "$terminal_count" -ne 1 ]; then
    report_failure "$relative" \
      "question $id has $terminal_count terminal status fields; expected exactly one" \
      "add exactly one '- Status: Resolved' or '- Status: Deferred' field"
    status=
  else
    status=$(printf '%s\n' "$body" \
      | grep -Eo 'Status:\**[[:space:]]*(Resolved|Deferred)' \
      | sed -E 's/.*(Resolved|Deferred)/\1/' \
      | head -n 1)
  fi

  if ! printf '%s\n' "$body" | grep -Eq '(^|[^[:alnum:]-])D-[0-9]{2}([^[:digit:]]|$)'; then
    report_failure "$relative" "question $id has no D-NN decision identifier" \
      "add the governing decision, for example '- Decision: D-01'"
  fi
  if ! printf '%s\n' "$body" | grep -Eq 'Evidence:\**[[:space:]]*[^[:space:]*]'; then
    report_failure "$relative" "question $id has no non-empty Evidence field" \
      "add '- Evidence: path/to/source or another source-backed citation'"
  fi
  if ! printf '%s\n' "$body" \
    | grep -Eq 'Implementation owner:\**[[:space:]].*([A-Z][A-Z0-9]*-[0-9]{2}|v[0-9]+)'; then
    report_failure "$relative" \
      "question $id has no Implementation owner naming a requirement or version boundary" \
      "add '- Implementation owner: REQ-NN'"
  fi
  if ! printf '%s\n' "$body" | grep -Eq 'Verified:\**[[:space:]]*[0-9]{4}-[0-9]{2}-[0-9]{2}([[:space:]]|$)'; then
    report_failure "$relative" "question $id has no YYYY-MM-DD Verified field" \
      "add '- Verified: YYYY-MM-DD'"
  fi
  if [ "$status" = Deferred ] \
    && ! printf '%s\n' "$body" | grep -Eq 'Reason:\**[[:space:]]*[^[:space:]*]'; then
    report_failure "$relative" "deferred question $id has no non-empty Reason field" \
      "add '- Reason: <evidence-backed reason for deferral>'"
  fi
}

check_open_questions() {
  local ledger=$SUBJECT_ROOT/docs/ai/80_OPEN_QUESTIONS.md
  local relative=docs/ai/80_OPEN_QUESTIONS.md
  local in_fence=0
  local line
  local line_number=0
  local current_id=
  local current_line=0
  local current_body=
  local entry_count=0
  local id
  local item
  local count
  local -a seen_ids=()
  local -a seen_counts=()
  local -a pinned_ids=(1 2 3 4 6 8 10 11 14 15 16 17 18 19 22 24 25 26 27 29 30 31 32 33)

  if [ ! -f "$ledger" ]; then
    report_failure "$relative" "open-question ledger is missing" \
      "restore the numbered decision ledger"
    return
  fi

  while IFS= read -r line || [ -n "$line" ]; do
    line_number=$((line_number + 1))
    case "$line" in
      \`\`\`*|~~~*)
        if [ "$in_fence" -eq 0 ]; then
          in_fence=1
        else
          in_fence=0
        fi
        ;;
    esac
    [ "$in_fence" -eq 0 ] || continue

    if [[ "$line" =~ ^###[[:space:]]+([0-9]+)\.([[:space:]]|$) ]]; then
      if [ -n "$current_id" ]; then
        validate_ledger_entry "$relative:$current_line" "$current_id" "$current_body"
      fi

      current_id=${BASH_REMATCH[1]}
      current_line=$line_number
      current_body=$line
      entry_count=$((entry_count + 1))

      count=0
      for item in "${!seen_ids[@]}"; do
        if [ "${seen_ids[$item]}" = "$current_id" ]; then
          count=${seen_counts[$item]}
          seen_counts[$item]=$((count + 1))
          break
        fi
      done
      if [ "$count" -eq 0 ]; then
        seen_ids+=("$current_id")
        seen_counts+=(1)
      else
        report_failure "$relative:$current_line" \
          "question identifier $current_id is duplicated" \
          "keep one numbered entry for question $current_id"
      fi
    elif [ -n "$current_id" ]; then
      current_body+=$'\n'$line
    fi
  done < "$ledger"

  if [ -n "$current_id" ]; then
    validate_ledger_entry "$relative:$current_line" "$current_id" "$current_body"
  fi

  if [ "$entry_count" -eq 0 ]; then
    report_failure "$relative" "contains no numbered question entries" \
      "preserve numbered headings in the resolved or deferred ledger"
  fi

  for id in "${pinned_ids[@]}"; do
    count=0
    for item in "${!seen_ids[@]}"; do
      if [ "${seen_ids[$item]}" = "$id" ]; then
        count=${seen_counts[$item]}
        break
      fi
    done
    if [ "$count" -eq 0 ]; then
      report_failure "$relative" "captured question identifier $id is missing or renumbered" \
        "restore question $id with its terminal metadata"
    elif [ "$count" -ne 1 ]; then
      report_failure "$relative" "captured question identifier $id appears $count times" \
        "keep exactly one entry numbered $id"
    fi
  done
}

finish_check() {
  local label=$1
  if [ "$failure_count" -ne 0 ]; then
    printf '%s failed with %d finding(s).\n' "$label" "$failure_count" >&2
    return 1
  fi
  printf '%s passed.\n' "$label"
}

run_scan() {
  local docs_root=$SUBJECT_ROOT/docs/ai
  local found=0
  local file

  failure_count=0
  if [ ! -d "$docs_root" ]; then
    report_failure "docs/ai" "documentation root is missing" \
      "create docs/ai with at least one Markdown document"
  else
    while IFS= read -r -d '' file; do
      found=1
      scan_markdown_file "$file"
    done < <(find "$docs_root" -type f -name '*.md' -print0)
  fi

  if [ "$found" -eq 0 ]; then
    report_failure "docs/ai" "no Markdown documents were discovered" \
      "add at least one .md file beneath docs/ai"
  fi

  check_contract_register
  finish_check "AI documentation check"
}

run_open_questions_check() {
  failure_count=0
  check_open_questions
  finish_check "Open-question ledger check"
}

run_rendered_link_check() {
  local rendered_root
  local found
  local file

  failure_count=0
  for rendered_root in "$@"; do
    if [ ! -d "$rendered_root" ]; then
      report_failure "${rendered_root##*/}" \
        "rendered documentation root is missing" \
        "build the book before checking rendered links"
      continue
    fi

    rendered_root=$(resolve_portable_path "$rendered_root")
    found=0
    while IFS= read -r -d '' file; do
      found=1
      scan_rendered_html_file "$rendered_root" "$file"
    done < <(find "$rendered_root" -type f -name '*.html' ! -name 'print.html' -print0)

    if [ "$found" -eq 0 ]; then
      report_failure "${rendered_root##*/}" \
        "rendered documentation root contains no HTML files" \
        "build the book into this directory before checking links"
    fi
  done

  finish_check "Rendered documentation link check"
}

run_self_test() {
  local fixture_root
  local outside_root
  local failure_log
  local ledger
  local rendered_root
  local id
  local requirement
  local suffix
  local status
  local owner
  local evidence
  local -a pinned_ids=(1 2 3 4 6 8 10 11 14 15 16 17 18 19 22 24 25 26 27 29 30 31 32 33)

  fixture_root=$(mktemp -d)
  fixture_root=$(CDPATH= cd -P -- "$fixture_root" && pwd -P) || return 1
  SELF_TEST_ROOT=$fixture_root
  outside_root=$(mktemp -d)
  outside_root=$(CDPATH= cd -P -- "$outside_root" && pwd -P) || return 1
  SELF_TEST_OUTSIDE_ROOT=$outside_root
  mkdir -p "$fixture_root/docs/ai/guide"

  printf '%s\n' \
    '# Fixture index' \
    '' \
    'See [the guide](guide/../guide/details.md#details).' \
    'See [the second repeated heading](guide/details.md#repeated-heading-1).' \
    > "$fixture_root/docs/ai/README.md"
  printf '%s\n' \
    '# Fixture guide' \
    '' \
    '## Details' \
    '' \
    'A valid fixture.' \
    '' \
    '## Repeated heading' \
    '' \
    'First.' \
    '' \
    '## Repeated heading' \
    '' \
    'Second.' \
    > "$fixture_root/docs/ai/guide/details.md"

  SUBJECT_ROOT=$fixture_root
  if ! run_scan >/dev/null; then
    printf 'self-test failed: the clean fixture was rejected\n' >&2
    return 1
  fi

  failure_log=$fixture_root/failure.log
  rendered_root=$fixture_root/rendered-book
  mkdir -p "$rendered_root/guide"
  printf '%s\n' \
    '<html><head><link href="book.css" rel="stylesheet"></head>' \
    '<body><a href="guide/../guide/details.html#details">Details</a><a href="https://example.com/">External</a></body></html>' \
    > "$rendered_root/index.html"
  printf '%s\n' \
    '<html><body><h1 id="details">Details</h1><a href="../index.html">Home</a></body></html>' \
    > "$rendered_root/guide/details.html"
  : > "$rendered_root/book.css"
  if ! run_rendered_link_check "$rendered_root" >/dev/null; then
    printf 'self-test failed: valid rendered links were rejected\n' >&2
    return 1
  fi

  printf '%s\n' \
    '<html><body><a href="guide/../missing.html">Missing</a><a href="guide/details.html#absent">Bad fragment</a></body></html>' \
    > "$rendered_root/broken.html"
  if run_rendered_link_check "$rendered_root" >"$failure_log" 2>&1 \
    || ! grep -q 'rendered local reference.*missing target' "$failure_log" \
    || ! grep -q 'missing HTML fragment' "$failure_log"; then
    printf 'self-test failed: invalid rendered links were not rejected\n' >&2
    return 1
  fi
  rm -f "$rendered_root/broken.html"

  mkdir -p "$outside_root/rendered-nested"
  printf '%s\n' '<html><body>Outside target</body></html>' > "$outside_root/rendered-secret.html"
  printf '%s\n' '<html><body>In-root decoy</body></html>' > "$rendered_root/rendered-secret.html"
  ln -s "$outside_root/rendered-nested" "$rendered_root/rendered-pivot"
  ln -s 'rendered-pivot/../rendered-secret.html' "$rendered_root/nested-escape.html"
  printf '%s\n' \
    '<html><body><a href="nested-escape.html">Nested escape</a></body></html>' \
    > "$rendered_root/escape-probe.html"
  if run_rendered_link_check "$rendered_root" >"$failure_log" 2>&1 \
    || ! grep -q "rendered local reference 'nested-escape.html' escapes the book output" "$failure_log"; then
    printf 'self-test failed: nested rendered symlink traversal was not rejected\n' >&2
    return 1
  fi
  rm -f "$rendered_root/escape-probe.html" "$rendered_root/nested-escape.html" \
    "$rendered_root/rendered-pivot" "$rendered_root/rendered-secret.html"

  mkdir -p "$outside_root/source-nested"
  printf '%s\n' '# Outside source target' > "$outside_root/source-secret.md"
  printf '%s\n' '# In-root source decoy' > "$fixture_root/docs/ai/guide/source-secret.md"
  ln -s "$outside_root/source-nested" "$fixture_root/docs/ai/guide/source-pivot"
  ln -s 'source-pivot/../source-secret.md' "$fixture_root/docs/ai/guide/nested-escape.md"
  printf '%s\n' \
    '# Nested escape probe' \
    '' \
    'See [the nested escape](guide/nested-escape.md).' \
    > "$fixture_root/docs/ai/escape-probe.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q "local reference 'guide/nested-escape.md' escapes the repository" "$failure_log"; then
    printf 'self-test failed: nested source symlink traversal was not rejected\n' >&2
    return 1
  fi
  rm -f "$fixture_root/docs/ai/escape-probe.md" \
    "$fixture_root/docs/ai/guide/nested-escape.md" \
    "$fixture_root/docs/ai/guide/source-pivot" \
    "$fixture_root/docs/ai/guide/source-secret.md"

  printf '%s\n' '# Outside fixture' > "$outside_root/outside.md"
  ln -s "$outside_root/outside.md" "$fixture_root/docs/ai/guide/escape.md"

  printf '%s\n' \
    '# Broken fixture' \
    '' \
    '### Skipped heading level' \
    '' \
    'See [a missing page](guide/../missing.md).' \
    'See [a missing fragment](guide/details.md#absent-heading).' \
    'See [an invalid duplicate suffix](guide/details.md#repeated-heading-2).' \
    'See [an escaping symlink](guide/escape.md).' \
    '' \
    '# Second top-level heading' \
    > "$fixture_root/docs/ai/broken.md"
  if run_scan >"$failure_log" 2>&1; then
    printf 'self-test failed: the invalid fixture was accepted\n' >&2
    return 1
  fi
  for id in "missing target" "missing GitHub heading fragment" "escapes the repository" "heading level jumps" "top-level headings"; do
    if ! grep -q "$id" "$failure_log"; then
      printf 'self-test failed: invalid structure/reference case lacked %s diagnostic\n' "$id" >&2
      return 1
    fi
  done
  if [ "$(grep -c '^FAIL:' "$failure_log")" -lt 5 ]; then
    printf 'self-test failed: independent findings were not aggregated\n' >&2
    return 1
  fi

  rm -f "$fixture_root/docs/ai/broken.md"
  ledger=$fixture_root/docs/ai/80_OPEN_QUESTIONS.md
  {
    printf '%s\n\n%s\n\n' '# Fixture open-question ledger' '## Ledger'
    for id in "${pinned_ids[@]}" 40; do
      printf '### %s. Fixture question %s\n\n' "$id" "$id"
      printf '%s\n' \
        '- Status: Resolved' \
        '- Decision: D-01' \
        '- Evidence: `docs/ai/README.md`' \
        '- Implementation owner: CONT-01' \
        '- Verified: 2026-07-28' \
        ''
    done
  } > "$ledger"

  if ! run_open_questions_check >/dev/null; then
    printf 'self-test failed: complete open-question ledger was rejected\n' >&2
    return 1
  fi

  cp "$ledger" "$fixture_root/ledger.clean"

  awk '
    !changed && sub(/- Status: Resolved/, "- Status: Open") { changed = 1 }
    { print }
  ' "$fixture_root/ledger.clean" > "$ledger"
  if run_open_questions_check >"$failure_log" 2>&1 \
    || ! grep -q 'terminal status fields' "$failure_log"; then
    printf 'self-test failed: unresolved ledger status was not rejected\n' >&2
    return 1
  fi

  awk '
    !changed && sub(/- Status: Resolved/, "- Status: Deferred") { changed = 1 }
    { print }
  ' "$fixture_root/ledger.clean" > "$ledger"
  if run_open_questions_check >"$failure_log" 2>&1 \
    || ! grep -q 'deferred question 1 has no non-empty Reason' "$failure_log"; then
    printf 'self-test failed: deferred ledger entry without a reason was not rejected\n' >&2
    return 1
  fi

  awk '
    !changed && /- Evidence:/ { changed = 1; next }
    { print }
  ' "$fixture_root/ledger.clean" > "$ledger"
  if run_open_questions_check >"$failure_log" 2>&1 \
    || ! grep -q 'non-empty Evidence' "$failure_log"; then
    printf 'self-test failed: missing ledger metadata was not rejected\n' >&2
    return 1
  fi

  cp "$fixture_root/ledger.clean" "$ledger"
  sed -n '/^### 1\./,/^### 2\./{ /^### 2\./!p; }' "$fixture_root/ledger.clean" >> "$ledger"
  if run_open_questions_check >"$failure_log" 2>&1 \
    || ! grep -q 'identifier 1 is duplicated' "$failure_log"; then
    printf 'self-test failed: duplicate ledger identifier was not rejected\n' >&2
    return 1
  fi

  sed '/^### 1\./,/^### 2\./{ /^### 2\./!d; }' "$fixture_root/ledger.clean" > "$ledger"
  if run_open_questions_check >"$failure_log" 2>&1 \
    || ! grep -q 'identifier 1 is missing or renumbered' "$failure_log"; then
    printf 'self-test failed: missing ledger identifier was not rejected\n' >&2
    return 1
  fi

  awk '
    !changed && sub(/^### 1\./, "### 101.") { changed = 1 }
    { print }
  ' "$fixture_root/ledger.clean" > "$ledger"
  if run_open_questions_check >"$failure_log" 2>&1 \
    || ! grep -q 'identifier 1 is missing or renumbered' "$failure_log"; then
    printf 'self-test failed: renumbered ledger identifier was not rejected\n' >&2
    return 1
  fi

  cp "$fixture_root/ledger.clean" "$ledger"

  {
    printf '%s\n\n' '# Fixture contracts'
    printf '%s\n' '[package]' > "$fixture_root/Cargo.toml"
    printf '%s\n' \
      '| Contract | Audience | Status | Observed now | Locked target | Requirement | Owner | Compatibility / reversibility | Evidence | Last verified |' \
      '|---|---|---|---|---|---|---|---|---|---|'
    id=0
    while [ "$id" -le 56 ]; do
      printf -v suffix '%02d' "$id"
      requirement=CONT-01
      if [ "$id" -lt 8 ]; then
        printf -v requirement 'CONT-%02d' "$((id + 1))"
      fi
      case "$((id % 5))" in
        0) status=implemented ;;
        1) status=partially-implemented ;;
        2) status=locked-not-implemented ;;
        3) status=deferred ;;
        *) status=external-mutable ;;
      esac
      owner='CONT-01'
      [ "$id" -ne 27 ] || owner='v2 / RST-01'
      evidence='Planner source code'
      [ "$((id % 2))" -ne 0 ] || evidence='`docs/ai/README.md`'
      case "$id" in
        0) evidence='`Cargo.toml`' ;;
        1) evidence='`docs/ai`' ;;
        2) evidence='`docs/ai/README.md`; `docs/ai/guide/details.md`' ;;
        3) evidence='`bash scripts/check-ai-docs.sh --self-test`' ;;
      esac
      printf '| D-%s | Maintainers | %s | Current behavior | Locked behavior | %s | %s | Additive | %s | 2026-07-28 |\n' \
        "$suffix" "$status" "$requirement" "$owner" "$evidence"
      id=$((id + 1))
    done
  } > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if ! run_scan >/dev/null; then
    printf 'self-test failed: complete contract register was rejected\n' >&2
    return 1
  fi

  cp "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md" "$fixture_root/contracts.clean"

  sed '/^| D-01 |/s/| partially-implemented |/| nonsense-status |/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'contract row has invalid Status nonsense-status' "$failure_log"; then
    printf 'self-test failed: invalid contract status was not rejected\n' >&2
    return 1
  fi

  sed 's/^|---|---|---|---|---|---|---|---|---|---|$/|---|/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'contract table separator does not have 10 columns' "$failure_log"; then
    printf 'self-test failed: malformed contract separator was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-02 |/s/2026-07-28/07\/28\/2026/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'contract row Last verified is not YYYY-MM-DD' "$failure_log"; then
    printf 'self-test failed: malformed contract verification date was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-03 |/s/| CONT-01 |/| Owner TBD |/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'contract row Owner does not name a requirement or version boundary' "$failure_log"; then
    printf 'self-test failed: malformed contract owner was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-04 |/s/| `docs\/ai\/README.md` |/| TBD |/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'is not a repository locator, validation command, or named source/test surface' "$failure_log"; then
    printf 'self-test failed: malformed contract evidence was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-05 |/s/| Planner source code |/| `TBD` |/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q "contract row Evidence contains placeholder 'TBD'" "$failure_log"; then
    printf 'self-test failed: backticked evidence placeholder was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-06 |/s|`docs/ai/README.md`|`/absolute/evidence.md`|' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'is not repository-relative' "$failure_log"; then
    printf 'self-test failed: absolute evidence locator was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-07 |/s|Planner source code|`../outside.md`|' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'contains parent traversal' "$failure_log"; then
    printf 'self-test failed: parent-traversing evidence locator was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-08 |/s|`docs/ai/README.md`|`docs/ai/README.md`; `docs/ai/missing-evidence.md`|' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'has a missing repository target' "$failure_log"; then
    printf 'self-test failed: missing evidence locator was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-10 |/s|`docs/ai/README.md`|`file:///evidence.md`|' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'is not repository-relative' "$failure_log"; then
    printf 'self-test failed: local evidence URI was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-09 |/s/| Planner source code |/| `not-a-locator` |/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'is not a repository locator, validation command, or named source/test surface' "$failure_log"; then
    printf 'self-test failed: malformed evidence token was not rejected\n' >&2
    return 1
  fi

  sed '/^| D-01 |/d; s/^| D-00 |/| D-00 D-01 |/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'first cell is not exactly one D-NN identifier' "$failure_log"; then
    printf 'self-test failed: collapsed decision identifiers were not rejected\n' >&2
    return 1
  fi

  sed '/^| D-17 |/d' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'missing decision row D-17' "$failure_log"; then
    printf 'self-test failed: missing decision row was not rejected\n' >&2
    return 1
  fi

  cp "$fixture_root/contracts.clean" "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  sed -n '/^| D-17 |/p' "$fixture_root/contracts.clean" \
    >> "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'decision row D-17 appears 2 times' "$failure_log"; then
    printf 'self-test failed: duplicate decision row was not rejected\n' >&2
    return 1
  fi

  sed 's/^| D-17 |/| D-57 |/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'unknown decision identifier D-57' "$failure_log"; then
    printf 'self-test failed: unknown decision row was not rejected\n' >&2
    return 1
  fi

  sed 's/| D-07 | Maintainers | locked-not-implemented | Current behavior | Locked behavior | CONT-08 |/| D-07 | Maintainers | locked-not-implemented | Current behavior mentions CONT-08 | Locked behavior | CONT-01 |/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'missing CONT-08 coverage in the contract Requirement column' "$failure_log"; then
    printf 'self-test failed: requirement coverage outside its column was accepted\n' >&2
    return 1
  fi

  sed 's/| Planner source code |/| |/' \
    "$fixture_root/contracts.clean" \
    > "$fixture_root/contracts.invalid"
  mv "$fixture_root/contracts.invalid" "$fixture_root/docs/ai/15_PRODUCTION_CONTRACTS.md"
  if run_scan >"$failure_log" 2>&1 \
    || ! grep -q 'contract row has a missing field' "$failure_log"; then
    printf 'self-test failed: incomplete contract row was not rejected\n' >&2
    return 1
  fi

  printf 'AI documentation checker self-test passed.\n'
}

case "$MODE" in
  scan) run_scan ;;
  self-test) run_self_test ;;
  open-questions) run_open_questions_check ;;
  rendered-links) run_rendered_link_check "${RENDERED_ROOTS[@]}" ;;
esac
