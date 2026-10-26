#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/public-readiness/scan.sh [--current-only|--history-only]

Scans tracked files and/or Git history for values that must not be present
before publishing the repository. The current-tree scan is expected to pass.
Matches report paths and commit IDs, never matching file contents or subjects.
Exit 0 means no configured matches, 1 means matches found, and 2 means a scan
could not be completed. An error is not a clean publication result.
Set FUGUE_PRIVATE_INFRA_PATTERN to an operator-supplied extended regular expression
for private deployment identifiers; keep those values out of tracked files.
The history scan must pass before making the existing repository public; if it
does not, publish from a clean orphan snapshot or rewrite history first.
USAGE
}

mode="all"
case "${1:-}" in
  "")
    ;;
  --current-only)
    mode="current"
    ;;
  --history-only)
    mode="history"
    ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

if ! repo_root="$(git rev-parse --show-toplevel)"; then
  echo "Cannot identify the repository; publication is not verified." >&2
  exit 2
fi
if ! cd "$repo_root"; then
  echo "Cannot enter the repository; publication is not verified." >&2
  exit 2
fi

token_pattern='(BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY|AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9_]{36,}|github_pat_[A-Za-z0-9_]+|xox[baprs]-[A-Za-z0-9-]+|sk-[A-Za-z0-9]{20,}|ATATT[A-Za-z0-9_=-]{20,})'
# Keep deployment inventory out of tracked source. Supply known private values
# from the operator environment when auditing a repository for publication.
live_infra_pattern="${FUGUE_PRIVATE_INFRA_PATTERN:-}"

current_failures=0
history_failures=0
tracked_files=""

prepare_current() {
  if ! tracked_files="$(mktemp "${TMPDIR:-/tmp}/fugue-publication.XXXXXX")"; then
    echo "Cannot create the tracked-file inventory; publication is not verified." >&2
    return 2
  fi
  trap 'rm -f -- "$tracked_files"' EXIT
  if ! git ls-files -z -- . ':(exclude)scripts/public-readiness/scan.sh' > "$tracked_files"; then
    echo "Cannot read the tracked-file inventory; publication is not verified." >&2
    return 2
  fi
}

scan_current() {
  local pattern="$1"
  local label="$2"
  local matches
  local scan_status
  local file
  local found=0

  echo "== current tree: $label =="
  # Validate even an empty inventory. A missing search tool or invalid pattern
  # must never turn an unexecuted scan into a clean result.
  if rg --no-config --case-sensitive --text --quiet -e "$pattern" < /dev/null; then
    :
  else
    scan_status=$?
    if [[ "$scan_status" -ne 1 ]]; then
      printf 'Current-tree search failed (exit %s); publication is not verified.\n' "$scan_status" >&2
      return 2
    fi
  fi
  # Search actual working files, independently of Git's assume-unchanged and
  # skip-worktree hints. Check each read result before combining match results.
  while IFS= read -r -d '' file; do
    if [[ ! -f "$file" ]]; then
      printf 'Tracked path is not a readable regular file: %s\n' "$file" >&2
      return 2
    fi
    if matches="$(rg --no-config --case-sensitive --text --files-with-matches -e "$pattern" -- "./$file")"; then
      printf '%s\n' "$matches"
      found=1
    else
      scan_status=$?
      if [[ "$scan_status" -ne 1 ]]; then
        printf 'Current-tree search failed (exit %s); publication is not verified.\n' "$scan_status" >&2
        return 2
      fi
    fi
  done < "$tracked_files"
  if [[ "$found" -eq 1 ]]; then
    current_failures=$((current_failures + 1))
  else
    echo "ok"
  fi
}

scan_history() {
  local pattern="$1"
  local label="$2"
  local matches
  local scan_status

  echo "== history: $label =="
  # Collect the command's status before limiting displayed output. An early
  # closed pipe must not turn a large positive result into a clean scan.
  if matches="$(git log --all --text --no-ext-diff --no-textconv -G "$pattern" --format='%h %ad' --date=short --name-only -- . ':(exclude)scripts/public-readiness/scan.sh')"; then
    if [[ -n "$matches" ]]; then
      printf '%s\n' "$matches" | sed '/^$/d' | sed -n '1,200p'
      history_failures=$((history_failures + 1))
    else
      echo "ok"
    fi
  else
    scan_status=$?
    printf 'History search failed (exit %s); publication is not verified.\n' "$scan_status" >&2
    return 2
  fi
}

if [[ "$mode" == "all" || "$mode" == "current" ]]; then
  prepare_current
  scan_current "$token_pattern" "token/private-key-shaped literals"
  if [[ -n "$live_infra_pattern" ]]; then
    scan_current "$live_infra_pattern" "operator-supplied infrastructure identifiers"
  fi
fi

if [[ "$mode" == "all" || "$mode" == "history" ]]; then
  scan_history "$token_pattern" "token/private-key-shaped literals"
  if [[ -n "$live_infra_pattern" ]]; then
    scan_history "$live_infra_pattern" "operator-supplied infrastructure identifiers"
  fi
fi

if [[ "$current_failures" -ne 0 || "$history_failures" -ne 0 ]]; then
  echo
  echo "public readiness scan failed: current_failures=$current_failures history_failures=$history_failures" >&2
  exit 1
fi

echo
if [[ -z "$live_infra_pattern" ]]; then
  echo "No private infrastructure pattern supplied; only credential patterns were checked."
fi
echo "configured public readiness checks passed"
