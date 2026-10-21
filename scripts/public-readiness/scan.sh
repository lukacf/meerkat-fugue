#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/public-readiness/scan.sh [--current-only|--history-only]

Scans tracked files and/or Git history for values that must not be present
before publishing the repository. The current-tree scan is expected to pass.
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

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

token_pattern='(BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY|AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9_]{36,}|github_pat_[A-Za-z0-9_]+|xox[baprs]-[A-Za-z0-9-]+|sk-[A-Za-z0-9]{20,}|ATATT[A-Za-z0-9_=-]{20,})'
# Keep deployment inventory out of tracked source. Supply known private values
# from the operator environment when auditing a repository for publication.
live_infra_pattern="${FUGUE_PRIVATE_INFRA_PATTERN:-}"

current_failures=0
history_failures=0

scan_current() {
  local pattern="$1"
  local label="$2"
  local matches

  echo "== current tree: $label =="
  matches="$(
    git ls-files -z \
      | xargs -0 rg -n --hidden -S "$pattern" \
      | grep -v '^scripts/public-readiness/scan\.sh:' \
      || true
  )"
  if [[ -n "$matches" ]]; then
    printf '%s\n' "$matches"
    current_failures=$((current_failures + 1))
  else
    echo "ok"
  fi
}

scan_history() {
  local pattern="$1"
  local label="$2"

  echo "== history: $label =="
  if git log --all -G "$pattern" --format='%h %ad %s' --date=short --name-only -- . ':(exclude)scripts/public-readiness/scan.sh' \
    | sed '/^$/d' \
    | head -200 \
    | grep -q .
  then
    git log --all -G "$pattern" --format='%h %ad %s' --date=short --name-only -- . ':(exclude)scripts/public-readiness/scan.sh' \
      | sed '/^$/d' \
      | head -200
    history_failures=$((history_failures + 1))
  else
    echo "ok"
  fi
}

if [[ "$mode" == "all" || "$mode" == "current" ]]; then
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
