#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/public-readiness/create-public-snapshot.sh [--ref REF|--worktree] [--output DIR]

Creates a clean single-commit repository snapshot from a sanitized tree. Use this
instead of making the historical private repository public when history contains
operator inventory or other private context.

By default, the script snapshots the current worktree while respecting
`.gitignore`, so it can be smoke-tested before the readiness changes are
committed. Use `--ref main` or another ref for a committed tree.

The script does not push anywhere.
USAGE
}

ref=""
output=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ref)
      ref="${2:?--ref requires a value}"
      shift 2
      ;;
    --worktree)
      ref=""
      shift
      ;;
    --output)
      output="${2:?--output requires a value}"
      shift 2
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
done

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [[ -z "$output" ]]; then
  if [[ -n "$ref" ]]; then
    short_ref="$(git rev-parse --short "$ref")"
  else
    short_ref="worktree-$(git rev-parse --short HEAD)"
  fi
  output="${TMPDIR:-/tmp}/meerkat-fugue-public-${short_ref}"
fi

if [[ -e "$output" ]]; then
  echo "output already exists: $output" >&2
  exit 1
fi

mkdir -p "$output"
if [[ -n "$ref" ]]; then
  git archive --format=tar "$ref" | tar -x -C "$output"
else
  git ls-files -z --cached --modified --others --exclude-standard \
    | tar --null -T - -cf - \
    | tar -x -C "$output"
fi

(
  cd "$output"
  git init -q
  git add .
  git commit -q -m "Initial public snapshot"
  scripts/public-readiness/scan.sh --current-only
)

cat <<EOF
Created public snapshot:
  $output

Next steps:
  cd "$output"
  cargo fmt --check
  cargo test
  git remote add origin <new-public-repo-url>
  git push -u origin main
EOF
