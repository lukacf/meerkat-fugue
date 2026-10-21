# Public repository hygiene

This public repository begins with a clean source snapshot. Earlier private Git
history, issue discussions, runtime state, and operator configuration are not
part of this repository.

## Before publishing changes

Keep credentials in environment variables or a secret manager. Use example
infrastructure names and paths in documentation and tests. Do not commit local
backend profiles, runtime databases, console transcripts, or smoke artifacts.

Run the tracked credential-pattern check:

```bash
scripts/public-readiness/scan.sh --current-only
```

When auditing a new source import, also inspect its history and run a dedicated
secret scanner such as Gitleaks. A clean current tree does not remove sensitive
material from older commits or issue discussions.

The scan script accepts an additional `FUGUE_PRIVATE_INFRA_PATTERN` environment
variable for deployment identifiers that must not appear in an export. Keep
those private values outside tracked files. Without that variable, the script
checks credential patterns only.

## Exporting another clean snapshot

`scripts/public-readiness/create-public-snapshot.sh --ref main --output /path/to/export`
creates a new single-commit repository from a tracked source tree. It does not
push anywhere. Review the exported files and run the normal validation checks
before publishing; do not transfer private refs, issues, or pull-request history.
