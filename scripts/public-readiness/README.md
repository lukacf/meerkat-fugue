# Publication checks

Requires Bash, Git and ripgrep (`rg`). Run from the repository you intend to publish:

```sh
scripts/public-readiness/scan.sh --current-only
scripts/public-readiness/scan.sh --history-only
```

With no option, the script runs both checks. The current check searches tracked
working files, including binary content. Untracked and ignored private state is
outside its scope. The history check searches changed content across all local
Git refs, including values removed from the current version. Use a checkout of
the intended repository so unrelated local refs do not confuse the result.

The current check reads working files even when Git marks them assume-unchanged
or skip-worktree, without changing those flags. Missing, unreadable or non-file
tracked paths stop the scan. Stage an intentional deletion before checking the
remaining tracked tree.

Credential patterns are built in. Supply deployment identifiers separately as
an extended regular expression; keep real private values outside tracked files:

```sh
FUGUE_PRIVATE_INFRA_PATTERN='private[.]example[.]invalid' \
  scripts/public-readiness/scan.sh
```

| Exit | Meaning | Next step |
| --- | --- | --- |
| `0` | No configured patterns matched. | Continue the remaining publication checks; pattern matching is not an exhaustive secret audit. |
| `1` | A configured pattern matched. | Inspect the reported file or commit privately and resolve the finding before publication. |
| `2` | The scan could not complete. | Correct the regex or Git error and rerun. This is not a clean result. |

Findings show file paths and, for history, commit IDs and dates. They do not
print matching file contents or commit subjects into CI logs. History output
is limited to 200 lines after the search completes; that display limit does not
discard a positive result. A credential removed from the latest file can still
be present in published history.

The default CI invocation runs the current check with built-in credential
patterns. It does not know an operator's private deployment identifiers.
Complete that separate audit before publishing a deployment-related change.

Disposable-repository regression checks run with `npm run test:delivery`.
