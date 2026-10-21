# Durable local release

Agents coordinate the work. This command supplies a mechanical exact-commit gate,
immutable deployment artifacts, rollback, and a durable native completion message.
It never picks or sequences coding/review agents. Requires Python 3.9+, Git, `gh`
(authenticated), and `lsof` on the service host.

Keep receipts and service configuration **outside the Git checkout**. An attestation
requires a clean checkout. The reviewer must be a different agent from the test and
release owner. After making *any* correction, commit/push and repeat both attestations
for the new full SHA. Evidence is an accountable agent assertion, not a substitute
for running tests or inspecting the diff.

```sh
# Reviewer runs this after reviewing the exact committed HEAD.
python3 scripts/release/release.py attest --checkout "$PWD" --head "$HEAD_SHA" \
  --kind review --actor review-specialist \
  --evidence 'Reviewed diff and independently exercised the acceptance cases.' \
  --output /tmp/release-review.json
# Test owner runs this after all required checks pass on that same clean commit.
python3 scripts/release/release.py attest --checkout "$PWD" --head "$HEAD_SHA" \
  --kind test --actor delivery-specialist \
  --evidence 'cargo fmt --check; cargo test --all-targets --locked; browser suite: passed' \
  --output /tmp/release-tests.json
# The representative authorizes release, and the delivery specialist runs:
python3 scripts/release/release.py merge --checkout "$PWD" \
  --repo lukacf/meerkat-fugue --pr "$PR_NUMBER" --head "$HEAD_SHA" \
  --actor delivery-specialist --review /tmp/release-review.json \
  --tests /tmp/release-tests.json --require-check Rust --require-check Delivery \
  --output /tmp/release-merge.json
```

`merge` requires the local checkout to be clean and at the reviewed SHA, checks its
origin against the selected repository, compares both attestations to the remote PR
head/tree, requires the current main commit to be included in that reviewed tree,
checks the explicitly required CI jobs, merges with GitHub's expected-head compare-and-swap, and verifies
the resulting tree. If main changed and the merge tree differs, it refuses to emit
a deploy receipt. Rebase and review the combined tree before trying again. The
base/head are re-read immediately before merge; GitHub only provides atomic CAS
for the head SHA. A base change in the final API-call window is therefore caught
by the post-merge tree check, not an atomic base guard. Protected-branch strict
up-to-date checks can close this server-side race. Repeating
a successful merge after a lost response safely reconstructs its receipt.

Next update the checkout to the **merged commit from the receipt**, build from that
clean tree (`cargo build --locked --bin meerkat-fugue`, or `--release`), and deploy:

```sh
python3 scripts/release/release.py deploy --receipt /tmp/release-merge.json \
  --service /absolute/operator/service.json --binary target/debug/meerkat-fugue \
  --work-item work_REPLACE_WITH_NATIVE_WORK_ID
```

The operator's stable service file is configured once; subsequent releases reuse it:

```json
{
  "repository": "lukacf/meerkat-fugue",
  "url": "http://127.0.0.1:8798",
  "cwd": "/absolute/public/checkout",
  "args": ["/absolute/fugue.yaml", "--ensemble-config", "/absolute/ensemble.yaml", "--port", "8798"],
  "state_dir": "/absolute/operator/releases",
  "initial_binary": "/absolute/previous/meerkat-fugue",
  "health_timeout_seconds": 120,
  "notify_timeout_seconds": 180,
  "notify": {
    "path": "/api/repos/meerkat-fugue/meta/console/send",
    "identity": "meerkat-fugue:representative"
  }
}
```

`initial_binary` must be the currently running, known-good executable and is used
only before the first managed release. Keep it intact when building the candidate.
The service's native runtime/state paths in its arguments must stay unchanged.
Environment credentials are inherited; they are never serialized into a receipt.
The URL must be loopback HTTP; this local supervisor is not a remote deployment API.

`deploy` checks the candidate's compiled `build-info`, copies both binaries and the
supervisor to a unique transaction directory, snapshots every session and memory
flag, and starts an independent process session with no inherited pipes. It returns
the transaction/status path before cutover. **Do not launch a custom restart shell
or kill the hosting process yourself.** The supervisor owns shutdown, readiness,
rollback, and completion notification even when the requesting agent disappears.
Only one supervisor can own the service's deployment lock.

A successful health check requires the exact compiled commit/tree from
`/api/v1/build`, the new listener PID, and all original native sessions/memory flags.
Failure restores the immutable previous binary. The supervisor writes `status.json`
atomically and sends its outcome to the representative through native console ingress
with a stable idempotency key and an immutable saved payload, including across
notification retries. The representative must consume the receipt and obtain
independent post-deployment acceptance before completing the original work item.
`notification: accepted` means durable ingress acceptance, not completed verification.

If the native console remains unavailable past the notification deadline, the receipt
stays `notification: pending` with an error. Recovery can retry notification without
repeating deployment:

```sh
python3 scripts/release/release.py resume-notification /absolute/transaction
```

Offline regressions (including real detached child/HTTP rollback tests):

```sh
python3 -m unittest discover -s scripts/release -p 'test_*.py' -v
```
