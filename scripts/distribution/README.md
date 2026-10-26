# Fugue release bundles

Each archive contains all five Fugue executables, the pinned native rkat CLI,
licenses, and a file checksum manifest. Every Fugue executable reports the same
clean commit and tree with `build-info`. No credentials or operator state are
part of the archive. The executable set is `meerkat-fugue`, `fugue-linear-mcp`,
`fugue-jira-mcp`, `fugue-github-mcp` and `fugue-host-mcp`.

Build from a clean checkout on the target platform:

```sh
cargo build --release --bins --locked
python3 scripts/distribution/package.py --bin-dir target/release \
  --target x86_64-unknown-linux-gnu --output /tmp/fugue-dist
```

The release workflow produces a Linux x64 bundle built on Ubuntu 24.04. Use
Ubuntu 24.04 or a newer compatible glibc host. macOS and Linux ARM64 may be
packaged with native builds using the targets recorded in `rkat.json`.

Install using the checksum from the release receipt:

```sh
python3 scripts/distribution/install.py --archive /path/to/fugue.tar.gz \
  --sha256 EXPECTED_SHA256 --prefix /opt/fugue
/opt/fugue/bin/meerkat-fugue serve --state-dir /var/lib/fugue --port 8788
```

Run Fugue as a dedicated unprivileged account. Git, GitHub CLI (`gh`), Node.js,
and `npx` must be available in the service's PATH, together with `/opt/fugue/bin`.
Keep that account's HOME persistent: rkat owns the model account store there.
Repository and tracker credentials are entered through Fugue's setup page.

The installer verifies the complete archive before executing any candidate
binary, checks every binary's build identity, and then atomically switches the
`current` link. A repeated install verifies and reuses the exact same release.
An upgrade retains the earlier release as `previous`. If `current` cannot be
switched, the installer restores the earlier rollback selection before reporting
the error. It refuses unmanaged `current`, `previous` and executable paths before
running candidate binaries. It never modifies the application state directory. Stop the service and back up its state before an
upgrade; changing the executable link alone does not restart a running process
or roll back state migrations. Retain the matching state snapshot for rollback.

## If installation stops

Keep the error and the release checksum. Inspect the executable selections with
`readlink /opt/fugue/current` and `readlink /opt/fugue/previous`; `previous` does
not exist before the first successful upgrade. The running service keeps its
already loaded binary until you restart it.

After an interrupted installer process, rerun the same verified installation
command before using `previous` for rollback. The installer first recovers the
release switch: it restores the earlier rollback selection if activation did
not finish, or keeps the newly selected release and the release it replaced if
activation finished. This recovery can itself be retried after an interruption.
An I/O error after the active link changed does not mean that change was undone;
inspect the selections after recovery before restarting the service.

| Error | Next step |
| --- | --- |
| Download or file checksum mismatch | Download the archive again and compare it with the checksum for that exact release. |
| Release is not compatible | Select the bundle for this host's OS and architecture. |
| Executable identity or rkat version differs | Obtain a complete matching bundle; do not combine executables from different releases. |
| Unmanaged executable or release link | Inspect the named path and choose a separate installation prefix if it belongs to another installation. Do not delete it to force an upgrade. |
| Another process holds the installation lock | Let the other installer finish, then retry. Removing the lock file does not release that process's lock. |
| Existing release files or executable permissions changed | Inspect the current release and any retained copy of the bundle being installed. Restore the damaged release from a verified backup, including its regular files and executable permissions. Validation refusal preserves the recovered release selections; downloading the same archive does not repair that retained directory. |
| Activation or recovery reports an I/O error | Resolve the filesystem error and retry the same archive. Inspect the recovered `current` and `previous` selections before restarting the service. A verified staged release can be reused. |
| Invalid activation record or conflicting release selections | Preserve the installation directory, including `.activation.json`. Restore a consistent installation backup or choose a separate prefix. Do not edit or delete the recovery record to force an upgrade. |

An installer error does not require deleting application state, model accounts
or repository checkouts. If the service was already stopped for an upgrade,
verify the release and state snapshot before starting it again.

## Authenticated remote access

The listener remains on loopback. For a remote installation, put an authenticated
operator proxy in front of it and configure the service with both:

```sh
FUGUE_PUBLIC_ORIGIN=https://fugue.example.com
FUGUE_PROXY_TOKEN_FILE=/etc/fugue/proxy-token
FUGUE_LOGOUT_PATH=/auth/sign-out
```

Set the optional logout path to the proxy's same-origin sign-out endpoint. Fugue
then shows Sign out inside the shared Session menu. The proxy owns session
revocation and cookie removal.

The file must be owner-readable only (`0600`) and contain a random token of at
least 32 ASCII characters. The proxy must authenticate the user, apply an explicit
operator allowlist, discard client-supplied Authorization and identity headers,
and set `Authorization: Bearer <proxy-token>` on its private upstream request.
Preserve the actual public Host and Origin. Never expose the upstream listener
or put this proxy token in browser JavaScript, URLs, repositories, or logs.

**Every admitted user is a full operator of the entire Fugue instance**, including
all repository queues, connections, agents, and the native console. Use separate
instances when users must not share that authority. Forwarded email headers do
not authenticate a request. Native model authentication remains separate.

All repository browser URLs use the same origin under `/repositories/<id>/`.
The shared console stays at `/console`. Agent-local tool URLs remain loopback.
The proxy must preserve streaming responses and support the application's upload
limits. Test actual login, logout/session expiry, streaming, and reboot recovery
on the intended gateway before treating a deployment as accepted.

## Temporary dependency follow-up

The candidate uses exact public Core and Meta source revisions. Read
[Cargo.toml](../../Cargo.toml) for the selected revisions and
[Cargo.lock](../../Cargo.lock) for the resolved versions and commits. Every
Core package, including Meta's transitive dependencies, must resolve to the same
Core commit. The separately packaged rkat CLI uses [rkat.json](rkat.json); its
version can differ from the embedded runtime.

For an installed bundle, run `meerkat-fugue build-info` and compare its commit
and tree with `manifest.json`. Inspect the dependency files at that source
revision, not at the repository's current default branch. PR artifacts can name
a synthetic test-merge commit; verify its parents and tree against the intended
change. A stable release must identify its actual tag commit.

Fugue qualifies these source bindings through its own build, native recovery
and hosted ticket-cycle checks. Upstream CI and publication do not block starting
that qualification. Replace the temporary pins when a released pair passes the
same checks, then verify the new lock and release manifest. A source binding or
an upstream merge alone is not deployed acceptance.

This integration candidate is not qualified for production distribution.
Retain each candidate's CI, native recovery and bundle-installation evidence
with its exact source identity, including failed trials. Passing checks on an
older pair does not qualify a replacement. Complete the consumer checks and
fresh hosted acceptance on the final candidate.

The merge path also needs authoritative, durable review results bound to the
exact candidate. Native keyed admission prevents duplicate execution of a
profile's review flow when the same issue/round assignment is retried, including
after restart. It does not by itself establish reviewer independence, the current
review round, or approval to merge. Those policies and the common merge gate
remain release requirements. Matching a caller-supplied SHA or observing a fresh
PR is insufficient. Keep the release gates open until the final candidate passes
regression tests and hosted acceptance.
