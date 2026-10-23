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
An upgrade retains the earlier release as `previous`. If activating `current`
fails, the installer restores the prior `previous` link before reporting the
error. It refuses unmanaged `current`, `previous` and executable paths before
running candidate binaries. It never modifies the
application state directory. Stop the service and back up its state before an
upgrade; changing the executable link alone does not restart a running process
or roll back state migrations. Retain the matching state snapshot for rollback.

## If installation stops

Keep the error and the release checksum. Inspect the executable selections with
`readlink /opt/fugue/current` and `readlink /opt/fugue/previous`; `previous` does
not exist before the first successful upgrade. The running service keeps its
already loaded binary until you restart it.

| Error | Next step |
| --- | --- |
| Download or file checksum mismatch | Download the archive again and compare it with the checksum for that exact release. |
| Release is not compatible | Select the bundle for this host's OS and architecture. |
| Executable identity or rkat version differs | Obtain a complete matching bundle; do not combine executables from different releases. |
| Unmanaged executable or release link | Inspect the named path and choose a separate installation prefix if it belongs to another installation. Do not delete it to force an upgrade. |
| Another process holds the installation lock | Let the other installer finish, then retry. Removing the lock file does not release that process's lock. |
| Activation fails | Resolve the reported filesystem error, inspect `current` and `previous`, and retry the same archive. A verified staged release can be reused. |

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

The current candidate binds Core `e2e4b8f8bf1a306eb2cb1758ae75aab95a464a62`
and Meta `661025cb89ca4554beb1b248a9d59c1133ce9697`. Core includes the
checkpoint-recovery correction; Meta includes the console panels and sign-in
recovery changes. Every Core package, including Meta's transitive dependencies,
must resolve to that same Core commit in `Cargo.lock`.

Fugue qualifies these source bindings through its own build, native recovery
and hosted ticket-cycle checks. Upstream CI and publication do not block starting
that qualification. Replace the temporary pins when a released pair passes the
same checks, then verify the new lock and release manifest. A source binding or
an upstream merge alone is not deployed acceptance.
