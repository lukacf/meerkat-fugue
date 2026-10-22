# Fugue release bundles

Each archive contains all five Fugue executables, the pinned native rkat CLI,
licenses, and a file checksum manifest. Every Fugue executable reports the same
clean commit and tree with `build-info`. No credentials or operator state are
part of the archive.

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
An upgrade retains the earlier release as `previous`. It never modifies the
application state directory. Stop the service and back up its state before an
upgrade; changing the executable link alone does not restart a running process
or roll back state migrations. Retain the matching state snapshot for rollback.

## Authenticated remote access

The listener remains on loopback. For a remote installation, put an authenticated
operator proxy in front of it and configure the service with both:

```sh
FUGUE_PUBLIC_ORIGIN=https://fugue.example.com
FUGUE_PROXY_TOKEN_FILE=/etc/fugue/proxy-token
FUGUE_LOGOUT_PATH=/auth/sign-out
```

Set the optional logout path to the proxy's same-origin sign-out endpoint. Fugue
then shows a Sign out link in its shared navigation. The proxy owns session
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

The exact public Meta 0.9.3 backport is an intentional release dependency.
Track upstream PRs 674 and 688 and replace the pin only after a published runtime
passes persisted-session recovery, repository topology, and full ticket-cycle
acceptance. An upstream merge alone is not an instruction to upgrade a host.
