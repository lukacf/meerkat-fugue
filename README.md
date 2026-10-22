# Meerkat Fugue

[![CI](https://github.com/lukacf/meerkat-fugue/actions/workflows/ci.yml/badge.svg)](https://github.com/lukacf/meerkat-fugue/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)

Fugue turns issues into reviewed code changes with persistent agent teams.
Connect a GitHub or GitHub Enterprise repository, choose built-in tickets,
Linear or Jira, and let its team plan, implement, test and review the work.

Each repository has its own tracker project, queue, credentials and native
[Meerkat](https://github.com/lukacf/meerkat) mob. Specialists map the codebase,
remember their domains and collaborate with the execution agents. One shared
[Meta console](https://github.com/lukacf/meerkat-meta) exposes the teams,
conversations, memory, work graphs and topology.

## Install and start

Install Git, GitHub CLI, Node.js and Rust through [rustup](https://rustup.rs/).
The checkout pins its Rust toolchain. For a source installation:

```sh
git clone https://github.com/lukacf/meerkat-fugue.git
cd meerkat-fugue
cargo install --locked --path .
meerkat-fugue serve --port 8788
```

Open **[http://127.0.0.1:8788/](http://127.0.0.1:8788/)** and choose
**Repositories > Add repository**. Paste its URL, save repository and tracker
access, select a native rkat model account and prepare the team.

The [first-repository guide](docs/getting-started.mdx) walks through every step.
For packaged installation, the [bundle guide](scripts/distribution/README.md)
covers all five cooperating Fugue executables, pinned rkat, checksums and upgrades.
Source installations need rkat separately to create and select native model
accounts in setup; existing environment credentials are also supported.

## Work with your team

- **Home** brings together attention lanes, retries, active work and setup issues.
- **Board** keeps each repository's tracker lanes and operator actions together.
- **Agents** opens the shared native console, including Memory and Work Graph.
- **Repositories** handles preparation, connections and configuration.

Repository setup maps the code and creates specialists once. Ticket leads,
coders and reviewers then join the same repository mob. Their prompts name the
specialists and require domain consultation. Specialists subscribe to files and
folders; Fugue checks the remote default branch every minute and delivers
matching changes through native admission. Memory, subscriptions and notification
cursors persist across restarts.

Start with a small issue and explicit acceptance criteria. Inspect its workpad,
test evidence and independent review before requesting the final PR gate or
merge. The configured merge procedure rechecks the actual PR head and repository
policy. Missing CI checks are distinct from passed checks. Merging a PR does
not deploy a service unless a deployment procedure is explicitly configured.
See [daily work](docs/daily-work.mdx).

## Configure and host

Browser setup keeps credentials in private application state, outside Git.
`--state-dir /absolute/private/path` chooses that location. AI authentication
belongs to rkat; Fugue stores its native binding references. Browser operator
login and model-provider login are separate.

Advanced installations can run an explicit configuration:

```sh
meerkat-fugue fugue.yaml --port 8788
```

Use [fugue.example.yaml](fugue.example.yaml) for Linear or built-in tickets,
[fugue.jira.example.yaml](fugue.jira.example.yaml) for Jira, and the
[configuration reference](docs/configuration.mdx) for exact options.
[Repository teams](docs/ensemble.mdx) also describes preparing existing local
checkouts and connecting representatives across repositories.

Fugue is for trusted repositories and automation hosts. Agent tools and hooks
execute with the service user's permissions; it is not an OS sandbox. Keep the
listener on loopback or behind an authenticated HTTPS gateway. Every admitted
operator has access to the instance's repositories and controls. Keep deployment
configuration, credentials, databases and transcripts outside this public repo.
See [SECURITY.md](SECURITY.md).

Fugue uses Meerkat/Meta 0.9.3 with an exact public Meta backport for persistence,
shared-console identities, peer links, Memory and Work Graph. The
[upgrade guide](docs/upgrading-0.9.3.mdx) and
[operations guide](docs/operations.mdx) explain durable state and rollback.

## Documentation

Start at the [documentation home](docs/index.mdx).

- [Your first repository](docs/getting-started.mdx)
- [Daily work](docs/daily-work.mdx)
- [Jira and GitHub Enterprise](docs/jira-and-github-enterprise.mdx)
- [Repository teams](docs/ensemble.mdx)
- [Operations and recovery](docs/operations.mdx)
- [Troubleshooting](docs/troubleshooting.mdx)
- [Architecture](docs/architecture.mdx) and [Status API](docs/status-api.mdx)

## Development

```sh
cargo fmt --check
cargo check --all-targets --locked
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
```

The normal suite uses local fixtures. Ignored live E2E tests require an explicitly
configured disposable environment. Read [CONTRIBUTING.md](CONTRIBUTING.md),
[AGENTS.md](AGENTS.md) and the [development guide](docs/development.mdx).

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option. Contributions intentionally submitted for inclusion use those same terms
unless explicitly stated otherwise.
