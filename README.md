# Meerkat Fugue

[![CI](https://github.com/lukacf/meerkat-fugue/actions/workflows/ci.yml/badge.svg)](https://github.com/lukacf/meerkat-fugue/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)

Fugue automates issue-driven development with agent teams: planning work,
implementing changes, reviewing and testing them, and coordinating delivery.
Powered by [Meerkat](https://github.com/lukacf/meerkat) and
[Meerkat Meta](https://github.com/lukacf/meerkat-meta), it also builds persistent
teams of repository specialists that study the codebase, remember what they
learn, and coordinate across repositories.

Use the built-in issue store, **Fugue IMS**, or connect **Linear** or **Jira**.
GitHub and GitHub Enterprise repositories use the authenticated `gh` CLI.

## Install

You need Git and Rust through [rustup](https://rustup.rs/). This checkout pins Rust
1.97.0 in `rust-toolchain.toml`; Cargo installs that toolchain when needed. Node.js
and `npx` are also needed for the default Playwright MCP integration in issue workflows.

```bash
git clone https://github.com/lukacf/meerkat-fugue.git
cd meerkat-fugue
cargo install --locked --path .
```

This installs `meerkat-fugue`, `fugue-linear-mcp`, `fugue-jira-mcp`,
`fugue-github-mcp`, and `fugue-host-mcp`. Upgrade these together; the
[operations guide](docs/operations.mdx) covers atomically replacing the five binaries.

Fugue currently uses
Meerkat/Meta 0.9.3, with an exact upstream Meta backport for persisted-session
recovery. Cargo resolves that public Git dependency automatically; see the
[upgrade guide](docs/upgrading-0.9.3.mdx).

## Run issue-driven workflows

Fugue picks up eligible issues, creates isolated workspaces, runs agent teams,
and tracks review and delivery evidence. Choose your issue source:

- **Fugue IMS:** the built-in local issue store, with no Linear account or API key.
- **Linear:** an optional integration with your existing project and workflow.
- **Jira:** an optional integration with existing Cloud or Server/Data Center workflows.

Start with [fugue.example.yaml](fugue.example.yaml) and the
[configuration reference](docs/configuration.mdx). The example is configured for
Linear; to use Fugue IMS, set `kind: fugue-ims` and a SQLite `endpoint` in the
existing `linear` configuration section, and omit the Linear API key and external
Linear MCP server. The section also accepts the name `tracker` for all issue sources.
For Jira, start with [fugue.jira.example.yaml](fugue.jira.example.yaml) and the
[Jira and GitHub Enterprise guide](docs/jira-and-github-enterprise.mdx).
Configure repository hooks and agent tools for your environment before starting
the queue. For Linear, follow the [Linear workflow setup guide](docs/getting-started.mdx).

```bash
meerkat-fugue fugue.yaml --port 8788
```

Open **[http://127.0.0.1:8788/](http://127.0.0.1:8788/)** to follow the queue and
agent activity. Fugue IMS manages local issues in the same UI. With Linear,
Fugue maintains one durable workpad per issue; detailed agent activity remains
in Meta and WorkGraph.

## Start a repository team

Configure your model provider credentials in the shell that runs Fugue. For the
default OpenAI model, set `OPENAI_API_KEY` through your preferred secret manager
or shell environment. Keep credentials out of configuration committed to Git.

Point Fugue at one or more existing Git checkouts:

```bash
meerkat-fugue ensemble init ~/src/api ~/src/web --output ensemble.yaml
meerkat-fugue ensemble serve --config ensemble.yaml
```

Open **[http://127.0.0.1:8788/ensemble](http://127.0.0.1:8788/ensemble)**. No Linear
account or remote deployment is needed for repository teams.

On first start, Fugue inspects each repository, automatically designs and reviews
its specialist team, and has the specialists study their assigned domains. This
uses model tokens and can take a few minutes. Team identities, conversations,
completed studies, and native agent memory persist across restarts.

Representatives use `ensemble_delegate` for assignments that require a result.
Native flows capture the specialist's final output and return it to the
representative without relying on a separate peer message. Repeating the same
assignment key reuses its run; interrupted runs remain interrupted and report
their saved evidence after restart so the representative can reconcile progress.

For authorized delivery, the [release helper](scripts/release/README.md) checks
review and test receipts against the exact PR commit and required CI checks.
Its detached local supervisor verifies the running build and preserved agent
sessions, rolls back failed deployments, and notifies the representative after
restart. Configure the deployment target once before assigning delivery work.

In another terminal, give the representative an objective:

```bash
meerkat-fugue ensemble status
meerkat-fugue ensemble ask web "Coordinate with api to map the account-settings interfaces."
```

Use repository IDs from `ensemble.yaml`. Representatives are connected by default;
`ensemble init --no-peers` keeps teams separate. Use `--model` with `ensemble init`
to choose another supported model. See the [Ensemble guide](docs/ensemble.mdx) for
configuration, permissions, persistent memory, and native consoles.

To serve the issue queue and persistent repository teams together in the regular UI:

```bash
meerkat-fugue fugue.yaml --ensemble-config ensemble.yaml --port 8788
```

Overview, IMS, Ensemble, and repository consoles share one local server. See
[operations](docs/operations.mdx) for deployment, upgrades, and recovery.

## Execution and credentials

Fugue is intended for trusted repositories and automation hosts. It is **not an OS
sandbox**: enabled agent tools and repository hooks execute with the service user's
permissions. Choose credentials and tool access appropriate to the repositories
and tasks you want agents to handle.

Keep the operator UI on loopback or behind an authenticated access layer. Do not
commit provider keys, generated operator configs, runtime databases, or console
transcripts. See [SECURITY.md](SECURITY.md) for vulnerability reporting.

## Documentation

- [Ensemble](docs/ensemble.mdx): formation, specialists, peering, and memory.
- [Linear workflow setup](docs/getting-started.mdx): optional Linear integration and onboarding.
- [Jira and GitHub Enterprise](docs/jira-and-github-enterprise.mdx): private configuration, authentication, workflow mapping, and validation.
- [Configuration](docs/configuration.mdx): queue, agents, tools, and server settings.
- [Native mobs](docs/native-mobs.mdx): orchestration, review, and delivery contracts.
- [Architecture](docs/architecture.mdx): runtime boundaries and recovery.
- [Operations](docs/operations.mdx): deployment and monitoring.
- [Status API](docs/status-api.mdx): dashboards and HTTP endpoints.
- [Troubleshooting](docs/troubleshooting.mdx): diagnosis and recovery.

## Development

```bash
cargo fmt --check
cargo check --all-targets --locked
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
```

The normal test suite uses local fixtures. Live E2E tests are ignored by default
and require an explicitly configured disposable environment. See
[CONTRIBUTING.md](CONTRIBUTING.md), [AGENTS.md](AGENTS.md), and the
[development guide](docs/development.mdx) before contributing.

## License

Licensed under either the [MIT license](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option.
Unless you explicitly state otherwise, contributions intentionally submitted for
inclusion are licensed under those same terms.
