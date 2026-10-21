# Repository Setup

Use this when a repository needs to be connected to Fugue from scratch.

## Setup Checklist

For the live GCP installation, the Rust CLI automates the repo-specific work. Prepare the remote
host once, configure a backend profile once, then onboard repositories with `meerkat-fugue
/path/to/repo`.

1. Inspect the repository:
   - Determine the GitHub remote URL and default branch.
   - Read `AGENTS.md`, `CLAUDE.md`, or local agent instructions.
   - Identify normal validation commands and branch naming policy.
2. Configure or select the Fugue backend.
   - The host should already have the Fugue root, binaries, secrets, auth hook, queue root,
     workspace root, log root, and state root.
   - Use `meerkat-fugue configure backend gcp-systemd` to record the GCP host and flow-to-team
     mapping.
3. Create or choose a dedicated Linear project for Fugue automation.
   - Prefer `<repo> Fugue Queue` or a project name that includes the flow.
   - Do not point Fugue at a broad imported GitHub issue project unless the user explicitly wants
     agents processing every imported issue.
   - Configure lanes using `linear-lanes.md`.
4. Generate native config and prompts.
   - Use `meerkat-fugue stage /path/to/repo` for local config generation.
   - Use `meerkat-fugue /path/to/repo` to generate, deploy, install/update systemd, and start.
   - Keep secrets out of the file.
5. Configure environment variables where Fugue runs:
   - `LINEAR_API_KEY`
   - GitHub auth such as `GITHUB_TOKEN` or `GH_TOKEN`
   - Provider auth required by Meerkat/Meta
   - Any repo-specific tool credentials
6. Run a smoke test before widening authority:
   - Create one harmless Todo issue in the Linear project.
   - Start Fugue with the generated native queue.
   - Confirm the issue moves to `Active`, receives a `## Fugue Workpad`, and reaches the
     expected handoff state.

## Remote Host Setup

Run the host setup script on the GCP VM once per Fugue root. It creates the directory shape, a
`secrets.env` placeholder, and `prepare-auth.sh`. If given a binary source, it installs binaries
too.

```bash
scripts/gcp-systemd/setup-host.sh \
  --fugue-root /srv/fugue \
  --service-user fugue
```

The host must have usable secrets before services can do real work:

```text
LINEAR_API_KEY=<redacted>
GH_TOKEN=<redacted>
GITHUB_TOKEN=<redacted>
OPENAI_API_KEY=<redacted>
PATH=<redacted>
```

Do not copy secrets into generated `fugue.yaml`, prompt files, Linear comments, committed docs, or
final answers.

## Backend Configuration

Configure the operator machine once:

```bash
meerkat-fugue configure backend gcp-systemd \
  --name live \
  --gcp-project example-gcp-project \
  --zone europe-west1-b \
  --instance example-fugue-host \
  --remote-user fugue \
  --fugue-root /srv/fugue \
  --bin-dir /srv/fugue/bin \
  --flow-team implementation_mob=LUC \
  --default
```

Backend profiles are stored in `~/.config/meerkat-fugue/backends.yaml` unless
`MEERKAT_FUGUE_BACKENDS` points elsewhere.

## One-Command Repo Onboarding

From anywhere:

```bash
meerkat-fugue --dry-run /path/to/repo
meerkat-fugue /path/to/repo
```

The CLI:

- Infers the GitHub `origin` URL and repo name.
- Selects the mob flow and matching Linear team from the backend profile.
- Creates or reuses the Linear project and required workflow statuses.
- Generates native `fugue.yaml` plus prompt files.
- Deploys the queue under `/srv/fugue/queues/<queue-slug>/`.
- Creates or updates `/etc/systemd/system/fugue-<queue-slug>.service` on the GCP VM.
- Starts the service on the first free port in `4001..4099`.
- Reports the service name, port, dashboard tunnel command, Linear project, and config path.

Common options:

```bash
meerkat-fugue /path/to/repo \
  --backend live \
  --flow implementation_mob \
  --project-name "My Repo Fugue Queue" \
  --linear-team LUC \
  --replace-service
```

Safety behavior:

- The CLI requires `LINEAR_API_KEY` locally to create or reuse the Linear project.
- It uses local `gcloud` credentials for the configured project.
- It refuses to replace an existing remote systemd service unless `--replace-service` is passed or
  the existing unit matches the intended queue ownership.
- Linear workflow states are team-wide, not project-local.
- It does not copy secrets into native config. The remote service loads shared secrets from
  `~/fugue/secrets.env`.

## Local Config Generation

From any repo:

```bash
meerkat-fugue stage /path/to/repo \
  --github-repo-url https://github.com/owner/repo.git \
  --linear-project-slug my-repo-fugue-queue-abc123 \
  --linear-team LUC \
  --output .fugue
```

This renders native config and prompt files without deploying a service.

## Local Run Commands

Meerkat/Rust implementation:

```bash
cd /path/to/meerkat-fugue
LINEAR_API_KEY="$LINEAR_API_KEY" cargo run -- /path/to/fugue.yaml --port 4000
```

Use locally built Fugue and MCP binaries when testing queue generation:

```yaml
mcp_servers:
  linear:
    command: [/path/to/meerkat-fugue/target/debug/fugue-linear-mcp]
  github:
    command: [/path/to/meerkat-fugue/target/debug/fugue-github-mcp]
```

## Legacy Helpers

The skill still contains old Python helper scripts for reference. They generate v0 `WORKFLOW.md`
queues and should not be the default path for new GCP onboarding:

```bash
python3 .rkat/skills/meerkat-fugue/scripts/onboard_repo.py --help
python3 .rkat/skills/meerkat-fugue/scripts/render_workflow.py --help
```

## Smoke-Test Expectations

A healthy first issue should:

- Be in an eligible intake state (`Queue` or the queue's configured intake lane) and not blocked by non-terminal blockers.
- Move from intake into the configured public active lane, normally `Active`.
- Receive or update one persistent `## Fugue Workpad` comment.
- Produce a branch, validation notes, and a PR if implementation is needed.
- Keep planning, implementation, review, and remediation detail in labels, WorkGraph, Workpad, and Meta history.
- Move to `Blocked` only when human intervention is required, and `Done` only when the terminal success criteria are satisfied.
