# Example GCP Deployment

Use this as an example shape for an always-on Fugue installation. Keep live
operator inventory in private runbooks, not in the public repository.

## Current Inventory

- GCP project: `example-gcp-project`
- VM: `example-fugue-host`
- Zone: `europe-west1-b`
- Linux user: `fugue`
- Home: `/home/fugue`
- Fugue root: `/srv/fugue`
- Main Fugue service: `fugue-meerkat-fugue.service`
- Main Fugue port: `4001`

Important paths:

```text
/srv/fugue/bin/meerkat-fugue
/srv/fugue/bin/fugue-linear-mcp
/srv/fugue/bin/fugue-github-mcp
/srv/fugue/bin/rkat-rpc
/srv/fugue/secrets.env
/srv/fugue/prepare-auth.sh
/srv/fugue/source/meerkat-fugue
/srv/fugue/queues/<queue-slug>/fugue.yaml
/srv/fugue/queues/<queue-slug>/prompts
/srv/fugue/logs/<queue-slug>
/srv/fugue/workspaces/<queue-slug>
/srv/fugue/state/<queue-slug>
/srv/fugue/context/<queue-slug>
```

The same VM also runs the older Symphony service under `/home/fugue/symphony`.
Do not restart or edit `symphony-*.service` while operating Fugue unless explicitly asked.

## Connect

```bash
gcloud config set project example-gcp-project
gcloud compute ssh example-fugue-host --project example-gcp-project --zone europe-west1-b
```

Tunnel the Fugue dashboard/API:

```bash
gcloud compute ssh example-fugue-host \
  --project example-gcp-project \
  --zone europe-west1-b \
  -- -L 4001:127.0.0.1:4001
```

Then open `http://127.0.0.1:4001`.

## Service Operations

List Fugue services:

```bash
gcloud compute ssh example-fugue-host \
  --project example-gcp-project \
  --zone europe-west1-b \
  --command 'systemctl list-units "fugue-*.service" --all --no-pager'
```

On the VM:

```bash
sudo systemctl status fugue-meerkat-fugue.service
sudo systemctl restart fugue-meerkat-fugue.service
sudo systemctl daemon-reload
journalctl -u fugue-meerkat-fugue.service -f
```

Check API state from the VM:

```bash
curl -s http://127.0.0.1:4001/api/v1/state | jq
```

Check machine and disk shape:

```bash
gcloud compute instances describe example-fugue-host \
  --project example-gcp-project \
  --zone europe-west1-b \
  --format='value(machineType,status,networkInterfaces[0].accessConfigs[0].natIP)'

gcloud compute ssh example-fugue-host \
  --project example-gcp-project \
  --zone europe-west1-b \
  --command 'nproc; free -h; lsblk; df -h / /home || true'
```

## Current Unit Shape

The long-running main service may still use the legacy `WORKFLOW.md` compatibility path:

```text
WorkingDirectory=/srv/fugue/source/meerkat-fugue
EnvironmentFile=/srv/fugue/secrets.env
EnvironmentFile=/srv/fugue/queues/meerkat-fugue/env
ExecStartPre=/srv/fugue/prepare-auth.sh
ExecStart=/srv/fugue/bin/meerkat-fugue /srv/fugue/queues/meerkat-fugue/WORKFLOW.md --port 4001
Restart=always
RestartSec=15
```

New queues created by `meerkat-fugue /path/to/repo` use native config:

```text
WorkingDirectory=/srv/fugue/queues/<queue-slug>
EnvironmentFile=/srv/fugue/secrets.env
ExecStartPre=/srv/fugue/prepare-auth.sh
ExecStart=/srv/fugue/bin/meerkat-fugue /srv/fugue/queues/<queue-slug>/fugue.yaml --port <port>
Restart=always
RestartSec=15
```

If `systemctl cat` warns that a unit changed on disk, run:

```bash
sudo systemctl daemon-reload
sudo systemctl restart fugue-meerkat-fugue.service
```

## Secrets

`secrets.env` should define names such as:

```text
GH_TOKEN=<redacted>
GITHUB_TOKEN=<redacted>
LINEAR_API_KEY=<redacted>
OPENAI_API_KEY=<redacted>
PATH=<redacted>
```

Queue-specific `env` files should contain non-secret repo settings such as:

```text
SOURCE_REPO_URL=https://github.com/owner/repo.git
FUGUE_ROOT=/srv/fugue
FUGUE_WORKSPACE_ROOT=/srv/fugue/workspaces/<repo-slug>
RUST_LOG=meerkat_fugue=debug
```

Native queues normally do not need queue-specific env files because repository identity, Linear
project, hooks, MCP server commands, and prompt file paths live in `fugue.yaml`. Keep secrets in the
shared `secrets.env`.

Never print secret values in final answers, logs, skills, or committed files.

## Backend Profiles

Configure the operator machine with the live backend before onboarding repositories:

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

The profile is local operator state, stored at `~/.config/meerkat-fugue/backends.yaml` unless
`MEERKAT_FUGUE_BACKENDS` is set.

## Native Queue Onboarding

From the operator machine:

```bash
meerkat-fugue --dry-run /path/to/repo
meerkat-fugue /path/to/repo
```

The command infers the GitHub remote, resolves the flow/team mapping, creates or reuses the Linear
project and statuses, renders native config and prompts, uploads them with `gcloud`, installs or
updates the systemd unit, starts it, and prints the dashboard tunnel details.

## Deploying Updated Fugue

Validate locally first:

```bash
cd /path/to/meerkat-fugue
cargo fmt --check
cargo test
cargo run -- --help
```

The local machine is macOS and the VM is Linux, so do not copy a local macOS Rust binary to the VM.
Use one of these Linux-safe paths:

1. Build on the VM if a Rust toolchain is available.
2. Pull a Linux artifact from CI.
3. Build through the repository's supported Linux/BuildBuddy path, then transfer the artifact.

For a host-side pull/build/replace/restart update, run the reusable OTA helper
from the source checkout:

```bash
scripts/gcp-systemd/ota-update.sh \
  --fugue-root /srv/fugue \
  --service fugue-meerkat-fugue.service \
  --service-manager system
```

The helper fetches the configured branch, resets the host checkout, builds the
three Linux binaries into a warm Cargo target dir, installs
`meerkat-fugue`, `fugue-linear-mcp`, and `fugue-github-mcp` atomically, restarts
the systemd unit, and checks `http://127.0.0.1:4001/api/v1/state`. If the host
has a live process outside systemd, add `--replace-stray-processes` after
confirming the process belongs to the Fugue queue being updated.

For manual artifact deployment, install atomically on the VM:

```bash
install -m 0755 /tmp/meerkat-fugue /srv/fugue/bin/meerkat-fugue.new
mv /srv/fugue/bin/meerkat-fugue.new /srv/fugue/bin/meerkat-fugue
sudo systemctl restart fugue-meerkat-fugue.service
journalctl -u fugue-meerkat-fugue.service -n 100 --no-pager
curl -fsS http://127.0.0.1:4001/api/v1/state | jq
```

Deploying new MCP helper binaries follows the same rule: build or fetch Linux binaries, then replace
`/srv/fugue/bin/fugue-linear-mcp` and
`/srv/fugue/bin/fugue-github-mcp` atomically and restart only the
relevant Fugue services.

## Live Workflow Notes

Native Fugue queues should use:

- `intake_states`: `Todo`
- `in_flight_states`: `Planning`, `Implementing`, `Reviewing`
- `active_states`: `Todo`, `Planning`, `Implementing`, `Reviewing`
- `terminal_states`: `Done`, `Canceled`, `Cancelled`, `Duplicate`
- `agent.max_concurrent_agents`: `2`
- `agent.max_turns`: `200`
- workpad heading: `## Fugue Workpad`
- service root: `/srv/fugue`

After changing workflow or native config, restart the service when in doubt.

## Smoke Evidence

Keep live smoke evidence in a private operator runbook. Public docs should
describe the expected evidence shape only: generated queue slug, service name,
Linear project slug, test issue identifier, port, clone evidence, Workpad
creation, lifecycle lane progression, cleanup result, and binary hashes.

## Stage Deployment Loop

For Fugue-native migration stages:

1. Implement and validate locally.
2. Merge or otherwise produce a Linux-deployable artifact.
3. Deploy to the live Fugue service.
4. Run a live smoke issue through Linear.
5. Use the now-deployed Fugue service to create or implement the next stage.
