---
name: meerkat-fugue
description: Set up, configure, operate, deploy, or troubleshoot Meerkat Fugue automation that uses Linear issues as a work queue for Meerkat agents. Use when Codex needs to configure a Fugue backend, onboard a repository with `meerkat-fugue /path/to/repo`, create or reuse a Linear Fugue project/team/lane setup, write or edit native fugue.yaml config, tune worker concurrency, run real smoke/E2E flows, or operate the GCP systemd deployment on example-fugue-host under /srv/fugue.
---

# Meerkat Fugue

Use this skill for Linear-driven Meerkat Fugue work queues: backend setup, one-command repo
onboarding, Linear lane design, native `fugue.yaml` configuration, stage-by-stage self-hosting,
and the current GCP deployment.

## First Move

Identify the task and load only the relevant reference:

- New repo setup or onboarding: read `references/repo-setup.md`.
- Linear board lanes, project slug, active/terminal states, or workpad behavior: read
  `references/linear-lanes.md`.
- Native `fugue.yaml`, Meerkat runner settings, agent profiles, or mob templates:
  read `references/fugue-config.md`.
- Stage validation, real E2E, or using Fugue v0 to build Fugue native: read
  `references/stage-validation.md`.
- Live GCP operations, restart/rebuild/logs/status/deployment: read `references/gcp-live.md`.
- Native Kapellmeister mob target architecture: read `references/native-mob-plan.md`.

If setting up a new repository on the live GCP host, prefer:

```bash
meerkat-fugue configure backend gcp-systemd --help
meerkat-fugue --dry-run /path/to/repo
meerkat-fugue /path/to/repo
```

Configure the backend once per host/operator environment, then run onboarding from the repository
root or pass the repository path explicitly. Use `--dry-run` first if the repo already has a
similarly named queue or service.

If only generating local native config without deploying a service, prefer:

```bash
meerkat-fugue stage /path/to/repo --help
```

The old Python helper scripts in this skill are legacy/reference utilities. Do not choose them for
new GCP onboarding unless the Rust CLI is unavailable and the user explicitly accepts the fallback.

## System Split

There are two related but separate live systems on the same GCP VM. Do not mix their roots,
services, config blocks, or logs.

- **Symphony**: the older Codex queue, rooted at
  `/home/fugue/symphony`, with services named `symphony-*.service`.
- **Fugue**: the Meerkat/Rust queue, rooted at `/srv/fugue`, with
  services named `fugue-*.service`.

When operating Fugue, follow `references/gcp-live.md` and only touch `fugue-*` services unless the
user explicitly asks to operate Symphony too.

## Architecture Split

Fugue has one canonical production path:

- **Fugue native queue**: the Rust service in `/path/to/meerkat-fugue` reads `fugue.yaml`,
  polls Linear, creates per-issue workspaces, and runs native Meta execution mobs.
- **Backend provisioning**: remote-host setup prepares binaries, directories, secrets, auth hooks,
  and systemd shape; `meerkat-fugue configure backend` records how to deploy queues there.

Do not route production work through legacy `WORKFLOW.md`, legacy Rust workers, or
the retired Kapellmeister Rust fallback
it to native `fugue.yaml` before debugging lifecycle behavior.

Keep high-risk native behavior changes staged: test locally, deploy to a live smoke queue, and then
widen authority only after real E2E or live smoke evidence.

## Safety Rules

- Never write API keys or tokens into a skill, repository, Linear issue, prompt body, committed
  workflow, or final answer. Use environment variables or the live host's `secrets.env`.
- Treat `fugue.yaml`, backend profiles, and prompt files as executable automation policy, not prose
  prompt text.
- Do not disturb the live Symphony service while operating Fugue.
- Before increasing concurrency, confirm eligible Linear issues, CPU, disk, provider quota, GitHub
  rate limits, and repository build capacity.
- Keep repo-specific rules in the repo's `AGENTS.md`, `CLAUDE.md`, native `fugue.yaml`, prompt
  files, or Kapellmeister instructions; keep this skill generic.
- For deployment changes, prefer local validation, then live shadow/smoke validation, then broader
  queue authority.
