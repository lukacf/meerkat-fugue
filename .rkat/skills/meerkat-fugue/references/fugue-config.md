# Fugue Configuration

Use this for native `fugue.yaml`, legacy `WORKFLOW.md`, runner settings, and agent/mob
configuration.

## Native `fugue.yaml`

Native `fugue.yaml` is the normal config format for new queues. Generate it with:

```bash
meerkat-fugue stage /path/to/repo --help
meerkat-fugue --dry-run /path/to/repo
meerkat-fugue /path/to/repo
```

The repo-onboarding command writes `fugue.yaml` plus prompt files, deploys them to the configured
backend, and points the systemd service at the native config path.

Expected concepts:

```yaml
repo:
  github: owner/repo
  default_branch: main

linear:
  project_slug: meerkat-fugue-queue-04365fc284f2
  team: LUC
  provisioning:
    mode: create_missing
  lanes:
    parked: Backlog
    ready: Todo
    active: Active
    do_review: Review
    deploy: Deploy
    blocked: Blocked
    success: Done
    canceled: Canceled
    duplicate: Duplicate

linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: Active
      coder: Active
      reviewer: Active
    node_phase_labels:
      planner: fugue:phase:planning
      coder: fugue:phase:implementing
      reviewer: fugue:phase:reviewing
    aggregate:
      precedence: [Active]
      fallback: Active
    terminal_lanes:
      succeeded: Done
      failed: Active
      cancelled: Canceled

workspace:
  root: /srv/fugue/workspaces/meerkat-fugue-implementation-mob

hooks:
  after_create: |
    git clone 'https://github.com/owner/repo.git' .
    git config user.name "Fugue Agent"
    git config user.email "automation@example.com"
    git fetch origin main --prune

mcp_servers:
  linear:
    command: [/srv/fugue/bin/fugue-linear-mcp]
  github:
    command: [/srv/fugue/bin/fugue-github-mcp]

worker:
  native_launch_context_file: prompts/native-launch.md

agents:
  planner:
    model: gpt-5.5
    tools: ["mob", "mcp:linear", "mcp:github"]
  coder:
    model: gpt-5.5
    tools: ["mob", "shell", "git", "mcp:linear", "mcp:github"]

mob_templates:
  planner_coder_reviewer:
    linear_projection: planner_coder_reviewer
    members: ["planner", "coder"]
```

Native Meta issue mobs are prompt-driven by default. Leave `flow:` unset so
Fugue launches the issue lead and worker members without calling Meta
`mob/flow_run`.

Keep secrets out of `fugue.yaml`. Use environment variables loaded from the host's `secrets.env`.

## Legacy `WORKFLOW.md`

`WORKFLOW.md` is both config and prompt:

- YAML front matter before the second `---` is runtime configuration.
- Markdown after the second `---` is the issue prompt template.
- Do not treat the whole file as prompt text.

Common front matter:

```yaml
---
tracker:
  kind: linear
  api_key: $LINEAR_API_KEY
  project_slug: "project-slug-id"
  intake_states: [Todo]
  in_flight_states: [Planning, Implementing, Reviewing]
  active_states: [Todo, Planning, Implementing, Reviewing]
  terminal_states: [Done, Canceled, Cancelled, Duplicate]

polling:
  interval_ms: 30000

workspace:
  root: $FUGUE_WORKSPACE_ROOT

hooks:
  timeout_ms: 120000
  after_create: |
    git clone "$SOURCE_REPO_URL" .
    git config user.name "Fugue Agent"
    git config user.email "automation@example.com"
    git fetch origin main --prune
  before_run: |
    git status --short
    git fetch origin main --prune
  after_run: |
    if [ -d .git ]; then git status --short; fi
  before_remove: |
    if [ -d .git ]; then git status --short; fi

agent:
  max_concurrent_agents: 2
  max_turns: 200
  max_retry_backoff_ms: 300000
  max_concurrent_agents_by_state:
    Todo: 1
    Planning: 2
    Implementing: 2
    Reviewing: 2

meerkat:
  command: rkat-rpc
  realm_backend: sqlite
  enable_shell: true
  enable_builtins: true
  enable_mob: true
  enable_memory: true
  first_turn_activation_prompt: Begin the Meerkat Fugue Linear issue flow.
  turn_timeout_ms: 3600000
  read_timeout_ms: 10000
  stall_timeout_ms: 600000

server:
  port: 4001
---
```

## Meerkat Runner Notes

Legacy queues may still use a Meerkat runner block:

```yaml
meerkat:
  command: /srv/fugue/bin/rkat-rpc
  realm_backend: sqlite
  enable_shell: true
  enable_builtins: true
  enable_mob: true
  enable_memory: true
```

If `meerkat.realm` is omitted, Fugue should not pass `--realm`; `rkat-rpc` chooses its default. Use
structural `connection_ref` objects when binding to a specific Meerkat runtime identity:

```yaml
meerkat:
  connection_ref:
    realm: "realm-id"
    binding: "binding-id"
    profile: "optional-profile-id"
```

## Prompt Body Requirements

Native prompt files and legacy workflow prompt bodies should tell the worker:

- Which Linear state transitions are allowed.
- To move `Todo` to `Planning` when the issue mob begins planning.
- To keep exactly one `## Fugue Workpad` comment current.
- To preserve unrelated user changes.
- How to branch, validate, commit, push, and open a PR.
- How to record PR/check evidence and move through `Planning`, `Implementing`,
  `Reviewing`, and configured terminal or attention lanes.
- What to do when blocked by missing auth, secrets, services, or human decisions.

Prompt variables:

- `{{ issue.identifier }}`
- `{{ issue.title }}`
- `{{ issue.state }}`
- `{{ issue.labels }}`
- `{{ issue.url }}`
- `{{ issue.description }}`
- `{{ attempt }}`

## Native Mob Target

The direction is one persistent repo-scoped Meerkat mob led by a Kapellmeister. The Kapellmeister
owns policy decisions; the host owns credentials, delivery, idempotent tools, process supervision,
and filesystem safety.

Keep future authority expansions staged. Add or widen tool authority, Kapellmeister policy,
execution mob selection, and restart reconciliation in separate runnable stages with real smoke
evidence.
