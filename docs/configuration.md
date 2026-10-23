# Configuration reference

Fugue's native configuration file is `fugue.yaml` or `fugue.yml`. Native configs
are parsed into typed Rust structs at startup and fail fast on unknown fields or
invalid combinations.

new queues should use `fugue.yaml`.

## Native defaults

For `fugue.yaml`:

- omitted top-level `backend` means `native-execution`;
- omitted `mob.backend` inherits the top-level backend;
- omitted `mob.native` resolves to native runtime enabled for native execution;

Do not combine native execution with `mob.native.enabled: false`; Fugue rejects
ambiguous native-disable mixtures before dispatch.

## Environment variables

String values beginning with `$` can reference environment variables for fields
such as `linear.api_key`.

```yaml
linear:
  api_key: $LINEAR_API_KEY
```

If `linear.api_key` is omitted, Fugue also checks `LINEAR_API_KEY`.

## Top-level shape

```yaml
version: 1
backend: native-execution # optional; omitted is native-execution
tracker_lanes: { ... }
linear: { ... }
linear_projections: { ... }
selection: { ... }
agents: { ... }
mob_templates: { ... }
worker: { ... }
polling: { ... }
workspace: { ... }
hooks: { ... }
agent: { ... }
meerkat: { ... }
kapellmeister: { ... }
server: { ... }
```

## `linear`

Configures issue intake, universal lane anchors, and optional workflow-state
provisioning. Linear is a projection surface owned per issue by the issue lead;
Fugue observes Linear for external lane drift.

Fugue's canonical tracker lane roles are intentionally coarse:

```yaml
tracker_lanes:
  parked: Backlog
  intake: Queue
  active: Active
  review_action: Review
  deploy_action: Deploy
  blocked: Blocked
  success: Done
  canceled: Canceled
```

`Backlog`, `Queue`, `Active`, `Review`, `Deploy`, `Blocked`, `Done`, and
`Canceled` are the stable operator control surface. Planning, implementing,
reviewing, rework loops, template identity, and deploy flavor are internal
Fugue/Meta/WorkGraph/Workpad/config facts, not separate tracker statuses.
When `tracker_lanes` is present, Fugue maps `linear.lanes.ready` to the intake
role, `linear.lanes.active` to the active role, and Review/Deploy to action
lanes. Existing `linear.lanes` configs are still accepted for compatibility.

```yaml
linear:
  endpoint: https://api.linear.app/graphql # optional
  api_key: $LINEAR_API_KEY
  project_slug: your-project-slug
  team: LUC
  provisioning:
    mode: validate # or create_missing
  lanes:
    parked: Backlog
    ready: Queue
    active: Active
    do_review: Review
    deploy: Deploy
    blocked: Blocked
    success: Done
    canceled: Canceled
    duplicate: Duplicate
  labels:
    - fugue:template:meerkat-fugue
```

The lane anchors are semantic defaults used across templates:

- `parked`: intentionally not eligible for work.
- `ready`/`intake`: eligible for fresh dispatch.
- `active`: Fugue is actively working the issue; phase details live in labels,
  Workpad, WorkGraph, and Meta state.
- `do_review`/`review_action` and `deploy`/`deploy_action`: operator action
  inputs that trigger configured review or deploy procedures.
- `success`: terminal success.
- `canceled` and `duplicate`: terminal canceled states.

`linear.team` is required for projection preflight because Linear workflow
states are team-scoped. When `provisioning.mode` is `validate`, Fugue fails
preflight if required states are missing. When it is `create_missing`, Fugue
creates missing team-scoped workflow states. Fugue never deletes or renames
existing Linear states automatically.

`linear.states` is no longer accepted. Configure operator lanes under
`linear.lanes` and mob projection lanes under `linear_projections`.

## `linear_projections`

Maps mob flow node ids to Linear lanes. Member ids are used as a fallback only
when Meta does not expose node ids.

```yaml
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
```

If multiple nodes are active in parallel, Fugue displays the first active lane
from `aggregate.precedence`. Terminal Meta run status maps only through
`terminal_lanes`; PR/check side effects should be represented by mob nodes or
Workpad evidence, not hidden Fugue lifecycle states.

For old queues, Fugue can deterministically repair/report legacy lanes: `Todo`
to `Queue`; `Planning`, `Implementing`, and `Reviewing` to `Active` plus
`fugue:phase:*`; `Do Review` to `Review`; `Human Review` to `Blocked`; and
`In Progress` to `Active`. Repairs are explicit observations, not silent
renames of the Linear workflow.

## `selection`

Chooses which mob template handles an issue.

```yaml
selection:
  default_template: solo_worker
  by_label:
    docs: solo_worker
    native: implementation_mob
```

`by_label` maps Linear label names to template ids. Issues without a matching
label use `default_template`.

## `agents`

Defines reusable Meerkat agent profiles referenced by mob members.

```yaml
agents:
  coder:
    model: gpt-5.5
    provider: openai
    max_tokens: 8000
    tools: [mob, shell, git, mcp:linear, mcp:github]
    skills: []
    system_prompt: Implement inside the assigned workspace only.
    additional_instructions:
      - Keep the Fugue workpad current.
```

Supported fields include model/provider settings, output schema and structured
output retries, provider params, tool/skill lists, and extra instructions. Repo
tools such as Linear and GitHub must be packaged as MCP servers and referenced
with `mcp:<server-name>`; bare `linear`/`github` tool strings are rejected so a
member cannot silently start with an empty tool catalog. The helper MCP binaries
are part of the issue-mob runtime contract: `fugue-linear-mcp` owns Linear
Workpad updates, `fugue-github-mcp` owns GitHub PR/check handoff evidence,
and `fugue-host-mcp` owns host-level Fugue control-plane actions. It hides
`github_merge_pr` by default; expose merge only for an explicit repo-level merge
authority by setting `FUGUE_GITHUB_ALLOW_MERGE=true` on that MCP server. Deploy
or restart them with `meerkat-fugue`, and include all four binary hashes in
post-deploy evidence. The special `mob` tool means
Meerkat-provided mob tooling (`tools.mob=true`), not a Fugue-defined pseudo-tool.

Fugue provides Playwright MCP to native agents by default. The default server is:

```yaml
mcp_servers:
  playwright:
    command: [npx, -y, "@playwright/mcp@latest", --headless]
```

If a queue defines its own `mcp_servers.playwright`, Fugue preserves that
override. Otherwise native configs materialize the default server above and add
`mcp:playwright` to every materialized issue-agent profile. Production hosts
therefore need Node.js, `npx`, the Playwright package, and browser dependencies
available to the Fugue service user.

## `mob_templates`

Defines native execution mob shapes.

```yaml
mob_templates:
  solo_worker:
    members:
      - coder

  implementation_mob:
    linear_projection: planner_coder_reviewer
    max_review_cycles: 2
    members:
      - name: planner
        profile: planner
      - name: coder
        profile: coder
      - name: reviewer
        profile: reviewer
```

Members may be plain profile names or `{name, profile}` pairs. Template backend
inherits the top-level backend unless explicitly set. Native issue mobs are
prompt-driven by default: Fugue launches the issue lead plus worker members and
the issue lead coordinates planner, coder, and reviewer through comms. Leave
`flow:` unset unless the template intentionally uses Meta `mob/flow_run`.
Native planner/coder/reviewer mobs must keep review cycles bounded with
`max_review_cycles`.

## `worker`

Configures prompt templates handed to workers and native mob launch context.
Use either inline template text or a file path for each template, not both.
Relative files are resolved relative to the config file.

```yaml
worker:
  prompt_file: prompts/fugue-worker.md
  native_launch_context_file: prompts/native-launch-context.md
```

The prompt template receives an `issue` object and optional `attempt` counter.
The native launch context also receives workspace, template, backend, flow, and
member data.

## `polling`

```yaml
polling:
  interval_ms: 30000
```

Controls the regular Linear poll cadence. Operators can also request an
immediate poll with `POST /api/v1/refresh` when the status API is enabled.

## `workspace`

```yaml
workspace:
  root: ./fugue_workspaces
```

Each issue gets a directory below this root. Workspace names are derived from
issue identifiers by replacing non-`[A-Za-z0-9._-]` characters with `_`.

## `hooks`

```yaml
hooks:
  timeout_ms: 60000
  after_create: |
    git clone https://github.com/your-org/your-repo.git .
  before_run: |
    git fetch --all --prune
  after_run: |
    git status --short
  before_remove: |
    git clean -fdx
```

Hooks run as `sh -lc <script>` with the issue workspace as `cwd`.

- `after_create` and `before_run` failures abort the current attempt.
- `after_run` and `before_remove` failures are logged and ignored.
- Output is truncated in logs.

## `agent`

Runtime limits for Fugue's orchestration layer.

```yaml
agent:
  max_concurrent_agents: 3
  max_turns: 20
  max_retry_backoff_ms: 300000
  max_concurrent_agents_by_state:
    todo: 1
```

`max_concurrent_agents_by_state` keys are normalized state names and values are
positive concurrency limits.

## `meerkat`

Low-level Meerkat/Meta session settings.

```yaml
meerkat:
  command: rkat-rpc
  realm_backend: sqlite
  model: gpt-5.5
  max_sessions: 4096
  enable_shell: true
  enable_builtins: true
  enable_mob: true
  first_turn_activation_prompt: Begin.
  turn_timeout_ms: 3600000
  read_timeout_ms: 5000
  stall_timeout_ms: 300000
```

Additional supported fields include realm/instance paths, connection refs,
provider params, built-in feature flags, keep-alive, preloaded skills, app
context, shell environment, and flow tool overlays.

## `mob`

Advanced backend/runtime override. Most native configs should omit this section.

```yaml
mob:
  backend: native-execution
  native:
    enabled: true
    shadow: false
```


## `kapellmeister`

Kapellmeister is disabled/shadow-only by default. Shadow mode records
recommendations and comparisons without allowing production mutations.

```yaml
kapellmeister:
  shadow_mode:
    enabled: false
    state_path: .fugue/kapellmeister-shadow.json
  authority_mode: guarded_labeled
  safe_selectors:
    linear_labels:
      - kapellmeister-safe
```

Guarded authority must be narrowed with safe selectors. Kapellmeister decisions
either execute through guarded native host tools or stop; they do not fall back
to another execution path.

## `independent_review` and `deploy` action lanes

Review and Deploy are action lanes: an operator or meta-agent moves an issue to
one of these lanes to request the configured procedure. They are not passive
phase projections from planner/coder/reviewer execution.

For the built-in tracker, `tracker.project_slug` is the repository scope and is
required even when the SQLite database contains only one repository. A shared
database does not share queues: issue, comment and attachment reads and writes
stay within the configured scope. Issue keys cannot be moved to another
repository by creating or updating an issue with the same key.

For Linear, direct issue lookup verifies project membership before returning
issue details or changing assignments, states or comments. Bulk status lookup
and comment feeds carry the configured project filter. An issue moved outside
the configured project is no longer available through that repository's tools.

Review obligations use the repository mob's native WorkGraph realm and default
namespace. Independent-review completion rejects other scopes so blockers remain
visible to terminal and merge checks. Tracker IDs are resolved to canonical issue
keys before those checks. Lifecycle tool calls must use tracker state names so
the configured terminal policy can be evaluated.

`linear_claim_issue` can also request a lane change through `target_state`.
That change receives the terminal-obligation check before either assignment or
transition. Claiming is restricted to issue leads and repository coordinators;
workers report ownership and lifecycle needs to their lead. An assignment-only
claim does not complete the issue or clear review obligations.
Both direct tools and MCP calls apply the same public-lane and action-lane
policy, including accepted dotted tool names. MCP state changes also require
Fugue's WorkGraph configuration; a standalone server without that configuration
refuses the change before contacting the tracker. Read-only calls and
assignment-only claims remain available to authorized callers.

```yaml
independent_review:
  enabled: true
  lane: Review
  success_lane: Active
  rework_lane: Queue
  blocked_lane: Blocked

deploy:
  enabled: true
  action_lane: Deploy
  procedure: .fugue/deploy.md
  ready_lane: Active
  rework_lane: Queue
  blocked_lane: Blocked
  success_lane: Done
  allowed_repositories:
    - owner: lukacf
      name: meerkat-fugue
```

`deploy.action_lane` is the canonical alias for the lane that triggers deploy;
`deploy.lane` remains accepted for compatibility. `deploy.procedure` names the
repo-configured deploy procedure file. The existing `meerkat-fugue` guarded
merge + OTA behavior remains the default deploy procedure when `require_ota_update`
and `ota_update` are configured.

For a repository that needs PR merging without an application deployment, use
the explicit `merge-only` procedure:

```yaml
deploy:
  enabled: true
  action_lane: Merge
  procedure: merge-only
  require_ota_update: false
  ready_lane: Accepted
  rework_lane: Working
  blocked_lane: Blocked
  success_lane: Closed
  allowed_repositories:
    - host: git.example.com
      owner: developer
      name: sandbox
```

Use status names already available in the configured tracker workflow. The
repository allowlist, base branch, exact PR head, CI, and WorkGraph gates still
apply. A successful merge-only action records `status: merged`, the reviewed
head and merge commit, and no OTA evidence. The configured success lane is
terminal for queue intake. Replaying the same completed action is idempotent.

## `server`

```yaml
server:
  port: 0
```

Enables the optional local status API. CLI `--port` overrides this value for the
current process. The bind address defaults to `127.0.0.1` and can be changed
with `--host`.

## `ota_update`

```yaml
ota_update:
  enabled: true
  repo_path: /srv/meerkat-fugue/current
  branch: main
  remote: origin
  build_command: cargo build --release
  health_check_command: target/release/meerkat-fugue --help >/dev/null
  deploy_command: >-
    install -m 0755 target/release/meerkat-fugue /usr/local/bin/meerkat-fugue &&
    install -m 0755 target/release/fugue-linear-mcp /usr/local/bin/fugue-linear-mcp &&
    install -m 0755 target/release/fugue-github-mcp /usr/local/bin/fugue-github-mcp &&
    install -m 0755 target/release/fugue-host-mcp /usr/local/bin/fugue-host-mcp &&
    systemctl restart meerkat-fugue
  lock_path: /var/lock/meerkat-fugue-ota.lock
  trigger_token_env: FUGUE_OTA_TRIGGER_TOKEN
  timeout_ms: 600000
  require_clean: true
```

Configures an operator-owned over-the-air update path for the live Fugue server.
It can be triggered with `meerkat-fugue --ota-update ./fugue.yaml` or by POSTing
the status API at `/api/v1/ota-update` with `Authorization: Bearer $TOKEN` from
`trigger_token_env`. Fugue serializes updates with a lock file, records the
current SHA, fetches and resolves the configured remote branch, creates a staging
git worktree at the target SHA, runs build and optional health check commands in
staging, and only then runs the deployment replacement command from staging. The
live checkout is not reset before build/health pass. The deploy command executes
as the Fugue process user, so keep it atomic and operator reviewed. Production
deploy commands should install the service binary and the helper MCP binaries
(`fugue-linear-mcp`, `fugue-jira-mcp`, `fugue-github-mcp`, and `fugue-host-mcp`) from the same
staging build before restart so member Workpad/GitHub tool behavior matches the
service contract.
