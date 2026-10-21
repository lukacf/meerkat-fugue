# Operator guide

This guide is for the person running Meerkat Fugue against a real Linear queue.
It focuses on safe setup, day-to-day operations, monitoring, handoff, and
rollback.

## Operating model

Fugue is a long-running service. On each poll it:

1. reads eligible Linear issues from the configured project/state lanes;
2. skips issues that are blocked, already running, terminal, or backing off;
3. creates or reuses a deterministic workspace directory for the issue;
4. runs configured setup/sync hooks;
5. launches the selected native Meerkat mob template;
6. reconciles Meta, workspace, Linear, and GitHub evidence into status and the
   issue workpad.

The service is intended for trusted automation hosts. It does not create a VM,
container, or OS sandbox for worker commands.

## Production setup checklist

- [ ] Create a dedicated Linear API key with access to the target project.
- [ ] Confirm the Linear workflow has every state required by `linear.lanes`
      and the selected `linear_projections`, or opt in to
      `linear.provisioning.mode: create_missing` with an explicit team.
- [ ] Confirm workers have GitHub credentials for the repositories they will
      modify.
- [ ] Install and smoke-test Meerkat/Meta native execution tooling on the
      service host.
- [ ] Deploy `meerkat-fugue`, `fugue-linear-mcp`, `fugue-jira-mcp`, `fugue-github-mcp`,
      and `fugue-host-mcp` together so the service and companion MCP tool
      contracts stay in sync.
      Install or restart all five binaries atomically; issue mob members use
      the helper MCP binaries for Linear Workpad updates and GitHub PR/check/
      merge evidence, so deploying only the service can leave stale member tool
      behavior behind.
- [ ] Record binary hash evidence for every Fugue binary in the issue-mob path:
      `meerkat-fugue`, `fugue-linear-mcp`, `fugue-jira-mcp`, `fugue-github-mcp`, and
      `fugue-host-mcp`.
- [ ] Prepare the remote host once:

  ```bash
  scripts/gcp-systemd/setup-host.sh \
    --fugue-root /srv/fugue \
    --service-user fugue \
    --bin-source /path/to/linux/release/bin
  ```

- [ ] Configure the deploy backend once:

  ```bash
  meerkat-fugue configure backend gcp-systemd \
    --name live-gcp \
    --gcp-project example-gcp-project \
    --zone europe-west1-b \
    --instance example-fugue-host \
    --remote-user fugue \
    --fugue-root /srv/fugue \
    --bin-dir /srv/fugue/bin \
    --flow-team implementation_mob=LUC \
    --default
  ```

- [ ] Onboard a repository with `meerkat-fugue /path/to/repo`.
- [ ] Keep secrets in environment variables such as `LINEAR_API_KEY`, not in
      committed YAML.
- [ ] Set `workspace.root` to a durable path with enough disk space.
- [ ] Make `hooks.after_create` clone the intended repository and
      `hooks.before_run` safely sync it.
- [ ] Review `prompts/fugue-worker.md` and your Linear handoff policy.
- [ ] Run the non-destructive validation suite:

  ```bash
  cargo fmt --check
  cargo test
  cargo run -- --help
  ```

## Starting Fugue

```bash
export LINEAR_API_KEY=lin_api_...
RUST_LOG=meerkat_fugue=info cargo run -- ./fugue.yaml --port 0
```

Notes:

- Fugue starts queues from `./fugue.yaml` by default.
- Use `--port 0` or `server.port: 0` to bind a free loopback port. Use a fixed
  port if your service manager or dashboard expects one.
- The default bind address is `127.0.0.1`. Expose it beyond loopback only behind
  your own access controls.

## Monitoring

Enable the status surface with `--port` or `server.port`.

| Endpoint | Purpose |
| --- | --- |
| `GET /` | Small HTML dashboard for humans. |
| `GET /api/v1/state` | Full orchestrator snapshot as JSON. |
| `GET /api/v1/{issue_identifier}` | Issue-scoped running row, native events, and last error context. |
| `POST /api/v1/refresh` | Ask the poll loop to wake immediately. |

Inspect these fields first when debugging:

- `running[]` — currently executing issues, policy owner, backend, workspace,
  and native recovery details.
- `retrying[]` — failed attempts waiting for retry backoff.
- `last_errors` / issue-scoped error context — hook, worker, timeout, or
  reconciliation failures.
- `kapellmeister_shadow` — shadow recommendations/comparisons when configured.
- `native_recovery.review_required` — evidence mismatches that require a human
  instead of silent completion.

Rows report `backend: native_mob` and include a `native_recovery` object.

### Native Meta stall heartbeat

The Meerkat worker stall guard (`meerkat.stall_timeout_ms`) protects native Meta rows together with the status heartbeat boundary. When a native running row has no new structural events before the
stall timeout, Fugue asks Meta for the run status before canceling anything.
If Meta reports `pending`, `running`, or `waiting_for_input`, Fugue records a
`native_mob_status_heartbeat` instead of issuing `mob.cancel_execution`.

Operators can validate the boundary without SSH spelunking:

- `GET /api/v1/state` shows the native row `last_event` as
  `native_mob_status_heartbeat` and keeps `backend: native_mob`.
- `GET /api/v1/{issue_identifier}` includes the heartbeat in `recent_events`
  with a message such as `stall_timeout_guard: Meta reports run ... is
  Running`.
## Linear handoff

The worker prompt should preserve one persistent Linear comment headed
`## Fugue Workpad` with these sections:

- Plan
- Acceptance criteria
- Progress
- Validation
- Pull request or Handoff

For recovery and handoff, treat both `## Fugue Workpad` and issue-specific
headings such as `## LUC-123 Workpad` as canonical Workpad comments. Production
smokes should leave exactly one issue-specific Workpad for the current issue; if
a generic placeholder and an issue-specific comment both exist, consolidate into
the issue-specific comment instead of creating another generic Fugue Workpad.
When both forms exist, preserve the most evidence-rich comment: prefer the one
with the current issue-specific plan, checked implementation/validation items,
PR URL, check conclusions, merge SHA, and completion notes. Never replace durable PR or
merge handoff evidence with a blank or placeholder progress update just because
the process restarted before a member emitted fresh text.

Use Linear-renderable task lists (`- [ ]` and `- [x]`). Avoid emoji-only status
markers because they are hard to diff and query.

The Workpad is an issue-facing contract, not a runtime diagnostic dump. The
planner should author the issue-specific plan, the coder should check off
implementation and validation evidence, the reviewer should record the gate
result, and the repo agent should record PR/check/merge/deploy evidence. Fugue
may briefly write a "pending planner-authored plan" placeholder before planner
output exists, but it must not synthesize fake issue plans or append Meta tool
catalogs, readiness gates, flow ids, or default validation commands as if they
were issue work.

### Post-quarantine MCP Workpad contract

After quarantining a poisoned pre-fix Meta store, treat a new production run as
green only when the Workpad proves the current MCP path is healthy end to end:

- [ ] Fugue MCP servers answer `tools/list` with protocol-compatible tool
      descriptors: every listed tool uses `inputSchema`; provider-style
      `input_schema` is not exposed on the MCP boundary.
- [ ] The repo mob definition exposes configured MCP servers to every
      planner/coder/reviewer profile that needs Workpad, Linear, GitHub, PR, or
      check evidence (`mcp:linear` and `mcp:github` in the native profile tool
      list).
- [ ] The planner publishes an issue-specific Workpad plan with detailed
      Markdown checkboxes for protected behavior, focused tests/static checks,
      and acceptance criteria.
- [ ] Planner, coder, and reviewer members do not call `linear_move_issue` for
      lifecycle lane movement. Fugue observes Meta flow/member state, then
      guarded native flows require a typed Kapellmeister `move` decision to
      project or repair the Linear lane.
- [ ] The coder checks off implementation items and records concrete validation
      evidence, not opaque summaries such as `text:"ok"`.
- [ ] The reviewer records a gate outcome with findings or green evidence and
      explicit PR/no-PR evidence before the issue can move to `Done`.
- [ ] Member-facing MCP tools provide Workpad, PR, check, and merge evidence
      after the deploy; do not treat service health alone as a production smoke
      pass.
- [ ] Production logs include the native launch shape (`rkat-rpc ... --tcp
      127.0.0.1:<port> --context-root <worktree>`) and `mob/flow_run`, and do
      not include `Transport closed`, `missing_tracker_project_slug`,
      member-owned lifecycle lane mutation attempts, flow-authority recovery
      mismatch, retry loops, stale active rows, `Unexpected response type`, or
      empty MCP catalog behavior.

### Native observation boundary

For native `fugue.yaml` configs, Linear lanes are the issue lead's per-issue
projection of the selected mob's state. Fugue observes Linear for external
lane drift and reports mismatches to Kapellmeister and the issue mob:

- `linear.lanes.ready` / the `tracker_lanes.intake` role (normally `Queue`) is
  an observation source, not a host command. Fugue may report "issue entered
  Queue"; Kapellmeister decides whether
  to start or resume an issue mob.
- `linear_projections.*.node_lanes` document the issue lead's normal in-flight
  projection. Once an issue reaches one of these lanes, the issue mob owns
  lifecycle progress and the host must not dispatch a duplicate mob just
  because the issue is still poll-active.
- If an external tool moves an issue to another lane, Fugue reports the drift
  instead of repairing it from host-side flow semantics.
- Configs that use only `tracker.active_states` are invalid for the native
  control plane; define intake, projection, and terminal lanes explicitly.

Canonical planner/coder/reviewer projection:

- `Queue` / intake: work Fugue observes and reports to Kapellmeister.
- `Active`: planner, coder, and reviewer execution all project here. Detailed
  phases are represented as labels such as `fugue:phase:planning`, Workpad
  checkboxes, WorkGraph obligations, and Meta runtime state.
- `Review`: an operator action lane requesting the configured extra review
  procedure; it is not the ordinary reviewer phase.
- `Deploy`: an operator action lane requesting the configured deploy procedure.
- Legacy `Todo`, `Planning`, `Implementing`, `Reviewing`, `Do Review`,
  `Rework`, `Human Review`, and `In Progress` lanes should be explicitly
  repaired or reported into the canonical lanes instead of treated as new Fugue
  lifecycle states.
- `Done`: requested work is complete and required PRs are merged.

## Handling common incidents

### Hook failure

Symptoms: issue attempt exits before the worker starts; status shows a hook
error.

1. Inspect the issue workspace under `workspace.root`.
2. Re-run the failing hook manually from that directory.
3. Fix `fugue.yaml` or repository credentials.
4. Trigger `POST /api/v1/refresh` or wait for the next poll.

### Worker or Meta failure

Symptoms: issue appears in retry/backoff or `native_recovery.review_required` is
set.

1. Open the issue workpad and PR, if one exists.
2. Inspect `GET /api/v1/{issue_identifier}`.
3. If durable evidence disagrees (run id, branch, PR number, head SHA), leave
   the issue in the reviewer node's projected lane, record the operator finding
   in the Workpad, and resolve manually.
4. If the failure is transient, allow the configured retry backoff to retry.

A terminal Meta `Cancelled` event observed during restart is a recovery signal,
not standalone proof that the Linear issue is canceled. First reconcile the
durable Workpad, workspace, branch, PR, checks, and merge evidence. If the
Workpad already contains valid handoff or completion evidence, continue the
handoff path; if evidence is incomplete but retryable, retry or return to the
projected planner/reviewer lane; only move to a canceled lane when the
operator-facing evidence shows the requested work was intentionally abandoned.

After changing recovery behavior, verify with a fresh issue rather than a reused
or already-poisoned Workpad. The verification Workpad should record the new run,
PR, check, merge evidence when applicable, and the final Linear completion
handoff so a later restart can reconstruct the same outcome.

### Duplicate or missing PR evidence

Native completion requires coherent handoff evidence. Fugue should block
completion and keep the Workpad/projected reviewer-lane evidence current when
it observes duplicate PRs, missing PR handoff, run-id mismatch, or branch/head
disagreement.

Resolve by choosing the correct PR, updating the Workpad, closing duplicates if
appropriate, and returning the issue to the planner node's projected lane for
focused rework or keeping it in the reviewer node's projected lane until the
evidence is coherent.

## Rollback

Rollback is configuration-only.

To pause production lifecycle work, stop the service or move affected issues to
a configured non-active operator lane. Do not return the queue to the retired
another execution backend as a workaround for native Meta/Kapellmeister failures.

Remove the rollback switch to return to native execution after the incident.

## Real E2E warning

The ignored real E2E suite creates live Linear projects/issues, GitHub
repositories/PRs, and real Meerkat sessions. Run it only in an
operator-approved disposable environment or an explicitly approved repository.

```bash
MEERKAT_FUGUE_REAL_E2E=1 \
MEERKAT_FUGUE_E2E_POLICY_MODE=native_only \
MEERKAT_FUGUE_E2E_KEEP_RESOURCES=1 \
cargo test --test real_e2e -- --ignored --nocapture
```

Never run destructive live E2E tests as part of routine documentation or unit
validation.
