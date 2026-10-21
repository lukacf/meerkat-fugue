# Architecture

Fugue is intentionally small: one Tokio service, typed configuration, a Linear
tracker, workspace management, worker/native mob launch, and an optional Axum
status surface.

## Component diagram

```text
                 +-------------------------+
                 | fugue.yaml|
                 +-----------+-------------+
                             |
                             v
                 +-------------------------+
                 | WorkflowConfigManager   |
                 | parse / validate /      |
                 | render prompts          |
                 +-----------+-------------+
                             |
                             v
+----------------+   +-------+--------+   +--------------------+
| Linear GraphQL |<->| Orchestrator   |<->| WorkspaceManager   |
| tracker/tools  |   | poll/dispatch  |   | dirs + hooks       |
+----------------+   | retry/recover  |   +--------------------+
                     | shared state   |
                     +---+--------+---+
                         |        |
                         |        v
                         |   +-------------------+
                         |   | HTTP status API   |
                         |   +-------------------+
                         v
                 +-------------------------+
                 | Native Meerkat mob /    |
                 +-------------------------+
```

## Source tree map

- `src/main.rs` — CLI, logging, HTTP server startup, orchestrator task, Ctrl-C
  shutdown.
- `src/config.rs` — `WorkflowConfigManager`, native config parsing,
  typed defaults, prompt rendering, validation.
- `src/orchestrator.rs` — poll loop, dispatch decisions, retries, backoff,
  running state, native recovery, handoff reconciliation.
- `src/tracker.rs` and `src/linear_tools.rs` — Linear GraphQL client and typed
  Linear tool layer.
- `src/workspace.rs` and `src/workspace_tools.rs` — workspace path invariants,
  hook execution, cleanup tool provenance.
- `src/mob_tools.rs`, `src/mob_event_observer.rs`, and
  `src/native_mob_contract.rs` — native Meta execution, event observation,
  and lifecycle evidence checks.
- `src/kapellmeister*.rs` — shadow/guarded policy contracts and host plumbing.
- `src/github_tools.rs` — typed GitHub operations used for PR handoff and CI.
- `src/http.rs` — dashboard and JSON status routes.
- `tests/` — integration and source-only contract tests. `real_e2e` is ignored
  unless explicitly enabled.

## Issue lifecycle

1. **Poll** — the orchestrator fetches issues in configured intake/active lanes.
2. **Filter** — it skips blocked, terminal, already running, claimed, or
   backing-off issues.
3. **Select template** — Linear labels choose a configured mob template, falling
   back to `selection.default_template`.
4. **Prepare workspace** — Fugue creates/reuses the issue workspace and runs
   `after_create` / `before_run` hooks.
5. **Launch worker** — configs launch a native Meerkat mob.
6. **Observe** — Meta events, workspace evidence, Linear workpad state, and
   GitHub PR state are folded into orchestrator state.
7. **Reconcile** — successful runs must satisfy handoff invariants. Failures,
   timeouts, stalls, and incomplete evidence either retry with backoff or stop
   for human review.
8. **Cleanup** — terminal-state startup cleanup and terminal reconciliation
   remove workspaces through replay-safe workspace cleanup.

## Status and recovery

The optional status API exposes the orchestrator's in-memory view plus durable
recovery evidence.

Native running rows include `native_recovery` fields such as:

- Meta run id;
- workspace path, branch, head SHA, and dirty flag;
- Linear branch/workpad run evidence and PR mentions;
- GitHub PR URL, state, and head SHA when available;
- `review_required` when evidence is missing or inconsistent.

Examples that require human review instead of silent completion:

- Linear workpad run id does not match the observed Meta run;
- Linear branch and GitHub PR head disagree;
- workspace head SHA and PR head SHA disagree;
- the workpad or Meta event stream references a different PR number;
- a successful native run lacks durable PR handoff evidence;
- an issue remains in `Review` or `Active` with unresolved review findings
  after external check reruns. `Rework` and old phase-named lanes are legacy
  intake/compatibility signals; accepted active work should project to `Active`
  with planning, implementing, or reviewing recorded as phase labels/evidence.

This recovery model is designed so a restarted Fugue process can reconstruct
active work from durable Meta, workspace, Linear, and GitHub evidence instead
of trusting stale memory.

## Policy boundaries

Fugue distinguishes three concerns:

- **Execution backend** — native Meerkat mob by default for `fugue.yaml`, or
  native Meerkat mob execution.
- **Policy owner** — Rust orchestrator by default; Kapellmeister may run in
  shadow or guarded modes depending on config.
- **Human workflow** — Linear state movement and PR handoff rules encoded in
  worker prompts and reviewed by operators.

Kapellmeister shadow mode is read-only and records recommendations for review.
Guarded authority must be explicitly narrowed with safe selectors. Production
decisions either execute through guarded native host tools or stop; they do not
fall back to another execution path.

## Concurrency and retry model

- `agent.max_concurrent_agents` caps total running work.
- `agent.max_concurrent_agents_by_state` can further cap specific normalized
  Linear state names.
- Worker failures, timeouts, stalls, and turn failures schedule exponential
  backoff up to `agent.max_retry_backoff_ms`.
- Hook failures before worker launch abort the attempt; best-effort cleanup
  hooks are logged if they fail.

## Trust model

Fugue enforces application-level invariants: typed config, path normalization,
under-root workspace checks, replay-safe cleanup, and handoff evidence checks.
It does not sandbox shell commands, repository code, Meerkat tools, or
credentials. Run it only where those capabilities are appropriate.
