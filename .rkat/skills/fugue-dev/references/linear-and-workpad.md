# Linear Projection And Workpad

## Linear is projection

Linear lanes mirror the selected mob template/projection. They are not the lifecycle authority.

Config should define semantic anchors and projection profiles, for example:

```yaml
linear:
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
```

Public lane names are coarse operator states. Planner/coder/reviewer phases should project to `Active` and use labels/evidence/Meta runtime state for detail, not public Linear states such as `Planning`, `Implementing`, or `Reviewing`.

## Projection rules

- `linear.lanes.ready` is the fresh intake lane.
- `linear.lanes.parked` is intentionally ignored.
- `linear.lanes.success`, `canceled`, `duplicate`, and configured blocked/human-needed lanes are terminal or operator lanes.
- `linear_projections.*.node_lanes` map Meta flow node ids to active display lanes. In normal Fugue configs planner/coder/reviewer all project to `Active`. Fall back to member ids only when Meta does not expose node ids.
- Parallel active nodes display one lane by configured aggregate precedence.
- Terminal Meta run status maps only through `terminal_lanes` or typed terminal outcomes.
- PR/check side effects should be represented by mob nodes or typed evidence, not hidden Fugue lane inference.
- External drift should repair to the projected lane for current mob state, not to legacy `In Progress` or `Human Review` assumptions.

## Lane provisioning

Linear workflow states are team-scoped. Provisioning must be explicit.

- `provisioning.mode: validate`: fail preflight with exact missing lanes.
- `provisioning.mode: create_missing`: require explicit `linear.team` and create missing states with typed meanings.
- Never delete or rename existing Linear states automatically.
- Prefer a dedicated Linear team/workflow for clean Fugue lanes when collateral damage is possible.

## Who may move lanes

Default rule: Kapellmeister/repo authority moves lifecycle lanes. Planner, coder, reviewer, and other children may read the issue and update Workpad evidence, but should not call lifecycle state mutation tools.

If a template intentionally grants a child lane-mutation authority, make it explicit in config, prompts, tests, and console evidence.

## Workpad contract

There should be exactly one persistent issue-facing Workpad comment headed `## Fugue Workpad` or an issue-specific equivalent.

- Planner writes the detailed checkbox plan, protected behavior, test strategy, and acceptance criteria.
- Coder checks off implementation and validation items and records exact commands/evidence.
- Reviewer writes the gate result, findings, and PR/check interpretation.
- Kapellmeister/repo agent records PR creation, checks, merge, deploy/hash evidence, and final completion.

The Workpad is not a runtime diagnostic dump. Do not paste tool catalogs, readiness gates, raw flow launch receipts, or default validation checklists as if they are the issue plan.

## Polling and refresh

`polling.interval_ms` controls regular Linear polling. Operators can request immediate polling with `POST /api/v1/refresh` when the status API is enabled. Do not use slow polling as an excuse for missing Meta signals; live Meta events should reach Kapellmeister through the runtime/event path.

## Common lane bugs

- Issue appears in Planning, Implementing, or Reviewing: legacy defaults or external automation are leaking in; repair to `Active` plus phase labels.
- Issue stuck in `Active`: usually no terminal outcome, no event delivery to Kapellmeister, or a child waiting on authority it does not have.
- Issue in unexpected Linear states like `In Progress` or `Human Review`: legacy defaults or external automation are leaking in; repair via projection config.
- Ready-for-review issue moved back to Todo does not restart: poll/dispatch eligibility, terminal/retired identity handling, and stale running rows need inspection.
