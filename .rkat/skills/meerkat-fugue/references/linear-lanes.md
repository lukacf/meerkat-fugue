# Linear Lanes

Use this when configuring Linear so Fugue can use issues as a work queue.

## Recommended Project

Create a dedicated Linear project per repository and mob flow:

- Example: `Meerkat Fugue Implementation`
- Keep GitHub-imported backlog projects separate unless the user wants agents processing those
  imported issues.
- Use one Linear team per mob configuration or flow when possible. Configure the flow-to-team
  mapping in the backend profile with `--flow-team flow=TEAMKEY`.
- Use the project's `slugId` as `linear.project_slug` in native `fugue.yaml`.

## Board View

In the Linear project:

1. Open the project `Issues` view.
2. Set Display to `Board`.
3. Set Columns to `Status`.
4. Enable `Show empty columns` while configuring lanes.
5. Hide terminal columns if desired, but keep the statuses available.

## Default Lane Semantics

Use these lanes unless the repository has a stronger workflow:

| Lane | Fugue role |
| --- | --- |
| `Backlog` | Parked. Not in `active_states`; Fugue should ignore it. |
| `Queue` / `Todo` | Ready queue. Fugue may claim it if unblocked. Prefer `Queue` for new projects; older queues may still use `Todo`. |
| `Active` | Native planner/coder/reviewer execution is active. The current phase belongs in labels, Workpad, WorkGraph, and Meta runtime state. |
| `Review` | Operator action lane for independent review. |
| `Deploy` | Operator action lane for merge/deploy automation. |
| `Blocked` | Human intervention is needed. |
| `Planning`, `Implementing`, `Reviewing` | Legacy public phase lanes. Do not create or project to them; repair to `Active` plus phase labels. |
| `In Progress`, `In Review`, `Merging`, `Human Review` | Legacy states from older queues or external automation; repair to canonical lanes. |
| `Rework` | Legacy intake/control lane. Prefer `Queue` with blocker evidence or `Active` remediation, depending on the configured queue. |
| `Done` | Terminal complete. |
| `Canceled` / `Cancelled` | Terminal stopped. Include both spellings for safety. |
| `Duplicate` | Terminal duplicate. |

Recommended native config:

```yaml
linear:
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
```

Legacy `WORKFLOW.md` queues express the same idea as `tracker.active_states` and
`tracker.terminal_states`.

## Mob Flow Selection

Fugue should not hardcode one universal lane set. Native `fugue.yaml` maps Linear states to
configured meanings such as intake, active, human review, merge-ready, complete, and canceled.
Different teams may use different lane names.

Use labels or tags to select mob templates:

```yaml
selection:
  default_template: standard_planner_coder_reviewer
  by_label:
    small-fix: solo_coder
    risky-change: senior_review_mob
```

## PR Review Gate

Keep accepted issue-mob work in the public `Active` lane. Planning, implementation, local review,
PR handoff, and remediation are internal phase evidence recorded in labels, WorkGraph, Workpad, and
Meta runtime history, not public Linear phase lanes.

Required behavior for the worker or native mob:

1. After accepting queued work, move the issue to `Active`.
2. While `Active`, record PR/check/review state in the Workpad and WorkGraph.
3. If CI fails or a blocking review is present, record the finding as a blocking WorkGraph item and
   keep or return the issue to `Active` for focused replanning/remediation.
4. Use `Blocked` only when a human decision, missing credential, or other external intervention is
   required.

## Concurrency By Lane

Use global concurrency for total worker count and per-state caps for queue shape:

```yaml
agent:
  max_concurrent_agents: 3
  max_turns: 200
  max_retry_backoff_ms: 300000
  max_concurrent_agents_by_state:
    Queue: 1
    Active: 3
    Review: 1
    Deploy: 1
    Blocked: 1
```

The current implementation normalizes state names for comparisons. Prefer exact lane names in
examples so humans can map config to Linear quickly.

## Blockers

Fugue should not dispatch `Todo` issues blocked by non-terminal blockers. Use Linear's blocking
relationship for prerequisite issues. Once blockers reach a terminal state, the issue becomes
eligible again.

## Workpad Convention

Ask agents to create or update exactly one comment headed:

```markdown
## Fugue Workpad
```

Keep plan, acceptance criteria, validation, PR link, blockers, and handoff state in that comment.
Use Linear-renderable task lists:

```markdown
- [ ] Pending item
- [x] Completed item
```
