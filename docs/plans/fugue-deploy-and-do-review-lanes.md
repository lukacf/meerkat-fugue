# Fugue Deploy And Review Lanes Plan

> Historical design note: this document is retained as background for the
> deploy/review action-lane design. The canonical current lane model is
> documented in `docs/configuration.md`, `docs/configuration.mdx`, and
> `docs/native-mobs.mdx`: tracker lanes are `Backlog`, `Queue`, `Active`,
> `Review`, `Deploy`, `Blocked`, `Done`, and `Canceled`; planner/coder/reviewer
> progress is internal phase labels/evidence/runtime state, not tracker status.
> Any older names discussed here are legacy compatibility inputs that must be
> repaired or reported into the canonical model.

> The spawning and caller-supplied verdict design below is also historical.
> Current review admission selects existing specialists in the repository mob
> and verifies native results against the current request. See the
> [current review contract](../development.mdx#current-review-request).

## Summary

Add two repo-level control lanes to Fugue without turning the host into a second orchestrator:

- `Deploy`: only enabled for `lukacf/meerkat-fugue`; performs guarded PR merge plus production OTA update for an issue that already has durable PR handoff evidence.
- `Review`: starts an independent review mob. A review orchestrator inspects the issue and PR, chooses three reviewer roles/focus areas, spawns those reviewers, aggregates typed verdicts, and either green-lights the issue or sends concrete findings to normal Active handling.

Both lanes are Linear/operator control inputs. Fugue host observes them and informs Kapellmeister. Kapellmeister remains the repo authority that starts the correct mob/template and moves lanes. The host provides mechanisms, tools, idempotency, and safety checks only.

## Goals

- Make `meerkat-fugue` production deploys first-class and observable.
- Keep merge/deploy authority repo-scoped and guarded.
- Make independent review a reusable mob template, not a hardcoded planner/coder/reviewer variant.
- Ensure review findings become actionable work for the normal implementation mob.
- Preserve the invariant that Linear is projection plus operator input, while Meta/Fugue runtime evidence remains lifecycle truth.

## Non-Goals

- No general deploy lane for arbitrary repositories in the first implementation.
- No host-side direct merge or OTA triggered only by polling Linear.
- No planner/coder/reviewer sequencing encoded in the new review system.
- No Workpad prose as the primary green/red signal.
- No automatic terminal success if merge or OTA evidence is missing.

## Lane Model

This plan predates the canonical lane simplification and should be read as historical design context. The current tracker model uses coarse operator lanes:

- `Queue`: operator requests ordinary issue-mob pickup.
- `Active`: Fugue is actively planning, implementing, reviewing, or doing focused replan work; detailed phase is recorded as labels/evidence/runtime state.
- `Review`: operator requests the configured review procedure.
- `Deploy`: operator requests the configured deployment procedure.
- `Blocked`: human intervention.
- `Done`, `Canceled`, `Duplicate`: terminal.

Legacy/noncanonical lane names from this plan are compatibility inputs only. They must be repaired or reported deterministically into the canonical model rather than taught as current steady-state tracker lanes.

The lane names must come from config. Do not hardcode them into runtime policy except in the generated `meerkat-fugue` queue config.

## Deploy Lane

### Trigger

The operator moves an issue to `Deploy`.

Fugue host observes:

```json
{
  "kind": "linear_lane_changed",
  "issue": "LUC-631",
  "from": "Review",
  "to": "Deploy",
  "classification": "control_active",
  "running_mob": false
}
```

The host sends this as a normal runtime input to Kapellmeister. It does not merge, deploy, or move the issue by itself.

### Preconditions

Kapellmeister/deploy mob must verify:

- Repository is exactly `lukacf/meerkat-fugue`.
- Issue has durable PR evidence:
  - PR URL or PR number in Workpad or structured handoff evidence.
  - PR belongs to `lukacf/meerkat-fugue`.
  - PR base is `main`.
  - PR head SHA is fresh and recorded.
- PR is open and mergeable.
- Required CI is green for the exact head SHA.
- There are no unresolved blocking WorkGraph items for the issue realm.
- No unresolved review findings remain.
- Merge policy is authorized for this repo and lane.
- OTA target is the configured production Fugue service.

If any precondition fails, the deploy path records a typed blocked result and projects to the configured canonical lane:

- missing PR evidence: `Review` or `Active` with exact missing handoff note, depending on repo policy;
- failed CI or merge conflict: `Active` with focused replan/blocker evidence;
- missing auth / unreachable GCP / unsafe deploy: `Blocked`.

### Execution

Use a dedicated repo-level deploy template:

```text
Kapellmeister
  deploy_orchestrator
```

The deploy orchestrator uses guarded tools:

- `github_inspect_pr`
- `github_check_ci`
- `github_merge_pr`
- `fugue_run_ota_update` or equivalent host/GCP deploy tool
- `linear_inspect_issue`
- `linear_upsert_workpad`
- `linear_move_issue`

The deploy itself should use the existing OTA helper:

```text
scripts/gcp-systemd/ota-update.sh
```

The tool should return typed deploy evidence:

- requested issue identifier;
- PR number and URL;
- base branch;
- head branch;
- merged SHA;
- merge method;
- deployed checkout SHA;
- installed binary hashes;
- service name;
- health URL;
- health result;
- console/API smoke result;
- started_at / completed_at;
- failure category if any.

### Output

On success:

- Add or update the canonical `## Fugue Workpad`.
- Add deploy evidence under a stable section, for example `### Deployment`.
- Move issue to `Done`.
- Retire/hide completed runtime identities according to normal policy.

On failure:

- Add exact failure evidence to the Workpad.
- If implementation changes are needed, move to `Active` with focused replan/blocker evidence.
- If human/operator action is needed, move to `Blocked`.
- Do not retry merge/deploy loops indefinitely.

## Review Lane

### Trigger

The operator moves an issue to `Review`.

Fugue host observes the lane change and sends a typed observation to Kapellmeister. The host does not spawn reviewers directly from the polling loop.

### Mob Shape

Use a new template:

```text
Kapellmeister
  review_orchestrator
    reviewer_1
    reviewer_2
    reviewer_3
```

The review orchestrator is a long-lived autonomous member for the review run. It may dynamically spawn reviewers using Meta native mob/delegate APIs. The three reviewers are selected by the orchestrator after inspecting the issue, Workpad, PR diff, and touched modules.

Example reviewer choices:

- Rust correctness and API boundary reviewer.
- Meta/runtime integration reviewer.
- Product/console/operator workflow reviewer.
- Test coverage and regression reviewer.
- Security/secrets/public-readiness reviewer.
- Deploy/ops reviewer.

The orchestrator should choose exactly three, write the choices and rationale to the Workpad, then spawn those reviewers with strict neutral prompts.

### Reviewer Contract

Each reviewer must independently inspect the PR diff against base. Fugue should not pre-chew the diff into the prompt. The reviewer has shell/GitHub tools and must fetch what it needs.

Each reviewer emits a typed verdict:

```json
{
  "schema_version": "fugue.review.v1",
  "reviewer_id": "runtime-boundary",
  "focus": "Meta runtime boundary and console behavior",
  "verdict": "green | red | blocked",
  "summary": "short human-readable summary",
  "findings": [
    {
      "severity": "blocker | major | minor | note",
      "title": "short title",
      "detail": "specific evidence",
      "file": "optional path",
      "line": 123
    }
  ],
  "evidence": {
    "base": "main",
    "head_sha": "...",
    "commands": ["..."],
    "checked_paths": ["..."]
  }
}
```

`blocked` means the reviewer could not complete the review due to tooling/auth/context failure. It is not green.

### Aggregation

The review orchestrator waits for all three typed verdicts or a bounded timeout.

Rules:

- all three `green`: record review evidence and move to `Review`, `Deploy`, or the configured success/attention lane depending on repo policy;
- any `red`: create one consolidated Linear issue comment with findings, add/update blocking WorkGraph items, and move to `Active` for focused replanning/remediation;
- any `blocked`: move to `Blocked` unless the blocker is clearly retryable and within a bounded retry policy;
- timeout: move to `Blocked` with missing reviewer identities and last known evidence.

The consolidated review comment should be separate from the Workpad, because it is operator/actionable review feedback. The Workpad should contain the review summary, reviewer roster, verdicts, and links to the comment / WorkGraph items.

### Focused Replan Flow

When non-green review findings exist:

1. Review orchestrator adds a Linear comment beginning with a stable marker such as:

   ```text
   Fugue independent review findings
   ```

2. It creates or updates WorkGraph items in namespace `fugue`, realm `<issue>`, labels:

   ```text
   fugue, issue:<issue>, review, blocking
   ```

3. It moves or keeps the issue in `Active` for focused replanning/remediation.
4. Kapellmeister observes the blocker evidence and starts/resumes the normal implementation mob in `Active`.
5. Planner/coder/reviewer use the findings and WorkGraph items as obligations.
6. Only the normal reviewer or a later independent review run may close review-derived blocking items.

This keeps the implementation path unchanged: red review does not invent a new tracker status or worker role. It feeds the usual mob with stronger obligations.

## Configuration Shape

Current repo-local config concepts should use the canonical lane roles and the
review/deploy action-lane config. Older keys from the original plan, such as a
separate review-ready lane, are historical context only.

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

linear:
  lanes:
    ready: Queue
    active: Active
    blocked: Blocked
    deploy: Deploy
    do_review: Review

deploy:
  enabled: true
  action_lane: Deploy
  procedure: .fugue/deploy.md
  allowed_repositories:
    - owner: lukacf
      name: meerkat-fugue
  require_ci_success: true
  require_no_blocking_workgraph: true
  require_ota_update: true

independent_review:
  enabled: true
  lane: Review
  non_green_lane: Active
  blocked_lane: Blocked
  success_lane: Review
  reviewer_count: 3
  require_all_green: true
  max_duration_minutes: 30
```

If this is too much config initially, hard-wire the policy in the generated production `fugue.yaml` for `lukacf/meerkat-fugue` only, but keep the parser/model shaped so it can generalize later.

## Tooling Needed

Deploy lane likely needs one new host/repo tool:

```text
fugue_run_ota_update
```

Inputs:

- issue identifier;
- PR number or URL;
- expected head SHA;
- expected base branch;
- service;
- backend profile or explicit GCP target;
- idempotency key.

Outputs:

- accepted/rejected;
- merged SHA;
- deployed checkout SHA;
- installed binary hashes;
- health status;
- stdout/stderr summary path;
- failure category.

The tool must be guarded:

- loopback-only host path;
- Basic Auth / repo authority if exposed through MCP;
- repo selector check;
- expected SHA check;
- no deploy if PR head changed after review;
- no deploy if CI is stale/failing.

Review lane may use existing Meta spawn/delegate tools plus existing GitHub/Linear/WorkGraph tools. Avoid adding Fugue-specific reviewer spawning if Meta already provides the right primitive.

## State And Idempotency

Required idempotency keys:

- `deploy:<issue>:<pr>:<head-sha>`
- `review:<issue>:<pr>:<head-sha>:<run-generation>`
- `review-finding:<issue>:<reviewer-id>:<finding-hash>`
- `review-comment:<issue>:<pr>:<head-sha>`

Repeated observations of `Deploy` or `Review` must not duplicate deploys, review comments, or reviewer mobs.

Use a small domain job index only for:

- issue identifier;
- requested lane/control action;
- PR number/head SHA;
- started/completed timestamps;
- current Meta run/member identities;
- terminal outcome evidence ids.

Do not mirror Meta runtime state in a Fugue shadow registry.

## Safety Rules

- `Deploy` only works for `lukacf/meerkat-fugue` until explicitly widened.
- Merge and OTA require exact PR head SHA.
- Merge does not imply deploy unless OTA succeeds.
- OTA does not mark Done unless production health and board/API smoke pass.
- Review green does not merge.
- CI green alone does not merge.
- One red reviewer blocks the review.
- One blocked reviewer blocks the review unless a bounded retry succeeds.
- Findings must be durable before moving/keeping the issue in Active for remediation.

## Test Plan

### Unit Tests

- Lane classifier recognizes `Deploy` and `Review` as configured control lanes.
- Repo policy rejects deploy for non-`lukacf/meerkat-fugue`.
- Deploy precondition checker rejects missing PR, wrong repo, stale SHA, failing CI, unresolved WorkGraph items, and missing auth.
- Deploy precondition checker accepts open PR with exact green head SHA and no blockers.
- OTA output parser records installed hashes and service health.
- Review orchestrator prompt/context model asks it to choose exactly three reviewers.
- Reviewer verdict schema accepts green/red/blocked and rejects malformed findings.
- Aggregator sends any non-green verdict to Active/focused remediation with durable comment + WorkGraph items.
- Aggregator sends all-green verdict to `Review`, `Deploy`, or configured success/attention lane.
- Idempotency prevents duplicate review comments and duplicate deploy attempts.

### Integration Tests With SQLite IMS

- Create issue with PR metadata, move to `Review`, run fake reviewer outputs: three green -> configured review/deploy attention lane.
- Create issue with one red reviewer -> consolidated comment, WorkGraph blocking item, lane `Active`.
- Create issue with one blocked reviewer -> lane `Blocked`.
- Create issue with PR metadata, move to `Deploy`, fake merge+OTA success -> `Done` plus deployment evidence.
- Deploy precondition failure returns to `Review`, `Active`, or `Blocked` according to failure category.

### Live Production Smoke

Use a disposable `meerkat-fugue` issue and PR:

1. Start from configured review/deploy attention lane.
2. Move to `Review`.
3. Observe review orchestrator plus three dynamic reviewers in console.
4. Force one non-green finding on first pass and confirm focused remediation path.
5. Let normal mob address finding and return to review.
6. Run all-green review.
7. Move to `Deploy`.
8. Verify PR merge, OTA update, production health, console/API smoke, and Done lane.

Do not use polluted issues as the green proof.

## Rollout Plan

1. Add config model and lane classification for repo control lanes.
2. Add typed review verdict and deploy outcome schemas.
3. Add review mob template and prompts.
4. Add deploy tool boundary and precondition checker.
5. Add SQLite IMS tests for control-lane behavior.
6. Add local Meta console smoke for review topology.
7. Deploy with `Review` enabled but `Deploy` in dry-run/shadow mode.
8. Validate a real PR review cycle.
9. Enable `Deploy` for `lukacf/meerkat-fugue` only.
10. Run one disposable end-to-end issue from review to deploy to Done.

## Open Questions

- Should all-green review move to `Review`, a repo-specific attention lane, or directly to `Deploy` when auto mode is enabled?
- Should deploy require explicit operator lane movement every time, or can a green final review auto-enter `Deploy` later?
- Should reviewer findings be added as Linear comments only, GitHub review comments too, or both?
- Should WorkGraph item closure require the normal reviewer, the independent review orchestrator, or either one?
- Should OTA smoke include Browser console verification on every deploy or only API/health plus periodic Browser smoke?
