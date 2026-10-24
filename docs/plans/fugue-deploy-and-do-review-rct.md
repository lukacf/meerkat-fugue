# RCT Trace: Fugue Deploy And Review Action Lanes

> Historical validation note: this trace was captured while the deploy/review
> action-lane design still used older lane names in local SQLite evidence. The
> canonical current lane model is `Backlog`, `Queue`, `Active`, `Review`,
> `Deploy`, `Blocked`, `Done`, and `Canceled`. Planner/coder/reviewer progress
> is internal phase labels, WorkGraph, Workpad, and Meta runtime state, not
> tracker status. Entries below are restated in canonical terms; older local
> evidence names should be interpreted as legacy compatibility inputs repaired
> or reported into the canonical model.

> The old HTTP verdict-submission and Workpad-only deploy retry examples below
> are historical evidence, not qualification of current review or merge
> authorization. Current acceptance must follow the
> [native review request contract](../development.mdx#current-review-request)
> and the hosted delivery checks in that guide.

## Requirement Traceability Matrix

| ID | Requirement | Phase | Runtime Caller | Test/Evidence | Status |
|----|-------------|-------|----------------|---------------|--------|
| TYPE-001 | Config can represent repo-limited deploy lane policy. | 0 | config parser | `cargo test -q deploy --lib` (`deploy_lane_config_parses_repo_limited_policy`) | TYPED |
| TYPE-002 | Config can represent independent review lane policy. | 0 | config parser | `cargo test -q independent_review --lib` (`independent_review_config_parses_exact_three_reviewer_policy`) | TYPED |
| TYPE-003 | Review verdict schema captures reviewer id, focus, green/red/blocked verdict, findings, and evidence. | 0 | Meta structured output ingestion | `cargo test -q independent_review --lib` (`independent_review_verdict_round_trips_with_typed_status`) | TYPED |
| TYPE-004 | Deploy outcome schema captures merge, OTA, hashes, health, and failure category. | 0 | deploy gate/tool output | `cargo test -q deploy --lib` (`deploy_outcome_round_trips_success_evidence`) | TYPED |
| REQ-001 | `Deploy` is a configured control lane for `lukacf/meerkat-fugue` only. | 1 | Linear lane classifier / Kapellmeister signal handling | `cargo test -q classifier_uses_configured_deploy_and_review_control_lanes --lib`; `cargo test -q native_config_materializes_deploy_and_do_review_control_lanes --lib` | WIRED |
| REQ-002 | Host observes `Deploy` lane movement and informs Kapellmeister; host does not merge/deploy from polling alone. | 2 | Orchestrator poll loop | `cargo check -q --all-targets`; prompt/static wiring updated, real-entrypoint proof still pending | WIRED |
| REQ-003 | Deploy preconditions reject wrong repo, missing PR, stale SHA, non-green CI, unresolved WorkGraph blockers, missing auth, or unsafe OTA. | 1 | Deploy gate planner | `cargo test -q deploy_gate_ --lib` | WIRED |
| REQ-004 | Deploy success merges exact PR head and runs OTA, then records deploy evidence and moves issue to `Done`. | 3 | Kapellmeister deploy tool path | Local HTTP E2E on `127.0.0.1:43191` with fake `gh` + local OTA repo: `POST /api/v1/kapellmeister/deploy` moved `LOCAL-2` to `Done`; `cargo test -q deploy_projection_records_workpad_and_success_lane_through_ims_adapter --lib` | VALIDATED |
| REQ-005 | Deploy failure records exact evidence and routes to `Review`, `Active`, or `Blocked` by failure category. | 3 | Kapellmeister deploy tool path | Local HTTP E2E on `127.0.0.1:43191`: `POST /api/v1/kapellmeister/deploy` with failed CI projected `LOCAL-4` to canonical remediation state and recorded `ci_not_green`; `cargo test -q deploy_failure_projection_records_workpad_and_target_lane_through_ims_adapter --lib` | VALIDATED |
| REQ-006 | `Review` is a configured control lane that starts an independent review mob through Kapellmeister. | 2 | Orchestrator/Kapellmeister host tool | `cargo test -q guarded_kapellmeister_can_start_independent_review_for_safe_repo --lib` | WIRED |
| REQ-007 | Review orchestrator chooses exactly three reviewer roles/focus areas from PR/issue context. | 2 | Review mob prompt/definition | `independent_review_native_launch_template` prompt; real Meta delegate proof pending | WIRED |
| REQ-008 | Each reviewer independently inspects the PR diff and emits typed verdict; Fugue does not pre-chew the diff. | 2 | Review mob member contract | typed `IndependentReviewVerdict`; prompt requires self-inspection; live proof pending | WIRED |
| REQ-009 | All-green review records evidence and moves to the configured canonical success or attention lane. | 3 | Review aggregation plan | Local HTTP E2E on `127.0.0.1:43191`: `POST /api/v1/kapellmeister/complete-independent-review` with three green verdicts moved `LOCAL-3` to the configured review attention lane; `cargo test -q independent_review --lib` (`independent_review_aggregation_all_green_moves_to_success_lane`) | VALIDATED |
| REQ-010 | Any red review creates a consolidated Linear comment, WorkGraph blocking items, and projects to `Active` for focused remediation. | 3 | Review completion tool path | Local HTTP E2E on `127.0.0.1:43191`: `POST /api/v1/kapellmeister/complete-independent-review` projected `LOCAL-1` to remediation state, created one WorkGraph blocker and findings comment; `cargo test -q independent_review_red_projection_records_comment_workgraph_and_rework_lane --lib` | VALIDATED |
| REQ-011 | Any blocked or timed-out review moves to `Blocked` unless a bounded retry succeeds. | 3 | Review aggregation plan | `cargo test -q independent_review --lib` (`independent_review_aggregation_timeout_blocks`) | WIRED |
| REQ-012 | Repeated `Deploy` or `Review` observations do not duplicate deploys, review comments, or reviewer mobs. | 3 | Deploy Workpad idempotency marker; review WorkGraph dedupe; Meta launch idempotency | Local HTTP E2E repeated `complete-independent-review` returned `findings_comment_id: null`; repeated `deploy` returned `deploy idempotency key is already recorded`; `cargo test -q deploy_ --lib`; `cargo test -q independent_review_red_projection_records_comment_workgraph_and_rework_lane --lib` | VALIDATED |
| REQ-013 | Existing Linear Workpad heading fix is included in the PR. | 0 | Linear Workpad upsert | `cargo test -q workpad_` | WIRED |
| INV-001 | Kapellmeister remains repo-level authority; host mechanisms do not make semantic merge/review decisions from Linear alone. | all | Orchestrator + host tools | `cargo test -q repo_signal_prompt_routes_review_and_deploy_to_kapellmeister_tools --lib`; guarded host-tool tests; local runtime smoke showed lane observations with host tools rather than direct host merge/deploy | VALIDATED |
| INV-002 | Linear remains projection/operator input; Meta/Fugue runtime evidence remains lifecycle truth. | all | Lane classifier + runtime wiring | `cargo test -q concurrent_runtime_bootstrap_is_single_flight --lib`; local `/console/experience` and `/api/v1/state` both came from the canonical Meta runtime after restart | VALIDATED |
| E2E-001 | SQLite IMS flow: issue with PR metadata -> `Review` -> three green -> configured success or attention lane. | 4 | HTTP/API runtime | Local Fugue server `127.0.0.1:43191`, issue `LOCAL-3`; three green verdicts completed to configured review attention with Independent Review Workpad evidence and no findings WorkGraph items | VALIDATED |
| E2E-002 | SQLite IMS flow: issue with PR metadata -> `Review` -> one red -> comment + WorkGraph + `Active`. | 4 | HTTP/API runtime | Local Fugue server `127.0.0.1:43191`, issue `LOCAL-1`; Browser issue UI confirmed focused remediation state, Independent Review Workpad, and finding text | VALIDATED |
| E2E-003 | SQLite IMS flow: issue with PR metadata -> `Deploy` -> fake merge+OTA success -> `Done`. | 4 | HTTP/API runtime | Local Fugue server `127.0.0.1:43191`, issue `LOCAL-2`; Browser issue UI confirmed Done, Deployment Workpad, and deploy idempotency key | VALIDATED |
| E2E-004 | Production Browser smoke proves both new workflows are observable and interactable. | 5 | Browser + live Fugue | TBD | MISSING |

## Typed But Unwired

- `fugue_run_deploy`: HTTP/direct/MCP tool path and SQLite projection tests exist; full fake merge+OTA real-entrypoint flow still needs local E2E proof.
- `fugue_complete_independent_review`: HTTP/direct/MCP tool path and SQLite projection tests exist; full console-visible review mob flow still needs local E2E proof.
- No typed contract remains purely unwired, but all workflow MUSTs remain below `VALIDATED` until the real-entrypoint and Browser E2E phases pass.

## Evidence Log

- 2026-05-26 local checks: `cargo fmt --check`, `cargo check -q --all-targets`, `cargo test -q --lib` passed at 538 library tests.
- 2026-05-26 local API E2E: disposable Fugue server at `127.0.0.1:43191` with SQLite IMS under `/tmp/fugue-review-deploy-e2e.8LPGuF`; `LOCAL-1` red independent review completed to focused remediation with one WorkGraph blocker and no duplicate comment on replay.
- 2026-05-26 local API E2E: `LOCAL-2` deploy used fake `gh` plus local git OTA repo and completed to `Done`; replay skipped merge/OTA through the Workpad idempotency marker.
- 2026-05-26 local API E2E: `LOCAL-3` all-green independent review completed to configured review attention through `POST /api/v1/kapellmeister/complete-independent-review`.
- 2026-05-26 local API E2E: `LOCAL-4` deploy with `ci_status: failure` returned HTTP 409, projected to focused remediation, and recorded failed deploy evidence with `ci_not_green`.
- 2026-05-26 Browser local UI smoke: `/issues`, `/issues/LOCAL-1`, `/issues/LOCAL-2`, `/issues/LOCAL-3`, and `/issues/LOCAL-4` rendered the validated states and Workpad sections. `/console` loaded the stock Meta console, but the local smoke did not prove live review-member topology because no autonomous review mob was launched in this fake-API path.
- 2026-05-26 local runtime smoke after single-flight fixes: fresh restart from the same SQLite IMS config reported `/api/v1/state.last_error == null`, one running mob, and `/console/experience` listed `LOCAL-1/issue_lead`, `LOCAL-1/worker`, and `kapellmeister`; Browser `/console` showed the same three-agent roster immediately.

## Phase Plan

### Phase 0: Types And Boundary Contracts

- TYPE-001: deploy lane policy parses and validates repository selector and OTA requirements.
- TYPE-002: independent review policy parses reviewer count, lanes, timeout, and aggregation mode.
- TYPE-003: independent review verdict schema round-trips and rejects invalid verdicts.
- TYPE-004: deploy outcome schema round-trips with merge/OTA evidence.
- REQ-013: keep Workpad heading fix covered by focused tests.

Done when:

- Config unit tests prove policy parsing/validation.
- Schema unit tests prove typed verdict/outcome behavior.
- `cargo test -q workpad_` remains green.

### Phase 1: Pure Decision Logic

- REQ-001: lane classifier identifies `Deploy` and `Review` as configured action lanes and `Queue`/`Active` as ordinary pickup/remediation lanes.
- REQ-003: deploy precondition planner returns typed success/failure routes.
- REQ-009/010/011: review aggregator returns typed projection/comment/workgraph plans.

Done when:

- Unit tests prove each decision branch, including negative controls for wrong repo and stale SHA.

### Phase 2: Runtime Wiring

- REQ-002: Kapellmeister receives deploy lane observations without host direct deploy.
- REQ-006/007/008: `Review` starts review orchestrator mob and prompts it to select exactly three reviewers.
- INV-001/002: static and runtime tests prove host does not directly merge/deploy from polling.

Done when:

- Integration tests prove lane observation -> Kapellmeister host tool -> Meta run starts for review/deploy.

### Phase 3: Tool Execution

- REQ-004/005: deploy tool merges exact PR head and runs OTA or routes failure.
- REQ-010/012: non-green review writes one durable comment and idempotent WorkGraph obligations.

Done when:

- SQLite IMS integration tests prove success/failure routes through real Fugue API or CLI entrypoints.

### Phase 4: Local E2E

- E2E-001/002/003: local real-entrypoint SQLite IMS flows.

Done when:

- Local console/API smoke shows addressable members and full issue lifecycle for review and deploy.

### Phase 5: PR, Merge, Deploy, Production Browser E2E

- E2E-004: PR created, merged, production deployed, and Browser validates both new workflows.

Done when:

- PR is merged.
- Production reports deployed SHA.
- Browser evidence captures the full review and deploy flows.

- 2026-05-26: Production deploy-lane smoke exercised after PR #207 deployment.
- 2026-05-26: Follow-up production deploy-lane smoke verifies OTA cancellation cleanup.

- 2026-05-26: Detached deploy task smoke verifies client disconnect does not interrupt OTA.
- 2026-05-26: Final production deploy-lane smoke verifies retry evidence and OTA completion.
