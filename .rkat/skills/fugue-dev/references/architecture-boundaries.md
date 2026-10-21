# Architecture Boundaries

## Desired authority model

Fugue is a semi-mechanical engine around Meerkat/Meta, not a second orchestrator pretending to be one.

- Meta owns mob runtime, durable members, session history, event replay, console REST/SSE/RPC, and member delivery.
- Meerkat mob state owns lifecycle truth: flow nodes, member state, structured outputs, and terminal outcomes.
- Kapellmeister owns repo/project-level coordination: issue intake, template selection, issue lead creation, PR handoff, merge/deploy evidence, and Linear lane projection.
- Issue lead agents own issue-level coordination: keep Kapellmeister informed, launch or communicate with the selected execution submob, consolidate member evidence, and ask for repo-level handoff.
- Planner/coder/reviewer or other specialist members own bounded work products, not global lifecycle policy.
- Fugue host owns mechanisms: polling, normalized Linear/GitHub signals, workspace setup, MCP adapters, idempotent tool execution, service lifecycle, safety gates, and durable evidence recording.

## Topology to preserve

Do not flatten everything into the old trio.

The target shape is:

```text
Kapellmeister (repo/project Meerkat)
  peers with issue lead LUC-123
    issue lead coordinates selected template/submob
      planner
      coder
      reviewer
```

Other templates may be story writer -> illustrator -> critic -> publisher, review panels, deploy agents, or domain-specific expert groups. Do not build assumptions that only one planner/coder/reviewer mob type can exist in a Fugue instance.

## Code map in meerkat-fugue

Start with these files for most changes:

- `src/config.rs`: typed config, legacy compatibility, projections, validation.
- `src/orchestrator.rs`: polling, dispatch decisions, recovery, retry/backoff.
- `src/meta_runtime.rs`: embedded Meta composition, native member tools, persistence and shutdown.
- `src/mob_tools.rs`: typed launch contract and offline host adapters; no RPC subprocess execution or native database probing.
- `src/mob_event_observer.rs`: Meta event observation and durable cursors.
- `src/native_mob_contract.rs`: native execution contract and evidence checks.
- `src/native_reviewer_gate.rs`: structured reviewer gate schema and extraction.
- `src/kapellmeister*.rs`: repo-level authority and guarded/shadow policy paths.
- `src/http.rs`: status/console serving. Prefer real Meta console substrate over Fugue bridges.
- `src/linear_tools.rs`: typed Linear helper MCP/tool boundary.
- `src/github_tools.rs`: typed GitHub helper MCP/tool boundary.
- `fugue.example.yaml`: canonical config example.
- `prompts/fugue-worker.md` and `prompts/native-launch-context.md`: prompt contracts.

## What Fugue must not own

Fugue must not semantically reimplement:

- Meta session history or replay.
- Meta console timeline stitching.
- Meerkat mob/member scheduling.
- Planner/coder/reviewer turn loops.
- Linear workflow as a state machine.
- PR/check/merge policy inside child members.

If a missing capability is general Meta or Meerkat substrate, fix it upstream or add a thin temporary adapter with tests and a removal plan.

## MCP and tool ownership

Hard lesson: mobs are coordinators, members are tool consumers.

- Do not make a mob definition the canonical owner of MCP source identity.
- Native profiles declare `ToolConfig.mcp_servers`; Meerkat owns each session’s MCP router and provenance. Do not start a second shared router or duplicate module process.
- Member/profile `tools` allowlist source IDs such as `mcp:linear`, `mcp:github`, `shell`, `git`, and `mob`.
- Bare `linear` or `github` pseudo-tools should fail config validation.
- `tools: [mob]` means Meerkat's native mob tool surface (`delegate`, `mob_create`, `mob_spawn_member`, `mob_run_flow`, etc.), not Fugue-owned facsimiles.

## Production retake rule

For invasive architecture changes, the first operational step is to stop/mask the live Fugue service and associated RPC/mob service so the old runtime cannot pick up or mutate Linear issues while the retake is in progress.
