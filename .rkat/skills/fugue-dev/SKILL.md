---
name: fugue-dev
description: Develop, review, or debug Meerkat Fugue code and architecture safely. Use when working in /path/to/meerkat-fugue or changing Fugue's Meta runtime integration, Kapellmeister authority, Linear projection, Workpad contract, console behavior, structured outputs, production smoke testing, deployment flow, or Meerkat/Meta dependency wiring.
---

# Fugue Dev

Use this skill when changing Fugue itself. It captures the hard-won architectural constraints that keep Fugue from regressing into host-side orchestration, fake Meta console behavior, Linear-as-authority, or Workpad-prose state machines.

For operating an existing Fugue queue, also load `$meerkat-fugue`. For upstream Meta work, load `$meta-platform`. For upstream Meerkat runtime/session/mob behavior, load `$meerkat-platform` and treat `/path/to/meerkat` as read-only unless the user explicitly says otherwise.

## First Move

Before designing or editing:

1. Read `/path/to/meerkat-fugue/AGENTS.md` and any repo-local instructions.
2. Run `git status --short` and do not overwrite unrelated work.
3. Identify whether this is config/runtime, Linear projection, console/Meta, structured-output/deadlock, production/deploy, or dependency work.
4. Load only the matching reference file(s) below.
5. If the change can mutate the live queue, park production first or explicitly confirm the user wants live mutation.

## Reference Map

- Architecture authority, topology, and code ownership: `references/architecture-boundaries.md`.
- Meta runtime, console, sessions, history, and send behavior: `references/meta-console.md`.
- Linear projection, lanes, Workpad, and issue lifecycle: `references/linear-and-workpad.md`.
- Structured output, reviewer gates, failures, and deadlock prevention: `references/structured-output-and-deadlocks.md`.
- Production deploy, smoke tests, crates, and browser validation: `references/production-smoke-and-deploy.md`.
- Known traps and scars that should block questionable designs: `references/hard-learned-lessons.md`.

## Non-Negotiables

- Meta/Meerkat mob state is the lifecycle source of truth. Linear is an operator-facing projection.
- Fugue host code is mechanism: queue policy, normalized signals, tool adapters, idempotency, safety, evidence recording. It must not own planner/coder/reviewer turn sequencing.
- Kapellmeister is the repo/project-level authority. Issue-level agents coordinate per issue. Planner/coder/reviewer members execute bounded roles.
- Use real Meta runtime and console surfaces. Do not grow another Fugue-only Meta-shaped facade unless it is a temporary diagnostic shim with a removal plan.
- `tools: [mob]` means Meerkat-provided mob tools, not Fugue-defined tools.
- Members consume MCP tools from scoped server IDs such as `mcp:linear` and `mcp:github`. Mobs coordinate; they are not the owner of MCP source identity.
- Structured outputs and typed Meta events drive control decisions. Workpad prose is evidence for humans, not the primary lifecycle authority.
- Kapellmeister or repo-level authority moves Linear lifecycle lanes. Children may inspect Linear and update the Workpad but should not call lifecycle state mutations unless a config explicitly grants that authority.
- Terminal states must be explicit. Never leave a flow able to wait forever in Planning, Implementing, or Reviewing.
- Console visibility must reflect addressable Meerkats, not hidden container mobs. Retired terminal mobs should not linger in the live roster.

## Implementation Loop

Use this loop for substantive Fugue changes:

1. Write or update tests first at the boundary that failed: config parsing, projection evaluator, runtime registry, console timeline, structured gate, Linear/GitHub tool adapter, or production-smoke harness.
2. Implement with typed Rust boundaries. Avoid `serde_json::Value` past parsing boundaries, `.unwrap()`/`.expect()` in library code, silent mechanism-error swallowing, and collect-then-join formatting in repeated paths.
3. Validate locally with the narrow test first, then `cargo fmt --check`, `cargo check -q`, `cargo test -q`, and `cargo run -- --help` when feasible.
4. For console-visible changes, validate with the Browser/browser-use skill against the real console. Confirm refresh and live streaming, not just initial HTML.
5. For production behavior, use fresh Linear issues only. Old polluted issues are useful forensic evidence but not a green smoke.
6. Record what was tested and what was not. If production, include binary/crate versions, commit/hash evidence, PR/check evidence, and console transcript evidence.

## Design Smell Checklist

Pause and re-think if a design does any of these:

- Hardcodes `Planning`, `Implementing`, `Reviewing`, `In Progress`, `Human Review`, or `Ready for review` outside projection config.
- Parses arbitrary Workpad prose to decide whether a reviewer gate is green.
- Launches planner/coder/reviewer directly from a host polling loop.
- Reimplements Meta timeline stitching, identity routing, session history, or send acknowledgements in Fugue.
- Treats a Linear lane change as proof that a mob step completed.
- Makes issue mobs visible as chat targets when they are containers, not addressable Meerkats.
- Lets children merge PRs or move terminal lanes without repo-level authority.
- Solves a production deadlock by manually pushing the issue to the next lane instead of fixing the missing event/state contract.
- Adds git/path dependencies to local Meerkat or Meta worktrees when a crates.io release is available.
