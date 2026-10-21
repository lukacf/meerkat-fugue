# Hard-Learned Lessons

## Do not rebuild upstream substrate in Fugue

Fugue accumulated bridges and facades because it needed to move quickly. Treat those as warning signs, not patterns. Runtime, durable sessions, console history, replay, roster projection, and member sends belong in Meta/Meerkat. Fugue should integrate and configure them.

## Stop lane-pushing as debugging

Many historical incidents were temporarily unstuck by moving Linear issues between Planning, Implementing, Reviewing, Ready for review, Rework, or Todo. That is not a fix. The root bug is usually missing typed output, missing event delivery, missing authority, broken topology, or stale runtime state.

## No parallel state machine

Linear projection should not become a second lifecycle machine. It should evaluate current Meta flow/member/run state plus typed terminal outcomes and produce one display lane.

## No single workflow assumption

Planner/coder/reviewer is only one template. Fugue must support different mobs in one repo over time: story production, review panels, documentation teams, deploy mobs, domain expert swarms, and future per-user agents. Projection and topology must be template-driven.

## Issue mobs are not chat identities

An issue mob/container may be useful internally, but the operator should chat with addressable Meerkats: Kapellmeister, issue lead, planner, coder, reviewer, or other members. If the console says `fugue/kapellmeister is an issue mob projection`, something is badly mis-modeled; Kapellmeister is the repo-level Meerkat.

## Kapellmeister needs real authority

If Kapellmeister is expected to hand off PRs or merge, it must have explicit GitHub/PR tools and policy. Do not let it discover at runtime that it has no merge or PR capability. Conversely, do not let children silently merge unless the workflow explicitly grants that.

## Workpad is not a log sink

The Workpad should help a human understand the plan, implementation, validation, review, and handoff. It should not contain giant tool lists, readiness gates, raw Meta receipts, repeated generic boilerplate, or stale placeholder sections that overwrite real evidence.

## Structured outputs beat magic strings

If control depends on `APPROVE`, `READY`, or prose in a comment, the design is fragile. Prefer typed schemas with validation and retry policy. Workpad text can mirror the typed artifact for humans.

## Opaque LLM failures are not enough

`LlmFailure` without structured context is insufficient for orchestration. The system needs to know whether useful structured output was persisted before failure, whether the failure is terminal/retryable, and which member/turn/run failed.

## Retire terminal runtime objects

Expired or terminal mobs should be retired and hidden from live console roster. Keeping thousands of dead live-looking agents confuses operators and masks the actual topology.

## Basic auth is okay for toy public console, raw RPC is not

A simple public-IP console can start with Basic Auth and IP allowlist. Backend RPC/control planes should stay loopback-only. Browser-visible console send should be scoped to addressable member interactions, not arbitrary host mutations.

## Fresh smoke or it did not happen

A fix is not proven by recovering a polluted issue. Create fresh issues and observe the complete path. Reused issues can contain stale Workpads, wrong branch links, old PRs, poisoned Meta state, and manual lane mutations.

## Crate release drift matters

Fugue should consume released Meerkat/Meta crates where possible. If a needed fix is in a local checkout or git pin, document it and retire the pin once crates.io has the release.

## Review with production humility

A green unit test is necessary but not enough for console/runtime changes. Browser-refresh behavior, live stream continuity, and restart/re-admission are where the old bugs hid.
