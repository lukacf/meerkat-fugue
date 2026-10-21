# Structured Output And Deadlocks

## Principle

Typed artifacts drive control. Workpad prose is human evidence and fallback context, not primary authority.

Use Meerkat structured output support through agent/profile output schemas and consume structured output from Meerkat/Meta events. If the event surface is missing a required typed payload, fix the upstream surface or add a temporary typed side channel with tests. Do not string-parse arbitrary Workpad paragraphs as the primary green/red decision.

## Important upstream history

Meerkat PR #627 added optional `structured_output` to relevant agent and mob events, including run/interaction completion and flow completion paths. Meta later confirmed pass-through in projections and SDK surfaces.

Fugue should assume structured output is available in modern Meerkat/Meta, but dependency bumps must verify the event fields still reach the exact Fugue runtime path.

## Reviewer gate contract

Reviewer gates must be typed. A green reviewer gate can trigger repo-level PR handoff even if a later cleanup/finalization mechanism fails. A terminal reviewer/member failure without durable typed green evidence must park or block safely without retry loops.

Test both sides:

- green structured reviewer output plus later host-mechanics failure still reaches Kapellmeister PR handoff;
- terminal LLM failure without a typed green gate parks as blocked/human-needed;
- no arbitrary Workpad prose parsing is the primary authority.

`src/native_reviewer_gate.rs` is the first place to inspect for current gate schemas and tests.

## Terminal outcomes

Every template needs explicit end states. Examples:

- success / done;
- ready_for_review / ready_for_pr_handoff;
- blocked / human_needed;
- failed / cannot_do;
- cancelled;
- duplicate;
- rework_requested with bounded cycle count.

Do not let members wait forever for unclear instructions. If a member lacks tools, credentials, a deploy URL, approval, or production-smoke evidence, it should emit a typed blocked/human-needed outcome and notify the issue lead/Kapellmeister.

## LLM failure handling

A Meerkat LLM failure should surface as a structured failure with enough metadata for Fugue policy:

- member identity and session;
- interaction/turn/run/flow ids;
- whether any valid structured output was already persisted;
- terminal vs retryable classification;
- provider/model error category when safe to reveal;
- whether cleanup/finalization failed after the useful artifact.

If Meerkat only reports opaque `Terminal failure: Failed (LlmFailure): LLM failure terminal turn`, Fugue can classify safely but cannot infer green. Ask upstream for richer error surfacing instead of burying it in string matching.

## Deadlock smell examples

- Reviewer blocks on manual browser smoke but has no deploy URL or browser tool. Correct action: typed human-needed/deploy-smoke-needed outcome to Kapellmeister, not endless Reviewing.
- Kapellmeister cannot merge because it lacks GitHub/PR tools. Correct action: grant repo-level authority/tooling or route to human-needed, not ask the child to merge.
- Issue lead cannot reach planner/coder/reviewer. Correct action: topology/reachability event and repair/reconcile, not lane-pushing.
- Host manually moves issue to the next lane to unstick it. Correct action: find which typed event/output was missing.

## Cycle bounds

Planner/coder/reviewer loops must have bounded review cycles such as `max_review_cycles`. Red review returns to a configured plan/rework node with findings. Exhausted cycles should become blocked/human-needed with evidence.
