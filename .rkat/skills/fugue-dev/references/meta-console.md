# Meta Runtime And Console

## Principle

Use Meta as Meta. The console should be backed by Meta runtime state, session history, identity routing, and event replay, not by a Fugue-specific imitation.

Fugue may wrap the edge with auth/IP allowlist and may append app-domain frames, but it should not own console chronology, session readback, live/history stitching, or member send acknowledgement semantics.

## Required console substrate behavior

A correct console integration has:

- multiple registered runtimes under stable runtime keys;
- an aggregator-owned durable console log with monotonic aggregate cursor;
- canonical `query_timeline(identity, after, limit)` over durable frames;
- replayable timeline SSE with `snapshot_started`, replayed frames, `snapshot_complete`, then live frames;
- identity-first send such as `send(identity, content, idempotency_key)`;
- immediate persisted user-input frame before dispatch;
- idempotent retry without duplicate user messages;
- deterministic refresh with no timestamp-only merge;
- no history/live duplicate frames;
- server-side visibility policy for roster, inspect, send, timeline, replay, and redaction.

## Visibility rules

The live roster should show addressable Meerkats:

- Kapellmeister;
- issue lead agents;
- currently addressable specialist members such as planner, coder, reviewer.

It should not show hidden container mobs as chat targets. Terminal retired mobs should not linger in the live roster. Retired members may remain readable as historical identities only when the console has explicit retired-history affordances.

Health/reachability should use reason codes, not prose guesses, for states such as ready, busy, retired, hidden_by_policy, unreachable_runtime_down, unreachable_member_missing, and unreachable_peer_no_ack.

## Known Meta lessons

- Fugue pins Meerkat and `meerkat-meta` to 0.9.3. Build from `UnifiedRuntime::builder().definition(...)` with `persistent_state(...)`, explicitly paired `persistent_mob_storage(...)` and `.mob_storage(...)`, and incremental session persistence. Do not copy historical session-constructor workarounds into Fugue.
- Keep the historical `mobkit-runtime` directory name for existing installations; package renaming must not create an empty state root. See `docs/upgrading-0.9.3.mdx`.
- Session history and durable session snapshots are different artifacts. Transcript rows can persist while authoritative resume snapshots are missing.
- Session caps must be configuration, not a hard-coded small limit. Fugue may run many thousands of sessions over time.
- Console stream replay must not fail for a fresh browser after a long-lived aggregate log grows beyond a snapshot cap. Fresh viewers need tail-snapshot semantics, persisted client cursors, or paged replay handoff.
- Session-history watermarks must be scoped per runtime and session, not only runtime plus source kind.
- Send idempotency must not depend on redacted display payloads. Compare stable request fingerprints or dedupe keys.

## Console debugging checklist

When the browser shows stale, missing, or out-of-order chat:

1. Confirm whether user input was persisted as a `user_input` frame before dispatch.
2. Confirm the timeline query and SSE stream use the same aggregate cursor/log.
3. Refresh and compare transcript order to the pre-refresh live view.
4. Check whether frames came from session history, live events, or app-domain frames, and whether dedupe keys collide.
5. Inspect identity resolution: browser identity should map to an addressable member, not a hidden mob projection.
6. Verify send acknowledgement has an interaction id, input frame id, cursor, and delivered/failed update.
7. If this is generally useful, push it into Meta console substrate rather than adding a Fugue-only bridge.

## Edge security

Simple public testing can use Basic Auth plus IP allowlist around `/console`. Backend Meerkat/Meta RPC should remain loopback-only. Browser-visible routes may send messages to addressable members but must not expose arbitrary host control or raw RPC mutation surfaces.
