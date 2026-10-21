# EXAMPLE-1 Kapellmeister private peer wiring smoke evidence

Issue: EXAMPLE-1, "Fugue smoke: verify Kapellmeister private peer wiring".

Observation time: 2026-01-01T00:00:00Z.

Native execution context:

- Session id: `native-execution:EXAMPLE-1`.
- Workspace key: `EXAMPLE-1`.
- Flow/template: `planner-coder-reviewer` / `planner_coder_reviewer`.
- Members launched for this issue mob: `planner`, `coder`, and `reviewer`.
- Worktree: `/srv/fugue/workspaces/example-repo/EXAMPLE-1`.

Host lifecycle evidence:

- Kapellmeister host signal `sig-example` reported `worker_started`
  for issue `EXAMPLE-1`, workspace `EXAMPLE-1`, attempt `1`, observed at
  `2026-01-01T00:00:00Z`, cursor
  `EXAMPLE-1:native_mob_started:1`.
- Superseding/enriching Kapellmeister host signal `sig-example`
  reported `worker_started` for issue `EXAMPLE-1`, attempt `1`, session id
  `native-execution:EXAMPLE-1`, observed at
  `2026-01-01T00:00:00Z`, cursor
  `EXAMPLE-1:native_mob_structural_event:1`.
- The host signal is lifecycle authority for this smoke; Linear is only the
  projection.

Issue member evidence from the live native execution launch context:

| Role | Member name | Flow evidence | Runtime state |
| --- | --- | --- | --- |
| planner | `planner` | Native execution launch declared `Members: planner (planner), coder (coder), reviewer (reviewer)` | live issue-mob member for the current flow |
| coder | `coder` | This evidence was captured during the active Coder node for session `native-execution:EXAMPLE-1` | current `Implementing` member |
| reviewer | `reviewer` | Native execution launch declared `Members: planner (planner), coder (coder), reviewer (reviewer)` | live issue-mob member for the current flow |

Private peer/comms evidence from the issue coder member:

- `peers()` returned exactly one trusted peer named
  `fugue-repo-kapellmeister/kapellmeister/kapellmeister`.
- Current canonical peer id observed by this coder: `uuid-example`.
- Address: `inproc://fugue-repo-kapellmeister/kapellmeister/kapellmeister`.
- Sendable kinds: `peer_message`, `peer_request`, `peer_response`.
- Initial reachability was `unknown` before the comms interaction; follow-up
  `peers()` after the current coder request reported reachability `reachable`.
- Non-mutating `checksum_token` request
  `uuid-example` was sent from the issue coder to this
  repo-level peer for subject
  `EXAMPLE-1 private peer wiring smoke from coder to fugue/kapellmeister at 2026-01-01T00:00:00Z session native-execution:EXAMPLE-1`.
- The request tool returned status `sent` with a reserved stream for request
  `uuid-example`; the subsequent reachability
  observation of `reachable` proves the repo-level peer was reachable through
  comms and not merely projected in the console.
- Non-mutating `peer_message` envelope
  `uuid-example` was sent from the issue coder to this
  repo-level peer as a smoke handoff/status message for the current session;
  the tool returned status `sent`.

Current coder revalidation for this implementing node:

- Kapellmeister host signal reported `worker_started` for issue `EXAMPLE-1`,
  workspace `EXAMPLE-1`, attempt `1`, observed at
  `2026-01-01T00:00:00Z`, cursor
  `EXAMPLE-1:native_mob_started:1`.
- `peers()` from the issue coder member returned the same trusted private
  repo peer named `fugue-repo-kapellmeister/kapellmeister/kapellmeister` at
  `inproc://fugue-repo-kapellmeister/kapellmeister/kapellmeister`.
- Current canonical peer id observed by this coder node:
  `uuid-example`.
- Non-mutating `checksum_token` request
  `uuid-example` was sent from the issue coder to the
  repo peer for subject
  `EXAMPLE-1 private peer wiring smoke from coder to fugue/kapellmeister at 2026-01-01T00:00:00Z session native-execution:EXAMPLE-1`.
- Follow-up `peers()` from this coder node reported reachability `reachable`
  for the trusted repo peer after the request.
- Non-mutating `peer_message` envelope
  `uuid-example` was sent from the issue coder to this
  repo-level peer as a status handoff; the tool returned status `sent`.

No runtime wiring code, trust configuration, or peer discovery behavior was
changed for this smoke.
