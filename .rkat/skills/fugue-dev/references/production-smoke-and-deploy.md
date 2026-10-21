# Production Smoke And Deploy

## Production safety

Before invasive runtime changes, stop and mask the production Fugue service and associated RPC/mob service. Do not let the old runtime keep mutating Linear while code or config is being retaken.

Do not print secrets. Keep API keys in environment variables or live host secret files, never in skill docs, prompts, Linear comments, commits, or final answers.

## Dependency policy

Prefer released crates.io versions for Meerkat and Meta crates. Temporary git/dev pins are acceptable only while waiting for a required upstream release, and should have an automation or explicit follow-up to retire them.

When changing Meerkat/Meta dependencies:

1. Verify the latest required crates are available and resolve.
2. Remove obsolete patch/git/dev entries.
3. Update `Cargo.lock`.
4. Run `cargo fmt --check`, `cargo check -q`, `cargo test -q`, and `cargo run -- --help` when feasible.
5. Record exact crate versions and validation results.

Do not modify `/path/to/meerkat` writeably unless explicitly asked. Other agents may be working there. Read-only investigation is fine.

## Local validation baseline

Run the narrow test first, then the broad set:

```bash
cargo fmt --check
cargo check -q
cargo test -q
cargo run -- --help
```

Add focused tests for every bug:

- config materialization and validation;
- projection evaluator edge cases;
- Linear provisioning exact missing lanes;
- Kapellmeister signal delivery and PR handoff;
- Meta runtime registry restore/restart;
- console timeline replay/live dedupe;
- structured reviewer gate and terminal failure behavior.

## Browser and console smoke

For console changes, do not trust screenshots alone. Use Browser/browser-use or Playwright to verify:

- stock Meta console loads at the deployed URL;
- roster shows Kapellmeister and addressable issue/member agents, not hidden container mobs;
- topology shows expected peering edges;
- sending a message immediately renders a user-input frame;
- assistant/tool responses stream live without manual refresh;
- refresh gives the same transcript in the same order;
- completed/retired members are handled according to policy;
- no stale expired Meerkats linger in the live roster.

## Full production smoke

Use fresh Linear issues only. Old issues with polluted comments or hand-pushed lanes are forensic evidence, not proof of a fixed system.

A credible smoke for a code-improvement issue verifies end to end:

1. Create a fresh issue in the ready lane.
2. Observe Kapellmeister receive the normalized Linear signal.
3. Observe issue lead and selected members appear in console.
4. Planner writes an issue-specific Workpad plan.
5. Coder updates Workpad with implementation and exact validation evidence.
6. Reviewer emits a typed gate artifact and Workpad gate evidence.
7. Kapellmeister records PR handoff, checks, merge/deploy or human-needed outcome.
8. Linear lane projection follows mob state without hardcoded lane repairs.
9. Browser refresh shows full transcripts and no missing/out-of-order user inputs.
10. No retry loops, stale active rows, expired live members, or hidden terminal deadlocks remain.

## Deployment evidence

For deploys, record hashes or versions for all binaries in the issue-mob path:

- `meerkat-fugue`;
- `fugue-linear-mcp`;
- `fugue-github-mcp`;
- relevant Meerkat/Meta crate versions.

Restart helper MCP servers with the service when their contracts changed. A healthy service with stale helper MCP binaries is not a green deployment.
