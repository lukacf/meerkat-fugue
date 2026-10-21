# Stage Validation

Use this when changing native Fugue behavior, backend onboarding, or live queue authority.

## Self-Hosting Rule

Use Fugue to build Fugue:

- Split each stage into Linear issues in the Fugue queue.
- Keep every issue small enough to review independently.
- Let the current deployed queue implement the stage slice when appropriate.
- Require independent or human review for orchestration, deployment, credentials, and live queue
  ownership changes.
- Deploy each accepted stage before using it to create or implement the next stage.

## Validation Ladder

Use the smallest validation set that proves the change, then climb when the blast radius grows.

1. Local static checks:

   ```bash
   cargo fmt --check
   cargo test
   cargo run -- --help
   ```

2. Local Fugue smoke:

   ```bash
   LINEAR_API_KEY="$LINEAR_API_KEY" cargo run -- ./fugue.example.yaml --port 4000
   ```

   Use a harmless Linear issue and confirm workpad creation and state movement.

3. Real E2E when changing live external flow:

   ```bash
   set -a
   source ~/.zshrc
   set +a

   MEERKAT_FUGUE_REAL_E2E=1 \
   cargo test --test real_e2e -- --ignored --nocapture
   ```

4. Live deploy smoke:

   - Configure a GCP backend profile if one is not already present.
   - Onboard a harmless repository with `meerkat-fugue --dry-run /path/to/repo`, then
     `meerkat-fugue /path/to/repo`.
   - Create or move one test issue to `Todo`.
   - Watch logs and Linear state.
   - Confirm workpad, branch/PR behavior, handoff, and cleanup expectations.

## Stage Gate Expectations

### Documentation and Planning

- Docs render as normal Markdown.
- README links are valid.
- No runtime behavior changed unless intentional.

### Config Schema

- Invalid known sections fail before dispatch.
- Legacy `WORKFLOW.md` remains runnable.
- New config has typed Rust models, not raw `serde_json::Value` plumbing.

### Backend Onboarding

- `configure backend gcp-systemd` writes a backend profile without secrets.
- Dry-run onboarding reports the queue slug, Linear target, remote paths, and systemd plan.
- Real onboarding creates or reuses Linear resources, deploys native config and prompts, installs
  or updates systemd, starts the service, and reports tunnel/status information.
- Cleanup disables and removes smoke services when they were temporary.

### Tool Extraction

- Tools have typed inputs and outputs.
- Mutation tools are idempotent where possible.
- Duplicate signals do not duplicate comments, PRs, worktrees, or workers.

### Kapellmeister Shadow Mode

- The Kapellmeister can observe without mutating production state.
- It resumes from durable cursors after restart.
- Its recommendations are logged and comparable to v0 decisions.

### Limited Authority

- Write tools are label-gated or repo-gated.
- There is an operator fallback to v0 policy.
- Human lane moves are interpreted by configured rules.

### Native Execution Mobs

- Execution mobs are selected from config.
- Mob events are observed through durable Meta subscriptions.
- Restart reconciliation works through persisted Meerkat/Meta state.
- The real E2E suite or a live smoke issue proves native mob behavior before authority widens.

## Live Rollout Discipline

Never let a new stage become the sole authority over the live queue on first deploy. Prefer:

1. Shadow mode.
2. Label-gated authority.
3. One test repository.
4. Main Fugue repository.
5. Default behavior.
