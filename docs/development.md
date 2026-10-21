# Development guide

## Local validation

Run the safe validation set before opening a PR:

```bash
cargo fmt --check
cargo test
cargo run -- --help
```

The normal unit/integration tests are designed to run without live Linear,
GitHub, or Meerkat resources.

## Real E2E suite

`tests/real_e2e.rs` is ignored by default because it creates live Linear and
GitHub resources and runs real Meerkat sessions. Do not run it casually.

Only run it in an operator-approved disposable environment or a repository that
has explicitly been approved for native execution:

```bash
set -a
source ~/.zshrc
set +a

MEERKAT_FUGUE_REAL_E2E=1 \
MEERKAT_FUGUE_E2E_POLICY_MODE=native_only \
MEERKAT_FUGUE_E2E_KEEP_RESOURCES=1 \
cargo test --test real_e2e -- --ignored --nocapture
```

Useful controls:

- `MEERKAT_FUGUE_E2E_LINEAR_TEAM_KEY` — defaults to `LUC`.
- `MEERKAT_FUGUE_E2E_GITHUB_OWNER` — defaults to `gh api user --jq .login`.
- `MEERKAT_FUGUE_E2E_ISSUE_COUNT` — defaults to `2`.
- `MEERKAT_FUGUE_E2E_POLICY_MODE` — common values: `guarded_kapellmeister`, `native_mob`, `native_default`, and `native_only`.
- `MEERKAT_FUGUE_E2E_MERGING_STATE` — defaults to `Merging`.
- `MEERKAT_FUGUE_E2E_IN_REVIEW_STATE` — defaults to `In Review`.
- `MEERKAT_FUGUE_E2E_CREATE_MISSING_LINEAR_STATES` — defaults to `true`.
- `MEERKAT_FUGUE_E2E_KEEP_RESOURCES=1` — keeps temporary resources for
  debugging.

## Code style

Follow the repository rules in `AGENTS.md`:

- prefer typed structs/enums over `serde_json::Value` except at boundaries;
- return `Result` instead of swallowing mechanism errors;
- avoid `unwrap()`/`expect()` in library code;
- use allocation-conscious iterators and display helpers on hot formatting
  paths;
- keep domain models separate from billing/usage metadata;
- use `IndexMap` when iteration order matters.

## Documentation changes

Documentation should be operator-actionable:

- state whether a command is safe or destructive;
- name required credentials/resources;
- include rollback or handoff guidance for live operations;
- keep examples aligned with `fugue.example.yaml`, backend onboarding docs, and current CLI help;
- preserve the native Meta execution model.

When changing config fields, update:

- `fugue.example.yaml`;
- `docs/configuration.md`;
- backend/onboarding docs when generated queue shape changes;
- relevant operator/development notes;
- tests that assert parsing/default behavior.
