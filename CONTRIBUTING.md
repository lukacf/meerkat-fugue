# Contributing to Meerkat Fugue

Bug reports, documentation fixes, and focused pull requests are welcome. For a
large behavior or architecture change, open an issue to discuss the intended
outcome first. Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## Set up and validate

Install Git and Rust through rustup. The repository's `rust-toolchain.toml` pins
the development toolchain. Clone the repository, create a branch, then run:

```bash
cargo fmt --check
cargo check --all-targets --locked
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
```

Add a focused regression test when it demonstrates a behavior change. Include
the commands and results in your PR, along with any checks you could not run.
The normal suite needs no provider or Linear credentials. Live E2E tests are
opt-in and create real resources; use only an authorized disposable environment.
Browser-test setup is documented in [tests/luc654-ui-validation.md](tests/luc654-ui-validation.md).

## Keep changes reviewable

Follow the Rust rules in [AGENTS.md](AGENTS.md). Keep native Meerkat/Meta state as
the authority for agent lifecycle; do not introduce a parallel host-side agent
orchestrator. Read the [architecture guide](docs/architecture.mdx) when changing
runtime integration.

Keep each PR focused, explain the user-visible behavior, and update relevant
documentation and examples. Preserve existing unrelated changes. Use example
repository paths and infrastructure names in docs and fixtures.

Never commit credentials, `.env` files, local operator configuration, runtime
state, or unredacted console transcripts. Run `scripts/public-readiness/scan.sh
--current-only` before submitting changes that include operational examples.

## License

By intentionally submitting a contribution for inclusion, you agree that it is
licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at the recipient's
option, unless you explicitly state otherwise. Preserve third-party notices and
identify any new third-party material in the PR.
