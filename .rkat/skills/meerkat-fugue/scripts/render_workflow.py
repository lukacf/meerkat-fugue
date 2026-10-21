#!/usr/bin/env python3
"""Render a starter Meerkat Fugue v0 WORKFLOW.md."""

from __future__ import annotations

import argparse
from pathlib import Path


ACTIVE = ["Queue", "Active", "Review", "Deploy", "Blocked"]
TERMINAL = ["Done", "Canceled", "Duplicate"]


def yaml_list(values: list[str], indent: int = 4) -> str:
    pad = " " * indent
    return "\n".join(f"{pad}- {value}" for value in values)


def render(args: argparse.Namespace) -> str:
    active = [state.strip() for state in args.active_states.split(",") if state.strip()]
    terminal = [state.strip() for state in args.terminal_states.split(",") if state.strip()]

    return f"""---
tracker:
  kind: linear
  api_key: $LINEAR_API_KEY
  project_slug: "{args.project_slug}"
  active_states:
{yaml_list(active)}
  terminal_states:
{yaml_list(terminal)}

polling:
  interval_ms: {args.poll_interval_ms}

workspace:
  root: $FUGUE_WORKSPACE_ROOT

hooks:
  timeout_ms: {args.hook_timeout_ms}
  after_create: |
    set -eu
    git clone "{args.repo_url}" .
    git config user.name "Fugue Agent"
    git config user.email "{args.git_email}"
    git fetch origin {args.default_branch} --prune
  before_run: |
    set -eu
    git status --short
    git fetch origin {args.default_branch} --prune
  after_run: |
    set +e
    if [ -d .git ]; then git status --short; fi
  before_remove: |
    set +e
    if [ -d .git ]; then git status --short; fi

agent:
  max_concurrent_agents: {args.max_agents}
  max_turns: {args.max_turns}
  max_retry_backoff_ms: 300000
  max_concurrent_agents_by_state:
    Queue: {min(args.max_agents, 1)}
    Active: {args.max_agents}
    Review: {min(args.max_agents, 1)}
    Deploy: 1
    Blocked: 1

meerkat:
  command: {args.rkat_command}
  realm_backend: sqlite
  enable_shell: true
  enable_builtins: true
  enable_mob: true
  enable_memory: true
  first_turn_activation_prompt: Begin the Meerkat Fugue Linear issue flow.
  turn_timeout_ms: 3600000
  read_timeout_ms: 10000
  stall_timeout_ms: 600000
  additional_instructions:
    - Work only inside the issue workspace Fugue created for you.
    - Use branch names prefixed with codex/.
    - Keep one persistent Linear comment headed exactly `## Fugue Workpad` updated with plan, progress, validation, and handoff state.
    - 'Format the workpad with Linear-renderable Markdown task lists: use `- [ ]` for pending plan, acceptance, validation, and handoff items, and update completed items to `- [x]`. Do not use emoji-only status markers in place of checkboxes.'
    - Prefer small, reviewable pull requests. If no code change is required, explain why in the workpad and move the issue to Blocked or Done as appropriate.

server:
  port: {args.port}
---
# Meerkat Fugue Issue Work

You are Fugue working unattended on Linear issue `{{{{ issue.identifier }}}}` for `{args.repo_name}`.

## Issue

Title: {{{{ issue.title }}}}

{{% if issue.description %}}
Description:

{{{{ issue.description }}}}
{{% endif %}}

## Linear UX Contract

- Create or update exactly one persistent Linear comment headed `## Fugue Workpad`.
- Keep the workpad useful for humans: Plan, Acceptance criteria, Progress, Validation, Pull request or Handoff.
- Use Linear-renderable Markdown task lists in the workpad: `- [ ]` for pending items and `- [x]` for completed items. Do not use emoji-only status markers instead of checkboxes.
- Move queued issues to Active when an issue mob accepts work.
- Keep internal phase details such as planning, implementing, and reviewing in labels, WorkGraph, Workpad, and Meta evidence; do not create public phase lanes for them.
- Use Review only as an explicit operator action for independent review.
- Use Deploy only as an explicit operator action for merge/deploy automation.
- Move to Blocked when a human decision, missing credential, external blocker, or no-code handoff is required.
- Move to Done only after the requested work is complete and any required PR has been merged.

## Repo Rules

- Work only in the current workspace.
- Branch from `origin/{args.default_branch}` using `codex/{{{{ issue.identifier }}}}` plus a short slug.
- Run the relevant validation before handoff. Start with `{args.validation}` unless the issue narrows or expands the validation set.
- Do not run destructive real E2E tests unless the Linear issue explicitly asks for them and names the required live resources.
- Open a GitHub PR against `{args.default_branch}` when code changes are needed and include the PR URL in the workpad.
- If the issue is blocked or does not need a code change, leave the evidence in the workpad and move to Blocked or Done as appropriate.

{{% if attempt %}}
This is retry or continuation attempt {{{{ attempt }}}}. Inspect the existing workspace, branch, Linear workpad, and any open PR before making changes.
{{% endif %}}
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-name", required=True)
    parser.add_argument("--repo-url", required=True)
    parser.add_argument("--project-slug", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--max-agents", type=int, default=2)
    parser.add_argument("--max-turns", type=int, default=200)
    parser.add_argument("--poll-interval-ms", type=int, default=30000)
    parser.add_argument("--hook-timeout-ms", type=int, default=120000)
    parser.add_argument("--port", type=int, default=4001)
    parser.add_argument("--git-email", default="automation@example.com")
    parser.add_argument("--default-branch", default="main")
    parser.add_argument("--rkat-command", default="rkat-rpc")
    parser.add_argument("--validation", default="cargo fmt --check, cargo test, and cargo run -- --help")
    parser.add_argument("--active-states", default=",".join(ACTIVE))
    parser.add_argument("--terminal-states", default=",".join(TERMINAL))
    args = parser.parse_args()

    if args.max_agents <= 0:
        parser.error("--max-agents must be positive")
    if args.max_turns <= 0:
        parser.error("--max-turns must be positive")
    if args.poll_interval_ms <= 0:
        parser.error("--poll-interval-ms must be positive")
    if args.hook_timeout_ms <= 0:
        parser.error("--hook-timeout-ms must be positive")

    text = render(args)
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
