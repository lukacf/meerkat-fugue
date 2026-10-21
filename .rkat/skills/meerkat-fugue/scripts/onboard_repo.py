#!/usr/bin/env python3
"""Legacy helper for onboarding a repository to an operator-owned Fugue host."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_GCP_PROJECT = "example-gcp-project"
DEFAULT_GCP_ZONE = "europe-west1-b"
DEFAULT_INSTANCE = "example-fugue-host"
DEFAULT_REMOTE_USER = "fugue"
DEFAULT_FUGUE_ROOT = "/srv/fugue"
DEFAULT_GIT_EMAIL = "automation@example.com"

LANES: list[tuple[str, str, str, float]] = [
    ("Backlog", "backlog", "#bec2c8", 0.0),
    ("Queue", "unstarted", "#f2c94c", 100.0),
    ("Active", "started", "#5e6ad2", 200.0),
    ("Review", "started", "#26b5ce", 300.0),
    ("Deploy", "started", "#00a86b", 400.0),
    ("Blocked", "started", "#eb5757", 500.0),
    ("Done", "completed", "#27ae60", 600.0),
    ("Canceled", "canceled", "#828282", 700.0),
    ("Duplicate", "canceled", "#828282", 800.0),
]


@dataclass(frozen=True)
class Team:
    id: str
    key: str
    name: str


@dataclass(frozen=True)
class Project:
    id: str
    name: str
    slug_id: str
    url: str


def run(cmd: list[str], *, input_text: str | None = None, dry_run: bool = False) -> str:
    printable = " ".join(shlex.quote(part) for part in cmd)
    if dry_run:
        print(f"DRY-RUN: {printable}")
        return ""
    proc = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        message = proc.stderr.strip() or proc.stdout.strip()
        raise SystemExit(f"command failed ({proc.returncode}): {printable}\n{message}")
    return proc.stdout


def git(args: list[str], cwd: Path) -> str:
    return run(["git", *args], dry_run=False).strip()


def repo_root(cwd: Path) -> Path:
    try:
        return Path(git(["rev-parse", "--show-toplevel"], cwd)).resolve()
    except SystemExit as exc:
        raise SystemExit("run this script from inside a git repository") from exc


def normalize_repo_url(value: str) -> str:
    value = value.strip()
    ssh_match = re.match(r"git@github\.com:(?P<owner>[^/]+)/(?P<repo>.+?)(?:\.git)?$", value)
    if ssh_match:
        return f"https://github.com/{ssh_match.group('owner')}/{ssh_match.group('repo')}.git"
    https_match = re.match(r"https://github\.com/(?P<owner>[^/]+)/(?P<repo>.+?)(?:\.git)?/?$", value)
    if https_match:
        return f"https://github.com/{https_match.group('owner')}/{https_match.group('repo')}.git"
    return value


def repo_name_from_url(url: str, root: Path) -> str:
    match = re.search(r"[:/]([^/:]+?)(?:\.git)?/?$", url)
    if match:
        return match.group(1)
    return root.name


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "repo"


class LinearClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def graphql(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
        request = urllib.request.Request(
            "https://api.linear.app/graphql",
            data=body,
            headers={
                "Authorization": self.api_key,
                "Content-Type": "application/json",
                "User-Agent": "meerkat-fugue-onboard-repo",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SystemExit(f"Linear API request failed: HTTP {exc.code}\n{detail}") from exc
        if payload.get("errors"):
            raise SystemExit(f"Linear GraphQL error:\n{json.dumps(payload['errors'], indent=2)}")
        return payload["data"]

    def teams(self) -> list[Team]:
        data = self.graphql(
            """
            query Teams {
              teams(first: 100, includeArchived: false) {
                nodes { id key name }
              }
            }
            """
        )
        return [Team(node["id"], node["key"], node["name"]) for node in data["teams"]["nodes"]]

    def states_for_team(self, team_id: str) -> list[dict[str, Any]]:
        data = self.graphql(
            """
            query TeamStates($teamId: ID!) {
              workflowStates(
                first: 100
                includeArchived: false
                filter: { team: { id: { eq: $teamId } } }
              ) {
                nodes { id name type color position }
              }
            }
            """,
            {"teamId": team_id},
        )
        return data["workflowStates"]["nodes"]

    def ensure_workflow_states(self, team: Team, *, dry_run: bool) -> None:
        existing = {state["name"].casefold(): state for state in self.states_for_team(team.id)}
        for name, state_type, color, position in LANES:
            if name.casefold() in existing:
                continue
            print(f"Creating Linear workflow state: {name}")
            if dry_run:
                continue
            self.graphql(
                """
                mutation CreateWorkflowState($input: WorkflowStateCreateInput!) {
                  workflowStateCreate(input: $input) {
                    success
                    workflowState { id name type }
                  }
                }
                """,
                {
                    "input": {
                        "teamId": team.id,
                        "name": name,
                        "type": state_type,
                        "color": color,
                        "position": position,
                    }
                },
            )

    def find_project(self, name: str) -> Project | None:
        after: str | None = None
        while True:
            data = self.graphql(
                """
                query Projects($after: String) {
                  projects(first: 100, after: $after, includeArchived: false) {
                    nodes { id name slugId url }
                    pageInfo { hasNextPage endCursor }
                  }
                }
                """,
                {"after": after},
            )
            for node in data["projects"]["nodes"]:
                if node["name"].casefold() == name.casefold():
                    return Project(node["id"], node["name"], node["slugId"], node["url"])
            page = data["projects"]["pageInfo"]
            if not page["hasNextPage"]:
                return None
            after = page["endCursor"]

    def create_project(self, name: str, team: Team, description: str, *, dry_run: bool) -> Project:
        if dry_run:
            return Project("dry-run", name, slugify(name), "https://linear.app/project/dry-run")
        data = self.graphql(
            """
            mutation CreateProject($input: ProjectCreateInput!) {
              projectCreate(input: $input) {
                success
                project { id name slugId url }
              }
            }
            """,
            {
                "input": {
                    "name": name,
                    "teamIds": [team.id],
                    "description": description,
                    "icon": "Cube",
                    "color": "#26b5ce",
                }
            },
        )
        project = data["projectCreate"]["project"]
        return Project(project["id"], project["name"], project["slugId"], project["url"])


def choose_team(client: LinearClient, selector: str | None) -> Team:
    teams = client.teams()
    if selector:
        for team in teams:
            if selector.casefold() in {team.id.casefold(), team.key.casefold(), team.name.casefold()}:
                return team
        raise SystemExit(f"Linear team not found: {selector}")
    if len(teams) == 1:
        return teams[0]
    choices = ", ".join(f"{team.name} ({team.key})" for team in teams)
    raise SystemExit(f"multiple Linear teams found; pass --team. Available: {choices}")


def render_workflow(
    *,
    script_dir: Path,
    output: Path,
    repo_name: str,
    repo_url: str,
    project_slug: str,
    max_agents: int,
    max_turns: int,
    git_email: str,
    default_branch: str,
    rkat_command: str,
    port: int,
) -> None:
    renderer = script_dir / "render_workflow.py"
    run(
        [
            sys.executable,
            str(renderer),
            "--repo-name",
            repo_name,
            "--repo-url",
            repo_url,
            "--project-slug",
            project_slug,
            "--max-agents",
            str(max_agents),
            "--max-turns",
            str(max_turns),
            "--git-email",
            git_email,
            "--default-branch",
            default_branch,
            "--rkat-command",
            rkat_command,
            "--port",
            str(port),
            "--output",
            str(output),
        ]
    )


def remote(instance: str, project: str, zone: str, command: str, *, dry_run: bool = False) -> str:
    return run(
        [
            "gcloud",
            "compute",
            "ssh",
            instance,
            "--project",
            project,
            "--zone",
            zone,
            "--command",
            command,
        ],
        dry_run=dry_run,
    )


def remote_scp(
    local: Path,
    remote_path: str,
    *,
    instance: str,
    project: str,
    zone: str,
    dry_run: bool = False,
) -> None:
    run(
        [
            "gcloud",
            "compute",
            "scp",
            "--project",
            project,
            "--zone",
            zone,
            str(local),
            f"{instance}:{remote_path}",
        ],
        dry_run=dry_run,
    )


def choose_port(instance: str, project: str, zone: str, start: int, end: int, *, dry_run: bool) -> int:
    if dry_run:
        return start
    code = f"""
python3 - <<'PY'
import socket
used = set()
for port in range({start}, {end + 1}):
    sock = socket.socket()
    sock.settimeout(0.05)
    try:
        if sock.connect_ex(("127.0.0.1", port)) == 0:
            used.add(port)
    finally:
        sock.close()
for port in range({start}, {end + 1}):
    if port not in used:
        print(port)
        raise SystemExit(0)
raise SystemExit("no free port in range")
PY
"""
    return int(remote(instance, project, zone, code).strip())


def systemd_unit(*, remote_user: str, root: str, queue_slug: str, port: int) -> str:
    source = f"{root}/source/meerkat-fugue"
    workflow = f"{root}/queues/{queue_slug}/WORKFLOW.md"
    queue_env = f"{root}/queues/{queue_slug}/env"
    return f"""[Unit]
Description=Fugue queue {queue_slug}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={remote_user}
WorkingDirectory={source}
EnvironmentFile={root}/secrets.env
EnvironmentFile={queue_env}
ExecStartPre={root}/prepare-auth.sh
ExecStart={root}/bin/meerkat-fugue "{workflow}" --port {port}
Restart=always
RestartSec=15
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
"""


def deploy_to_gcp(
    *,
    workflow_path: Path,
    repo_url: str,
    queue_slug: str,
    service_name: str,
    port: int,
    args: argparse.Namespace,
) -> None:
    root = args.remote_fugue_root.rstrip("/")
    queue_dir = f"{root}/queues/{queue_slug}"
    workspace_root = f"{root}/workspaces/{queue_slug}"
    logs_root = f"{root}/logs/{queue_slug}"
    state_root = f"{root}/state/{queue_slug}"
    context_root = f"{root}/context/{queue_slug}"

    if not args.replace_service:
        remote(
            args.instance,
            args.gcp_project,
            args.zone,
            "set -eu; "
            f"if systemctl cat {shlex.quote(service_name + '.service')} >/dev/null 2>&1; then "
            f"echo 'service already exists: {service_name}.service' >&2; exit 42; fi",
            dry_run=args.dry_run,
        )

    remote(
        args.instance,
        args.gcp_project,
        args.zone,
        "set -eu; "
        f"mkdir -p {shlex.quote(queue_dir)} {shlex.quote(workspace_root)} "
        f"{shlex.quote(logs_root)} {shlex.quote(state_root)} {shlex.quote(context_root)}",
        dry_run=args.dry_run,
    )

    with tempfile.TemporaryDirectory(prefix="meerkat-fugue-onboard-") as tmpdir:
        tmp = Path(tmpdir)
        env_file = tmp / "env"
        env_file.write_text(
            "\n".join(
                [
                    f"SOURCE_REPO_URL={repo_url}",
                    f"FUGUE_ROOT={root}",
                    f"FUGUE_WORKSPACE_ROOT={workspace_root}",
                    "RUST_LOG=meerkat_fugue=debug",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        unit_file = tmp / f"{service_name}.service"
        unit_file.write_text(
            systemd_unit(
                remote_user=args.remote_user,
                root=root,
                queue_slug=queue_slug,
                port=port,
            ),
            encoding="utf-8",
        )
        remote_scp(
            workflow_path,
            f"{queue_dir}/WORKFLOW.md",
            instance=args.instance,
            project=args.gcp_project,
            zone=args.zone,
            dry_run=args.dry_run,
        )
        remote_scp(
            env_file,
            f"{queue_dir}/env",
            instance=args.instance,
            project=args.gcp_project,
            zone=args.zone,
            dry_run=args.dry_run,
        )
        remote_scp(
            unit_file,
            f"/tmp/{service_name}.service",
            instance=args.instance,
            project=args.gcp_project,
            zone=args.zone,
            dry_run=args.dry_run,
        )

    remote(
        args.instance,
        args.gcp_project,
        args.zone,
        "set -eu; "
        f"chmod 600 {shlex.quote(queue_dir + '/WORKFLOW.md')} {shlex.quote(queue_dir + '/env')}; "
        f"sudo mv {shlex.quote('/tmp/' + service_name + '.service')} "
        f"{shlex.quote('/etc/systemd/system/' + service_name + '.service')}; "
        "sudo systemctl daemon-reload; "
        f"sudo systemctl enable --now {shlex.quote(service_name + '.service')}; "
        f"sudo systemctl restart {shlex.quote(service_name + '.service')}; "
        "sleep 3; "
        f"systemctl is-active {shlex.quote(service_name + '.service')}; "
        f"curl -fsS http://127.0.0.1:{port}/api/v1/state | python3 -m json.tool | sed -n '1,40p'",
        dry_run=args.dry_run,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--team", help="Linear team key, name, or id. Defaults to the only team.")
    parser.add_argument("--project-name", help="Linear project name. Defaults to '<repo> Fugue Queue'.")
    parser.add_argument("--repo-url", help="Git remote URL. Defaults to origin, normalized for GitHub HTTPS.")
    parser.add_argument("--repo-name", help="Display repo name. Defaults from remote URL or directory.")
    parser.add_argument("--queue-slug", help="Remote queue slug. Defaults from repo name.")
    parser.add_argument("--service-name", help="systemd service name. Defaults to fugue-<queue-slug>.")
    parser.add_argument("--max-agents", type=int, default=2)
    parser.add_argument("--max-turns", type=int, default=200)
    parser.add_argument("--port", type=int, help="Remote dashboard/API port. Defaults to first free 4001-4099.")
    parser.add_argument("--port-start", type=int, default=4001)
    parser.add_argument("--port-end", type=int, default=4099)
    parser.add_argument("--git-email", default=DEFAULT_GIT_EMAIL)
    parser.add_argument("--default-branch", default="main")
    parser.add_argument("--gcp-project", default=DEFAULT_GCP_PROJECT)
    parser.add_argument("--zone", default=DEFAULT_GCP_ZONE)
    parser.add_argument("--instance", default=DEFAULT_INSTANCE)
    parser.add_argument("--remote-user", default=DEFAULT_REMOTE_USER)
    parser.add_argument("--remote-fugue-root", default=DEFAULT_FUGUE_ROOT)
    parser.add_argument("--workflow-out", type=Path, help="Optional local copy of generated WORKFLOW.md.")
    parser.add_argument("--skip-gcp", action="store_true", help="Only create Linear project and workflow.")
    parser.add_argument("--replace-service", action="store_true", help="Replace an existing remote systemd service with the same name.")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without mutating Linear or GCP.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_agents <= 0:
        raise SystemExit("--max-agents must be positive")
    if args.max_turns <= 0:
        raise SystemExit("--max-turns must be positive")
    if args.port_start > args.port_end:
        raise SystemExit("--port-start must be <= --port-end")

    api_key = os.environ.get("LINEAR_API_KEY")
    if not api_key:
        raise SystemExit("LINEAR_API_KEY is required")

    root = repo_root(Path.cwd())
    remote_url = args.repo_url or git(["remote", "get-url", "origin"], root)
    repo_url = normalize_repo_url(remote_url)
    repo_name = args.repo_name or repo_name_from_url(repo_url, root)
    queue_slug = slugify(args.queue_slug or repo_name)
    service_name = args.service_name or f"fugue-{queue_slug}"
    project_name = args.project_name or f"{repo_name} Fugue Queue"

    client = LinearClient(api_key)
    team = choose_team(client, args.team)
    print(f"Linear team: {team.name} ({team.key})")
    client.ensure_workflow_states(team, dry_run=args.dry_run)

    project = client.find_project(project_name)
    if project:
        print(f"Linear project exists: {project.name} ({project.slug_id})")
    else:
        print(f"Creating Linear project: {project_name}")
        project = client.create_project(
            project_name,
            team,
            f"Dedicated Fugue automation queue for {repo_name}.",
            dry_run=args.dry_run,
        )

    port = args.port or choose_port(
        args.instance,
        args.gcp_project,
        args.zone,
        args.port_start,
        args.port_end,
        dry_run=args.dry_run or args.skip_gcp,
    )
    print(f"Repo URL: {repo_url}")
    print(f"Queue slug: {queue_slug}")
    print(f"Service: {service_name}.service")
    print(f"Port: {port}")
    print(f"Linear project URL: {project.url}")

    script_dir = Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory(prefix="meerkat-fugue-workflow-") as tmpdir:
        tmp_workflow = Path(tmpdir) / "WORKFLOW.md"
        render_workflow(
            script_dir=script_dir,
            output=tmp_workflow,
            repo_name=repo_name,
            repo_url="$SOURCE_REPO_URL",
            project_slug=project.slug_id,
            max_agents=args.max_agents,
            max_turns=args.max_turns,
            git_email=args.git_email,
            default_branch=args.default_branch,
            rkat_command=f"{args.remote_fugue_root.rstrip('/')}/bin/rkat-rpc",
            port=port,
        )
        if args.workflow_out:
            args.workflow_out.write_text(tmp_workflow.read_text(encoding="utf-8"), encoding="utf-8")
            print(f"Wrote local workflow: {args.workflow_out}")
        if not args.skip_gcp:
            deploy_to_gcp(
                workflow_path=tmp_workflow,
                repo_url=repo_url,
                queue_slug=queue_slug,
                service_name=service_name,
                port=port,
                args=args,
            )

    print("Onboarding complete.")
    if not args.skip_gcp:
        print(
            "Tunnel dashboard with: "
            f"gcloud compute ssh {args.instance} --project {args.gcp_project} --zone {args.zone} -- -L {port}:127.0.0.1:{port}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
