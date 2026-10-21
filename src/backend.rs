use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::onboarding::{
    GeneratedOnboardingFiles, NewRepoOnboardingOptions, OnboardingMode, render_new_repo_onboarding,
};

const DEFAULT_CONFIG_RELATIVE_PATH: &str = ".config/meerkat-fugue/backends.yaml";
const DEFAULT_PORT_START: u16 = 4001;
const DEFAULT_PORT_END: u16 = 4099;
const DEFAULT_FLOW: &str = "implementation_mob";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct BackendStore {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(default)]
    pub backends: BTreeMap<String, BackendProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BackendProfile {
    GcpSystemd(GcpSystemdBackend),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GcpSystemdBackend {
    pub gcp_project: String,
    pub zone: String,
    pub instance: String,
    pub remote_user: String,
    pub fugue_root: String,
    #[serde(default = "default_bin_dir")]
    pub bin_dir: String,
    #[serde(default = "default_port_start")]
    pub port_start: u16,
    #[serde(default = "default_port_end")]
    pub port_end: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_team: Option<String>,
    #[serde(default)]
    pub flow_teams: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ConfigureGcpSystemdInput {
    pub name: String,
    pub gcp_project: String,
    pub zone: String,
    pub instance: String,
    pub remote_user: String,
    pub fugue_root: String,
    pub bin_dir: Option<String>,
    pub port_start: u16,
    pub port_end: u16,
    pub default_team: Option<String>,
    pub flow_teams: BTreeMap<String, String>,
    pub make_default: bool,
}

#[derive(Debug, Clone)]
pub struct RepoOnboardInput {
    pub repo: PathBuf,
    pub backend: Option<String>,
    pub flow: Option<String>,
    pub project_name: Option<String>,
    pub linear_team: Option<String>,
    pub port: Option<u16>,
    pub replace_service: bool,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct RepoOnboardReport {
    pub backend_name: String,
    pub queue_slug: String,
    pub service_name: String,
    pub port: u16,
    pub project_slug: String,
    pub project_url: String,
    pub tunnel_command: String,
}

#[derive(Debug, Clone)]
struct RepoIdentity {
    url: String,
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TeamNode {
    id: String,
    key: String,
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ProjectNode {
    name: String,
    #[serde(rename = "slugId")]
    slug_id: String,
    url: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TeamsData {
    teams: Nodes<TeamNode>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProjectsData {
    projects: PagedNodes<ProjectNode>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProjectCreateData {
    #[serde(rename = "projectCreate")]
    project_create: ProjectCreatePayload,
}

#[derive(Debug, Clone, Deserialize)]
struct ProjectCreatePayload {
    project: ProjectNode,
}

#[derive(Debug, Clone, Deserialize)]
struct WorkflowStatesData {
    #[serde(rename = "workflowStates")]
    workflow_states: Nodes<WorkflowStateNode>,
}

#[derive(Debug, Clone, Deserialize)]
struct WorkflowStateNode {
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Nodes<T> {
    nodes: Vec<T>,
}

#[derive(Debug, Clone, Deserialize)]
struct PagedNodes<T> {
    nodes: Vec<T>,
    #[serde(rename = "pageInfo")]
    page_info: PageInfo,
}

#[derive(Debug, Clone, Deserialize)]
struct PageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

struct LinearProvisioner {
    client: reqwest::Client,
}

const REQUIRED_STATES: &[(&str, &str, &str, f64)] = &[
    ("Backlog", "backlog", "#bec2c8", 0.0),
    ("Queue", "unstarted", "#f2c94c", 100.0),
    ("Active", "started", "#26b5ce", 200.0),
    ("Review", "started", "#8b5cf6", 300.0),
    ("Deploy", "started", "#5e6ad2", 400.0),
    ("Blocked", "started", "#f2994a", 500.0),
    ("Done", "completed", "#27ae60", 600.0),
    ("Canceled", "canceled", "#828282", 700.0),
    ("Duplicate", "canceled", "#828282", 800.0),
];

pub fn configure_gcp_systemd_backend(input: ConfigureGcpSystemdInput) -> anyhow::Result<PathBuf> {
    if input.port_start > input.port_end {
        anyhow::bail!("--port-start must be <= --port-end");
    }
    let path = backend_store_path()?;
    let mut store = load_backend_store_from_path(&path)?;
    let fugue_root = trim_trailing_slash(&non_empty(input.fugue_root, "--fugue-root")?);
    let bin_dir =
        trim_trailing_slash(&input.bin_dir.unwrap_or_else(|| format!("{fugue_root}/bin")));
    let backend = BackendProfile::GcpSystemd(GcpSystemdBackend {
        gcp_project: non_empty(input.gcp_project, "--gcp-project")?,
        zone: non_empty(input.zone, "--zone")?,
        instance: non_empty(input.instance, "--instance")?,
        remote_user: non_empty(input.remote_user, "--remote-user")?,
        fugue_root,
        bin_dir,
        port_start: input.port_start,
        port_end: input.port_end,
        default_team: input.default_team,
        flow_teams: input.flow_teams,
    });
    store.backends.insert(input.name.clone(), backend);
    if input.make_default || store.default.is_none() {
        store.default = Some(input.name);
    }
    write_backend_store_to_path(&path, &store)?;
    Ok(path)
}

pub async fn onboard_repo(input: RepoOnboardInput) -> anyhow::Result<RepoOnboardReport> {
    let store = load_backend_store()?;
    let backend_name = resolve_backend_name(&store, input.backend.as_deref())?;
    let backend = store
        .backends
        .get(&backend_name)
        .ok_or_else(|| anyhow::anyhow!("configured backend {backend_name:?} was not found"))?;
    match backend {
        BackendProfile::GcpSystemd(gcp) => onboard_repo_to_gcp(&backend_name, gcp, input).await,
    }
}

fn load_backend_store() -> anyhow::Result<BackendStore> {
    load_backend_store_from_path(&backend_store_path()?)
}

fn load_backend_store_from_path(path: &Path) -> anyhow::Result<BackendStore> {
    if !path.exists() {
        return Ok(BackendStore::default());
    }
    let contents = fs::read_to_string(path)
        .map_err(|err| anyhow::anyhow!("failed to read {}: {err}", path.display()))?;
    serde_yaml::from_str(&contents)
        .map_err(|err| anyhow::anyhow!("failed to parse {}: {err}", path.display()))
}

fn write_backend_store_to_path(path: &Path, store: &BackendStore) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| anyhow::anyhow!("failed to create {}: {err}", parent.display()))?;
    }
    let contents = serde_yaml::to_string(store)?;
    fs::write(path, contents)
        .map_err(|err| anyhow::anyhow!("failed to write {}: {err}", path.display()))
}

fn backend_store_path() -> anyhow::Result<PathBuf> {
    if let Ok(path) = std::env::var("MEERKAT_FUGUE_BACKENDS")
        && !path.trim().is_empty()
    {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var("HOME").map_err(|_| anyhow::anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(DEFAULT_CONFIG_RELATIVE_PATH))
}

fn resolve_backend_name(store: &BackendStore, requested: Option<&str>) -> anyhow::Result<String> {
    if let Some(name) = requested {
        if store.backends.contains_key(name) {
            return Ok(name.to_string());
        }
        anyhow::bail!("backend {name:?} is not configured");
    }
    if let Some(name) = &store.default {
        if store.backends.contains_key(name) {
            return Ok(name.clone());
        }
        anyhow::bail!("default backend {name:?} is not configured");
    }
    anyhow::bail!(
        "no Fugue backend configured; run `meerkat-fugue configure backend gcp-systemd ...`"
    )
}

async fn onboard_repo_to_gcp(
    backend_name: &str,
    backend: &GcpSystemdBackend,
    input: RepoOnboardInput,
) -> anyhow::Result<RepoOnboardReport> {
    let identity = repo_identity(&input.repo)?;
    let flow = input.flow.unwrap_or_else(|| DEFAULT_FLOW.to_string());
    let team_selector = input
        .linear_team
        .or_else(|| backend.flow_teams.get(&flow).cloned())
        .or_else(|| backend.default_team.clone())
        .ok_or_else(|| anyhow::anyhow!("no Linear team configured for flow {flow:?}"))?;
    let queue_slug = slugify(&format!("{}-{flow}", identity.name));
    let service_name = format!("fugue-{queue_slug}");
    let port = match input.port {
        Some(port) => port,
        None => choose_remote_port(backend, input.dry_run)?,
    };
    let api_key = std::env::var("LINEAR_API_KEY")
        .map_err(|_| anyhow::anyhow!("LINEAR_API_KEY is required for repo onboarding"))?;
    let linear = LinearProvisioner::new(&api_key)?;
    let team = linear.resolve_team(&team_selector).await?;
    linear.ensure_workflow_states(&team, input.dry_run).await?;
    let project_name = input
        .project_name
        .unwrap_or_else(|| format!("{} Fugue Queue", identity.name));
    let project = match linear.find_project(&project_name).await? {
        Some(project) => project,
        None => {
            linear
                .create_project(
                    &project_name,
                    &team,
                    &format!("Dedicated Fugue automation queue for {}.", identity.name),
                    input.dry_run,
                )
                .await?
        }
    };
    let queue_dir = format!("{}/queues/{queue_slug}", backend.fugue_root);
    let workspace_root = format!("{}/workspaces/{queue_slug}", backend.fugue_root);
    let options = NewRepoOnboardingOptions {
        mode: OnboardingMode::NativeDefault,
        linear_api_key: "$LINEAR_API_KEY",
        linear_project_slug: &project.slug_id,
        linear_team: &team.key,
        github_repo_url: &identity.url,
        workspace_root: &workspace_root,
        linear_mcp_command: &format!("{}/fugue-linear-mcp", backend.bin_dir),
        github_mcp_command: &format!("{}/fugue-github-mcp", backend.bin_dir),
        server_port: Some(port),
    };
    let generated = render_new_repo_onboarding(&options);
    validate_generated_files(&generated)?;
    deploy_gcp_queue(
        backend,
        &identity,
        &queue_slug,
        &service_name,
        port,
        &queue_dir,
        &workspace_root,
        &generated,
        input.replace_service,
        input.dry_run,
    )?;
    Ok(RepoOnboardReport {
        backend_name: backend_name.to_string(),
        queue_slug,
        service_name,
        port,
        project_slug: project.slug_id,
        project_url: project.url,
        tunnel_command: format!(
            "gcloud compute ssh {} --project {} --zone {} -- -L {port}:127.0.0.1:{port}",
            backend.instance, backend.gcp_project, backend.zone
        ),
    })
}

impl LinearProvisioner {
    fn new(api_key: &str) -> anyhow::Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(api_key)?,
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(45))
            .build()?;
        Ok(Self { client })
    }

    async fn graphql<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        variables: Value,
    ) -> anyhow::Result<T> {
        let payload: Value = self
            .client
            .post("https://api.linear.app/graphql")
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(errors) = payload.get("errors") {
            anyhow::bail!("Linear GraphQL error: {errors}");
        }
        let data = payload
            .get("data")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Linear response did not include data"))?;
        Ok(serde_json::from_value(data)?)
    }

    async fn resolve_team(&self, selector: &str) -> anyhow::Result<TeamNode> {
        let selector = selector.trim();
        if selector.is_empty() {
            anyhow::bail!("Linear team selector must not be empty");
        }
        let data: TeamsData = self
            .graphql(
                "query Teams { teams(first: 100, includeArchived: false) { nodes { id key name } } }",
                json!({}),
            )
            .await?;
        data.teams
            .nodes
            .into_iter()
            .find(|team| {
                [team.id.as_str(), team.key.as_str(), team.name.as_str()]
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(selector))
            })
            .ok_or_else(|| anyhow::anyhow!("Linear team not found: {selector}"))
    }

    async fn ensure_workflow_states(&self, team: &TeamNode, dry_run: bool) -> anyhow::Result<()> {
        let data: WorkflowStatesData = self
            .graphql(
                "query TeamStates($teamId: ID!) { workflowStates(first: 100, includeArchived: false, filter: { team: { id: { eq: $teamId } } }) { nodes { name } } }",
                json!({ "teamId": team.id }),
            )
            .await?;
        let existing: Vec<String> = data
            .workflow_states
            .nodes
            .into_iter()
            .map(|state| state.name.to_ascii_lowercase())
            .collect();
        for (name, state_type, color, position) in REQUIRED_STATES {
            if existing
                .iter()
                .any(|candidate| candidate == &name.to_ascii_lowercase())
            {
                continue;
            }
            if dry_run {
                println!("DRY-RUN: create Linear workflow state {name}");
                continue;
            }
            let _: Value = self
                .graphql(
                    "mutation CreateWorkflowState($input: WorkflowStateCreateInput!) { workflowStateCreate(input: $input) { success workflowState { id name type } } }",
                    json!({
                        "input": {
                            "teamId": team.id,
                            "name": name,
                            "type": state_type,
                            "color": color,
                            "position": position,
                        }
                    }),
                )
                .await?;
        }
        Ok(())
    }

    async fn find_project(&self, name: &str) -> anyhow::Result<Option<ProjectNode>> {
        let mut after: Option<String> = None;
        loop {
            let data: ProjectsData = self
                .graphql(
                    "query Projects($after: String) { projects(first: 100, after: $after, includeArchived: false) { nodes { id name slugId url } pageInfo { hasNextPage endCursor } } }",
                    json!({ "after": after }),
                )
                .await?;
            if let Some(project) = data
                .projects
                .nodes
                .into_iter()
                .find(|project| project.name.eq_ignore_ascii_case(name))
            {
                return Ok(Some(project));
            }
            if !data.projects.page_info.has_next_page {
                return Ok(None);
            }
            after = data.projects.page_info.end_cursor;
        }
    }

    async fn create_project(
        &self,
        name: &str,
        team: &TeamNode,
        description: &str,
        dry_run: bool,
    ) -> anyhow::Result<ProjectNode> {
        if dry_run {
            return Ok(ProjectNode {
                name: name.to_string(),
                slug_id: slugify(name),
                url: "https://linear.app/project/dry-run".to_string(),
            });
        }
        let data: ProjectCreateData = self
            .graphql(
                "mutation CreateProject($input: ProjectCreateInput!) { projectCreate(input: $input) { success project { id name slugId url } } }",
                json!({
                    "input": {
                        "name": name,
                        "teamIds": [team.id.as_str()],
                        "description": description,
                        "icon": "Cube",
                        "color": "#26b5ce",
                    }
                }),
            )
            .await?;
        Ok(data.project_create.project)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit host composition boundary"
)]
fn deploy_gcp_queue(
    backend: &GcpSystemdBackend,
    identity: &RepoIdentity,
    queue_slug: &str,
    service_name: &str,
    port: u16,
    queue_dir: &str,
    workspace_root: &str,
    generated: &GeneratedOnboardingFiles,
    replace_service: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
    if !replace_service {
        remote(
            backend,
            &format!(
                "set -eu; if systemctl cat {} >/dev/null 2>&1; then echo {} >&2; exit 42; fi",
                shell_quote(&format!("{service_name}.service")),
                shell_quote(&format!("service already exists: {service_name}.service"))
            ),
            dry_run,
        )?;
    }
    remote(
        backend,
        &format!(
            "set -eu; mkdir -p {} {} {} {} {}",
            shell_quote(queue_dir),
            shell_quote(workspace_root),
            shell_quote(&format!("{}/logs/{queue_slug}", backend.fugue_root)),
            shell_quote(&format!("{}/state/{queue_slug}", backend.fugue_root)),
            shell_quote(&format!("{}/context/{queue_slug}", backend.fugue_root)),
        ),
        dry_run,
    )?;
    let temp = unique_temp_dir("meerkat-fugue-gcp-deploy")?;
    fs::create_dir_all(&temp)?;
    let result = (|| {
        write_generated_files(&temp, generated, true)?;
        let env_path = temp.join("env");
        fs::write(
            &env_path,
            format!(
                "SOURCE_REPO_URL={}\nFUGUE_ROOT={}\nFUGUE_WORKSPACE_ROOT={workspace_root}\nRUST_LOG=meerkat_fugue=info\n",
                identity.url, backend.fugue_root
            ),
        )?;
        let unit_path = temp.join(format!("{service_name}.service"));
        fs::write(&unit_path, systemd_unit(backend, queue_slug, port))?;
        remote_scp_dir(backend, &temp, queue_dir, dry_run)?;
        remote_scp_file(
            backend,
            &unit_path,
            &format!("/tmp/{service_name}.service"),
            dry_run,
        )?;
        remote(
            backend,
            &format!(
                "set -eu; chmod -R go-rwx {}; sudo mv {} {}; sudo systemctl daemon-reload; sudo systemctl enable --now {}; sudo systemctl restart {}; sleep 2; systemctl is-active {}",
                shell_quote(queue_dir),
                shell_quote(&format!("/tmp/{service_name}.service")),
                shell_quote(&format!("/etc/systemd/system/{service_name}.service")),
                shell_quote(&format!("{service_name}.service")),
                shell_quote(&format!("{service_name}.service")),
                shell_quote(&format!("{service_name}.service")),
            ),
            dry_run,
        )?;
        Ok::<(), anyhow::Error>(())
    })();
    let cleanup = fs::remove_dir_all(&temp);
    result?;
    cleanup.map_err(|err| anyhow::anyhow!("failed to remove {}: {err}", temp.display()))?;
    Ok(())
}

fn remote(backend: &GcpSystemdBackend, command: &str, dry_run: bool) -> anyhow::Result<String> {
    run_command(
        Command::new("gcloud").args([
            "compute",
            "ssh",
            &backend.instance,
            "--project",
            &backend.gcp_project,
            "--zone",
            &backend.zone,
            "--command",
            command,
        ]),
        dry_run,
    )
}

fn remote_scp_file(
    backend: &GcpSystemdBackend,
    local: &Path,
    remote_path: &str,
    dry_run: bool,
) -> anyhow::Result<()> {
    run_command(
        Command::new("gcloud").args([
            "compute",
            "scp",
            "--project",
            &backend.gcp_project,
            "--zone",
            &backend.zone,
            local
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid path"))?,
            &format!("{}:{remote_path}", backend.instance),
        ]),
        dry_run,
    )?;
    Ok(())
}

fn remote_scp_dir(
    backend: &GcpSystemdBackend,
    local_dir: &Path,
    remote_dir: &str,
    dry_run: bool,
) -> anyhow::Result<()> {
    run_command(
        Command::new("gcloud").args([
            "compute",
            "scp",
            "--recurse",
            "--project",
            &backend.gcp_project,
            "--zone",
            &backend.zone,
            &format!("{}/.", local_dir.display()),
            &format!("{}:{remote_dir}", backend.instance),
        ]),
        dry_run,
    )?;
    Ok(())
}

fn run_command(command: &mut Command, dry_run: bool) -> anyhow::Result<String> {
    if dry_run {
        println!("DRY-RUN: {:?}", command);
        return Ok(String::new());
    }
    let output = command.output()?;
    if !output.status.success() {
        anyhow::bail!(
            "command failed with {}: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn choose_remote_port(backend: &GcpSystemdBackend, dry_run: bool) -> anyhow::Result<u16> {
    if dry_run {
        return Ok(backend.port_start);
    }
    let script = format!(
        "python3 - <<'PY'\nimport socket\nfor port in range({}, {}):\n    sock = socket.socket()\n    sock.settimeout(0.05)\n    try:\n        if sock.connect_ex(('127.0.0.1', port)) != 0:\n            print(port)\n            raise SystemExit(0)\n    finally:\n        sock.close()\nraise SystemExit('no free port in range')\nPY",
        backend.port_start,
        u32::from(backend.port_end) + 1
    );
    let stdout = remote(backend, &script, false)?;
    Ok(stdout.trim().parse()?)
}

fn systemd_unit(backend: &GcpSystemdBackend, queue_slug: &str, port: u16) -> String {
    let queue_dir = format!("{}/queues/{queue_slug}", backend.fugue_root);
    format!(
        "[Unit]\nDescription=Fugue queue {queue_slug}\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nUser={user}\nWorkingDirectory={root}\nEnvironmentFile={root}/secrets.env\nEnvironmentFile={queue_dir}/env\nExecStartPre={root}/prepare-auth.sh\nExecStart={bin}/meerkat-fugue {queue_dir}/fugue.yaml --port {port}\nRestart=always\nRestartSec=15\nKillSignal=SIGTERM\nTimeoutStopSec=30\n\n[Install]\nWantedBy=multi-user.target\n",
        user = backend.remote_user,
        root = backend.fugue_root,
        bin = backend.bin_dir,
    )
}

fn repo_identity(repo: &Path) -> anyhow::Result<RepoIdentity> {
    let root_output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(repo)
        .output()?;
    if !root_output.status.success() {
        anyhow::bail!("{} is not inside a git repository", repo.display());
    }
    let root = PathBuf::from(String::from_utf8(root_output.stdout)?.trim()).canonicalize()?;
    let url_output = Command::new("git")
        .args(["remote", "get-url", "origin"])
        .current_dir(&root)
        .output()?;
    if !url_output.status.success() {
        anyhow::bail!("git remote origin is required for repo onboarding");
    }
    let url = normalize_repo_url(String::from_utf8(url_output.stdout)?.trim());
    let name = repo_name_from_url(&url)
        .or_else(|| {
            root.file_name()
                .and_then(|name| name.to_str())
                .map(str::to_string)
        })
        .ok_or_else(|| anyhow::anyhow!("could not infer repository name"))?;
    Ok(RepoIdentity { url, name })
}

fn normalize_repo_url(value: &str) -> String {
    let value = value.trim();
    if let Some(rest) = value.strip_prefix("git@github.com:") {
        return format!("https://github.com/{}", ensure_git_suffix(rest));
    }
    if value.starts_with("https://github.com/") {
        return ensure_git_suffix(value);
    }
    value.to_string()
}

fn ensure_git_suffix(value: &str) -> String {
    let trimmed = value.trim_end_matches('/');
    if trimmed.ends_with(".git") {
        trimmed.to_string()
    } else {
        format!("{trimmed}.git")
    }
}

fn repo_name_from_url(value: &str) -> Option<String> {
    value
        .trim_end_matches('/')
        .trim_end_matches(".git")
        .rsplit(['/', ':'])
        .next()
        .filter(|name| !name.is_empty())
        .map(str::to_string)
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !slug.is_empty() {
            slug.push('-');
            last_dash = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        "repo".to_string()
    } else {
        slug
    }
}

fn validate_generated_files(generated: &GeneratedOnboardingFiles) -> anyhow::Result<()> {
    let staging_dir = unique_temp_dir("meerkat-fugue-validate")?;
    fs::create_dir_all(&staging_dir)?;
    let result = (|| {
        write_generated_files(&staging_dir, generated, true)?;
        generated.validate_in_dir(&staging_dir)?;
        Ok::<(), anyhow::Error>(())
    })();
    let cleanup = fs::remove_dir_all(&staging_dir);
    result?;
    cleanup.map_err(|err| anyhow::anyhow!("failed to remove {}: {err}", staging_dir.display()))?;
    Ok(())
}

fn write_generated_files(
    output_dir: &Path,
    generated: &GeneratedOnboardingFiles,
    force: bool,
) -> anyhow::Result<()> {
    write_generated_file(
        output_dir,
        generated.config_path,
        &generated.config_contents,
        force,
    )?;
    if let (Some(path), Some(contents)) = (generated.prompt_path, &generated.prompt_contents) {
        write_generated_file(output_dir, path, contents, force)?;
    }
    if let (Some(path), Some(contents)) = (
        generated.native_launch_context_path,
        &generated.native_launch_context_contents,
    ) {
        write_generated_file(output_dir, path, contents, force)?;
    }
    Ok(())
}

fn write_generated_file(
    output_dir: &Path,
    relative_path: &str,
    contents: &str,
    force: bool,
) -> anyhow::Result<()> {
    let path = output_dir.join(relative_path);
    if path.exists() && !force {
        anyhow::bail!("{} already exists", path.display());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents)?;
    Ok(())
}

fn unique_temp_dir(prefix: &str) -> anyhow::Result<PathBuf> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id())))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn trim_trailing_slash(value: &str) -> String {
    value.trim_end_matches('/').to_string()
}

fn non_empty(value: String, flag: &str) -> anyhow::Result<String> {
    if value.trim().is_empty() {
        anyhow::bail!("{flag} must not be empty");
    }
    Ok(value)
}

const fn default_port_start() -> u16 {
    DEFAULT_PORT_START
}

const fn default_port_end() -> u16 {
    DEFAULT_PORT_END
}

fn default_bin_dir() -> String {
    "bin".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugify_adds_flow_to_repo_shape() {
        assert_eq!(
            slugify("Meerkat Fugue-implementation_mob"),
            "meerkat-fugue-implementation-mob"
        );
    }

    #[test]
    fn normalizes_github_ssh_origin() {
        assert_eq!(
            normalize_repo_url("git@github.com:owner/repo.git"),
            "https://github.com/owner/repo.git"
        );
    }

    #[test]
    fn backend_store_round_trips_gcp_profile() {
        let mut store = BackendStore {
            default: Some("live".to_string()),
            backends: BTreeMap::new(),
        };
        store.backends.insert(
            "live".to_string(),
            BackendProfile::GcpSystemd(GcpSystemdBackend {
                gcp_project: "project".to_string(),
                zone: "zone".to_string(),
                instance: "vm".to_string(),
                remote_user: "user".to_string(),
                fugue_root: "/srv/fugue".to_string(),
                bin_dir: "/srv/fugue/bin".to_string(),
                port_start: 4001,
                port_end: 4099,
                default_team: Some("FUG".to_string()),
                flow_teams: BTreeMap::from([("implementation_mob".to_string(), "FUG".to_string())]),
            }),
        );
        let rendered = serde_yaml::to_string(&store).expect("serialize");
        let parsed: BackendStore = serde_yaml::from_str(&rendered).expect("parse");
        assert_eq!(parsed.default.as_deref(), Some("live"));
        assert!(parsed.backends.contains_key("live"));
    }
}
