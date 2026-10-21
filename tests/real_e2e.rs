use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use meerkat_fugue::config::{WorkflowConfigManager, parse_native_workflow};
use meerkat_fugue::orchestrator::Orchestrator;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const REQUIRED_REAL_E2E_ENV: &str = "MEERKAT_FUGUE_REAL_E2E";
const WORKPAD_HEADING: &str = "## Fugue Workpad";
const GUARDED_KAPELLMEISTER_LABEL: &str = "kapellmeister-safe";
const E2E_POLICY_MODE_ENV: &str = "MEERKAT_FUGUE_E2E_POLICY_MODE";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "destructive real Linear/GitHub/Meerkat E2E; set MEERKAT_FUGUE_REAL_E2E=1"]
async fn real_linear_github_meerkat_flow_creates_prs_and_reaches_done() -> Result<()> {
    init_e2e_tracing();
    if std::env::var(REQUIRED_REAL_E2E_ENV).ok().as_deref() != Some("1") {
        eprintln!("skipping real E2E because {REQUIRED_REAL_E2E_ENV}=1 is not set");
        return Ok(());
    }

    let config = RealE2eConfig::from_env().await?;
    let mut context = RealE2eContext::provision(&config).await?;
    let result = run_real_e2e(&config, &mut context).await;
    let cleanup = context.cleanup().await;
    result.and(cleanup)
}

fn init_e2e_tracing() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("meerkat_fugue=debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_test_writer()
        .try_init();
}

async fn run_real_e2e(config: &RealE2eConfig, context: &mut RealE2eContext) -> Result<()> {
    let manager = WorkflowConfigManager::load(context.workflow_path.clone())
        .with_context(|| format!("failed to load {}", context.workflow_path.display()))?;
    manager.validate_dispatch()?;

    let (_refresh_tx, refresh_rx) = mpsc::channel(8);
    let mut orchestrator = Orchestrator::new(manager, refresh_rx);
    let shutdown = CancellationToken::new();
    let orchestrator_shutdown = shutdown.clone();
    let orchestrator_handle =
        tokio::spawn(async move { orchestrator.run_until_shutdown(orchestrator_shutdown).await });

    let flow_result: Result<()> = async {
        let issue_ids = context
            .issues
            .iter()
            .map(|issue| issue.id.clone())
            .collect::<Vec<_>>();
        let issue_identifiers = context
            .issues
            .iter()
            .map(|issue| issue.identifier.clone())
            .collect::<Vec<_>>();

        wait_for_issue_states(
            &context.linear,
            &issue_ids,
            &config.implementing_state,
            config.plan_timeout,
        )
        .await?;

        wait_for_workpads(
            &context.linear,
            &issue_ids,
            WorkpadExpectation::Planning,
            config.plan_timeout,
        )
        .await?;

        let pull_requests =
            wait_for_pull_requests(&context.github_repo, &issue_identifiers, config.pr_timeout)
                .await?;

        wait_for_workpads(
            &context.linear,
            &issue_ids,
            WorkpadExpectation::PullRequest,
            config.plan_timeout,
        )
        .await?;

        wait_for_issue_states(
            &context.linear,
            &issue_ids,
            &config.reviewing_state,
            config.done_timeout,
        )
        .await?;

        for pull_request in pull_requests.values() {
            merge_pull_request(&context.github_repo, pull_request.number).await?;
        }

        wait_for_issue_states(
            &context.linear,
            &issue_ids,
            &config.done_state,
            config.done_timeout,
        )
        .await?;

        wait_for_completion_workpads(
            &context.linear,
            &issue_ids,
            &config.done_state,
            config.plan_timeout,
        )
        .await
    }
    .await;

    shutdown.cancel();
    let shutdown_result = match orchestrator_handle.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(anyhow!("orchestrator task join failed: {err}")),
    };
    flow_result.and(shutdown_result)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RealE2ePolicyMode {
    GuardedKapellmeister,
    NativeMob,
    NativeDefault,
}

impl RealE2ePolicyMode {
    fn from_env() -> Result<Self> {
        Self::parse(&env_or_default(E2E_POLICY_MODE_ENV, "native_default"))
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "guarded" | "guarded_kapellmeister" | "kapellmeister" => Ok(Self::GuardedKapellmeister),
            "native" | "native_mob" | "native_execution" => Ok(Self::NativeMob),
            "native_only" | "native-default" | "native_default" | "native_defaults" => {
                Ok(Self::NativeDefault)
            }
            other => bail!(
                "{E2E_POLICY_MODE_ENV} must be guarded_kapellmeister, native_mob, or native_only/native_default, got {other}"
            ),
        }
    }

    fn is_guarded(self) -> bool {
        matches!(self, Self::GuardedKapellmeister)
    }
}

struct RealE2eConfig {
    linear_api_key: String,
    linear_endpoint: String,
    linear_team_key: String,
    github_owner: String,
    repo_private: bool,
    keep_resources: bool,
    create_missing_linear_states: bool,
    issue_count: usize,
    repo_prefix: String,
    rkat_command: String,
    policy_mode: RealE2ePolicyMode,
    backlog_state: String,
    todo_state: String,
    planning_state: String,
    implementing_state: String,
    reviewing_state: String,
    canceled_state: String,
    cancelled_state: String,
    duplicate_state: String,
    done_state: String,
    plan_timeout: Duration,
    pr_timeout: Duration,
    done_timeout: Duration,
}

impl RealE2eConfig {
    async fn from_env() -> Result<Self> {
        let linear_api_key = env_or_zshrc("LINEAR_API_KEY")
            .ok_or_else(|| anyhow!("LINEAR_API_KEY must be set or present in ~/.zshrc"))?;
        let github_owner = match std::env::var("MEERKAT_FUGUE_E2E_GITHUB_OWNER") {
            Ok(owner) if !owner.trim().is_empty() => owner,
            _ => command_stdout("gh", ["api", "user", "--jq", ".login"], None)
                .await?
                .trim()
                .to_string(),
        };
        if github_owner.is_empty() {
            bail!("could not determine GitHub owner; set MEERKAT_FUGUE_E2E_GITHUB_OWNER");
        }

        Ok(Self {
            linear_api_key,
            linear_endpoint: env_or_default(
                "MEERKAT_FUGUE_E2E_LINEAR_ENDPOINT",
                "https://api.linear.app/graphql",
            ),
            linear_team_key: env_or_default("MEERKAT_FUGUE_E2E_LINEAR_TEAM_KEY", "LUC"),
            github_owner,
            repo_private: env_bool("MEERKAT_FUGUE_E2E_PRIVATE_REPO", true),
            keep_resources: env_bool("MEERKAT_FUGUE_E2E_KEEP_RESOURCES", false),
            create_missing_linear_states: env_bool(
                "MEERKAT_FUGUE_E2E_CREATE_MISSING_LINEAR_STATES",
                true,
            ),
            issue_count: env_usize("MEERKAT_FUGUE_E2E_ISSUE_COUNT", 2)?,
            repo_prefix: env_or_default("MEERKAT_FUGUE_E2E_REPO_PREFIX", "meerkat-fugue-e2e"),
            rkat_command: env_or_default("MEERKAT_FUGUE_E2E_RKAT_COMMAND", "rkat-rpc"),
            policy_mode: RealE2ePolicyMode::from_env()?,
            backlog_state: env_or_default("MEERKAT_FUGUE_E2E_BACKLOG_STATE", "Backlog"),
            todo_state: env_or_default("MEERKAT_FUGUE_E2E_TODO_STATE", "Todo"),
            planning_state: env_or_default("MEERKAT_FUGUE_E2E_PLANNING_STATE", "Planning"),
            implementing_state: env_or_default(
                "MEERKAT_FUGUE_E2E_IMPLEMENTING_STATE",
                "Implementing",
            ),
            reviewing_state: env_or_default("MEERKAT_FUGUE_E2E_REVIEWING_STATE", "Reviewing"),
            canceled_state: env_or_default("MEERKAT_FUGUE_E2E_CANCELED_STATE", "Canceled"),
            cancelled_state: env_or_default("MEERKAT_FUGUE_E2E_CANCELLED_STATE", "Cancelled"),
            duplicate_state: env_or_default("MEERKAT_FUGUE_E2E_DUPLICATE_STATE", "Duplicate"),
            done_state: env_or_default("MEERKAT_FUGUE_E2E_DONE_STATE", "Done"),
            plan_timeout: env_duration_minutes("MEERKAT_FUGUE_E2E_PLAN_TIMEOUT_MIN", 20)?,
            pr_timeout: env_duration_minutes("MEERKAT_FUGUE_E2E_PR_TIMEOUT_MIN", 45)?,
            done_timeout: env_duration_minutes("MEERKAT_FUGUE_E2E_DONE_TIMEOUT_MIN", 30)?,
        })
    }

    fn active_states(&self) -> Vec<String> {
        vec![
            self.todo_state.clone(),
            self.planning_state.clone(),
            self.implementing_state.clone(),
            self.reviewing_state.clone(),
        ]
    }

    fn required_states(&self) -> Vec<RequiredState> {
        vec![
            RequiredState::new(&self.backlog_state, "backlog", "#bec2c8", 0.0),
            RequiredState::new(&self.todo_state, "unstarted", "#f2c94c", 100.0),
            RequiredState::new(&self.planning_state, "started", "#5e6ad2", 200.0),
            RequiredState::new(&self.implementing_state, "started", "#5e6ad2", 300.0),
            RequiredState::new(&self.reviewing_state, "started", "#26b5ce", 400.0),
            RequiredState::new(&self.done_state, "completed", "#27ae60", 700.0),
            RequiredState::new(&self.canceled_state, "canceled", "#95a2b3", 800.0),
            RequiredState::new(&self.duplicate_state, "canceled", "#95a2b3", 900.0),
        ]
    }
}

struct RealE2eContext {
    linear: LinearE2eClient,
    _temp_dir: TempDir,
    github_repo: GitHubRepo,
    project: LinearProject,
    issues: Vec<LinearIssue>,
    workflow_path: PathBuf,
    keep_resources: bool,
}

impl RealE2eContext {
    async fn provision(config: &RealE2eConfig) -> Result<Self> {
        let run_id = unique_run_id()?;
        let linear = LinearE2eClient::new(&config.linear_api_key, &config.linear_endpoint)?;
        let team = linear.team_by_key(&config.linear_team_key).await?;
        let states = linear
            .ensure_states(
                &team.id,
                config.required_states(),
                config.create_missing_linear_states,
            )
            .await?;
        let guarded_label = if config.policy_mode.is_guarded() {
            Some(
                linear
                    .ensure_label(&team.id, GUARDED_KAPELLMEISTER_LABEL)
                    .await?,
            )
        } else {
            None
        };
        let github_repo = create_github_repo(config, &run_id).await?;
        if let Err(err) = seed_github_repo(&github_repo).await {
            cleanup_partial(
                &linear,
                Some(&github_repo),
                None,
                &[],
                config.keep_resources,
            )
            .await;
            return Err(err);
        }

        let project = match linear
            .create_project(
                &team.id,
                &format!("Meerkat Fugue E2E {run_id}"),
                &format!(
                    "Temporary real E2E project for {}. Repo: {}",
                    env!("CARGO_PKG_NAME"),
                    github_repo.full_name
                ),
            )
            .await
        {
            Ok(project) => project,
            Err(err) => {
                cleanup_partial(
                    &linear,
                    Some(&github_repo),
                    None,
                    &[],
                    config.keep_resources,
                )
                .await;
                return Err(err);
            }
        };
        let todo_state = states
            .get(&config.todo_state)
            .ok_or_else(|| anyhow!("Todo state was not provisioned"))?;
        let mut issues = Vec::with_capacity(config.issue_count);
        for index in 1..=config.issue_count {
            match linear
                .create_issue(&CreateIssue {
                    team_id: &team.id,
                    project_id: &project.id,
                    state_id: &todo_state.id,
                    title: &format!("Fugue E2E issue {index} {run_id}"),
                    description: &issue_description(index, &run_id, &github_repo),
                    label_ids: guarded_label
                        .as_ref()
                        .map(|label| vec![label.id.clone()])
                        .unwrap_or_default(),
                })
                .await
            {
                Ok(issue) => issues.push(issue),
                Err(err) => {
                    cleanup_partial(
                        &linear,
                        Some(&github_repo),
                        Some(&project),
                        &issues,
                        config.keep_resources,
                    )
                    .await;
                    return Err(err);
                }
            }
        }

        let temp_dir = match tempfile::tempdir().context("create E2E temp dir") {
            Ok(temp_dir) => temp_dir,
            Err(err) => {
                cleanup_partial(
                    &linear,
                    Some(&github_repo),
                    Some(&project),
                    &issues,
                    config.keep_resources,
                )
                .await;
                return Err(err);
            }
        };
        let workflow_path = temp_dir.path().join("fugue.yaml");
        if let Err(err) = std::fs::write(
            &workflow_path,
            workflow_contents(config, &github_repo, &project.slug_id, temp_dir.path()),
        )
        .with_context(|| format!("write {}", workflow_path.display()))
        {
            cleanup_partial(
                &linear,
                Some(&github_repo),
                Some(&project),
                &issues,
                config.keep_resources,
            )
            .await;
            return Err(err);
        }

        Ok(Self {
            linear,
            _temp_dir: temp_dir,
            github_repo,
            project,
            issues,
            workflow_path,
            keep_resources: config.keep_resources,
        })
    }

    async fn cleanup(&mut self) -> Result<()> {
        if self.keep_resources {
            eprintln!(
                "keeping E2E resources: Linear project {}, GitHub repo {}",
                self.project.url, self.github_repo.full_name
            );
            return Ok(());
        }

        let mut errors = Vec::new();
        for issue in &self.issues {
            if let Err(err) = self.linear.archive_issue(&issue.id).await {
                errors.push(format!("archive {} failed: {err:#}", issue.identifier));
            }
        }
        if let Err(err) = self.linear.delete_project(&self.project.id).await {
            errors.push(format!("delete project failed: {err:#}"));
        }
        if let Err(err) = delete_github_repo(&self.github_repo).await {
            errors.push(format!("delete repo failed: {err:#}"));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!("{}", errors.join("; "))
        }
    }
}

async fn cleanup_partial(
    linear: &LinearE2eClient,
    github_repo: Option<&GitHubRepo>,
    project: Option<&LinearProject>,
    issues: &[LinearIssue],
    keep_resources: bool,
) {
    if keep_resources {
        return;
    }
    for issue in issues {
        let _ = linear.archive_issue(&issue.id).await;
    }
    if let Some(project) = project {
        let _ = linear.delete_project(&project.id).await;
    }
    if let Some(repo) = github_repo {
        let _ = delete_github_repo(repo).await;
    }
}

#[derive(Debug, Clone)]
struct RequiredState {
    name: String,
    state_type: String,
    color: String,
    position: f64,
}

impl RequiredState {
    fn new(name: &str, state_type: &str, color: &str, position: f64) -> Self {
        Self {
            name: name.to_string(),
            state_type: state_type.to_string(),
            color: color.to_string(),
            position,
        }
    }
}

#[derive(Debug, Clone)]
struct GitHubRepo {
    full_name: String,
    clone_url: String,
}

#[derive(Clone)]
struct LinearE2eClient {
    client: reqwest::Client,
    endpoint: String,
}

impl LinearE2eClient {
    fn new(api_key: &str, endpoint: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        let auth = HeaderValue::from_str(api_key).context("LINEAR_API_KEY is not header-safe")?;
        headers.insert(AUTHORIZATION, auth);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .default_headers(headers)
            .build()
            .context("build Linear client")?;
        Ok(Self {
            client,
            endpoint: endpoint.to_string(),
        })
    }

    async fn graphql<T: DeserializeOwned>(&self, query: &str, variables: Value) -> Result<T> {
        let response = self
            .client
            .post(&self.endpoint)
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await
            .context("Linear GraphQL request failed")?;
        let status = response.status();
        let body = response
            .json::<GraphqlResponse<T>>()
            .await
            .context("decode Linear GraphQL response")?;
        if !status.is_success() {
            bail!("Linear GraphQL HTTP status {status}");
        }
        if let Some(errors) = body.errors {
            bail!(
                "Linear GraphQL errors: {}",
                errors
                    .iter()
                    .map(|error| error.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            );
        }
        body.data
            .ok_or_else(|| anyhow!("Linear GraphQL response missing data"))
    }

    async fn team_by_key(&self, key: &str) -> Result<LinearTeam> {
        #[derive(Deserialize)]
        struct TeamByKeyData {
            teams: Nodes<LinearTeam>,
        }

        let data: TeamByKeyData = self
            .graphql(
                r#"
                query TeamByKey($key: String!) {
                  teams(filter: { key: { eq: $key } }, first: 1) {
                    nodes {
                      id
                      key
                      name
                      states { nodes { id name type } }
                    }
                  }
                }
                "#,
                json!({ "key": key }),
            )
            .await?;
        data.teams
            .nodes
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Linear team with key {key} not found"))
    }

    async fn ensure_states(
        &self,
        team_id: &str,
        required: Vec<RequiredState>,
        create_missing: bool,
    ) -> Result<BTreeMap<String, LinearState>> {
        let mut states = self
            .team_by_id(team_id)
            .await?
            .states
            .nodes
            .into_iter()
            .map(|state| (state.name.clone(), state))
            .collect::<BTreeMap<_, _>>();

        for required_state in required {
            if states.contains_key(&required_state.name) {
                continue;
            }
            if !create_missing {
                bail!(
                    "Linear state {:?} is missing; set MEERKAT_FUGUE_E2E_CREATE_MISSING_LINEAR_STATES=1 to create it",
                    required_state.name
                );
            }
            let created = self.create_state(team_id, &required_state).await?;
            states.insert(created.name.clone(), created);
        }
        Ok(states)
    }

    async fn team_by_id(&self, id: &str) -> Result<LinearTeam> {
        #[derive(Deserialize)]
        struct TeamByIdData {
            team: LinearTeam,
        }

        let data: TeamByIdData = self
            .graphql(
                r#"
                query TeamById($id: String!) {
                  team(id: $id) {
                    id
                    key
                    name
                    states { nodes { id name type } }
                  }
                }
                "#,
                json!({ "id": id }),
            )
            .await?;
        Ok(data.team)
    }

    async fn create_state(&self, team_id: &str, state: &RequiredState) -> Result<LinearState> {
        #[derive(Deserialize)]
        struct CreateStateData {
            #[serde(rename = "workflowStateCreate")]
            workflow_state_create: WorkflowStatePayload,
        }

        let data: CreateStateData = self
            .graphql(
                r#"
                mutation CreateWorkflowState($input: WorkflowStateCreateInput!) {
                  workflowStateCreate(input: $input) {
                    success
                    workflowState { id name type }
                  }
                }
                "#,
                json!({
                    "input": {
                        "teamId": team_id,
                        "name": state.name,
                        "type": state.state_type,
                        "color": state.color,
                        "position": state.position,
                    }
                }),
            )
            .await?;
        if !data.workflow_state_create.success {
            bail!("workflowStateCreate returned success=false");
        }
        Ok(data.workflow_state_create.workflow_state)
    }

    async fn ensure_label(&self, team_id: &str, name: &str) -> Result<LinearLabel> {
        #[derive(Deserialize)]
        struct LabelsData {
            #[serde(rename = "issueLabels")]
            issue_labels: Nodes<LinearLabel>,
        }
        let existing: LabelsData = self
            .graphql(
                r#"
                query IssueLabelByName($teamId: ID!, $name: String!) {
                  issueLabels(
                    filter: { team: { id: { eq: $teamId } }, name: { eq: $name } }
                    first: 1
                  ) { nodes { id } }
                }
                "#,
                json!({ "teamId": team_id, "name": name }),
            )
            .await?;
        if let Some(label) = existing.issue_labels.nodes.into_iter().next() {
            return Ok(label);
        }

        #[derive(Deserialize)]
        struct CreateLabelData {
            #[serde(rename = "issueLabelCreate")]
            issue_label_create: LabelPayload,
        }
        let created: CreateLabelData = self
            .graphql(
                r#"
                mutation CreateIssueLabel($input: IssueLabelCreateInput!) {
                  issueLabelCreate(input: $input) {
                    success
                    issueLabel { id }
                  }
                }
                "#,
                json!({
                    "input": {
                        "teamId": team_id,
                        "name": name,
                        "color": "#8a63d2",
                    }
                }),
            )
            .await?;
        if !created.issue_label_create.success {
            bail!("issueLabelCreate returned success=false");
        }
        created
            .issue_label_create
            .issue_label
            .ok_or_else(|| anyhow!("issueLabelCreate returned no issueLabel"))
    }

    async fn create_project(
        &self,
        team_id: &str,
        name: &str,
        description: &str,
    ) -> Result<LinearProject> {
        #[derive(Deserialize)]
        struct CreateProjectData {
            #[serde(rename = "projectCreate")]
            project_create: ProjectPayload,
        }

        let data: CreateProjectData = self
            .graphql(
                r#"
                mutation CreateProject($input: ProjectCreateInput!) {
                  projectCreate(input: $input) {
                    success
                    project { id slugId url }
                  }
                }
                "#,
                json!({
                    "input": {
                        "name": name,
                        "description": description,
                        "teamIds": [team_id],
                        "priority": 3,
                    }
                }),
            )
            .await?;
        if !data.project_create.success {
            bail!("projectCreate returned success=false");
        }
        let project = data
            .project_create
            .project
            .ok_or_else(|| anyhow!("projectCreate returned no project"))?;
        if project.slug_id.is_empty() {
            bail!("projectCreate returned empty slugId");
        }
        Ok(project)
    }

    async fn delete_project(&self, project_id: &str) -> Result<()> {
        #[derive(Deserialize)]
        struct DeleteProjectData {
            #[serde(rename = "projectDelete")]
            project_delete: DeletePayload,
        }

        let data: DeleteProjectData = self
            .graphql(
                r#"
                mutation DeleteProject($id: String!) {
                  projectDelete(id: $id) { success }
                }
                "#,
                json!({ "id": project_id }),
            )
            .await?;
        if data.project_delete.success {
            Ok(())
        } else {
            bail!("projectDelete returned success=false")
        }
    }

    async fn create_issue(&self, issue: &CreateIssue<'_>) -> Result<LinearIssue> {
        #[derive(Deserialize)]
        struct CreateIssueData {
            #[serde(rename = "issueCreate")]
            issue_create: IssuePayload,
        }

        let data: CreateIssueData = self
            .graphql(
                r#"
                mutation CreateIssue($input: IssueCreateInput!) {
                  issueCreate(input: $input) {
                    success
                    issue { id identifier title state { name } }
                  }
                }
                "#,
                json!({
                    "input": {
                        "teamId": issue.team_id,
                        "projectId": issue.project_id,
                        "stateId": issue.state_id,
                        "title": issue.title,
                        "description": issue.description,
                        "labelIds": issue.label_ids,
                        "priority": 3,
                    }
                }),
            )
            .await?;
        if !data.issue_create.success {
            bail!("issueCreate returned success=false");
        }
        data.issue_create
            .issue
            .ok_or_else(|| anyhow!("issueCreate returned no issue"))
    }

    async fn archive_issue(&self, issue_id: &str) -> Result<()> {
        #[derive(Deserialize)]
        struct ArchiveIssueData {
            #[serde(rename = "issueArchive")]
            issue_archive: DeletePayload,
        }

        let data: ArchiveIssueData = self
            .graphql(
                r#"
                mutation ArchiveIssue($id: String!) {
                  issueArchive(id: $id, trash: true) { success }
                }
                "#,
                json!({ "id": issue_id }),
            )
            .await?;
        if data.issue_archive.success {
            Ok(())
        } else {
            bail!("issueArchive returned success=false")
        }
    }

    async fn issues_with_comments(&self, issue_ids: &[String]) -> Result<Vec<IssueWithComments>> {
        #[derive(Deserialize)]
        struct IssuesData {
            issues: Nodes<IssueWithComments>,
        }

        let data: IssuesData = self
            .graphql(
                r#"
                query IssuesWithComments($ids: [ID!]!, $first: Int!) {
                  issues(filter: { id: { in: $ids } }, first: $first) {
                    nodes {
                      state { name }
                      comments(first: 100) { nodes { body } }
                    }
                  }
                }
                "#,
                json!({ "ids": issue_ids, "first": issue_ids.len().max(1) }),
            )
            .await?;
        Ok(data.issues.nodes)
    }
}

struct CreateIssue<'a> {
    team_id: &'a str,
    project_id: &'a str,
    state_id: &'a str,
    title: &'a str,
    description: &'a str,
    label_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GraphqlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphqlError>>,
}

#[derive(Debug, Deserialize)]
struct GraphqlError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct Nodes<T> {
    nodes: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct LinearTeam {
    id: String,
    states: Nodes<LinearState>,
}

#[derive(Debug, Clone, Deserialize)]
struct LinearState {
    id: String,
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct LinearLabel {
    id: String,
}

#[derive(Debug, Deserialize)]
struct LabelPayload {
    success: bool,
    #[serde(rename = "issueLabel")]
    issue_label: Option<LinearLabel>,
}

#[derive(Debug, Deserialize)]
struct WorkflowStatePayload {
    success: bool,
    #[serde(rename = "workflowState")]
    workflow_state: LinearState,
}

#[derive(Debug, Clone, Deserialize)]
struct LinearProject {
    id: String,
    #[serde(rename = "slugId")]
    slug_id: String,
    url: String,
}

#[derive(Debug, Deserialize)]
struct ProjectPayload {
    success: bool,
    project: Option<LinearProject>,
}

#[derive(Debug, Clone, Deserialize)]
struct LinearIssue {
    id: String,
    identifier: String,
}

#[derive(Debug, Deserialize)]
struct IssuePayload {
    success: bool,
    issue: Option<LinearIssue>,
}

#[derive(Debug, Deserialize)]
struct DeletePayload {
    success: bool,
}

#[derive(Debug, Deserialize)]
struct IssueWithComments {
    state: NamedNode,
    comments: Nodes<CommentNode>,
}

#[derive(Debug, Deserialize)]
struct NamedNode {
    name: String,
}

#[derive(Debug, Deserialize)]
struct CommentNode {
    body: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PullRequest {
    number: u64,
    title: String,
    head_ref_name: String,
    is_draft: bool,
}

async fn create_github_repo(config: &RealE2eConfig, run_id: &str) -> Result<GitHubRepo> {
    let name = format!("{}-{run_id}", config.repo_prefix);
    let visibility = if config.repo_private {
        "--private"
    } else {
        "--public"
    };
    command_stdout(
        "gh",
        [
            "repo",
            "create",
            &format!("{}/{}", config.github_owner, name),
            visibility,
            "--clone=false",
            "--description",
            "Temporary Meerkat Fugue real E2E repository",
        ],
        None,
    )
    .await?;
    let full_name = format!("{}/{}", config.github_owner, name);
    Ok(GitHubRepo {
        clone_url: format!("https://github.com/{full_name}.git"),
        full_name,
    })
}

async fn seed_github_repo(repo: &GitHubRepo) -> Result<()> {
    let temp_dir = tempfile::tempdir().context("create temporary seed repo")?;
    std::fs::write(
        temp_dir.path().join("README.md"),
        "# Meerkat Fugue E2E\n\nTemporary repository for a real E2E run.\n",
    )
    .context("write seed README")?;
    std::fs::write(temp_dir.path().join(".gitignore"), "target\n.DS_Store\n")
        .context("write seed gitignore")?;
    command_stdout("git", ["init"], Some(temp_dir.path())).await?;
    command_stdout(
        "git",
        ["config", "user.email", "meerkat-fugue-e2e@example.invalid"],
        Some(temp_dir.path()),
    )
    .await?;
    command_stdout(
        "git",
        ["config", "user.name", "Meerkat Fugue E2E"],
        Some(temp_dir.path()),
    )
    .await?;
    command_stdout("git", ["add", "."], Some(temp_dir.path())).await?;
    command_stdout(
        "git",
        ["commit", "-m", "Initial E2E fixture"],
        Some(temp_dir.path()),
    )
    .await?;
    command_stdout("git", ["branch", "-M", "main"], Some(temp_dir.path())).await?;
    command_stdout(
        "git",
        ["remote", "add", "origin", &repo.clone_url],
        Some(temp_dir.path()),
    )
    .await?;
    command_stdout(
        "git",
        ["push", "-u", "origin", "main"],
        Some(temp_dir.path()),
    )
    .await?;
    Ok(())
}

async fn delete_github_repo(repo: &GitHubRepo) -> Result<()> {
    command_stdout("gh", ["repo", "delete", &repo.full_name, "--yes"], None)
        .await
        .map(|_| ())
}

#[derive(Debug, Clone, Copy)]
enum WorkpadExpectation {
    Planning,
    PullRequest,
    Done,
}

impl WorkpadExpectation {
    fn matches(self, body: &str) -> bool {
        let lower = body.to_ascii_lowercase();
        let has_plan = lower.contains("plan");
        let has_acceptance = lower.contains("acceptance");
        let has_validation = lower.contains("validation");
        let has_pr = lower.contains("github.com") && lower.contains("pull");
        let has_done = lower.contains("done") || lower.contains("merged");
        let has_reviewer = lower.contains("reviewer");
        let has_evidence = lower.contains("evidence")
            || lower.contains("cargo ")
            || lower.contains("check")
            || lower.contains("merge");
        let has_task_item = has_markdown_task_item(body);
        let has_completed_task = has_markdown_completed_task(body);
        match self {
            Self::Planning => {
                has_plan && has_acceptance && has_task_item && !is_opaque_workpad_summary(body)
            }
            Self::PullRequest => {
                has_plan
                    && has_acceptance
                    && has_validation
                    && has_pr
                    && has_evidence
                    && has_task_item
                    && has_completed_task
                    && !is_opaque_workpad_summary(body)
            }
            Self::Done => {
                has_plan
                    && has_acceptance
                    && has_validation
                    && has_pr
                    && has_done
                    && has_reviewer
                    && has_evidence
                    && has_task_item
                    && has_completed_task
                    && !is_opaque_workpad_summary(body)
            }
        }
    }
}

fn is_opaque_workpad_summary(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.contains("text:\"ok\"")
        || lower.contains("\"text\":\"ok\"")
        || lower.lines().any(|line| {
            matches!(
                line.trim(),
                "ok" | "text: ok" | "text:\"ok\"" | r#"{"text":"ok"}"#
            )
        })
}

fn has_markdown_task_item(body: &str) -> bool {
    body.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed.starts_with("- [ ] ")
            || trimmed.starts_with("- [x] ")
            || trimmed.starts_with("- [X] ")
    })
}

fn has_markdown_completed_task(body: &str) -> bool {
    body.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed.starts_with("- [x] ") || trimmed.starts_with("- [X] ")
    })
}

async fn wait_for_workpads(
    linear: &LinearE2eClient,
    issue_ids: &[String],
    expectation: WorkpadExpectation,
    timeout: Duration,
) -> Result<()> {
    wait_until(timeout, Duration::from_secs(10), || async {
        let issues = linear.issues_with_comments(issue_ids).await?;
        let ready = issues.iter().all(|issue| {
            let workpads = issue
                .comments
                .nodes
                .iter()
                .filter_map(|comment| comment.body.as_deref())
                .filter(|body| body.contains(WORKPAD_HEADING))
                .collect::<Vec<_>>();
            workpads.len() == 1 && expectation.matches(workpads[0])
        });
        if ready { Ok(Some(())) } else { Ok(None) }
    })
    .await
    .with_context(|| format!("timed out waiting for {expectation:?} workpads"))
}

async fn wait_for_completion_workpads(
    linear: &LinearE2eClient,
    issue_ids: &[String],
    done_state: &str,
    timeout: Duration,
) -> Result<()> {
    wait_until(timeout, Duration::from_secs(10), || async {
        let issues = linear.issues_with_comments(issue_ids).await?;
        let ready = issues.iter().all(|issue| {
            let workpads = issue
                .comments
                .nodes
                .iter()
                .filter_map(|comment| comment.body.as_deref())
                .filter(|body| body.contains(WORKPAD_HEADING))
                .collect::<Vec<_>>();
            if workpads.len() != 1 {
                return false;
            }
            WorkpadExpectation::Done.matches(workpads[0])
                || (issue.state.name == done_state
                    && WorkpadExpectation::PullRequest.matches(workpads[0]))
        });
        if ready { Ok(Some(())) } else { Ok(None) }
    })
    .await
    .with_context(|| {
        format!("timed out waiting for Done workpads or externally completed {done_state} issues")
    })
}

async fn wait_for_issue_states(
    linear: &LinearE2eClient,
    issue_ids: &[String],
    state_name: &str,
    timeout: Duration,
) -> Result<()> {
    wait_until(timeout, Duration::from_secs(10), || async {
        let issues = linear.issues_with_comments(issue_ids).await?;
        let ready = issues.iter().all(|issue| issue.state.name == state_name);
        if ready { Ok(Some(())) } else { Ok(None) }
    })
    .await
    .with_context(|| format!("timed out waiting for Linear issues to reach {state_name}"))
}

async fn wait_for_pull_requests(
    repo: &GitHubRepo,
    issue_identifiers: &[String],
    timeout: Duration,
) -> Result<BTreeMap<String, PullRequest>> {
    wait_until(timeout, Duration::from_secs(10), || async {
        let prs = list_pull_requests(repo).await?;
        Ok(unique_pull_requests_by_issue(&prs, issue_identifiers))
    })
    .await
    .context("timed out waiting for GitHub PRs")
}

fn unique_pull_requests_by_issue(
    prs: &[PullRequest],
    issue_identifiers: &[String],
) -> Option<BTreeMap<String, PullRequest>> {
    let mut by_issue = BTreeMap::new();
    for issue_identifier in issue_identifiers {
        let matches = prs
            .iter()
            .filter(|pr| pr_mentions_issue(pr, issue_identifier))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [pr] if !pr.is_draft => {
                by_issue.insert(issue_identifier.clone(), (*pr).clone());
            }
            _ => return None,
        }
    }
    Some(by_issue)
}

async fn list_pull_requests(repo: &GitHubRepo) -> Result<Vec<PullRequest>> {
    let output = command_stdout(
        "gh",
        [
            "pr",
            "list",
            "--repo",
            &repo.full_name,
            "--state",
            "open",
            "--json",
            "number,title,headRefName,isDraft",
        ],
        None,
    )
    .await?;
    serde_json::from_str(&output).context("decode gh pr list output")
}

fn pr_mentions_issue(pr: &PullRequest, issue_identifier: &str) -> bool {
    let needle = issue_identifier.to_ascii_lowercase();
    pr.title.to_ascii_lowercase().contains(&needle)
        || pr.head_ref_name.to_ascii_lowercase().contains(&needle)
}

async fn merge_pull_request(repo: &GitHubRepo, number: u64) -> Result<()> {
    wait_until(
        Duration::from_secs(300),
        Duration::from_secs(10),
        || async {
            let result = command_stdout(
                "gh",
                [
                    "pr",
                    "merge",
                    &number.to_string(),
                    "--repo",
                    &repo.full_name,
                    "--squash",
                    "--delete-branch",
                    "--subject",
                    &format!("Merge E2E PR #{number}"),
                    "--body",
                    "Merged by Meerkat Fugue real E2E.",
                ],
                None,
            )
            .await;
            match result {
                Ok(_) => Ok(Some(())),
                Err(_) => Ok(None),
            }
        },
    )
    .await
    .with_context(|| format!("timed out waiting to merge PR #{number}"))
}

async fn wait_until<T, F, Fut>(timeout: Duration, interval: Duration, mut check: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    let started = Instant::now();
    loop {
        if let Some(value) = check().await? {
            return Ok(value);
        }
        if started.elapsed() >= timeout {
            bail!("condition was not met within {timeout:?}");
        }
        tokio::time::sleep(interval).await;
    }
}

async fn command_stdout<I, S>(program: &str, args: I, cwd: Option<&Path>) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(program);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = command
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("failed to run {program}"))?;
    if output.status.success() {
        String::from_utf8(output.stdout).context("command stdout was not UTF-8")
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{program} failed with {}: {stderr}", output.status)
    }
}

fn workflow_contents(
    config: &RealE2eConfig,
    repo: &GitHubRepo,
    project_slug: &str,
    temp_root: &Path,
) -> String {
    native_workflow_contents(config, repo, project_slug, temp_root)
}

fn native_workflow_contents(
    config: &RealE2eConfig,
    repo: &GitHubRepo,
    project_slug: &str,
    temp_root: &Path,
) -> String {
    let active_state_concurrency = config
        .active_states()
        .into_iter()
        .map(|state| format!("    {state}: {}", config.issue_count))
        .collect::<Vec<_>>()
        .join("\n");
    let provisioning_mode = if config.create_missing_linear_states {
        "create_missing"
    } else {
        "validate"
    };
    let kapellmeister_config = if config.policy_mode.is_guarded() {
        format!(
            "\nkapellmeister:\n  shadow_mode:\n    enabled: true\n  authority_mode: guarded_labeled\n  safe_selectors:\n    linear_labels:\n      - {}\n",
            GUARDED_KAPELLMEISTER_LABEL
        )
    } else {
        String::new()
    };
    format!(
        r#"version: 1
linear:
  api_key: "{}"
  project_slug: "{}"
  team: "{}"
  provisioning:
    mode: {}
  labels: []

linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: {}
      coder: {}
      reviewer: {}
    aggregate:
      precedence: [{}, {}, {}]
      fallback: {}
    terminal_lanes:
      succeeded: {}
      failed: {}
      cancelled: {}

polling:
  interval_ms: 2000

workspace:
  root: "{}"

hooks:
  timeout_ms: 120000
  after_create: |
    git clone {} .
    git config user.name "Fugue Agent"
    git config user.email "meerkat-fugue-e2e@example.invalid"
  before_run: |
    git fetch --all --prune
  before_remove: |
    git status --short || true

agent:
  max_concurrent_agents: {}
  max_turns: 8
  max_retry_backoff_ms: 60000
  max_concurrent_agents_by_state:
{}

meerkat:
  command: "{}"
  state_root: "{}"
  enable_shell: true
  enable_builtins: true
  enable_mob: true
  first_turn_activation_prompt: Begin the real Meerkat Fugue E2E issue flow.
  turn_timeout_ms: 1800000
  read_timeout_ms: 10000
  stall_timeout_ms: 600000

selection:
  default_template: implementation_mob

mcp_servers:
  linear:
    command: [fugue-linear-mcp]
  github:
    command: [fugue-github-mcp]
  playwright:
    command: [npx, -y, "@playwright/mcp@latest", --headless]

agents:
  planner:
    tools: [mob, mcp:linear, mcp:github]
    system_prompt: "Plan the requested E2E file change."
  coder:
    tools: [mob, shell, git, mcp:linear, mcp:github]
  reviewer:
    tools: [mob, shell, git, mcp:linear, mcp:github]
    system_prompt: "Review the E2E change and report detailed findings if validation is missing."

mob_templates:
  implementation_mob:
    flow: planner-coder-reviewer
    linear_projection: planner_coder_reviewer
    max_review_cycles: 2
    members:
      - name: planner
        profile: planner
      - name: coder
        profile: coder
      - name: reviewer
        profile: reviewer

worker:
  prompt_template: |
    You are Fugue working unattended on Linear issue `{{{{ issue.identifier }}}}` for Meerkat Fugue's real E2E fixture.

    Issue context:
    Identifier: {{{{ issue.identifier }}}}
    Title: {{{{ issue.title }}}}
    Current status: {{{{ issue.state }}}}
    Labels: {{{{ issue.labels }}}}
    URL: {{{{ issue.url }}}}
    Description:
    {{% if issue.description %}}
    {{{{ issue.description }}}}
    {{% else %}}
    No description provided.
    {{% endif %}}

    Repository: {}
    Project slug: {}

    Repository rules:
    - Follow repository agent instructions when present.
    - Keep edits scoped to this Linear issue.
    - Preserve unrelated user changes.

    Linear workflow:
    - `Backlog`: parked. Do not work unless explicitly moved to an active state.
    - `{}`: active queue. Fugue projects the issue to `{}` when the issue mob begins planning.
    - `{}`: planner owns the Workpad plan or focused rework plan.
    - `{}`: coder implements the current Workpad plan.
    - `{}`: reviewer gates local implementation and PR/check evidence.
    - `{}`, `{}`, `{}`, and `{}`: terminal. Do nothing.

    Execution requirements:
    1. If the issue is `{}`, treat it as queued. The issue lead owns `linear_move_issue` lifecycle movement and should project `{}` when planning begins.
    2. Create or update exactly one persistent Linear comment headed `{WORKPAD_HEADING}`.
    3. Keep the workpad current with plan, acceptance criteria, validation, notes, and PR link. Use Linear-renderable Markdown task lists: `- [ ]` for pending items and `- [x]` for completed items. Do not use emoji-only status markers instead of checkboxes.
    4. The plan must name protected behavior, focused tests/static checks, and acceptance criteria before implementation starts.
    5. Report lifecycle facts to the issue lead while making the requested file change; worker members do not mutate lifecycle lanes directly.
    6. Create a branch named `meerkat/{{{{ issue.identifier }}}}-<short-slug>` from `main`.
    7. Make exactly the file change requested in the issue description.
    8. Run focused validation. For this fixture, verify the requested file content with shell commands.
    9. Commit, push, and open a non-draft PR against `main`.
    10. Record validation and PR evidence in the Workpad; the issue lead projects `{}` from mob/reviewer state.
    11. If PR checks or review are red, record detailed findings in the workpad for focused replanning through `{}`.
    12. Do not record completion as accepted until the PR is merged and the issue lead can project `{}` from accepted evidence.

    Use Fugue's Linear tools for inspection and Workpad updates: `linear_inspect_issue`, `linear_list_project_states`, and `linear_upsert_workpad`. Planner, coder, and reviewer report lifecycle facts to the issue lead; the issue lead owns `linear_move_issue` lifecycle movement. Do not create separate Linear comments for progress; update the single `## Fugue Workpad` comment. Use `gh` for GitHub.

    {{% if attempt %}}
    This is retry or continuation attempt {{{{ attempt }}}}. Inspect the existing workspace before editing.
    {{% endif %}}
  native_launch_context_template: |
    Native E2E launch context for {{{{ issue.identifier }}}} in {{{{ worktree }}}} using {{{{ template_id }}}} with {{{{ members }}}}.
{}"#,
        yaml_escape(&config.linear_api_key),
        yaml_escape(project_slug),
        yaml_escape(&config.linear_team_key),
        provisioning_mode,
        yaml_inline_string(&config.planning_state),
        yaml_inline_string(&config.implementing_state),
        yaml_inline_string(&config.reviewing_state),
        yaml_inline_string(&config.reviewing_state),
        yaml_inline_string(&config.implementing_state),
        yaml_inline_string(&config.planning_state),
        yaml_inline_string(&config.reviewing_state),
        yaml_inline_string(&config.done_state),
        yaml_inline_string(&config.reviewing_state),
        yaml_inline_string(&config.canceled_state),
        yaml_escape(&temp_root.join("workspaces").display().to_string()),
        repo.clone_url,
        config.issue_count,
        active_state_concurrency,
        yaml_escape(&config.rkat_command),
        yaml_escape(&temp_root.join("meerkat-state").display().to_string()),
        repo.full_name,
        project_slug,
        config.todo_state,
        config.planning_state,
        config.planning_state,
        config.implementing_state,
        config.reviewing_state,
        config.done_state,
        config.canceled_state,
        config.cancelled_state,
        config.duplicate_state,
        config.todo_state,
        config.planning_state,
        config.reviewing_state,
        config.planning_state,
        config.done_state,
        kapellmeister_config,
    )
}

fn issue_description(index: usize, run_id: &str, repo: &GitHubRepo) -> String {
    format!(
        r#"Create or update `e2e/issue-{index}.txt`.

The file must contain exactly this line:

meerkat-fugue real e2e {run_id} issue {index}

This issue belongs to repo `{}`. Follow the E2E workflow instructions from the prompt."#,
        repo.full_name
    )
}

fn unique_run_id() -> Result<String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before Unix epoch")?
        .as_millis();
    Ok(format!("{}-{}", millis, std::process::id()))
}

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> Result<usize> {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => value
            .parse::<usize>()
            .with_context(|| format!("{key} must be a positive integer")),
        _ => Ok(default),
    }
}

fn env_duration_minutes(key: &str, default: u64) -> Result<Duration> {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => value
            .parse::<u64>()
            .map(Duration::from_secs)
            .map(|duration| duration * 60)
            .with_context(|| format!("{key} must be an integer number of minutes")),
        _ => Ok(Duration::from_secs(default * 60)),
    }
}

fn env_or_zshrc(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| read_zshrc_var(key))
}

fn read_zshrc_var(key: &str) -> Option<String> {
    let home = std::env::var_os("HOME")?;
    let contents = std::fs::read_to_string(Path::new(&home).join(".zshrc")).ok()?;
    contents
        .lines()
        .find_map(|line| parse_shell_assignment(line, key))
}

fn parse_shell_assignment(line: &str, key: &str) -> Option<String> {
    let trimmed = line.trim();
    let trimmed = trimmed.strip_prefix("export ").unwrap_or(trimmed);
    let (name, value) = trimmed.split_once('=')?;
    if name.trim() != key {
        return None;
    }
    let value = value.trim().trim_matches('"').trim_matches('\'');
    (!value.is_empty()).then(|| value.to_string())
}

fn yaml_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn yaml_inline_string(value: &str) -> String {
    format!("\"{}\"", yaml_escape(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_template_matches_linear_fugue_lane_contract() {
        let config = test_config();
        let repo = GitHubRepo {
            full_name: "owner/repo".to_string(),
            clone_url: "https://github.com/owner/repo.git".to_string(),
        };
        let workflow = workflow_contents(&config, &repo, "project-slug", Path::new("/tmp/e2e"));

        assert!(workflow.contains("linear_projections:"));
        assert!(workflow.contains("planner: \"Planning\""));
        assert!(workflow.contains("coder: \"Implementing\""));
        assert!(workflow.contains("reviewer: \"Reviewing\""));
        assert!(workflow.contains("succeeded: \"Done\""));
        assert!(workflow.contains("cancelled: \"Canceled\""));
        assert!(!workflow.contains("  states:"));
        assert!(workflow.contains(
            "Create or update exactly one persistent Linear comment headed `## Fugue Workpad`."
        ));
        assert!(workflow.contains("Use Linear-renderable Markdown task lists"));
        assert!(workflow.contains("If PR checks or review are red"));
        assert!(workflow.contains("issue lead owns `linear_move_issue`"));
        assert!(workflow.contains("Do not record completion as accepted until the PR is merged"));
        assert!(!workflow.contains("Human Review"));
        assert!(!workflow.contains("Rework"));
        assert!(!workflow.contains("Merging"));
        assert!(!workflow.contains("In Review"));
        assert!(!workflow.contains("authority_mode: guarded_labeled"));
    }

    #[test]
    fn guarded_workflow_template_selects_kapellmeister_authority() {
        let mut config = test_config();
        config.policy_mode = RealE2ePolicyMode::GuardedKapellmeister;
        let repo = GitHubRepo {
            full_name: "owner/repo".to_string(),
            clone_url: "https://github.com/owner/repo.git".to_string(),
        };
        let workflow = workflow_contents(&config, &repo, "project-slug", Path::new("/tmp/e2e"));

        assert!(workflow.contains("kapellmeister:\n  shadow_mode:\n    enabled: true"));
        assert!(workflow.contains("authority_mode: guarded_labeled"));
        assert!(workflow.contains("linear_labels:\n      - kapellmeister-safe"));
    }

    #[test]
    fn native_mob_workflow_template_selects_native_execution_backend() {
        let mut config = test_config();
        config.policy_mode = RealE2ePolicyMode::NativeMob;
        let repo = GitHubRepo {
            full_name: "owner/repo".to_string(),
            clone_url: "https://github.com/owner/repo.git".to_string(),
        };
        let workflow = workflow_contents(&config, &repo, "project-slug", Path::new("/tmp/e2e"));

        assert!(workflow.contains("flow: planner-coder-reviewer"));
        assert!(workflow.contains("max_review_cycles: 2"));
        assert!(workflow.contains("playwright:"));
        assert!(workflow.contains("command: [npx, -y, \"@playwright/mcp@latest\", --headless]"));
        assert!(workflow.contains("default_template: implementation_mob"));
        assert!(!workflow.contains("authority_mode: guarded_labeled"));

        let definition =
            parse_native_workflow(Path::new("/tmp/e2e/fugue.yaml"), &workflow).unwrap();
        let mob = definition.config.get("mob").unwrap();
        let mob_templates = &mob["templates"];
        assert_eq!(
            mob_templates["implementation_mob"]["flow"],
            "planner-coder-reviewer"
        );
        assert_eq!(mob_templates["implementation_mob"]["max_review_cycles"], 2);
    }

    #[test]
    fn native_default_workflow_template_omits_backend_and_selects_native_execution() {
        let mut config = test_config();
        config.policy_mode = RealE2ePolicyMode::NativeDefault;
        let repo = GitHubRepo {
            full_name: "owner/repo".to_string(),
            clone_url: "https://github.com/owner/repo.git".to_string(),
        };
        let workflow = workflow_contents(&config, &repo, "project-slug", Path::new("/tmp/e2e"));

        assert!(workflow.starts_with("version: 1\nlinear:"));
        assert!(workflow.contains("\n  team: \"LUC\"\n"));
        assert!(workflow.contains("\n  provisioning:\n    mode: validate\n"));
        assert!(!workflow.contains("backend: native-execution"));
        assert!(workflow.contains("default_template: implementation_mob"));
        assert!(workflow.contains("flow: planner-coder-reviewer"));
        assert!(workflow.contains("max_review_cycles: 2"));
        assert!(workflow.contains("tools: [mob, mcp:linear, mcp:github]"));
        assert!(workflow.contains("tools: [mob, shell, git, mcp:linear, mcp:github]"));
        assert!(workflow.contains("playwright:"));
        assert!(workflow.contains("worker:\n  prompt_template: |"));
        assert!(workflow.contains("native_launch_context_template: |"));
        assert!(workflow.contains("state_root: \"/tmp/e2e/meerkat-state\""));

        let definition = parse_native_workflow(Path::new("/tmp/e2e/fugue.yaml"), &workflow)
            .expect("native-default E2E fixture is valid native YAML");
        assert_eq!(definition.config.get("backend"), None);
        let config = meerkat_fugue::config::ServiceConfig::from_definition(
            PathBuf::from("/tmp/e2e/fugue.yaml"),
            &definition,
        )
        .expect("native-default fixture should build a service config");

        assert_eq!(
            config.mob.backend,
            meerkat_fugue::config::MobExecutionBackend::NativeExecution
        );
        assert!(config.mob.native.enabled);
        assert!(!config.mob.native.shadow);
        assert_eq!(
            config
                .mob
                .templates
                .get("implementation_mob")
                .map(|template| template.backend),
            Some(meerkat_fugue::config::MobExecutionBackend::NativeExecution)
        );
        assert!(config.validate_dispatch().is_ok());
    }

    #[test]
    fn stage7d_native_only_policy_alias_drives_native_default_e2e_smoke() {
        assert!(matches!(
            RealE2ePolicyMode::parse("native_only").unwrap(),
            RealE2ePolicyMode::NativeDefault
        ));
    }

    #[test]
    fn pull_request_matching_requires_exactly_one_pr_per_issue() {
        let matching = PullRequest {
            number: 1,
            title: "Fix LUC-169".to_string(),
            head_ref_name: "meerkat/LUC-169-fix".to_string(),
            is_draft: false,
        };
        let duplicate = PullRequest {
            number: 2,
            title: "Retry LUC-169".to_string(),
            head_ref_name: "meerkat/LUC-169-retry".to_string(),
            is_draft: false,
        };
        let issue_identifiers = vec!["LUC-169".to_string()];

        let unique =
            unique_pull_requests_by_issue(std::slice::from_ref(&matching), &issue_identifiers)
                .expect("single matching PR should be accepted");
        assert_eq!(unique["LUC-169"].number, 1);

        let duplicate_prs = [matching, duplicate];
        assert!(unique_pull_requests_by_issue(&duplicate_prs, &issue_identifiers).is_none());
    }

    #[test]
    fn workpad_expectations_require_single_codex_style_workpad_content() {
        let planning =
            "## Fugue Workpad\n\n### Plan\n- [ ] Do it\n\n### Acceptance criteria\n- Works";
        assert!(WorkpadExpectation::Planning.matches(planning));
        assert!(!WorkpadExpectation::PullRequest.matches(planning));

        let pr = "## Fugue Workpad\n\nPlan\nAcceptance\nValidation\nPull request: https://github.com/o/r/pull/1";
        assert!(!WorkpadExpectation::PullRequest.matches(pr));

        let pr = "## Fugue Workpad\n\nPlan\n- [x] Do it\n\nAcceptance\n- [x] Works\n\nValidation\n- [x] cargo test\n\nPull request: https://github.com/o/r/pull/1";
        assert!(WorkpadExpectation::PullRequest.matches(pr));
        assert!(!WorkpadExpectation::Done.matches(pr));

        let done_without_reviewer = "## Fugue Workpad\n\nPlan\n- [x] Do it\n\nAcceptance\n- [x] Works\n\nValidation\n- [x] cargo test\n\nPull request: https://github.com/o/r/pull/1\nMerged and Done";
        assert!(!WorkpadExpectation::Done.matches(done_without_reviewer));

        let opaque = "## Fugue Workpad\n\n### Plan\n- [x] text:\"ok\"\n\n### Acceptance criteria\n- [x] ok\n\n### Validation\n- [x] ok\n\n### Review\n- [x] Reviewer ok\n\nPull request: https://github.com/o/r/pull/1\nMerged and Done";
        assert!(!WorkpadExpectation::Done.matches(opaque));

        let done = "## Fugue Workpad\n\nPlan\n- [x] Do it\n\nAcceptance\n- [x] Works\n\nValidation evidence\n- [x] cargo test\n\nReview\n- [x] Reviewer gate green; no findings.\n\nPull request: https://github.com/o/r/pull/1\nMerged and Done";
        assert!(WorkpadExpectation::Done.matches(done));
    }

    fn test_config() -> RealE2eConfig {
        RealE2eConfig {
            linear_api_key: "linear-key".to_string(),
            linear_endpoint: "https://api.linear.app/graphql".to_string(),
            linear_team_key: "LUC".to_string(),
            github_owner: "owner".to_string(),
            repo_private: true,
            keep_resources: false,
            create_missing_linear_states: false,
            issue_count: 2,
            repo_prefix: "repo".to_string(),
            rkat_command: "rkat-rpc".to_string(),
            policy_mode: RealE2ePolicyMode::NativeDefault,
            backlog_state: "Backlog".to_string(),
            todo_state: "Todo".to_string(),
            planning_state: "Planning".to_string(),
            implementing_state: "Implementing".to_string(),
            reviewing_state: "Reviewing".to_string(),
            canceled_state: "Canceled".to_string(),
            cancelled_state: "Cancelled".to_string(),
            duplicate_state: "Duplicate".to_string(),
            done_state: "Done".to_string(),
            plan_timeout: Duration::from_secs(1),
            pr_timeout: Duration::from_secs(1),
            done_timeout: Duration::from_secs(1),
        }
    }
}
