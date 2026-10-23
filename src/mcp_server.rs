use serde_json::{Map, Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::agent::{execute_github_fugue_tool, external_tools_for_tracker};
use crate::config::TrackerConfig;
use crate::independent_review::independent_review_completion_input_schema;
use crate::linear_tools::execute_linear_fugue_tool;
use crate::tools::{
    BranchName, FugueToolOperation, GitHubOwner, GitHubPullRequestNumber, GitHubPullRequestRef,
    GitHubRepoName, GitHubRepoRef, GitRef, LinearIssueId, LinearIssueIdentifier, LinearIssueRef,
    LinearUserRef,
};
use crate::workgraph_gate::WorkGraphTerminalGate;

pub async fn run_linear_mcp_server(config: TrackerConfig) -> anyhow::Result<()> {
    if config.kind == "jira" {
        crate::jira::JiraClient::from_config(&config)?;
    }
    let authority = LinearMcpAuthority::from_env();
    let tools = linear_tools(&config, authority);
    let scope = LinearMcpScope::from_env();
    let memory = Arc::new(Mutex::new(LinearMcpCallMemory::default()));
    let server_name = if config.kind == "jira" {
        "fugue-jira-mcp"
    } else {
        "fugue-linear-mcp"
    };
    run_stdio_mcp(server_name, tools, move |name, arguments| {
        let config = config.clone();
        let scope = scope.clone();
        let authority = authority;
        let memory = Arc::clone(&memory);
        async move {
            if let Some(operation) = linear_fugue_operation(&name) {
                if !linear_mcp_authority_allows(authority, operation) {
                    return (
                        true,
                        json!({
                            "error": format!(
                                "Linear MCP tool {name} requires projection authority; issue-mob members may write Workpad evidence, while issue leads own per-issue lifecycle state changes"
                            )
                        })
                        .to_string(),
                    );
                }
                let arguments = match apply_linear_mcp_scope(&config, &scope, &name, arguments) {
                    Ok(arguments) => arguments,
                    Err(error) => {
                        return (true, json!({"error": error}).to_string());
                    }
                };
                let arguments = apply_linear_mcp_memory(&memory, &name, arguments);
                remember_linear_mcp_issue_argument(&memory, &arguments);
                execute_guarded_linear_mcp_tool(
                    &config, &name, &arguments,
                    WorkGraphTerminalGate::from_env(&config).as_ref(),
                ).await
            } else {
                (
                    true,
                    json!({"error": format!("unsupported Linear MCP tool {name}")}).to_string(),
                )
            }
        }
    })
    .await
}

pub(crate) async fn execute_guarded_linear_mcp_tool(
    config: &TrackerConfig,
    name: &str,
    arguments: &Value,
    gate: Option<&WorkGraphTerminalGate>,
) -> (bool, String) {
    let lane_move = match crate::workgraph_gate::lane_move_for_tool(name, arguments) {
        Ok(input) => input,
        Err(error) => return (true, json!({"error": error.to_string()}).to_string()),
    };
    if let Some(input) = lane_move
        && let Some(gate) = gate
        && let Err(message) = gate.guard_terminal_move(config, &input).await
    {
        return (
            true,
            json!({"error": "unresolved_workgraph_obligations", "message": message}).to_string(),
        );
    }
    execute_linear_fugue_tool(config, name, arguments).await
}

pub async fn run_github_mcp_server() -> anyhow::Result<()> {
    let scope = GitHubMcpScope::from_env()?;
    let tools = github_tools(scope.allow_merge);
    let memory = Arc::new(Mutex::new(GitHubMcpCallMemory::default()));
    run_stdio_mcp(
        "fugue-github-mcp",
        tools,
        move |name, arguments| {
            let scope = scope.clone();
            let memory = Arc::clone(&memory);
            async move {
                if name == FugueToolOperation::GitHubMergePr.provider_callback_name()
                    && !scope.allow_merge
                {
                    return (
                        true,
                        json!({
                            "error": "github_merge_pr is disabled for this Fugue GitHub MCP server; configure FUGUE_GITHUB_ALLOW_MERGE=true only for an explicit repo-level merge authority"
                        })
                        .to_string(),
                    );
                }
                let arguments = apply_github_mcp_memory(
                    &memory,
                    &name,
                    apply_github_mcp_scope(&scope, &name, arguments),
                );
                if let Some(response) = github_mcp_scope_boundary_response(&scope, &name, &arguments)
                {
                    return (false, response.to_string());
                }
                let (is_error, content, _) = execute_github_fugue_tool(&name, &arguments);
                if !is_error {
                    remember_github_mcp_call(&memory, &name, &content);
                }
                (is_error, content)
            }
        },
    )
    .await
}

pub async fn run_host_mcp_server() -> anyhow::Result<()> {
    let tools = vec![
        json!({
            "name": "fugue_start_issue_mob",
        "description": "Ask the Fugue host to start or resume the configured native issue mob for an issue. Use after reconciling a host-observed Queue/Active/open-without-runtime signal. Tracker lanes are coarse operator controls; project running planner/coder/reviewer work to Active and record detailed phase as labels, WorkGraph, Workpad, or Meta runtime evidence.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "issue_identifier": {
                        "type": "string",
                        "description": "Linear issue identifier, for example LUC-526"
                    }
                },
                "required": ["issue_identifier"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "fugue_start_final_pr_gate",
            "description": "Ask the Fugue host to start or resume the repo-level final PR gate from a typed FinalPrGateStartRequest.",
            "input_schema": {
                "type": "object",
                "description": "Typed FinalPrGateStartRequest payload from a final_pr_gate_requested signal.",
                "additionalProperties": true
            }
        }),
        json!({
            "name": "fugue_start_independent_review",
            "description": "Ask the Fugue host to start or resume the independent review orchestrator mob from a typed IndependentReviewStartRequest.",
            "input_schema": {
                "type": "object",
                "description": "Typed IndependentReviewStartRequest payload prepared after inspecting issue and PR evidence.",
                "additionalProperties": true
            }
        }),
        json!({
            "name": "fugue_complete_independent_review",
            "description": "Submit typed independent reviewer verdicts to Fugue so the host can aggregate them, persist evidence, and move to the configured review outcome lane. Every verdict.schema_version must be exactly \"fugue.review.v1\". Include workgraph_scope copied exactly from the reviewer obligation items so red-review blockers are created in the same WorkGraph realm and namespace.",
            "input_schema": independent_review_completion_input_schema()
        }),
        json!({
            "name": "fugue_run_deploy",
            "description": "Run the guarded Fugue deploy lane action from a typed DeployStartRequest.",
            "input_schema": {
                "type": "object",
                "description": "Typed DeployStartRequest payload with issue, PR snapshot, recorded head, CI status, WorkGraph blocker count, and idempotency key.",
                "additionalProperties": true
            }
        }),
    ];
    let endpoint =
        env_string("FUGUE_HOST_API_URL").unwrap_or_else(|| "http://127.0.0.1:4001".to_string());
    let username = env_string("FUGUE_CONSOLE_USERNAME");
    let password = env_string("FUGUE_CONSOLE_PASSWORD");
    run_stdio_mcp("fugue-host-mcp", tools, move |name, arguments| {
        let endpoint = endpoint.clone();
        let username = username.clone();
        let password = password.clone();
        async move {
            match name.as_str() {
                "fugue_start_issue_mob" => {
                    execute_host_start_issue_mob(
                        &endpoint,
                        username.as_deref(),
                        password.as_deref(),
                        arguments,
                    )
                    .await
                }
                "fugue_start_final_pr_gate" => {
                    execute_host_start_final_pr_gate(
                        &endpoint,
                        username.as_deref(),
                        password.as_deref(),
                        arguments,
                    )
                    .await
                }
                "fugue_start_independent_review" => {
                    execute_host_start_independent_review(
                        &endpoint,
                        username.as_deref(),
                        password.as_deref(),
                        arguments,
                    )
                    .await
                }
                "fugue_complete_independent_review" => {
                    execute_host_complete_independent_review(
                        &endpoint,
                        username.as_deref(),
                        password.as_deref(),
                        arguments,
                    )
                    .await
                }
                "fugue_run_deploy" => {
                    execute_host_run_deploy(
                        &endpoint,
                        username.as_deref(),
                        password.as_deref(),
                        arguments,
                    )
                    .await
                }
                _ => (
                    true,
                    json!({"error": format!("unsupported Fugue host MCP tool {name}")}).to_string(),
                ),
            }
        }
    })
    .await
}

async fn execute_host_start_issue_mob(
    endpoint: &str,
    username: Option<&str>,
    password: Option<&str>,
    arguments: Value,
) -> (bool, String) {
    let Some(issue_identifier) = arguments
        .get("issue_identifier")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return (
            false,
            json!({"error": "issue_identifier must be a non-empty string"}).to_string(),
        );
    };
    let url = format!(
        "{}/api/v1/kapellmeister/start-issue",
        endpoint.trim_end_matches('/')
    );
    let client = reqwest::Client::new();
    let mut request = client
        .post(url)
        .json(&json!({ "issue_identifier": issue_identifier }));
    if let (Some(username), Some(password)) = (username, password) {
        request = request.basic_auth(username, Some(password));
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status();
            match response.text().await {
                Ok(body) => {
                    let content = if body.trim().is_empty() {
                        json!({
                            "error": "Fugue host returned an empty response body",
                            "status": status.as_u16(),
                        })
                        .to_string()
                    } else {
                        body
                    };
                    (false, content)
                }
                Err(error) => (
                    false,
                    json!({"error": format!("failed to read Fugue host response: {error}")})
                        .to_string(),
                ),
            }
        }
        Err(error) => (
            false,
            json!({"error": format!("failed to call Fugue host: {error}")}).to_string(),
        ),
    }
}

async fn execute_host_start_final_pr_gate(
    endpoint: &str,
    username: Option<&str>,
    password: Option<&str>,
    arguments: Value,
) -> (bool, String) {
    let url = format!(
        "{}/api/v1/kapellmeister/start-final-pr-gate",
        endpoint.trim_end_matches('/')
    );
    let client = reqwest::Client::new();
    let mut request = client.post(url).json(&arguments);
    if let (Some(username), Some(password)) = (username, password) {
        request = request.basic_auth(username, Some(password));
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status();
            match response.text().await {
                Ok(body) => {
                    let content = if body.trim().is_empty() {
                        json!({
                            "error": "Fugue host returned an empty response body",
                            "status": status.as_u16(),
                        })
                        .to_string()
                    } else {
                        body
                    };
                    (false, content)
                }
                Err(error) => (
                    false,
                    json!({"error": format!("failed to read Fugue host response: {error}")})
                        .to_string(),
                ),
            }
        }
        Err(error) => (
            false,
            json!({"error": format!("failed to call Fugue host: {error}")}).to_string(),
        ),
    }
}

async fn execute_host_start_independent_review(
    endpoint: &str,
    username: Option<&str>,
    password: Option<&str>,
    arguments: Value,
) -> (bool, String) {
    let url = format!(
        "{}/api/v1/kapellmeister/start-independent-review",
        endpoint.trim_end_matches('/')
    );
    let client = reqwest::Client::new();
    let mut request = client.post(url).json(&arguments);
    if let (Some(username), Some(password)) = (username, password) {
        request = request.basic_auth(username, Some(password));
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status();
            match response.text().await {
                Ok(body) => {
                    let content = if body.trim().is_empty() {
                        json!({
                            "error": "Fugue host returned an empty response body",
                            "status": status.as_u16(),
                        })
                        .to_string()
                    } else {
                        body
                    };
                    (false, content)
                }
                Err(error) => (
                    false,
                    json!({"error": format!("failed to read Fugue host response: {error}")})
                        .to_string(),
                ),
            }
        }
        Err(error) => (
            false,
            json!({"error": format!("failed to call Fugue host: {error}")}).to_string(),
        ),
    }
}

async fn execute_host_complete_independent_review(
    endpoint: &str,
    username: Option<&str>,
    password: Option<&str>,
    arguments: Value,
) -> (bool, String) {
    let url = format!(
        "{}/api/v1/kapellmeister/complete-independent-review",
        endpoint.trim_end_matches('/')
    );
    let client = reqwest::Client::new();
    let mut request = client.post(url).json(&arguments);
    if let (Some(username), Some(password)) = (username, password) {
        request = request.basic_auth(username, Some(password));
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status();
            match response.text().await {
                Ok(body) => {
                    let content = if body.trim().is_empty() {
                        json!({
                            "error": "Fugue host returned an empty response body",
                            "status": status.as_u16(),
                        })
                        .to_string()
                    } else {
                        body
                    };
                    (false, content)
                }
                Err(error) => (
                    false,
                    json!({"error": format!("failed to read Fugue host response: {error}")})
                        .to_string(),
                ),
            }
        }
        Err(error) => (
            false,
            json!({"error": format!("failed to call Fugue host: {error}")}).to_string(),
        ),
    }
}

async fn execute_host_run_deploy(
    endpoint: &str,
    username: Option<&str>,
    password: Option<&str>,
    arguments: Value,
) -> (bool, String) {
    let url = format!(
        "{}/api/v1/kapellmeister/deploy",
        endpoint.trim_end_matches('/')
    );
    let client = reqwest::Client::new();
    let mut request = client.post(url).json(&arguments);
    if let (Some(username), Some(password)) = (username, password) {
        request = request.basic_auth(username, Some(password));
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status();
            match response.text().await {
                Ok(body) => {
                    let content = if body.trim().is_empty() {
                        json!({
                            "error": "Fugue host returned an empty response body",
                            "status": status.as_u16(),
                        })
                        .to_string()
                    } else {
                        body
                    };
                    (false, content)
                }
                Err(error) => (
                    false,
                    json!({"error": format!("failed to read Fugue host response: {error}")})
                        .to_string(),
                ),
            }
        }
        Err(error) => (
            false,
            json!({"error": format!("failed to call Fugue host: {error}")}).to_string(),
        ),
    }
}

#[derive(Debug, Clone, Default)]
struct LinearMcpScope {
    issue: Option<LinearIssueRef>,
}

#[derive(Debug, Clone, Default)]
struct LinearMcpCallMemory {
    last_issue: Option<LinearIssueRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LinearMcpAuthority {
    Workpad,
    Projection,
}

impl LinearMcpAuthority {
    fn from_env() -> Self {
        match env_string("FUGUE_LINEAR_MCP_AUTHORITY")
            .unwrap_or_else(|| "workpad".to_string())
            .as_str()
        {
            "projection" => Self::Projection,
            _ => Self::Workpad,
        }
    }
}

impl LinearMcpScope {
    fn from_env() -> Self {
        Self {
            issue: linear_issue_ref_from_env(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct GitHubMcpScope {
    repo: Option<GitHubRepoRef>,
    head_branch: Option<BranchName>,
    pull_request: Option<GitHubPullRequestRef>,
    allow_merge: bool,
}

#[derive(Debug, Clone, Default)]
struct GitHubMcpCallMemory {
    last_pull_request: Option<GitHubPullRequestRef>,
}

impl GitHubMcpScope {
    fn from_env() -> anyhow::Result<Self> {
        let head_branch = env_string("FUGUE_GITHUB_HEAD_BRANCH").map(BranchName);
        let pull_request = env_string("FUGUE_GITHUB_PR_NUMBER")
            .and_then(|value| value.parse::<u64>().ok())
            .map(|number| GitHubPullRequestRef::Number {
                number: GitHubPullRequestNumber(number),
            })
            .or_else(|| {
                head_branch
                    .clone()
                    .map(|head_branch| GitHubPullRequestRef::Branch { head_branch })
            });
        Ok(Self {
            repo: github_repo_ref_from_env()?,
            head_branch,
            pull_request,
            allow_merge: env_bool("FUGUE_GITHUB_ALLOW_MERGE"),
        })
    }
}

fn apply_linear_mcp_scope(
    config: &TrackerConfig,
    scope: &LinearMcpScope,
    name: &str,
    arguments: Value,
) -> std::result::Result<Value, String> {
    let Ok(operation) = name.parse::<FugueToolOperation>() else {
        return Ok(arguments);
    };
    let mut object = match arguments {
        Value::Object(object) => object,
        other => return Ok(other),
    };
    match operation {
        FugueToolOperation::LinearInspectIssue
        | FugueToolOperation::LinearClaimIssue
        | FugueToolOperation::LinearMoveIssue
        | FugueToolOperation::LinearUpsertWorkpad => {
            insert_default(&mut object, "issue", scope.issue.as_ref());
            normalize_linear_issue_argument(&mut object);
        }
        FugueToolOperation::LinearListRecentIssueChanges => {
            insert_default(&mut object, "issue", scope.issue.as_ref());
            normalize_linear_issue_argument(&mut object);
        }
        _ => {}
    }
    if operation == FugueToolOperation::LinearClaimIssue && !object.contains_key("assignee") {
        object.insert("assignee".to_string(), json!(LinearUserRef::Me));
    }
    if operation == FugueToolOperation::LinearMoveIssue && config.kind != "jira" {
        object.remove("expected_updated_at");
    }
    normalize_linear_state_argument(config, operation, &mut object, "state")?;
    normalize_linear_state_argument(config, operation, &mut object, "target_state")?;
    insert_default_linear_idempotency_key(operation, &mut object);
    Ok(Value::Object(object))
}

fn normalize_linear_issue_argument(object: &mut Map<String, Value>) {
    let Some(issue) = object.get_mut("issue") else {
        return;
    };
    match issue {
        Value::String(identifier) => {
            *issue = json!(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier(identifier.clone())
            });
        }
        Value::Object(issue_object) if !issue_object.contains_key("kind") => {
            if let Some(identifier) = issue_object
                .get("identifier")
                .and_then(Value::as_str)
                .map(ToString::to_string)
            {
                *issue = json!(LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier(identifier)
                });
            } else if let Some(id) = issue_object
                .get("id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
            {
                *issue = json!(LinearIssueRef::Id {
                    id: LinearIssueId(id)
                });
            }
        }
        _ => {}
    }
}

fn insert_default_linear_idempotency_key(
    operation: FugueToolOperation,
    object: &mut Map<String, Value>,
) {
    if object.contains_key("idempotency_key") || !operation.mutates() {
        return;
    }
    let issue = object
        .get("issue")
        .and_then(linear_issue_ref_fingerprint)
        .unwrap_or_else(|| "unscoped".to_string());
    let target = match operation {
        FugueToolOperation::LinearMoveIssue => object
            .get("state")
            .and_then(linear_state_ref_fingerprint)
            .unwrap_or_else(|| "unknown-state".to_string()),
        FugueToolOperation::LinearClaimIssue => object
            .get("target_state")
            .and_then(linear_state_ref_fingerprint)
            .unwrap_or_else(|| "claim".to_string()),
        FugueToolOperation::LinearUpsertWorkpad => object
            .get("heading")
            .and_then(Value::as_str)
            .map(stable_key_part)
            .unwrap_or_else(|| "workpad".to_string()),
        FugueToolOperation::LinearEnsureWorkflowStates => "ensure-states".to_string(),
        _ => return,
    };
    object.insert(
        "idempotency_key".to_string(),
        json!(format!(
            "mcp:{}:{issue}:{target}",
            operation.provider_callback_name()
        )),
    );
}

fn linear_issue_ref_fingerprint(value: &Value) -> Option<String> {
    value
        .get("identifier")
        .and_then(Value::as_str)
        .or_else(|| value.get("id").and_then(Value::as_str))
        .map(stable_key_part)
}

fn linear_state_ref_fingerprint(value: &Value) -> Option<String> {
    value
        .as_str()
        .or_else(|| value.get("name").and_then(Value::as_str))
        .or_else(|| value.get("id").and_then(Value::as_str))
        .map(stable_key_part)
}

fn stable_key_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn apply_linear_mcp_memory(
    memory: &Arc<Mutex<LinearMcpCallMemory>>,
    name: &str,
    arguments: Value,
) -> Value {
    let Ok(operation) = name.parse::<FugueToolOperation>() else {
        return arguments;
    };
    if !matches!(
        operation,
        FugueToolOperation::LinearInspectIssue
            | FugueToolOperation::LinearClaimIssue
            | FugueToolOperation::LinearMoveIssue
            | FugueToolOperation::LinearUpsertWorkpad
    ) {
        return arguments;
    }
    let mut object = match arguments {
        Value::Object(object) => object,
        other => return other,
    };
    if object.contains_key("issue") {
        return Value::Object(object);
    }
    let remembered = memory
        .lock()
        .ok()
        .and_then(|memory| memory.last_issue.clone());
    insert_default(&mut object, "issue", remembered.as_ref());
    Value::Object(object)
}

fn remember_linear_mcp_issue_argument(memory: &Arc<Mutex<LinearMcpCallMemory>>, arguments: &Value) {
    let Some(issue) = arguments
        .get("issue")
        .cloned()
        .and_then(|value| serde_json::from_value::<LinearIssueRef>(value).ok())
    else {
        return;
    };
    if let Ok(mut memory) = memory.lock() {
        memory.last_issue = Some(issue);
    }
}

fn apply_github_mcp_scope(scope: &GitHubMcpScope, name: &str, arguments: Value) -> Value {
    let Ok(operation) = name.parse::<FugueToolOperation>() else {
        return arguments;
    };
    let mut object = match arguments {
        Value::Object(object) => object,
        other => return other,
    };
    match operation {
        FugueToolOperation::GitHubInspectRepo
        | FugueToolOperation::GitHubInspectPr
        | FugueToolOperation::GitHubCreatePr
        | FugueToolOperation::GitHubCheckCi
        | FugueToolOperation::GitHubMergePr => {
            insert_default(&mut object, "repo", scope.repo.as_ref());
            if let Some(host) = scope.repo.as_ref().and_then(|repo| repo.host.as_ref())
                && let Some(repo) = object.get_mut("repo").and_then(Value::as_object_mut)
            {
                repo.entry("host".to_string())
                    .or_insert_with(|| json!(host));
            }
        }
        _ => {}
    }
    match operation {
        FugueToolOperation::GitHubInspectPr | FugueToolOperation::GitHubMergePr => {
            insert_default(&mut object, "pull_request", scope.pull_request.as_ref());
        }
        FugueToolOperation::GitHubCheckCi => {
            if let Some(head_branch) = &scope.head_branch {
                let should_use_scoped_branch = object
                    .get("git_ref")
                    .and_then(Value::as_str)
                    .is_none_or(is_full_git_sha);
                if should_use_scoped_branch {
                    object.insert("git_ref".to_string(), json!(GitRef(head_branch.0.clone())));
                }
            }
        }
        FugueToolOperation::GitHubCreatePr => {
            insert_default(&mut object, "head_branch", scope.head_branch.as_ref());
            if !object.contains_key("base_branch") {
                object.insert(
                    "base_branch".to_string(),
                    json!(BranchName("main".to_string())),
                );
            }
            if !object.contains_key("draft") {
                object.insert("draft".to_string(), Value::Bool(false));
            }
        }
        _ => {}
    }
    Value::Object(object)
}

fn apply_github_mcp_memory(
    memory: &Arc<Mutex<GitHubMcpCallMemory>>,
    name: &str,
    arguments: Value,
) -> Value {
    let Ok(operation) = name.parse::<FugueToolOperation>() else {
        return arguments;
    };
    if !matches!(
        operation,
        FugueToolOperation::GitHubInspectPr | FugueToolOperation::GitHubMergePr
    ) {
        return arguments;
    }
    let mut object = match arguments {
        Value::Object(object) => object,
        other => return other,
    };
    if object.contains_key("pull_request") {
        return Value::Object(object);
    }
    let remembered = memory
        .lock()
        .ok()
        .and_then(|memory| memory.last_pull_request.clone());
    insert_default(&mut object, "pull_request", remembered.as_ref());
    Value::Object(object)
}

fn remember_github_mcp_call(memory: &Arc<Mutex<GitHubMcpCallMemory>>, name: &str, content: &str) {
    let Ok(operation) = name.parse::<FugueToolOperation>() else {
        return;
    };
    if !matches!(
        operation,
        FugueToolOperation::GitHubInspectPr
            | FugueToolOperation::GitHubCreatePr
            | FugueToolOperation::GitHubMergePr
    ) {
        return;
    }
    let Ok(value) = serde_json::from_str::<Value>(content) else {
        return;
    };
    let Some(number) = value
        .pointer("/output/output/pull_request/number")
        .and_then(Value::as_u64)
    else {
        return;
    };
    if let Ok(mut memory) = memory.lock() {
        memory.last_pull_request = Some(GitHubPullRequestRef::Number {
            number: GitHubPullRequestNumber(number),
        });
    }
}

fn github_mcp_scope_boundary_response(
    scope: &GitHubMcpScope,
    name: &str,
    arguments: &Value,
) -> Option<Value> {
    let Ok(operation) = name.parse::<FugueToolOperation>() else {
        return None;
    };
    if operation != FugueToolOperation::GitHubInspectPr {
        return None;
    }
    if arguments
        .as_object()
        .is_some_and(|object| object.contains_key("pull_request"))
    {
        return None;
    }
    Some(json!({
        "status": "unavailable",
        "operation": FugueToolOperation::GitHubInspectPr.provider_callback_name(),
        "reason": "github_inspect_pr requires a pull_request argument or a scoped FUGUE_GITHUB_PR_NUMBER/FUGUE_GITHUB_HEAD_BRANCH environment value",
        "repo_scoped": scope.repo.is_some(),
        "head_branch_scoped": scope.head_branch.is_some(),
        "pull_request_scoped": scope.pull_request.is_some()
    }))
}

fn is_full_git_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn insert_default<T: serde::Serialize>(
    object: &mut Map<String, Value>,
    key: &str,
    value: Option<&T>,
) {
    if object.contains_key(key) {
        return;
    }
    if let Some(value) = value
        && let Ok(value) = serde_json::to_value(value)
    {
        object.insert(key.to_string(), value);
    }
}

fn normalize_linear_state_argument(
    config: &TrackerConfig,
    operation: FugueToolOperation,
    object: &mut Map<String, Value>,
    key: &str,
) -> std::result::Result<(), String> {
    let Some(value) = object.get(key) else {
        return Ok(());
    };
    let Some(name) = linear_state_name(value) else {
        return Ok(());
    };
    let name = name.trim();
    if name.is_empty() {
        return Ok(());
    }
    let configured_lanes = configured_linear_lanes(config);
    if !configured_lanes.is_empty() && !configured_linear_lanes_contain(&configured_lanes, name) {
        return Err(format!(
            "{} rejected unconfigured Linear state name {name:?} for {key}; allowed lanes are: {}",
            operation.provider_callback_name(),
            configured_lanes.join(", ")
        ));
    }
    object.insert(
        key.to_string(),
        json!(crate::tools::LinearStateRef::Name {
            name: name.to_string(),
        }),
    );
    Ok(())
}

fn linear_state_name(value: &Value) -> Option<&str> {
    match value {
        Value::String(name) => Some(name),
        Value::Object(object)
            if object
                .get("kind")
                .or_else(|| object.get("type"))
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == "name") =>
        {
            object.get("name").and_then(Value::as_str)
        }
        _ => None,
    }
}

fn configured_linear_lanes_contain(lanes: &[String], name: &str) -> bool {
    let normalized = crate::domain::normalize_state(name);
    lanes
        .iter()
        .any(|candidate| crate::domain::normalize_state(candidate) == normalized)
}

fn configured_linear_lanes(config: &TrackerConfig) -> Vec<String> {
    let mut lanes: Vec<String> = Vec::new();
    for lane in config
        .intake_states
        .iter()
        .chain(config.in_flight_states.iter())
        .chain(config.active_states.iter())
        .chain(config.terminal_states.iter())
    {
        if lane.trim().is_empty() {
            continue;
        }
        let normalized = crate::domain::normalize_state(lane);
        if !lanes
            .iter()
            .any(|candidate| crate::domain::normalize_state(candidate.as_str()) == normalized)
        {
            lanes.push(lane.clone());
        }
    }
    lanes
}

fn linear_issue_ref_from_env() -> Option<LinearIssueRef> {
    env_string("FUGUE_LINEAR_ISSUE_ID")
        .map(|id| LinearIssueRef::Id {
            id: LinearIssueId(id),
        })
        .or_else(|| {
            env_string("FUGUE_LINEAR_ISSUE_IDENTIFIER").map(|identifier| {
                LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier(identifier),
                }
            })
        })
}

fn github_repo_ref_from_env() -> anyhow::Result<Option<GitHubRepoRef>> {
    let host = env_string("FUGUE_GITHUB_HOST")
        .or_else(|| env_string("GH_HOST"))
        .map(crate::tools::GitHubHost::try_from)
        .transpose()
        .map_err(anyhow::Error::msg)?;
    let (Some(owner), Some(name)) = (
        env_string("FUGUE_GITHUB_OWNER"),
        env_string("FUGUE_GITHUB_REPO"),
    ) else {
        return Ok(None);
    };
    Ok(Some(GitHubRepoRef {
        host,
        owner: GitHubOwner(owner),
        name: GitHubRepoName(name),
    }))
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(key: &str) -> bool {
    std::env::var(key).ok().is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn linear_tools(config: &TrackerConfig, authority: LinearMcpAuthority) -> Vec<Value> {
    external_tools_for_tracker(config)
        .unwrap_or_default()
        .into_iter()
        .filter(|tool| {
            tool.get("name")
                .and_then(Value::as_str)
                .and_then(linear_fugue_operation)
                .is_some_and(|operation| linear_mcp_authority_allows(authority, operation))
        })
        .map(relax_linear_mcp_idempotency_schema)
        .collect()
}

fn relax_linear_mcp_idempotency_schema(mut tool: Value) -> Value {
    let Some(name) = tool.get("name").and_then(Value::as_str) else {
        return tool;
    };
    let Ok(operation) = name.parse::<FugueToolOperation>() else {
        return tool;
    };
    if !operation.mutates() {
        return tool;
    }
    let Some(required) = tool
        .pointer_mut("/input_schema/required")
        .and_then(Value::as_array_mut)
    else {
        return tool;
    };
    required.retain(|field| field.as_str() != Some("idempotency_key"));
    tool
}

fn linear_fugue_operation(name: &str) -> Option<FugueToolOperation> {
    name.parse::<FugueToolOperation>().ok().filter(|operation| {
        matches!(
            operation,
            FugueToolOperation::LinearInspectIssue
                | FugueToolOperation::LinearClaimIssue
                | FugueToolOperation::LinearMoveIssue
                | FugueToolOperation::LinearUpsertWorkpad
                | FugueToolOperation::LinearListProjectStates
                | FugueToolOperation::LinearEnsureWorkflowStates
                | FugueToolOperation::LinearListRecentIssueChanges
        )
    })
}

fn linear_mcp_authority_allows(
    authority: LinearMcpAuthority,
    operation: FugueToolOperation,
) -> bool {
    match authority {
        LinearMcpAuthority::Workpad => matches!(
            operation,
            FugueToolOperation::LinearInspectIssue
                | FugueToolOperation::LinearUpsertWorkpad
                | FugueToolOperation::LinearListRecentIssueChanges
        ),
        LinearMcpAuthority::Projection => matches!(
            operation,
            FugueToolOperation::LinearInspectIssue
                | FugueToolOperation::LinearClaimIssue
                | FugueToolOperation::LinearMoveIssue
                | FugueToolOperation::LinearUpsertWorkpad
                | FugueToolOperation::LinearListProjectStates
                | FugueToolOperation::LinearEnsureWorkflowStates
                | FugueToolOperation::LinearListRecentIssueChanges
        ),
    }
}

fn github_tools(allow_merge: bool) -> Vec<Value> {
    let config = TrackerConfig {
        jira: None,
        kind: "linear".to_string(),
        endpoint: "https://api.linear.app/graphql".to_string(),
        api_key: Some("tool-catalog-only".to_string()),
        project_slug: Some("tool-catalog-only".to_string()),
        intake_states: Vec::new(),
        in_flight_states: Vec::new(),
        active_states: Vec::new(),
        attachment_root: None,
        max_attachment_files: 20,
        max_attachment_bytes: 25 * 1024 * 1024,
        max_attachment_total_bytes: 100 * 1024 * 1024,
        terminal_states: Vec::new(),
    };
    external_tools_for_tracker(&config)
        .unwrap_or_default()
        .into_iter()
        .filter(|tool| {
            tool.get("name")
                .and_then(Value::as_str)
                .and_then(|name| name.parse::<FugueToolOperation>().ok())
                .is_some_and(|operation| {
                    matches!(
                        operation,
                        FugueToolOperation::GitHubInspectRepo
                            | FugueToolOperation::GitHubInspectPr
                            | FugueToolOperation::GitHubCreatePr
                            | FugueToolOperation::GitHubCheckCi
                    ) || (allow_merge && operation == FugueToolOperation::GitHubMergePr)
                })
        })
        .collect()
}

async fn run_stdio_mcp<F, Fut>(
    server_name: &'static str,
    tools: Vec<Value>,
    handler: F,
) -> anyhow::Result<()>
where
    F: Fn(String, Value) -> Fut,
    Fut: std::future::Future<Output = (bool, String)>,
{
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut reader = BufReader::new(stdin).lines();

    while let Some(line) = reader.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(err) => {
                write_response(
                    &mut stdout,
                    json!({
                        "jsonrpc": "2.0",
                        "id": null,
                        "error": {"code": -32700, "message": format!("Parse error: {err}")}
                    }),
                )
                .await?;
                continue;
            }
        };
        if request.get("id").is_none() {
            continue;
        }
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let response = match method {
            "initialize" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": { "tools": {} },
                    "serverInfo": {"name": server_name, "version": env!("CARGO_PKG_VERSION")}
                }
            }),
            "ping" => json!({"jsonrpc": "2.0", "id": id, "result": {}}),
            "tools/list" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "tools": protocol_tool_descriptors(&tools) }
            }),
            "tools/call" => {
                let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
                let name = params
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let (is_error, content) = handler(name, arguments).await;
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "content": [{"type": "text", "text": content}],
                        "isError": is_error
                    }
                })
            }
            _ => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": format!("Method not found: {method}")}
            }),
        };
        write_response(&mut stdout, response).await?;
    }

    Ok(())
}

async fn write_response(stdout: &mut io::Stdout, response: Value) -> anyhow::Result<()> {
    stdout.write_all(format!("{response}\n").as_bytes()).await?;
    stdout.flush().await?;
    Ok(())
}

fn protocol_tool_descriptors(tools: &[Value]) -> Vec<Value> {
    tools.iter().map(protocol_tool_descriptor).collect()
}

fn protocol_tool_descriptor(tool: &Value) -> Value {
    let mut descriptor = tool.clone();
    if let Some(object) = descriptor.as_object_mut()
        && !object.contains_key("inputSchema")
        && let Some(input_schema) = object.remove("input_schema")
    {
        object.insert("inputSchema".to_string(), input_schema);
    }
    descriptor
}

#[cfg(test)]
mod tests {
    #[test]
    fn enterprise_scope_supplies_missing_host_and_preserves_explicit_host() {
        let scope = GitHubMcpScope {
            repo: Some(
                serde_json::from_value(
                    json!({"host":"git.example.com","owner":"example","name":"app"}),
                )
                .expect("repo"),
            ),
            ..Default::default()
        };
        let inherited = apply_github_mcp_scope(
            &scope,
            "github_inspect_repo",
            json!({"repo":{"owner":"example","name":"app"}}),
        );
        assert_eq!(inherited["repo"]["host"], "git.example.com");
        let explicit = apply_github_mcp_scope(
            &scope,
            "github_inspect_repo",
            json!({"repo":{"host":"other.example.com","owner":"example","name":"app"}}),
        );
        assert_eq!(explicit["repo"]["host"], "other.example.com");
    }

    #[test]
    fn jira_moves_retain_optimistic_concurrency_guard() {
        let mut config = compact_tracker_config();
        config.kind = "jira".to_string();
        let args = apply_linear_mcp_scope(
            &config,
            &LinearMcpScope::default(),
            "linear_move_issue",
            json!({"issue":"TEST-1","state":"Planning","expected_updated_at":"2026-01-01T00:00:00Z"}),
        )
        .expect("scope");
        assert_eq!(args["expected_updated_at"], "2026-01-01T00:00:00Z");
    }
    use super::*;

    fn compact_tracker_config() -> TrackerConfig {
        TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("test-validation-key".to_string()),
            project_slug: Some("meerkat-fugue-queue".to_string()),
            intake_states: vec!["Todo".to_string()],
            in_flight_states: vec![
                "Planning".to_string(),
                "Implementing".to_string(),
                "Reviewing".to_string(),
            ],
            active_states: vec![
                "Todo".to_string(),
                "Planning".to_string(),
                "Implementing".to_string(),
                "Reviewing".to_string(),
            ],
            attachment_root: None,
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: vec![
                "Done".to_string(),
                "Canceled".to_string(),
                "Cancelled".to_string(),
                "Duplicate".to_string(),
            ],
        }
    }

    fn assert_protocol_compatible_tool_descriptors(tools: &[Value]) {
        assert!(!tools.is_empty(), "MCP catalog must not be empty");
        for tool in tools {
            let object = tool.as_object().expect("tool descriptor is object");
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .expect("tool descriptor has a string name");
            assert!(
                object.get("inputSchema").is_some_and(Value::is_object),
                "tool {name} must expose MCP protocol field inputSchema"
            );
            assert!(
                !object.contains_key("input_schema"),
                "tool {name} must not expose provider-style input_schema in tools/list"
            );
        }
    }

    #[test]
    fn linear_mcp_scope_supplies_issue_and_default_assignee() {
        let scope = LinearMcpScope {
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-362".to_string()),
            }),
        };

        let config = compact_tracker_config();
        let inspected = apply_linear_mcp_scope(&config, &scope, "linear_inspect_issue", json!({}))
            .expect("linear inspect scope is valid");
        assert_eq!(
            inspected
                .pointer("/issue/identifier")
                .and_then(Value::as_str),
            Some("LUC-362")
        );

        let claimed = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_claim_issue",
            json!({"idempotency_key": "claim-smoke"}),
        )
        .expect("linear claim scope is valid");
        assert_eq!(
            claimed.pointer("/issue/identifier").and_then(Value::as_str),
            Some("LUC-362")
        );
        assert_eq!(
            claimed.pointer("/assignee/kind").and_then(Value::as_str),
            Some("me")
        );
    }

    #[test]
    fn linear_mcp_scope_accepts_state_name_shorthand() {
        let scope = LinearMcpScope {
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-375".to_string()),
            }),
        };

        let config = compact_tracker_config();
        let claimed = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_claim_issue",
            json!({
                "target_state": "Implementing",
                "idempotency_key": "claim-coder"
            }),
        )
        .expect("configured target state shorthand is valid");
        assert_eq!(
            claimed
                .pointer("/target_state/name")
                .and_then(Value::as_str),
            Some("Implementing")
        );

        let moved = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_move_issue",
            json!({
                "state": "Reviewing",
                "idempotency_key": "move-reviewing"
            }),
        )
        .expect("configured state shorthand is valid");
        assert_eq!(
            moved.pointer("/state/name").and_then(Value::as_str),
            Some("Reviewing")
        );
    }

    #[test]
    fn linear_mcp_scope_normalizes_issue_identifier_shorthand() {
        let scope = LinearMcpScope::default();
        let config = compact_tracker_config();

        let inspected = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_inspect_issue",
            json!({"issue": {"identifier": "LUC-631"}}),
        )
        .expect("issue identifier shorthand is valid");

        assert_eq!(
            inspected.pointer("/issue/kind").and_then(Value::as_str),
            Some("identifier")
        );
        assert_eq!(
            inspected
                .pointer("/issue/identifier")
                .and_then(Value::as_str),
            Some("LUC-631")
        );
    }

    #[test]
    fn linear_mcp_scope_normalizes_string_issue_shorthand() {
        let scope = LinearMcpScope::default();
        let config = compact_tracker_config();

        let inspected = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_inspect_issue",
            json!({"issue": "LUC-631"}),
        )
        .expect("string issue shorthand is valid");

        assert_eq!(
            inspected.pointer("/issue/kind").and_then(Value::as_str),
            Some("identifier")
        );
        assert_eq!(
            inspected
                .pointer("/issue/identifier")
                .and_then(Value::as_str),
            Some("LUC-631")
        );
    }

    #[test]
    fn linear_mcp_scope_removes_stale_expected_updated_at_from_moves() {
        let scope = LinearMcpScope {
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-550".to_string()),
            }),
        };

        let config = compact_tracker_config();
        let moved = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_move_issue",
            json!({
                "state": "Planning",
                "expected_updated_at": "2026-05-14T22:00:56.445Z",
                "idempotency_key": "move-planning"
            }),
        )
        .expect("configured state shorthand is valid");

        assert_eq!(
            moved.pointer("/state/name").and_then(Value::as_str),
            Some("Planning")
        );
        assert!(
            moved.get("expected_updated_at").is_none(),
            "scoped Linear MCP moves are issue-scoped and idempotent; stale model-supplied optimistic timestamps must not block lifecycle projection"
        );
    }

    #[test]
    fn linear_mcp_scope_rejects_unconfigured_state_name_shorthand() {
        let scope = LinearMcpScope {
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-376".to_string()),
            }),
        };
        let config = compact_tracker_config();

        let err = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_claim_issue",
            json!({
                "target_state": "In Progress",
                "idempotency_key": "claim-unconfigured-lane"
            }),
        )
        .expect_err("unconfigured lanes must not be accepted unless configured");
        assert!(err.contains("unconfigured Linear state name"));
        assert!(err.contains("Implementing"));
        assert!(!err.contains("In Review"));

        let err = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_move_issue",
            json!({
                "state": { "kind": "name", "name": "Human Review" },
                "idempotency_key": "move-unconfigured-lane"
            }),
        )
        .expect_err("typed unconfigured lane names must not bypass the guard");
        assert!(err.contains("Human Review"));
    }

    #[test]
    fn linear_mcp_scope_accepts_state_name_when_standalone_config_has_no_lanes() {
        let scope = LinearMcpScope {
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-541".to_string()),
            }),
        };
        let config = TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: Vec::new(),
            in_flight_states: Vec::new(),
            active_states: Vec::new(),
            attachment_root: None,
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: Vec::new(),
        };

        let moved = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_move_issue",
            json!({
                "state": "Planning",
                "idempotency_key": "move-planning"
            }),
        )
        .expect("standalone MCP servers may receive only env-scoped Linear config");

        assert_eq!(
            moved.pointer("/issue/identifier").and_then(Value::as_str),
            Some("LUC-541")
        );
        assert_eq!(
            moved.pointer("/state/name").and_then(Value::as_str),
            Some("Planning")
        );
    }

    #[test]
    fn linear_mcp_scope_accepts_configured_handoff_state_name_shorthand() {
        let scope = LinearMcpScope {
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-377".to_string()),
            }),
        };
        let mut config = compact_tracker_config();
        config.terminal_states.push("Human Review".to_string());

        let moved = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_move_issue",
            json!({
                "state": { "kind": "name", "name": "Human Review" },
                "idempotency_key": "move-configured-human-review"
            }),
        )
        .expect("configured handoff lane is valid");
        assert_eq!(
            moved.pointer("/state/name").and_then(Value::as_str),
            Some("Human Review")
        );
    }

    #[test]
    fn github_mcp_scope_supplies_repo_branch_and_pr_defaults() {
        let scope = GitHubMcpScope {
            repo: Some(GitHubRepoRef {
                host: None,
                owner: GitHubOwner("lukacf".to_string()),
                name: GitHubRepoName("meerkat-fugue".to_string()),
            }),
            head_branch: Some(BranchName("luka/luc-362-docs".to_string())),
            pull_request: Some(GitHubPullRequestRef::Branch {
                head_branch: BranchName("luka/luc-362-docs".to_string()),
            }),
            allow_merge: false,
        };

        let ci = apply_github_mcp_scope(&scope, "github_check_ci", json!({}));
        assert_eq!(
            ci.pointer("/repo/owner").and_then(Value::as_str),
            Some("lukacf")
        );
        assert_eq!(
            ci.pointer("/repo/name").and_then(Value::as_str),
            Some("meerkat-fugue")
        );
        assert_eq!(
            ci.pointer("/git_ref").and_then(Value::as_str),
            Some("luka/luc-362-docs")
        );

        let pr = apply_github_mcp_scope(&scope, "github_inspect_pr", json!({}));
        assert_eq!(
            pr.pointer("/pull_request/head_branch")
                .and_then(Value::as_str),
            Some("luka/luc-362-docs")
        );
    }

    #[test]
    fn github_mcp_unscoped_pr_inspection_is_non_failing_boundary_response() {
        let scope = GitHubMcpScope {
            repo: Some(GitHubRepoRef {
                host: None,
                owner: GitHubOwner("lukacf".to_string()),
                name: GitHubRepoName("meerkat-fugue".to_string()),
            }),
            head_branch: None,
            pull_request: None,
            allow_merge: false,
        };
        let arguments = apply_github_mcp_scope(&scope, "github_inspect_pr", json!({}));

        let response = github_mcp_scope_boundary_response(&scope, "github_inspect_pr", &arguments)
            .expect("unscoped github_inspect_pr should produce a boundary response");

        assert_eq!(
            response.get("status").and_then(Value::as_str),
            Some("unavailable")
        );
        assert!(
            response
                .get("reason")
                .and_then(Value::as_str)
                .is_some_and(|reason| reason.contains("requires a pull_request argument"))
        );
    }

    #[test]
    fn github_mcp_call_memory_defaults_merge_to_recent_pr() {
        let memory = Arc::new(Mutex::new(GitHubMcpCallMemory::default()));
        remember_github_mcp_call(
            &memory,
            "github_create_pr",
            &json!({
                "status": "ok",
                "output": {
                    "operation": "github.create_pr",
                    "output": {
                        "pull_request": {
                            "number": 171
                        },
                        "created": false
                    }
                }
            })
            .to_string(),
        );

        let arguments = apply_github_mcp_memory(
            &memory,
            "github_merge_pr",
            json!({
                "method": "squash",
                "expected_head_sha": "24d2697f07c1c91d091281a843547953a6209069",
                "idempotency_key": "merge:LUC-519"
            }),
        );

        assert_eq!(
            arguments
                .pointer("/pull_request/number")
                .and_then(Value::as_u64),
            Some(171)
        );
    }

    #[test]
    fn github_mcp_scope_rewrites_sha_ci_ref_to_scoped_branch() {
        let scope = GitHubMcpScope {
            repo: Some(GitHubRepoRef {
                host: None,
                owner: GitHubOwner("lukacf".to_string()),
                name: GitHubRepoName("meerkat-fugue".to_string()),
            }),
            head_branch: Some(BranchName("luka/luc-397-docs".to_string())),
            pull_request: None,
            allow_merge: false,
        };

        let ci = apply_github_mcp_scope(
            &scope,
            "github_check_ci",
            json!({
                "git_ref": "2a52dde672dbcd500989ca3082e23eea1a88ff4d"
            }),
        );

        assert_eq!(
            ci.pointer("/git_ref").and_then(Value::as_str),
            Some("luka/luc-397-docs")
        );
    }

    #[test]
    fn github_mcp_scope_preserves_non_sha_ci_ref() {
        let scope = GitHubMcpScope {
            repo: Some(GitHubRepoRef {
                host: None,
                owner: GitHubOwner("lukacf".to_string()),
                name: GitHubRepoName("meerkat-fugue".to_string()),
            }),
            head_branch: Some(BranchName("luka/luc-397-docs".to_string())),
            pull_request: None,
            allow_merge: false,
        };

        let ci = apply_github_mcp_scope(
            &scope,
            "github_check_ci",
            json!({
                "git_ref": "release-candidate"
            }),
        );

        assert_eq!(
            ci.pointer("/git_ref").and_then(Value::as_str),
            Some("release-candidate")
        );
    }

    #[test]
    fn linear_workpad_mcp_catalog_excludes_projection_mutations() {
        let config = TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: Vec::new(),
            in_flight_states: Vec::new(),
            active_states: Vec::new(),
            attachment_root: None,
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: Vec::new(),
        };
        let names = linear_tools(&config, LinearMcpAuthority::Workpad)
            .into_iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
            .collect::<Vec<_>>();

        assert!(
            names.contains(
                &FugueToolOperation::LinearUpsertWorkpad
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            names.contains(
                &FugueToolOperation::LinearInspectIssue
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            !names.contains(
                &FugueToolOperation::LinearMoveIssue
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            !names.contains(
                &FugueToolOperation::LinearClaimIssue
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            !names.contains(
                &FugueToolOperation::LinearEnsureWorkflowStates
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(!names.contains(&"linear_graphql".to_string()));
        assert!(
            !names.contains(
                &FugueToolOperation::LinearAddComment
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            !names.contains(
                &FugueToolOperation::GitHubCreatePr
                    .provider_callback_name()
                    .to_string()
            )
        );
    }

    #[test]
    fn linear_mcp_call_memory_defaults_workpad_to_recent_issue() {
        let memory = Arc::new(Mutex::new(LinearMcpCallMemory::default()));
        remember_linear_mcp_issue_argument(
            &memory,
            &json!({
                "issue": {
                    "kind": "identifier",
                    "identifier": "LUC-519"
                }
            }),
        );

        let arguments = apply_linear_mcp_memory(
            &memory,
            "linear_upsert_workpad",
            json!({
                "heading": "## Fugue Workpad",
                "body_markdown": "status",
                "create_if_missing": true,
                "idempotency_key": "status:LUC-519"
            }),
        );

        assert_eq!(
            arguments
                .pointer("/issue/identifier")
                .and_then(Value::as_str),
            Some("LUC-519")
        );
    }

    #[test]
    fn linear_mcp_scope_synthesizes_idempotency_for_projection_moves() {
        let config = TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: Vec::new(),
            in_flight_states: vec!["Planning".to_string()],
            active_states: Vec::new(),
            attachment_root: None,
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: Vec::new(),
        };
        let scope = LinearMcpScope {
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-535".to_string()),
            }),
        };

        let arguments = apply_linear_mcp_scope(
            &config,
            &scope,
            "linear_move_issue",
            json!({ "state": "Planning" }),
        )
        .unwrap();

        assert_eq!(
            arguments
                .pointer("/issue/identifier")
                .and_then(Value::as_str),
            Some("LUC-535")
        );
        assert_eq!(
            arguments.pointer("/state/name").and_then(Value::as_str),
            Some("Planning")
        );
        assert_eq!(
            arguments.get("idempotency_key").and_then(Value::as_str),
            Some("mcp:linear_move_issue:luc-535:planning")
        );
    }

    #[test]
    fn linear_mcp_catalog_makes_idempotency_key_optional_for_agents() {
        let config = TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: Vec::new(),
            in_flight_states: Vec::new(),
            active_states: Vec::new(),
            attachment_root: None,
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: Vec::new(),
        };
        let tools = linear_tools(&config, LinearMcpAuthority::Projection);
        let move_tool = tools
            .iter()
            .find(|tool| tool.get("name").and_then(Value::as_str) == Some("linear_move_issue"))
            .unwrap();

        assert!(
            !move_tool
                .pointer("/input_schema/required")
                .and_then(Value::as_array)
                .unwrap()
                .iter()
                .any(|field| field.as_str() == Some("idempotency_key"))
        );
    }

    #[test]
    fn linear_projection_mcp_catalog_exposes_projection_mutations() {
        let config = TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: Vec::new(),
            in_flight_states: Vec::new(),
            active_states: Vec::new(),
            attachment_root: None,
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: Vec::new(),
        };
        let names = linear_tools(&config, LinearMcpAuthority::Projection)
            .into_iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
            .collect::<Vec<_>>();

        assert!(
            names.contains(
                &FugueToolOperation::LinearMoveIssue
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            names.contains(
                &FugueToolOperation::LinearClaimIssue
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            names.contains(
                &FugueToolOperation::LinearEnsureWorkflowStates
                    .provider_callback_name()
                    .to_string()
            )
        );
    }

    #[test]
    fn linear_mcp_tools_list_descriptors_use_protocol_input_schema() {
        let config = TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: Vec::new(),
            in_flight_states: Vec::new(),
            active_states: Vec::new(),
            attachment_root: None,
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: Vec::new(),
        };

        let descriptors =
            protocol_tool_descriptors(&linear_tools(&config, LinearMcpAuthority::Workpad));

        assert_protocol_compatible_tool_descriptors(&descriptors);
        assert!(descriptors.iter().any(|tool| {
            tool.get("name").and_then(Value::as_str)
                == Some(FugueToolOperation::LinearUpsertWorkpad.provider_callback_name())
        }));
        assert!(
            descriptors
                .iter()
                .all(|tool| { tool.get("name").and_then(Value::as_str) != Some("linear_graphql") })
        );
    }

    #[test]
    fn github_mcp_tool_catalog_is_github_only() {
        let names = github_tools(false)
            .into_iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
            .collect::<Vec<_>>();

        assert!(
            names.contains(
                &FugueToolOperation::GitHubInspectPr
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            names.contains(
                &FugueToolOperation::GitHubCreatePr
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            !names.contains(
                &FugueToolOperation::LinearUpsertWorkpad
                    .provider_callback_name()
                    .to_string()
            )
        );
        assert!(
            !names.contains(
                &FugueToolOperation::GitHubMergePr
                    .provider_callback_name()
                    .to_string()
            )
        );
    }

    #[test]
    fn github_mcp_tool_catalog_exposes_merge_only_when_configured() {
        let names = github_tools(true)
            .into_iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
            .collect::<Vec<_>>();

        assert!(
            names.contains(
                &FugueToolOperation::GitHubMergePr
                    .provider_callback_name()
                    .to_string()
            )
        );
    }

    #[test]
    fn github_mcp_tools_list_descriptors_use_protocol_input_schema() {
        let descriptors = protocol_tool_descriptors(&github_tools(false));

        assert_protocol_compatible_tool_descriptors(&descriptors);
        assert!(descriptors.iter().any(|tool| {
            tool.get("name").and_then(Value::as_str)
                == Some(FugueToolOperation::GitHubCreatePr.provider_callback_name())
        }));
    }
}
