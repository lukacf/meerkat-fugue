use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use reqwest::header::{
    AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, LOCATION,
};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use crate::config::TrackerConfig;
use crate::domain::{BlockerRef, Issue, IssueAttachment};
use crate::error::{FugueError, Result};

const LINEAR_ISSUE_STATES_PAGE_SIZE: usize = 50;

#[async_trait]
pub trait IssueTracker: Send + Sync {
    async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>>;
    async fn fetch_issues_by_states(&self, states: &[String]) -> Result<Vec<Issue>>;
    async fn fetch_issue_states_by_ids(&self, issue_ids: &[String]) -> Result<Vec<Issue>>;
    async fn graphql(&self, query: &str, variables: Value) -> Result<Value>;
}

#[derive(Debug, Clone)]
pub struct LinearClient {
    endpoint: String,
    project_slug: String,
    active_states: Vec<String>,
    client: reqwest::Client,
    download_client: reqwest::Client,
    attachment_limits: AttachmentDownloadLimits,
    attachment_root: Option<PathBuf>,
}

pub struct LinearDownloadedFile {
    pub content_type: Option<String>,
    pub byte_len: u64,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttachmentDownloadLimits {
    pub max_files: usize,
    pub max_file_bytes: u64,
    pub max_total_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct AttachmentDownloadBudget {
    limits: AttachmentDownloadLimits,
    downloaded_files: usize,
    downloaded_bytes: u64,
}

impl AttachmentDownloadLimits {
    pub fn from_config(config: &TrackerConfig) -> Self {
        Self {
            max_files: config.max_attachment_files,
            max_file_bytes: config.max_attachment_bytes,
            max_total_bytes: config.max_attachment_total_bytes,
        }
    }

    pub fn budget(self) -> AttachmentDownloadBudget {
        AttachmentDownloadBudget {
            limits: self,
            downloaded_files: 0,
            downloaded_bytes: 0,
        }
    }
}

impl AttachmentDownloadBudget {
    fn remaining_total_bytes(&self) -> u64 {
        self.limits
            .max_total_bytes
            .saturating_sub(self.downloaded_bytes)
    }

    pub(crate) fn reserve_file(&self, url: &str) -> Result<()> {
        if self.downloaded_files >= self.limits.max_files {
            return Err(FugueError::ResponseError(format!(
                "attachment download blocked for {url}: max attachment count {} exceeded",
                self.limits.max_files
            )));
        }
        Ok(())
    }

    pub(crate) fn check_content_length(
        &self,
        url: &str,
        content_length: Option<u64>,
    ) -> Result<()> {
        if let Some(content_length) = content_length {
            if content_length > self.limits.max_file_bytes {
                return Err(FugueError::ResponseError(format!(
                    "attachment download blocked for {url}: Content-Length {content_length} exceeds per-file limit {}",
                    self.limits.max_file_bytes
                )));
            }
            let remaining = self.remaining_total_bytes();
            if content_length > remaining {
                return Err(FugueError::ResponseError(format!(
                    "attachment download blocked for {url}: Content-Length {content_length} exceeds remaining aggregate attachment budget {remaining}"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn check_streamed_len(&self, url: &str, byte_len: u64) -> Result<()> {
        if byte_len > self.limits.max_file_bytes {
            return Err(FugueError::ResponseError(format!(
                "attachment download blocked for {url}: streamed bytes {byte_len} exceed per-file limit {}",
                self.limits.max_file_bytes
            )));
        }
        let remaining = self.remaining_total_bytes();
        if byte_len > remaining {
            return Err(FugueError::ResponseError(format!(
                "attachment download blocked for {url}: streamed bytes {byte_len} exceed remaining aggregate attachment budget {remaining}"
            )));
        }
        Ok(())
    }

    pub(crate) fn record_success(&mut self, byte_len: u64) {
        self.downloaded_files += 1;
        self.downloaded_bytes = self.downloaded_bytes.saturating_add(byte_len);
    }
}

struct LinearIssueCreateContext {
    project_id: String,
    team_id: String,
    state_id: String,
}

impl LinearClient {
    pub fn from_config(config: &TrackerConfig) -> Result<Self> {
        let api_key = config
            .api_key
            .clone()
            .ok_or(FugueError::MissingTrackerApiKey)?;
        let project_slug = config
            .project_slug
            .clone()
            .ok_or(FugueError::MissingTrackerProjectSlug)?;
        let mut headers = HeaderMap::new();
        let auth = HeaderValue::from_str(&api_key).map_err(|err| {
            FugueError::InvalidConfig(format!("tracker.api_key cannot be used as a header: {err}"))
        })?;
        headers.insert(AUTHORIZATION, auth);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(10_000))
            .timeout(Duration::from_millis(30_000))
            .default_headers(headers)
            .build()
            .map_err(|err| FugueError::LinearApiRequest(err.to_string()))?;
        let download_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_millis(30_000))
            .build()
            .map_err(|err| FugueError::LinearApiRequest(err.to_string()))?;
        Ok(Self {
            endpoint: config.endpoint.clone(),
            project_slug,
            active_states: config.active_states.clone(),
            client,
            download_client,
            attachment_limits: AttachmentDownloadLimits::from_config(config),
            attachment_root: config.attachment_root.clone(),
        })
    }

    pub fn candidate_query() -> &'static str {
        CANDIDATE_ISSUES_QUERY
    }

    pub fn states_query() -> &'static str {
        ISSUE_STATES_QUERY
    }

    pub(crate) fn attachment_root(&self) -> Option<&Path> {
        self.attachment_root.as_deref()
    }

    pub fn attachment_download_limits(&self) -> AttachmentDownloadLimits {
        self.attachment_limits
    }

    pub async fn create_issue_in_project(
        &self,
        title: &str,
        description: Option<&str>,
        state_name: &str,
    ) -> Result<Issue> {
        let context = self.issue_create_context(state_name).await?;
        let response = self
            .graphql(
                CREATE_PROJECT_ISSUE_QUERY,
                json!({
                    "input": {
                        "teamId": context.team_id,
                        "projectId": context.project_id,
                        "stateId": context.state_id,
                        "title": title,
                        "description": description.unwrap_or(""),
                    }
                }),
            )
            .await?;
        let issue = response
            .get("data")
            .and_then(|data| data.get("issueCreate"))
            .ok_or_else(|| {
                FugueError::LinearUnknownPayload("data.issueCreate missing".to_string())
            })?;
        if !issue
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(FugueError::ResponseError(
                "Linear issueCreate returned success=false".to_string(),
            ));
        }
        let node = issue.get("issue").ok_or_else(|| {
            FugueError::LinearUnknownPayload("issueCreate.issue missing".to_string())
        })?;
        normalize_issue(node)
    }

    pub async fn download_file_to_path(
        &self,
        url: &str,
        path: &Path,
        budget: &mut AttachmentDownloadBudget,
    ) -> Result<LinearDownloadedFile> {
        validate_attachment_download_url(url)?;
        budget.reserve_file(url)?;
        let response = self.send_attachment_download_request(url).await?;
        let status = response.status();
        if !status.is_success() {
            return Err(FugueError::LinearApiStatus(status.to_string()));
        }
        let content_length = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| response.content_length());
        budget.check_content_length(url, content_length)?;
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);

        let parent = path
            .parent()
            .ok_or_else(|| FugueError::Workspace("attachment path has no parent".into()))?;
        let pending = tempfile::Builder::new()
            .prefix(".fugue-download-")
            .tempfile_in(parent)
            .map_err(|error| FugueError::Workspace(error.to_string()))?;
        let mut file = tokio::fs::File::from_std(
            pending
                .reopen()
                .map_err(|error| FugueError::Workspace(error.to_string()))?,
        );
        let mut byte_len = 0u64;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|error| FugueError::LinearUnknownPayload(error.to_string()))?;
            byte_len = byte_len.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            budget.check_streamed_len(url, byte_len)?;
            file.write_all(&chunk)
                .await
                .map_err(|error| FugueError::Workspace(error.to_string()))?;
        }
        file.sync_all()
            .await
            .map_err(|error| FugueError::Workspace(error.to_string()))?;
        drop(file);
        pending
            .persist(path)
            .map_err(|error| FugueError::Workspace(error.to_string()))?;
        budget.record_success(byte_len);
        Ok(LinearDownloadedFile {
            content_type,
            byte_len,
            path: path.to_path_buf(),
        })
    }

    async fn send_attachment_download_request(&self, url: &str) -> Result<reqwest::Response> {
        if is_linear_upload_attachment_url(url) {
            let response = self
                .client
                .get(url)
                .send()
                .await
                .map_err(|err| FugueError::LinearApiRequest(err.to_string()))?;
            if response.status().is_redirection() {
                let redirected = redirect_location(response.url(), response.headers())?;
                validate_attachment_download_url(redirected.as_str())?;
                return self
                    .download_client
                    .get(redirected)
                    .send()
                    .await
                    .map_err(|err| FugueError::LinearApiRequest(err.to_string()));
            }
            return Ok(response);
        }

        self.download_client
            .get(url)
            .send()
            .await
            .map_err(|err| FugueError::LinearApiRequest(err.to_string()))
    }

    pub async fn materialize_issue_attachments(
        &self,
        issue: &Issue,
        workspace_path: &Path,
    ) -> Result<Issue> {
        if issue.attachments.is_empty() {
            return Ok(issue.clone());
        }
        let root = workspace_path
            .join(".fugue")
            .join("attachments")
            .join(sanitize_attachment_component(&issue.identifier));
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|err| FugueError::Workspace(err.to_string()))?;

        let mut materialized = issue.clone();
        let mut used = std::collections::BTreeSet::new();
        let mut budget = self.attachment_limits.budget();
        for (index, attachment) in materialized.attachments.iter_mut().enumerate() {
            if !is_allowed_attachment_download_url(&attachment.url) {
                continue;
            }
            let file_name = attachment_file_name(index, attachment, &mut used);
            let path = root.join(file_name);
            self.download_file_to_path(&attachment.url, &path, &mut budget)
                .await?;
            attachment.local_path = Some(path);
        }
        Ok(materialized)
    }

    async fn issue_create_context(&self, state_name: &str) -> Result<LinearIssueCreateContext> {
        let project_response = self
            .graphql(
                PROJECT_BY_SLUG_QUERY,
                json!({ "projectSlug": self.project_slug, "first": 1 }),
            )
            .await?;
        let project = project_response
            .get("data")
            .and_then(|data| data.get("projects"))
            .and_then(|projects| projects.get("nodes"))
            .and_then(Value::as_array)
            .and_then(|nodes| nodes.first())
            .ok_or_else(|| {
                FugueError::ResponseError(format!(
                    "Linear project not found for slug {}",
                    self.project_slug
                ))
            })?;
        let project_id = required_string(project, "id")?;
        let team_id = project_team_id(project)?;

        let states_response = self
            .graphql(
                WORKFLOW_STATES_FOR_TEAM_QUERY,
                json!({ "teamId": team_id, "first": 100 }),
            )
            .await?;
        let state_id = states_response
            .get("data")
            .and_then(|data| data.get("workflowStates"))
            .and_then(|states| states.get("nodes"))
            .and_then(Value::as_array)
            .and_then(|nodes| {
                nodes.iter().find_map(|node| {
                    let name = node.get("name").and_then(Value::as_str)?;
                    if name.eq_ignore_ascii_case(state_name) {
                        node.get("id")
                            .and_then(Value::as_str)
                            .map(ToString::to_string)
                    } else {
                        None
                    }
                })
            })
            .ok_or_else(|| {
                FugueError::ResponseError(format!(
                    "Linear workflow state {state_name:?} was not found for project {}",
                    self.project_slug
                ))
            })?;

        Ok(LinearIssueCreateContext {
            project_id,
            team_id,
            state_id,
        })
    }

    async fn fetch_by_states(&self, states: &[String]) -> Result<Vec<Issue>> {
        if states.is_empty() {
            return Ok(Vec::new());
        }
        let mut issues = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let variables = json!({
                "projectSlug": self.project_slug,
                "stateNames": states,
                "first": 50,
                "after": after,
            });
            let response = self.graphql(CANDIDATE_ISSUES_QUERY, variables).await?;
            let connection = response
                .get("data")
                .and_then(|data| data.get("issues"))
                .ok_or_else(|| {
                    FugueError::LinearUnknownPayload("data.issues missing".to_string())
                })?;
            issues.extend(normalize_issue_nodes(connection)?);
            let page_info = connection
                .get("pageInfo")
                .and_then(Value::as_object)
                .ok_or_else(|| FugueError::LinearUnknownPayload("pageInfo missing".to_string()))?;
            let has_next = page_info
                .get("hasNextPage")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !has_next {
                break;
            }
            after = page_info
                .get("endCursor")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            if after.is_none() {
                return Err(FugueError::LinearMissingEndCursor);
            }
        }
        Ok(issues)
    }

    pub async fn fetch_board_issues_by_states(&self, states: &[String]) -> Result<Vec<Issue>> {
        if states.is_empty() {
            return Ok(Vec::new());
        }
        let mut issues = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let variables = json!({
                "projectSlug": self.project_slug,
                "stateNames": states,
                "first": 100,
                "after": after,
            });
            let response = self.graphql(BOARD_ISSUES_QUERY, variables).await?;
            let connection = response
                .get("data")
                .and_then(|data| data.get("issues"))
                .ok_or_else(|| {
                    FugueError::LinearUnknownPayload("data.issues missing".to_string())
                })?;
            issues.extend(normalize_issue_nodes(connection)?);
            let page_info = connection
                .get("pageInfo")
                .and_then(Value::as_object)
                .ok_or_else(|| FugueError::LinearUnknownPayload("pageInfo missing".to_string()))?;
            let has_next = page_info
                .get("hasNextPage")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !has_next {
                break;
            }
            after = page_info
                .get("endCursor")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            if after.is_none() {
                return Err(FugueError::LinearMissingEndCursor);
            }
        }
        Ok(issues)
    }

    pub async fn fetch_open_project_issues(&self) -> Result<Vec<Issue>> {
        self.fetch_by_open_state_types(&["started", "unstarted", "backlog"])
            .await
    }

    async fn fetch_by_open_state_types(&self, state_types: &[&str]) -> Result<Vec<Issue>> {
        let mut issues = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let variables = json!({
                "projectSlug": self.project_slug,
                "stateTypes": state_types,
                "first": 50,
                "after": after,
            });
            let response = self.graphql(OPEN_PROJECT_ISSUES_QUERY, variables).await?;
            let connection = response
                .get("data")
                .and_then(|data| data.get("issues"))
                .ok_or_else(|| {
                    FugueError::LinearUnknownPayload("data.issues missing".to_string())
                })?;
            issues.extend(normalize_issue_nodes(connection)?);
            let page_info = connection
                .get("pageInfo")
                .and_then(Value::as_object)
                .ok_or_else(|| FugueError::LinearUnknownPayload("pageInfo missing".to_string()))?;
            let has_next = page_info
                .get("hasNextPage")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !has_next {
                break;
            }
            after = page_info
                .get("endCursor")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            if after.is_none() {
                return Err(FugueError::LinearMissingEndCursor);
            }
        }
        Ok(issues)
    }
}

#[async_trait]
impl IssueTracker for LinearClient {
    async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>> {
        self.fetch_by_states(&self.active_states).await
    }

    async fn fetch_issues_by_states(&self, states: &[String]) -> Result<Vec<Issue>> {
        self.fetch_by_states(states).await
    }

    async fn fetch_issue_states_by_ids(&self, issue_ids: &[String]) -> Result<Vec<Issue>> {
        if issue_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut issues = Vec::new();
        for variables in issue_state_query_variables(issue_ids) {
            let response = self.graphql(ISSUE_STATES_QUERY, variables).await?;
            let connection = response
                .get("data")
                .and_then(|data| data.get("issues"))
                .ok_or_else(|| {
                    FugueError::LinearUnknownPayload("data.issues missing".to_string())
                })?;
            issues.extend(normalize_issue_nodes(connection)?);
        }
        Ok(issues)
    }

    async fn graphql(&self, query: &str, variables: Value) -> Result<Value> {
        let response = self
            .client
            .post(&self.endpoint)
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await
            .map_err(|err| FugueError::LinearApiRequest(err.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|err| FugueError::LinearUnknownPayload(err.to_string()))?;
        let body: Value = serde_json::from_str(&text)
            .map_err(|err| FugueError::LinearUnknownPayload(err.to_string()))?;
        if let Some(errors) = body.get("errors") {
            let _ = errors;
            return Err(FugueError::LinearGraphqlErrors(body.to_string()));
        }
        if !status.is_success() {
            return Err(FugueError::LinearApiStatus(status.to_string()));
        }
        Ok(body)
    }
}

pub async fn execute_linear_graphql_tool(
    config: &TrackerConfig,
    arguments: &Value,
) -> (bool, String) {
    let parsed = match parse_linear_graphql_arguments(arguments) {
        Ok(parsed) => parsed,
        Err(err) => return (true, json!({"error": err}).to_string()),
    };
    let client = match LinearClient::from_config(config) {
        Ok(client) => client,
        Err(err) => return (true, json!({"error": err.to_string()}).to_string()),
    };
    match client.graphql(&parsed.query, parsed.variables).await {
        Ok(value) => (false, value.to_string()),
        Err(FugueError::LinearGraphqlErrors(body)) => (true, body),
        Err(err) => (true, json!({"error": err.to_string()}).to_string()),
    }
}

#[derive(Debug)]
struct LinearGraphqlArguments {
    query: String,
    variables: Value,
}

fn parse_linear_graphql_arguments(
    value: &Value,
) -> std::result::Result<LinearGraphqlArguments, String> {
    if let Some(query) = value.as_str() {
        validate_single_graphql_operation(query)?;
        return Ok(LinearGraphqlArguments {
            query: query.to_string(),
            variables: json!({}),
        });
    }
    let object = value
        .as_object()
        .ok_or_else(|| "arguments must be an object or GraphQL query string".to_string())?;
    let query = object
        .get("query")
        .and_then(Value::as_str)
        .filter(|query| !query.trim().is_empty())
        .ok_or_else(|| "query must be a non-empty string".to_string())?;
    validate_single_graphql_operation(query)?;
    let variables = object
        .get("variables")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !variables.is_object() {
        return Err("variables must be an object".to_string());
    }
    Ok(LinearGraphqlArguments {
        query: query.to_string(),
        variables,
    })
}

fn issue_state_query_variables(issue_ids: &[String]) -> Vec<Value> {
    issue_ids
        .chunks(LINEAR_ISSUE_STATES_PAGE_SIZE)
        .map(|ids| json!({ "ids": ids, "first": ids.len() }))
        .collect()
}

fn validate_single_graphql_operation(query: &str) -> std::result::Result<(), String> {
    let stripped = query
        .lines()
        .map(|line| line.split('#').next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    let operations = stripped
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .filter(|token| matches!(*token, "query" | "mutation" | "subscription"))
        .count();
    if operations == 1 || (operations == 0 && stripped.trim_start().starts_with('{')) {
        Ok(())
    } else {
        Err("query must contain exactly one GraphQL operation".to_string())
    }
}

fn normalize_issue_nodes(connection: &Value) -> Result<Vec<Issue>> {
    let nodes = connection
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| FugueError::LinearUnknownPayload("issues.nodes missing".to_string()))?;
    nodes.iter().map(normalize_issue).collect()
}

fn normalize_issue(node: &Value) -> Result<Issue> {
    let id = required_string(node, "id")?;
    let identifier = required_string(node, "identifier")?;
    let title = required_string(node, "title")?;
    let state = node
        .get("state")
        .and_then(|value| value.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| FugueError::LinearUnknownPayload("issue.state.name missing".to_string()))?
        .to_string();
    let labels = node
        .get("labels")
        .and_then(|labels| labels.get("nodes"))
        .and_then(Value::as_array)
        .map(|labels| {
            labels
                .iter()
                .filter_map(|label| label.get("name").and_then(Value::as_str))
                .map(|label| label.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default();
    let blocked_by = node
        .get("inverseRelations")
        .and_then(|relations| relations.get("nodes"))
        .and_then(Value::as_array)
        .map(|relations| {
            relations
                .iter()
                .filter(|relation| relation.get("type").and_then(Value::as_str) == Some("blocks"))
                .map(|relation| {
                    let issue = relation.get("issue").unwrap_or(&Value::Null);
                    BlockerRef {
                        id: issue
                            .get("id")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        identifier: issue
                            .get("identifier")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        state: issue
                            .get("state")
                            .and_then(|state| state.get("name"))
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let attachments = node
        .get("attachments")
        .and_then(|attachments| attachments.get("nodes"))
        .and_then(Value::as_array)
        .map(|attachments| {
            attachments
                .iter()
                .filter_map(|attachment| {
                    let url = attachment.get("url").and_then(Value::as_str)?.to_string();
                    Some(IssueAttachment {
                        id: attachment
                            .get("id")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        title: attachment
                            .get("title")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        url,
                        local_path: None,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(Issue {
        id,
        identifier,
        title,
        description: optional_string(node, "description"),
        priority: node.get("priority").and_then(Value::as_i64),
        state,
        branch_name: optional_string(node, "branchName"),
        url: optional_string(node, "url"),
        labels,
        blocked_by,
        attachments,
        created_at: parse_time(optional_string(node, "createdAt")),
        updated_at: parse_time(optional_string(node, "updatedAt")),
    })
}

fn required_string(node: &Value, key: &str) -> Result<String> {
    node.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| FugueError::LinearUnknownPayload(format!("issue.{key} missing")))
}

fn optional_string(node: &Value, key: &str) -> Option<String> {
    node.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn project_team_id(project: &Value) -> Result<String> {
    project
        .get("teams")
        .and_then(|teams| teams.get("nodes"))
        .and_then(Value::as_array)
        .and_then(|nodes| nodes.first())
        .and_then(|team| team.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| {
            FugueError::LinearUnknownPayload("project.teams.nodes[0].id missing".to_string())
        })
}

fn parse_time(value: Option<String>) -> Option<DateTime<Utc>> {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&Utc))
}

pub fn validate_attachment_download_url(url: &str) -> Result<()> {
    if is_allowed_attachment_download_url(url) {
        Ok(())
    } else {
        Err(FugueError::ResponseError(format!(
            "attachment download URL is not allowed: {url}"
        )))
    }
}

pub fn is_allowed_attachment_download_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let Some(host) = parsed.host_str().map(str::to_ascii_lowercase) else {
        return false;
    };

    if cfg!(test)
        && parsed.scheme() == "http"
        && matches!(host.as_str(), "127.0.0.1" | "localhost" | "[::1]" | "::1")
    {
        return true;
    }

    if parsed.scheme() != "https" {
        return false;
    }

    matches!(host.as_str(), "uploads.linear.app" | "uploads.linear.com")
        || is_allowed_linear_file_uploads_s3_host(&host)
}

fn is_linear_upload_attachment_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    matches!(
        parsed.host_str().map(str::to_ascii_lowercase).as_deref(),
        Some("uploads.linear.app" | "uploads.linear.com")
    )
}

fn redirect_location(base: &reqwest::Url, headers: &HeaderMap) -> Result<reqwest::Url> {
    let location = headers
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            FugueError::LinearUnknownPayload("attachment redirect missing Location".to_string())
        })?;
    base.join(location).map_err(|err| {
        FugueError::LinearUnknownPayload(format!("attachment redirect Location is invalid: {err}"))
    })
}

fn is_allowed_linear_file_uploads_s3_host(host: &str) -> bool {
    let labels = host.split('.').collect::<Vec<_>>();
    labels.len() >= 4
        && labels[0] == "linear-file-uploads"
        && labels[1] == "s3"
        && labels[labels.len() - 2] == "amazonaws"
        && labels[labels.len() - 1] == "com"
}

fn attachment_file_name(
    index: usize,
    attachment: &IssueAttachment,
    used: &mut std::collections::BTreeSet<String>,
) -> String {
    let url_name = attachment_file_name_from_url(&attachment.url);
    let title_name = attachment
        .title
        .as_deref()
        .map(sanitize_attachment_component)
        .filter(|name| !name.is_empty());
    let mut name = title_name
        .or_else(|| url_name.clone())
        .unwrap_or_else(|| "attachment".to_string());
    if Path::new(&name).extension().is_none()
        && let Some(extension) = url_name
            .as_deref()
            .and_then(|name| Path::new(name).extension())
            .and_then(|extension| extension.to_str())
            .filter(|extension| !extension.is_empty())
    {
        name.push('.');
        name.push_str(extension);
    }
    let base = format!("{:03}-{name}", index + 1);
    if used.insert(base.clone()) {
        return base;
    }
    for duplicate in 2usize.. {
        let candidate = format!("{:03}-{duplicate}-{name}", index + 1);
        if used.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("infinite duplicate filename sequence should always find a free name")
}

fn attachment_file_name_from_url(url: &str) -> Option<String> {
    let without_fragment = url.split('#').next().unwrap_or(url);
    let without_query = without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment);
    without_query
        .rsplit('/')
        .next()
        .map(sanitize_attachment_component)
        .filter(|name| !name.is_empty())
}

fn sanitize_attachment_component(value: &str) -> String {
    let mut sanitized = String::new();
    for character in value.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
            sanitized.push(character);
        } else if character.is_whitespace() {
            sanitized.push('-');
        }
    }
    let sanitized = sanitized.trim_matches(['.', '-']).to_string();
    if sanitized.len() > 96 {
        sanitized[..96].to_string()
    } else {
        sanitized
    }
}

const PROJECT_BY_SLUG_QUERY: &str = r#"
query FugueProjectBySlug($projectSlug: String!, $first: Int!) {
  projects(filter: { slugId: { eq: $projectSlug } }, first: $first) {
    nodes {
      id
      teams(first: 1) { nodes { id } }
    }
  }
}
"#;

const WORKFLOW_STATES_FOR_TEAM_QUERY: &str = r#"
query FugueWorkflowStatesForTeam($teamId: ID!, $first: Int!) {
  workflowStates(filter: { team: { id: { eq: $teamId } } }, first: $first) {
    nodes { id name }
  }
}
"#;

const CREATE_PROJECT_ISSUE_QUERY: &str = r#"
mutation FugueCreateProjectIssue($input: IssueCreateInput!) {
  issueCreate(input: $input) {
    success
    issue {
      id
      identifier
      title
      description
      priority
      branchName
      url
      createdAt
      updatedAt
      state { name }
      labels { nodes { name } }
      attachments(first: 100) { nodes { id title url } }
      inverseRelations {
        nodes {
          type
          issue {
            id
            identifier
            state { name }
          }
        }
      }
    }
  }
}
"#;

const CANDIDATE_ISSUES_QUERY: &str = r#"
query FugueCandidateIssues($projectSlug: String!, $stateNames: [String!], $first: Int!, $after: String) {
  issues(
    filter: {
      project: { slugId: { eq: $projectSlug } }
      state: { name: { in: $stateNames } }
    }
    first: $first
    after: $after
  ) {
    nodes {
      id
      identifier
      title
      description
      priority
      branchName
      url
      createdAt
      updatedAt
      state { name }
      labels { nodes { name } }
      attachments(first: 100) { nodes { id title url } }
      inverseRelations {
        nodes {
          type
          issue {
            id
            identifier
            state { name }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"#;

const BOARD_ISSUES_QUERY: &str = r#"
query FugueBoardIssues($projectSlug: String!, $stateNames: [String!], $first: Int!, $after: String) {
  issues(
    filter: {
      project: { slugId: { eq: $projectSlug } }
      state: { name: { in: $stateNames } }
    }
    first: $first
    after: $after
  ) {
    nodes {
      id
      identifier
      title
      priority
      branchName
      url
      createdAt
      updatedAt
      state { name }
      labels { nodes { name } }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"#;

const OPEN_PROJECT_ISSUES_QUERY: &str = r#"
query FugueOpenProjectIssues($projectSlug: String!, $stateTypes: [String!], $first: Int!, $after: String) {
  issues(
    filter: {
      project: { slugId: { eq: $projectSlug } }
      state: { type: { in: $stateTypes } }
    }
    first: $first
    after: $after
  ) {
    nodes {
      id
      identifier
      title
      description
      priority
      branchName
      url
      createdAt
      updatedAt
      state { name }
      labels { nodes { name } }
      attachments(first: 100) { nodes { id title url } }
      inverseRelations {
        nodes {
          type
          issue {
            id
            identifier
            state { name }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"#;

const ISSUE_STATES_QUERY: &str = r#"
query FugueIssueStates($ids: [ID!]!, $first: Int!) {
  issues(filter: { id: { in: $ids } }, first: $first) {
    nodes {
      id
      identifier
      title
      description
      priority
      branchName
      url
      createdAt
      updatedAt
      state { name }
      labels { nodes { name } }
      attachments(first: 100) { nodes { id title url } }
      inverseRelations {
        nodes {
          type
          issue {
            id
            identifier
            state { name }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_tracker_config(
        max_attachment_files: usize,
        max_attachment_bytes: u64,
        max_attachment_total_bytes: u64,
    ) -> TrackerConfig {
        TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: vec![],
            in_flight_states: vec![],
            active_states: vec![],
            attachment_root: None,
            max_attachment_files,
            max_attachment_bytes,
            max_attachment_total_bytes,
            terminal_states: vec![],
        }
    }

    fn test_issue_with_attachments(attachments: Vec<IssueAttachment>) -> Issue {
        Issue {
            id: "id".to_string(),
            identifier: "LUC-631".to_string(),
            title: "Attachments".to_string(),
            description: None,
            priority: None,
            state: "Todo".to_string(),
            branch_name: None,
            url: None,
            labels: Vec::new(),
            blocked_by: Vec::new(),
            attachments,
            created_at: None,
            updated_at: None,
        }
    }

    #[test]
    fn query_uses_slug_id_and_id_list_type() {
        assert!(CANDIDATE_ISSUES_QUERY.contains("slugId"));
        assert!(BOARD_ISSUES_QUERY.contains("slugId"));
        assert!(OPEN_PROJECT_ISSUES_QUERY.contains("state: { type: { in: $stateTypes } }"));
        assert!(ISSUE_STATES_QUERY.contains("$ids: [ID!]!"));
    }

    #[test]
    fn board_query_omits_detail_only_fields() {
        assert!(BOARD_ISSUES_QUERY.contains("query FugueBoardIssues"));
        for detail_only_field in ["description", "attachments", "inverseRelations"] {
            assert!(
                !BOARD_ISSUES_QUERY.contains(detail_only_field),
                "IMS board query should not fetch detail-only field {detail_only_field}"
            );
        }
        for board_field in ["identifier", "title", "state { name }", "labels"] {
            assert!(
                BOARD_ISSUES_QUERY.contains(board_field),
                "IMS board query should retain board field {board_field}"
            );
        }
    }

    #[test]
    fn project_create_context_uses_linear_project_teams_connection() {
        assert!(PROJECT_BY_SLUG_QUERY.contains("teams(first: 1)"));
        assert!(!PROJECT_BY_SLUG_QUERY.contains(" team { id }"));
        let project = json!({
            "id": "project-id",
            "teams": {"nodes": [{"id": "team-id"}]},
        });
        assert_eq!(project_team_id(&project).unwrap(), "team-id");
    }

    #[test]
    fn normalizes_labels_and_blockers() {
        let node = json!({
            "id": "id",
            "identifier": "ABC-1",
            "title": "Title",
            "description": null,
            "priority": 2,
            "branchName": "abc",
            "url": "https://linear.app/x",
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-02T00:00:00Z",
            "state": {"name": "Todo"},
            "labels": {"nodes": [{"name": "Bug"}]},
            "attachments": {"nodes": [{"id": "att-1", "title": "Design.png", "url": "https://uploads.linear.app/design.png"}]},
            "inverseRelations": {"nodes": [
                {"type": "blocks", "issue": {"id": "b", "identifier": "ABC-0", "state": {"name": "In Progress"}}}
            ]}
        });
        let issue = normalize_issue(&node).unwrap();
        assert_eq!(issue.labels, vec!["bug"]);
        assert_eq!(issue.blocked_by[0].identifier.as_deref(), Some("ABC-0"));
        assert_eq!(issue.attachments[0].id.as_deref(), Some("att-1"));
        assert_eq!(issue.attachments[0].title.as_deref(), Some("Design.png"));
        assert_eq!(issue.attachments[0].local_path, None);
    }

    #[test]
    fn attachment_download_policy_allows_only_linear_file_hosts() {
        assert!(is_allowed_attachment_download_url(
            "https://uploads.linear.app/team/file.png"
        ));
        assert!(is_allowed_attachment_download_url(
            "https://uploads.linear.com/team/file.png"
        ));
        assert!(is_allowed_attachment_download_url(
            "https://linear-file-uploads.s3.amazonaws.com/team/file.png"
        ));
        assert!(is_allowed_attachment_download_url(
            "https://linear-file-uploads.s3.us-east-1.amazonaws.com/team/file.png"
        ));
        assert!(!is_allowed_attachment_download_url(
            "ftp://uploads.linear.app/file.png"
        ));
        assert!(!is_allowed_attachment_download_url(
            "https://example.com/file.png"
        ));
        assert!(!is_allowed_attachment_download_url(
            "http://uploads.linear.app/file.png"
        ));
        assert!(!is_allowed_attachment_download_url(
            "https://linear-file-uploads.s3.evil.com/team/file.png"
        ));
        assert!(!is_allowed_attachment_download_url(
            "https://linear-file-uploads.s3amazonaws.com/team/file.png"
        ));
    }

    #[tokio::test]
    async fn attachment_downloads_do_not_send_linear_auth_or_json_headers() {
        let app = axum::Router::new().route(
            "/file.txt",
            axum::routing::get(|headers: axum::http::HeaderMap| async move {
                if headers.contains_key(axum::http::header::AUTHORIZATION)
                    || headers
                        .get(axum::http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        == Some("application/json")
                {
                    (axum::http::StatusCode::BAD_REQUEST, "leaked headers")
                } else {
                    (axum::http::StatusCode::OK, "downloaded contents")
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve attachment");
        });

        let workspace = tempfile::tempdir().expect("workspace");
        let client =
            LinearClient::from_config(&test_tracker_config(20, 1024, 4096)).expect("client");
        let issue = test_issue_with_attachments(vec![IssueAttachment {
            id: Some("att-1".to_string()),
            title: Some("safe.txt".to_string()),
            url: format!("http://{address}/file.txt"),
            local_path: None,
        }]);

        let materialized = client
            .materialize_issue_attachments(&issue, workspace.path())
            .await
            .expect("materialize without leaked headers");
        let path = materialized.attachments[0]
            .local_path
            .as_ref()
            .expect("local path");
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            "downloaded contents"
        );
    }

    #[tokio::test]
    async fn materializes_issue_attachments_under_workspace_with_safe_names() {
        let app = axum::Router::new().route(
            "/file.txt",
            axum::routing::get(|| async { "downloaded contents" }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve attachment");
        });

        let workspace = tempfile::tempdir().expect("workspace");
        let client = LinearClient::from_config(&test_tracker_config(
            20,
            25 * 1024 * 1024,
            100 * 1024 * 1024,
        ))
        .expect("client");
        let issue = test_issue_with_attachments(vec![IssueAttachment {
            id: Some("att-1".to_string()),
            title: Some("../bad name".to_string()),
            url: format!("http://{address}/file.txt"),
            local_path: None,
        }]);

        let materialized = client
            .materialize_issue_attachments(&issue, workspace.path())
            .await
            .expect("materialize");
        let path = materialized.attachments[0]
            .local_path
            .as_ref()
            .expect("local path");

        assert!(path.starts_with(workspace.path().join(".fugue/attachments/LUC-631")));
        assert_eq!(
            path.file_name().and_then(|name| name.to_str()),
            Some("001-bad-name.txt")
        );
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            "downloaded contents"
        );
    }

    #[tokio::test]
    async fn materialization_ignores_non_file_linear_link_attachments() {
        let workspace = tempfile::tempdir().expect("workspace");
        let client =
            LinearClient::from_config(&test_tracker_config(20, 1024, 4096)).expect("client");
        let issue = test_issue_with_attachments(vec![IssueAttachment {
            id: Some("att-github".to_string()),
            title: Some("PR link".to_string()),
            url: "https://github.com/lukacf/meerkat-fugue/pull/222".to_string(),
            local_path: None,
        }]);

        let materialized = client
            .materialize_issue_attachments(&issue, workspace.path())
            .await
            .expect("link attachments are metadata, not files");

        assert_eq!(materialized.attachments[0].local_path, None);
        let attachment_root = workspace.path().join(".fugue/attachments/LUC-631");
        assert_eq!(
            std::fs::read_dir(attachment_root)
                .expect("attachment root")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn attachment_redirect_to_disallowed_url_is_blocked_without_partial_file() {
        let app = axum::Router::new().route(
            "/redirect.txt",
            axum::routing::get(|| async {
                (
                    axum::http::StatusCode::FOUND,
                    [(axum::http::header::LOCATION, "http://example.com/evil.txt")],
                    "redirect",
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve attachment");
        });

        let workspace = tempfile::tempdir().expect("workspace");
        let client =
            LinearClient::from_config(&test_tracker_config(20, 1024, 4096)).expect("client");
        let issue = test_issue_with_attachments(vec![IssueAttachment {
            id: Some("att-1".to_string()),
            title: Some("redirect.txt".to_string()),
            url: format!("http://{address}/redirect.txt"),
            local_path: None,
        }]);

        let err = client
            .materialize_issue_attachments(&issue, workspace.path())
            .await
            .expect_err("redirect is not followed");

        assert!(err.to_string().contains("302") || err.to_string().contains("Found"));
        let path = workspace
            .path()
            .join(".fugue/attachments/LUC-631/001-redirect.txt");
        assert!(!path.exists());
        assert!(!path.with_extension("txt.part").exists());
    }

    #[tokio::test]
    async fn attachment_content_length_over_limit_is_rejected_before_success_path() {
        let app = axum::Router::new().route(
            "/large.txt",
            axum::routing::get(|| async {
                ([(axum::http::header::CONTENT_LENGTH, "6")], "abcdef")
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve attachment");
        });

        let workspace = tempfile::tempdir().expect("workspace");
        let client = LinearClient::from_config(&test_tracker_config(20, 3, 100)).expect("client");
        let issue = test_issue_with_attachments(vec![IssueAttachment {
            id: Some("att-1".to_string()),
            title: Some("large.txt".to_string()),
            url: format!("http://{address}/large.txt"),
            local_path: None,
        }]);

        let err = client
            .materialize_issue_attachments(&issue, workspace.path())
            .await
            .expect_err("oversized Content-Length is blocked");

        assert!(
            err.to_string()
                .contains("Content-Length 6 exceeds per-file limit 3")
        );
        assert!(
            !workspace
                .path()
                .join(".fugue/attachments/LUC-631/001-large.txt")
                .exists()
        );
    }

    #[tokio::test]
    async fn attachment_streaming_cap_removes_partial_file() {
        let app = axum::Router::new().route(
            "/stream.txt",
            axum::routing::get(|| async {
                let stream = futures::stream::iter([
                    Ok::<_, std::convert::Infallible>(axum::body::Bytes::from_static(b"abc")),
                    Ok::<_, std::convert::Infallible>(axum::body::Bytes::from_static(b"def")),
                ]);
                axum::body::Body::from_stream(stream)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve attachment");
        });

        let workspace = tempfile::tempdir().expect("workspace");
        let client = LinearClient::from_config(&test_tracker_config(20, 4, 100)).expect("client");
        let issue = test_issue_with_attachments(vec![IssueAttachment {
            id: Some("att-1".to_string()),
            title: Some("stream.txt".to_string()),
            url: format!("http://{address}/stream.txt"),
            local_path: None,
        }]);

        let err = client
            .materialize_issue_attachments(&issue, workspace.path())
            .await
            .expect_err("stream overrun is blocked");

        assert!(
            err.to_string()
                .contains("streamed bytes 6 exceed per-file limit 4")
        );
        let path = workspace
            .path()
            .join(".fugue/attachments/LUC-631/001-stream.txt");
        assert!(!path.exists());
        assert!(!path.with_extension("txt.part").exists());
    }

    #[tokio::test]
    async fn attachment_stream_read_error_removes_partial_file() {
        let app = axum::Router::new().route(
            "/stream-error.txt",
            axum::routing::get(|| async {
                let stream = futures::stream::iter([
                    Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"abc")),
                    Err::<axum::body::Bytes, _>(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "simulated read failure",
                    )),
                ]);
                axum::body::Body::from_stream(stream)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve attachment");
        });

        let workspace = tempfile::tempdir().expect("workspace");
        let client = LinearClient::from_config(&test_tracker_config(20, 100, 100)).expect("client");
        let issue = test_issue_with_attachments(vec![IssueAttachment {
            id: Some("att-1".to_string()),
            title: Some("stream-error.txt".to_string()),
            url: format!("http://{address}/stream-error.txt"),
            local_path: None,
        }]);

        let err = client
            .materialize_issue_attachments(&issue, workspace.path())
            .await
            .expect_err("stream read error is blocked");

        assert!(!err.to_string().is_empty());
        let path = workspace
            .path()
            .join(".fugue/attachments/LUC-631/001-stream-error.txt");
        assert!(!path.exists());
        assert!(!path.with_extension("txt.part").exists());
    }

    #[tokio::test]
    async fn attachment_count_and_aggregate_budgets_are_enforced() {
        let app = axum::Router::new().route("/file.txt", axum::routing::get(|| async { "abc" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve attachment");
        });

        let workspace = tempfile::tempdir().expect("workspace");
        let attachments = vec![
            IssueAttachment {
                id: Some("att-1".to_string()),
                title: Some("one.txt".to_string()),
                url: format!("http://{address}/file.txt?one"),
                local_path: None,
            },
            IssueAttachment {
                id: Some("att-2".to_string()),
                title: Some("two.txt".to_string()),
                url: format!("http://{address}/file.txt?two"),
                local_path: None,
            },
        ];

        let aggregate_client =
            LinearClient::from_config(&test_tracker_config(20, 10, 5)).expect("client");
        let aggregate_err = aggregate_client
            .materialize_issue_attachments(
                &test_issue_with_attachments(attachments.clone()),
                workspace.path(),
            )
            .await
            .expect_err("aggregate budget is blocked");
        assert!(
            aggregate_err
                .to_string()
                .contains("exceeds remaining aggregate attachment budget")
        );

        let count_client =
            LinearClient::from_config(&test_tracker_config(1, 10, 100)).expect("client");
        let count_err = count_client
            .materialize_issue_attachments(
                &test_issue_with_attachments(attachments),
                workspace.path(),
            )
            .await
            .expect_err("count budget is blocked");
        assert!(
            count_err
                .to_string()
                .contains("max attachment count 1 exceeded")
        );
    }

    #[test]
    fn issue_state_query_variables_are_bounded_and_cover_all_ids() {
        let issue_ids = (0..(LINEAR_ISSUE_STATES_PAGE_SIZE * 2 + 3))
            .map(|index| format!("issue-{index}"))
            .collect::<Vec<_>>();

        let batches = issue_state_query_variables(&issue_ids);

        assert_eq!(batches.len(), 3);
        let mut collected = Vec::new();
        for batch in &batches {
            let ids = batch
                .get("ids")
                .and_then(Value::as_array)
                .expect("ids array");
            let first = batch
                .get("first")
                .and_then(Value::as_u64)
                .expect("first value") as usize;
            assert!(first <= LINEAR_ISSUE_STATES_PAGE_SIZE);
            assert_eq!(first, ids.len());
            collected.extend(
                ids.iter()
                    .map(|id| id.as_str().expect("id string").to_string()),
            );
        }
        assert_eq!(collected, issue_ids);
    }

    #[test]
    fn validates_single_graphql_operation() {
        assert!(validate_single_graphql_operation("query Test { viewer { id } }").is_ok());
        assert!(validate_single_graphql_operation("{ viewer { id } }").is_ok());
        assert!(validate_single_graphql_operation("query A { a } mutation B { b }").is_err());
    }
}
