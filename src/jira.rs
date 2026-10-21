//! Jira REST adapter. Site, project, authentication and workflow names are configuration.
//!
//! The existing host tool payloads retain their historical `Linear*` Rust names.
//! Jira is translated at this boundary; no GraphQL or Linear service is involved.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Method, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::config::TrackerConfig;
use crate::domain::{BlockerRef, Issue, IssueAttachment};
use crate::error::{FugueError, Result};
use crate::tools::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JiraConfig {
    /// Literal email or `$ENV_VAR`. Defaults to the `JIRA_EMAIL` environment variable.
    pub email: Option<String>,
    pub auth: JiraAuth,
    pub api_version: JiraApiVersion,
    /// Additional JQL ANDed with the mandatory project scope. Do not include ORDER BY.
    pub jql: Option<String>,
    pub issue_type: Option<String>,
    /// Private JSON object containing site-specific fields for newly created issues.
    /// Core project, summary, description, and issue type fields cannot be overridden.
    pub create_fields_file: Option<String>,
    /// Private JSON object mapping destination status names or IDs to transition fields.
    pub transition_fields_file: Option<String>,
    /// Jira issue-link type names whose inward side blocks this issue.
    pub blocking_link_types: Vec<String>,
}

impl Default for JiraConfig {
    fn default() -> Self {
        Self {
            email: None,
            auth: JiraAuth::default(),
            api_version: JiraApiVersion::default(),
            jql: None,
            issue_type: None,
            create_fields_file: None,
            transition_fields_file: None,
            blocking_link_types: vec!["Blocks".to_string()],
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JiraAuth {
    #[default]
    Basic,
    Bearer,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum JiraApiVersion {
    #[serde(rename = "2")]
    V2,
    #[default]
    #[serde(rename = "3")]
    V3,
}

impl JiraApiVersion {
    fn segment(self) -> &'static str {
        match self {
            Self::V2 => "2",
            Self::V3 => "3",
        }
    }
}

#[derive(Clone)]
pub struct JiraClient {
    base: Url,
    client: reqwest::Client,
    project: String,
    config: JiraConfig,
    create_fields: BTreeMap<String, Box<serde_json::value::RawValue>>,
    transition_fields: BTreeMap<String, BTreeMap<String, Box<serde_json::value::RawValue>>>,
    active_states: Vec<String>,
    terminal_states: Vec<String>,
    attachment_limits: crate::tracker::AttachmentDownloadLimits,
}

fn invalid(message: impl Into<String>) -> FugueError {
    FugueError::InvalidConfig(message.into())
}

fn request_error(error: reqwest::Error) -> FugueError {
    // HTTP bodies may contain private issue data. Never include them or credentials.
    FugueError::JiraRequest(error.without_url().to_string())
}

fn env_value(value: &str) -> Option<String> {
    match value.strip_prefix('$') {
        Some(name) => std::env::var(name).ok(),
        None => Some(value.to_string()),
    }
    .filter(|value| !value.trim().is_empty())
}

fn jql_literal(value: &str) -> String {
    JqlLiteral(value).to_string()
}

struct JqlLiteral<'a>(&'a str);
impl std::fmt::Display for JqlLiteral<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::fmt::Write;
        f.write_char('"')?;
        for ch in self.0.chars() {
            match ch {
                '\\' => f.write_str("\\\\")?,
                '"' => f.write_str("\\\"")?,
                '\r' | '\n' => f.write_char(' ')?,
                ch => f.write_char(ch)?,
            }
        }
        f.write_char('"')
    }
}

struct JqlValues<'a>(&'a [String]);
impl std::fmt::Display for JqlValues<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, value) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{}", JqlLiteral(value))?;
        }
        Ok(())
    }
}

impl JiraClient {
    pub fn from_config(tracker: &TrackerConfig) -> Result<Self> {
        let endpoint = env_value(&tracker.endpoint)
            .ok_or_else(|| invalid("Jira requires tracker.endpoint or JIRA_BASE_URL"))?;
        let mut base = Url::parse(&endpoint)
            .map_err(|_| invalid("tracker.endpoint must be a Jira site URL"))?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(invalid(
                "Jira site URL must be HTTP(S) without credentials, query, or fragment",
            ));
        }
        base.set_path(&format!("{}/", base.path().trim_end_matches('/')));
        let project = tracker
            .project_slug
            .as_deref()
            .and_then(env_value)
            .filter(|value| !value.trim().is_empty())
            .ok_or(FugueError::MissingTrackerProjectSlug)?;
        let token = tracker
            .api_key
            .as_deref()
            .and_then(env_value)
            .ok_or(FugueError::MissingTrackerApiKey)?;
        let mut config = tracker.jira.clone().unwrap_or_default();
        if let Some(jql) = &config.jql {
            config.jql = Some(env_value(jql).ok_or_else(|| {
                invalid("tracker.jira.jql references a missing or empty environment variable")
            })?);
        }
        if let Some(issue_type) = &config.issue_type {
            config.issue_type = Some(env_value(issue_type).ok_or_else(|| {
                invalid(
                    "tracker.jira.issue_type references a missing or empty environment variable",
                )
            })?);
        }
        let create_fields =
            match &config.create_fields_file {
                Some(path) => {
                    let path = env_value(path).ok_or_else(|| invalid(
                    "tracker.jira.create_fields_file references a missing environment variable",
                ))?;
                    let contents = std::fs::read_to_string(path)
                        .map_err(|_| invalid("cannot read tracker.jira.create_fields_file"))?;
                    let fields: BTreeMap<String, Box<serde_json::value::RawValue>> =
                        serde_json::from_str(&contents).map_err(|_| {
                            invalid("tracker.jira.create_fields_file must contain a JSON object")
                        })?;
                    if ["project", "summary", "description", "issuetype"]
                        .iter()
                        .any(|key| fields.contains_key(*key))
                    {
                        return Err(invalid(
                            "Jira create fields cannot override core issue fields",
                        ));
                    }
                    fields
                }
                None => BTreeMap::new(),
            };
        let transition_fields = match &config.transition_fields_file {
            Some(path) => {
                let path = env_value(path).ok_or_else(|| invalid(
                    "tracker.jira.transition_fields_file references a missing environment variable",
                ))?;
                let contents = std::fs::read_to_string(path)
                    .map_err(|_| invalid("cannot read tracker.jira.transition_fields_file"))?;
                serde_json::from_str(&contents).map_err(|_| invalid(
                    "tracker.jira.transition_fields_file must map statuses to JSON field objects",
                ))?
            }
            None => BTreeMap::new(),
        };
        let mut authorization = match config.auth {
            JiraAuth::Basic => {
                use base64::Engine;
                let email = match config.email.as_deref() {
                    Some(value) => env_value(value),
                    None => std::env::var("JIRA_EMAIL")
                        .ok()
                        .filter(|value| !value.is_empty()),
                }
                .ok_or_else(|| {
                    invalid("Jira basic authentication requires tracker.jira.email or JIRA_EMAIL")
                })?;
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(format!("{email}:{token}"))
                )
            }
            JiraAuth::Bearer => format!("Bearer {token}"),
        };
        let mut header = reqwest::header::HeaderValue::from_str(&authorization)
            .map_err(|_| invalid("Jira credentials cannot be used as an HTTP header"))?;
        authorization.clear();
        header.set_sensitive(true);
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::AUTHORIZATION, header);
        headers.insert(
            reqwest::header::ACCEPT,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .default_headers(headers)
            .build()
            .map_err(request_error)?;
        Ok(Self {
            base,
            client,
            project,
            config,
            create_fields,
            transition_fields,
            active_states: tracker.active_states.clone(),
            terminal_states: tracker.terminal_states.clone(),
            attachment_limits: crate::tracker::AttachmentDownloadLimits::from_config(tracker),
        })
    }

    fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| invalid("Jira endpoint cannot be a base URL"))?
            .pop_if_empty()
            .extend(["rest", "api", self.config.api_version.segment()])
            .extend(segments.iter().copied());
        Ok(url)
    }

    async fn send<B: Serialize + ?Sized>(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<&B>,
    ) -> Result<reqwest::Response> {
        let mut request = self.client.request(method, self.url(segments)?);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(request_error)?;
        if !response.status().is_success() {
            return Err(FugueError::JiraApiStatus {
                status: response.status().as_u16(),
            });
        }
        Ok(response)
    }

    async fn get<T: DeserializeOwned>(&self, segments: &[&str]) -> Result<T> {
        self.send::<()>(Method::GET, segments, None)
            .await?
            .json()
            .await
            .map_err(request_error)
    }

    fn scoped_jql(&self, clause: Option<&str>) -> String {
        let mut jql = format!("project = {}", jql_literal(&self.project));
        if let Some(filter) = &self.config.jql {
            jql.push_str(&format!(" AND ({filter})"));
        }
        if let Some(clause) = clause {
            jql.push_str(&format!(" AND ({clause})"));
        }
        jql.push_str(" ORDER BY created ASC, key ASC");
        jql
    }

    async fn search(&self, clause: Option<&str>) -> Result<Vec<Issue>> {
        let jql = self.scoped_jql(clause);
        let mut issues = Vec::new();
        let mut start_at = 0;
        let mut token = None;
        let mut tokens = BTreeSet::new();
        loop {
            let request = SearchRequest {
                jql: &jql,
                fields: ISSUE_FIELDS,
                max_results: 100,
                start_at: (self.config.api_version == JiraApiVersion::V2).then_some(start_at),
                next_page_token: token.as_deref(),
            };
            let endpoint = if self.config.api_version == JiraApiVersion::V3 {
                vec!["search", "jql"]
            } else {
                vec!["search"]
            };
            let page: SearchPage = self
                .send(Method::POST, &endpoint, Some(&request))
                .await?
                .json()
                .await
                .map_err(request_error)?;
            let count = page.issues.len();
            for issue in page.issues {
                self.check_project(&issue)?;
                issues.push(self.normalize(issue));
            }
            if self.config.api_version == JiraApiVersion::V3 {
                if page.is_last == Some(true) {
                    break;
                }
                match page.next_page_token.filter(|value| !value.is_empty()) {
                    Some(next) if tokens.insert(next.clone()) => token = Some(next),
                    None if page.is_last.is_none() => break,
                    _ => return Err(invalid("Jira search pagination did not advance")),
                }
            } else {
                start_at += count;
                if count == 0 || page.total.is_some_and(|total| start_at >= total) {
                    break;
                }
            }
        }
        Ok(issues)
    }

    pub async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>> {
        self.fetch_issues_by_states(&self.active_states).await
    }

    pub async fn fetch_issues_by_states(&self, states: &[String]) -> Result<Vec<Issue>> {
        if states.is_empty() {
            return Ok(Vec::new());
        }
        let values = JqlValues(states);
        self.search(Some(&format!("status in ({values})"))).await
    }

    pub async fn fetch_open_project_issues(&self) -> Result<Vec<Issue>> {
        self.search(Some("statusCategory != Done")).await
    }

    pub async fn fetch_issue_states_by_ids(&self, ids: &[String]) -> Result<Vec<Issue>> {
        let mut issues = Vec::new();
        for chunk in ids.chunks(100) {
            let values = JqlValues(chunk);
            issues.extend(self.search(Some(&format!("id in ({values})"))).await?);
        }
        Ok(issues)
    }

    fn check_project(&self, issue: &JiraIssue) -> Result<()> {
        if !issue.fields.project.key.eq_ignore_ascii_case(&self.project)
            && issue.fields.project.id != self.project
        {
            return Err(invalid("Jira issue is outside the configured project"));
        }
        Ok(())
    }

    async fn fetch_issue(&self, issue: &LinearIssueRef) -> Result<JiraIssue> {
        let issue: JiraIssue = self.get(&["issue", issue_key(issue)]).await?;
        self.check_project(&issue)?;
        Ok(issue)
    }

    fn browse_url(&self, key: &str) -> String {
        let mut url = self.base.clone();
        if let Ok(mut path) = url.path_segments_mut() {
            path.pop_if_empty().extend(["browse", key]);
        }
        url.to_string()
    }

    fn normalize(&self, issue: JiraIssue) -> Issue {
        Issue {
            url: Some(self.browse_url(&issue.key)),
            id: issue.id,
            identifier: issue.key,
            title: issue.fields.summary,
            description: issue.fields.description.map(|body| body.markdown()),
            priority: issue.fields.priority.map(|priority| priority.id),
            state: issue.fields.status.name,
            branch_name: None,
            labels: issue.fields.labels,
            blocked_by: issue
                .fields
                .issuelinks
                .into_iter()
                .filter(|link| {
                    self.config
                        .blocking_link_types
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&link.link_type.name))
                })
                .filter_map(|link| {
                    link.inward_issue.map(|issue| BlockerRef {
                        id: Some(issue.id),
                        identifier: Some(issue.key),
                        state: Some(issue.fields.status.name),
                    })
                })
                .collect(),
            attachments: issue
                .fields
                .attachment
                .into_iter()
                .map(|attachment| IssueAttachment {
                    id: Some(attachment.id),
                    title: Some(attachment.filename),
                    url: attachment.content,
                    local_path: None,
                })
                .collect(),
            created_at: issue.fields.created,
            updated_at: Some(issue.fields.updated),
        }
    }

    fn snapshot(&self, issue: &JiraIssue) -> LinearIssueSnapshot {
        LinearIssueSnapshot {
            id: LinearIssueId(issue.id.clone()),
            identifier: LinearIssueIdentifier(issue.key.clone()),
            title: issue.fields.summary.clone(),
            description: issue.fields.description.as_ref().map(JiraBody::markdown),
            url: Some(self.browse_url(&issue.key)),
            state: issue.fields.status.snapshot(&self.terminal_states),
            assignee: issue.fields.assignee.as_ref().map(JiraUser::snapshot),
            branch_name: None,
            labels: issue.fields.labels.clone(),
            updated_at: issue.fields.updated,
            team_id: LinearTeamId(issue.fields.project.id.clone()),
        }
    }

    async fn comments_for_key(&self, key: &str) -> Result<Vec<JiraComment>> {
        let mut comments = Vec::new();
        let mut start = 0;
        loop {
            let response = self
                .client
                .get(self.url(&["issue", key, "comment"])?)
                .query(&[("startAt", start), ("maxResults", 100)])
                .send()
                .await
                .map_err(request_error)?;
            if !response.status().is_success() {
                return Err(FugueError::JiraApiStatus {
                    status: response.status().as_u16(),
                });
            }
            let page: CommentPage = response.json().await.map_err(request_error)?;
            let count = page.comments.len();
            comments.extend(page.comments);
            start += count;
            if start >= page.total {
                break;
            }
            if count == 0 {
                return Err(invalid("Jira comment pagination did not advance"));
            }
        }
        Ok(comments)
    }

    fn comment_url(&self, key: &str, id: &str) -> String {
        format!("{}?focusedCommentId={id}", self.browse_url(key))
    }

    pub async fn issue_comments(
        &self,
        issue: &LinearIssueRef,
    ) -> Result<Vec<LinearIssueCommentSnapshot>> {
        let issue = self.fetch_issue(issue).await?;
        Ok(self
            .comments_for_key(&issue.key)
            .await?
            .into_iter()
            .map(|comment| LinearIssueCommentSnapshot {
                url: Some(self.comment_url(&issue.key, &comment.id)),
                comment_id: LinearCommentId(comment.id),
                body_markdown: comment.body.markdown(),
            })
            .collect())
    }

    pub async fn inspect_issue(&self, input: InspectIssueInput) -> Result<InspectIssueOutput> {
        let issue = self.fetch_issue(&input.issue).await?;
        let comments = self.comments_for_key(&issue.key).await?;
        let workpad = find_workpad(&comments, "## Fugue Workpad", &issue.key)?.map(|comment| {
            LinearWorkpadSnapshot {
                comment_id: LinearCommentId(comment.id.clone()),
                heading: "## Fugue Workpad".to_string(),
                body_markdown: comment.body.markdown(),
                url: Some(self.comment_url(&issue.key, &comment.id)),
            }
        });
        Ok(InspectIssueOutput {
            issue: self.snapshot(&issue),
            workpad,
            attachments: Vec::new(),
        })
    }

    pub async fn workpad_body(
        &self,
        issue: &LinearIssueRef,
        heading: &str,
    ) -> Result<Option<String>> {
        let issue = self.fetch_issue(issue).await?;
        Ok(find_workpad(
            &self.comments_for_key(&issue.key).await?,
            heading,
            &issue.key,
        )?
        .map(|comment| comment.body.markdown()))
    }

    fn body(&self, markdown: &str) -> JiraBody {
        match self.config.api_version {
            JiraApiVersion::V2 => JiraBody::Text(markdown.to_string()),
            JiraApiVersion::V3 => JiraBody::Document(AdfDocument::from_markdown(markdown)),
        }
    }

    pub async fn upsert_workpad(&self, input: UpsertWorkpadInput) -> Result<UpsertWorkpadOutput> {
        let issue = self.fetch_issue(&input.issue).await?;
        let comments = self.comments_for_key(&issue.key).await?;
        let heading = crate::linear_tools::normalize_workpad_heading(&input.heading, &issue.key);
        let rendered = if input.body_markdown.trim_start().starts_with(&heading) {
            input.body_markdown.clone()
        } else {
            format!("{heading}\n\n{}", input.body_markdown)
        };
        if let Some(comment) = find_workpad(&comments, &heading, &issue.key)? {
            let previous = comment.body.markdown();
            let body =
                crate::review_findings::merge_preserved_review_findings(&rendered, Some(&previous));
            let changed = body != previous;
            if changed {
                let payload = CommentBody {
                    body: self.body(&body),
                };
                self.send(
                    Method::PUT,
                    &["issue", &issue.key, "comment", &comment.id],
                    Some(&payload),
                )
                .await?;
            }
            return Ok(UpsertWorkpadOutput {
                comment_id: LinearCommentId(comment.id.clone()),
                url: Some(self.comment_url(&issue.key, &comment.id)),
                created: false,
                updated: changed,
            });
        }
        if !input.create_if_missing {
            return Err(invalid(
                "Jira workpad not found and create_if_missing is false",
            ));
        }
        let payload = CommentBody {
            body: self.body(&rendered),
        };
        let comment: JiraComment = self
            .send(
                Method::POST,
                &["issue", &issue.key, "comment"],
                Some(&payload),
            )
            .await?
            .json()
            .await
            .map_err(request_error)?;
        Ok(UpsertWorkpadOutput {
            url: Some(self.comment_url(&issue.key, &comment.id)),
            comment_id: LinearCommentId(comment.id),
            created: true,
            updated: false,
        })
    }

    pub async fn add_comment(&self, input: AddCommentInput) -> Result<AddCommentOutput> {
        if input
            .attachments
            .iter()
            .any(|attachment| attachment.path.is_some())
        {
            return Err(invalid(
                "Jira comment uploads require an explicit Jira attachment upload; local paths cannot be published as links",
            ));
        }
        let issue = self.fetch_issue(&input.issue).await?;
        let text = render_comment_body_with_attachments(&input.body_markdown, &input.attachments);
        let payload = CommentBody {
            body: self.body(&text),
        };
        let comment: JiraComment = self
            .send(
                Method::POST,
                &["issue", &issue.key, "comment"],
                Some(&payload),
            )
            .await?
            .json()
            .await
            .map_err(request_error)?;
        Ok(AddCommentOutput {
            url: Some(self.comment_url(&issue.key, &comment.id)),
            comment_id: LinearCommentId(comment.id),
            attachments: Vec::new(),
        })
    }

    pub async fn move_issue(&self, input: MoveIssueInput) -> Result<MoveIssueOutput> {
        let issue = self.fetch_issue(&input.issue).await?;
        if input
            .expected_updated_at
            .is_some_and(|expected| expected != issue.fields.updated)
        {
            return Err(FugueError::JiraStaleIssue);
        }
        let current = self.snapshot(&issue);
        let matches = |status: &JiraStatus| match &input.state {
            LinearStateRef::Name { name } => status.name.eq_ignore_ascii_case(name),
            LinearStateRef::Id { id } => status.id == id.0,
        };
        if matches(&issue.fields.status) {
            return Ok(MoveIssueOutput {
                issue: current,
                changed: false,
                previous_state: None,
            });
        }
        let transitions: Transitions = self.get(&["issue", &issue.key, "transitions"]).await?;
        let mut candidates = transitions
            .transitions
            .into_iter()
            .filter(|transition| matches(&transition.to));
        let transition = candidates.next().ok_or_else(|| {
            invalid("Jira has no currently available transition to the requested status")
        })?;
        if candidates.next().is_some() {
            return Err(invalid(
                "Jira has multiple available transitions to the requested status; an unambiguous workflow is required",
            ));
        }
        if current.state.state_type == LinearStateType::Completed
            && transition.to.snapshot(&self.terminal_states).state_type
                != LinearStateType::Completed
        {
            return Err(invalid(
                "Refusing to reopen a terminal Jira issue without an explicit operator restart",
            ));
        }
        self.send(
            Method::POST,
            &["issue", &issue.key, "transitions"],
            Some(&TransitionBody {
                fields: self
                    .transition_fields
                    .get(&transition.to.id)
                    .or_else(|| self.transition_fields.get(&transition.to.name)),
                transition: JiraId { id: transition.id },
            }),
        )
        .await?;
        let updated = self.fetch_issue(&input.issue).await?;
        if !matches(&updated.fields.status) {
            return Err(invalid(
                "Jira accepted the transition but the requested status was not observed",
            ));
        }
        crate::linear_lane_state::record_internal_lane_change_from_env(
            &issue.key,
            &updated.fields.status.name,
        )?;
        Ok(MoveIssueOutput {
            issue: self.snapshot(&updated),
            changed: true,
            previous_state: Some(current.state),
        })
    }

    pub async fn claim_issue(&self, input: ClaimIssueInput) -> Result<ClaimIssueOutput> {
        let current = self.fetch_issue(&input.issue).await?;
        let target = match input.assignee {
            LinearUserRef::Id { id } => id.0,
            LinearUserRef::Me => {
                let user: JiraUser = self.get(&["myself"]).await?;
                user.id().to_string()
            }
        };
        let previous_assignee = current.fields.assignee.as_ref().map(JiraUser::snapshot);
        let changed = current.fields.assignee.as_ref().map(JiraUser::id) != Some(target.as_str());
        if changed {
            let payload = match self.config.api_version {
                JiraApiVersion::V3 => Assignment::Cloud {
                    account_id: &target,
                },
                JiraApiVersion::V2 => Assignment::Server { name: &target },
            };
            self.send(
                Method::PUT,
                &["issue", &current.key, "assignee"],
                Some(&payload),
            )
            .await?;
        }
        let issue = if let Some(state) = input.target_state {
            self.move_issue(MoveIssueInput {
                issue: input.issue,
                state,
                expected_updated_at: None,
                idempotency_key: input.idempotency_key,
            })
            .await?
            .issue
        } else {
            self.snapshot(&self.fetch_issue(&input.issue).await?)
        };
        Ok(ClaimIssueOutput {
            issue,
            claimed: changed,
            previous_assignee,
        })
    }

    pub async fn list_project_states(&self) -> Result<ListProjectStatesOutput> {
        let groups: Vec<ProjectStatuses> =
            self.get(&["project", &self.project, "statuses"]).await?;
        let mut ids = BTreeSet::new();
        let states = groups
            .into_iter()
            .flat_map(|group| group.statuses)
            .filter(|status| ids.insert(status.id.clone()))
            .map(|status| status.snapshot(&self.terminal_states))
            .collect();
        Ok(ListProjectStatesOutput { states })
    }

    pub async fn create_issue(
        &self,
        title: &str,
        description: Option<&str>,
        lane: &str,
    ) -> Result<Issue> {
        let payload = CreateIssueBody {
            fields: CreateIssueFields {
                additional: &self.create_fields,
                project: ProjectKey { key: &self.project },
                summary: title,
                description: self.body(description.unwrap_or_default()),
                issuetype: IssueTypeName {
                    name: self.config.issue_type.as_deref().unwrap_or("Task"),
                },
            },
        };
        let created: JiraId = self
            .send(Method::POST, &["issue"], Some(&payload))
            .await?
            .json()
            .await
            .map_err(request_error)?;
        let reference = LinearIssueRef::Id {
            id: LinearIssueId(created.id),
        };
        self.move_issue(MoveIssueInput {
            issue: reference.clone(),
            state: LinearStateRef::Name {
                name: lane.to_string(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey("jira-create-initial-status".to_string()),
        })
        .await?;
        Ok(self.normalize(self.fetch_issue(&reference).await?))
    }

    pub async fn materialize_issue_attachments(
        &self,
        issue: &Issue,
        workspace: &Path,
    ) -> Result<Issue> {
        use tokio::io::AsyncWriteExt;
        let mut materialized = issue.clone();
        let mut budget = self.attachment_limits.budget();
        let root = workspace
            .join(".fugue/attachments")
            .join(crate::domain::sanitize_workspace_key(&issue.identifier));
        if !issue.attachments.is_empty() {
            tokio::fs::create_dir_all(&root)
                .await
                .map_err(|err| FugueError::Workspace(err.to_string()))?;
        }
        for (index, attachment) in materialized.attachments.iter_mut().enumerate() {
            let url = Url::parse(&attachment.url)
                .map_err(|_| invalid("Jira attachment URL is invalid"))?;
            // Only the configured Jira origin receives credentials. Redirects must be
            // handled by the operator until a trusted download origin is configured.
            if url.origin() != self.base.origin()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(invalid(
                    "Jira attachment URL is outside the configured site",
                ));
            }
            budget.reserve_file(&attachment.url)?;
            // Jira serves the bytes directly when redirect=false, avoiding CDN auth leakage.
            let mut response = self
                .client
                .get(url)
                .query(&[("redirect", "false")])
                .send()
                .await
                .map_err(request_error)?;
            if !response.status().is_success() {
                return Err(FugueError::JiraApiStatus {
                    status: response.status().as_u16(),
                });
            }
            budget.check_content_length(&attachment.url, response.content_length())?;
            let filename = attachment
                .title
                .as_deref()
                .and_then(|title| Path::new(title).file_name())
                .and_then(|name| name.to_str())
                .unwrap_or("attachment");
            let path = root.join(format!("{index}-{filename}"));
            let temporary = root.join(format!(".{index}.download"));
            let result = async {
                let mut file = tokio::fs::File::create(&temporary)
                    .await
                    .map_err(|err| FugueError::Workspace(err.to_string()))?;
                let mut bytes = 0u64;
                while let Some(chunk) = response.chunk().await.map_err(request_error)? {
                    bytes = bytes.saturating_add(chunk.len() as u64);
                    budget.check_streamed_len(&attachment.url, bytes)?;
                    file.write_all(&chunk)
                        .await
                        .map_err(|err| FugueError::Workspace(err.to_string()))?;
                }
                file.flush()
                    .await
                    .map_err(|err| FugueError::Workspace(err.to_string()))?;
                tokio::fs::rename(&temporary, &path)
                    .await
                    .map_err(|err| FugueError::Workspace(err.to_string()))?;
                budget.record_success(bytes);
                Ok::<_, FugueError>(())
            }
            .await;
            if result.is_err()
                && let Err(error) = tokio::fs::remove_file(&temporary).await
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(FugueError::Workspace(format!(
                    "Jira download failed and temporary file cleanup failed: {error}"
                )));
            }
            result?;
            attachment.local_path = Some(path);
        }
        Ok(materialized)
    }

    pub async fn recent_issue_changes(
        &self,
        input: ListRecentIssueChangesInput,
    ) -> Result<ListRecentIssueChangesOutput> {
        if self.config.api_version == JiraApiVersion::V2 {
            return Err(invalid(
                "Jira changelog polling requires REST API version 3",
            ));
        }
        let issue_ref = input
            .issue
            .ok_or_else(|| invalid("Jira changelog queries require an explicitly scoped issue"))?;
        let issue = self.fetch_issue(&issue_ref).await?;
        let start = input
            .cursor
            .as_ref()
            .map(|cursor| cursor.0.parse::<usize>())
            .transpose()
            .map_err(|_| invalid("Jira changelog cursor must be an offset"))?
            .unwrap_or(0);
        let limit = usize::from(input.limit).clamp(1, 100);
        let response = self
            .client
            .get(self.url(&["issue", &issue.key, "changelog"])?)
            .query(&[("startAt", start), ("maxResults", limit)])
            .send()
            .await
            .map_err(request_error)?;
        if !response.status().is_success() {
            return Err(FugueError::JiraApiStatus {
                status: response.status().as_u16(),
            });
        }
        let page: ChangelogPage = response.json().await.map_err(request_error)?;
        let next = start + page.values.len();
        if next < page.total && next == start {
            return Err(invalid("Jira changelog pagination did not advance"));
        }
        let changes = page
            .values
            .into_iter()
            .filter(|entry| input.since.is_none_or(|since| entry.created > since))
            .flat_map(|entry| {
                let issue = issue_ref.clone();
                entry.items.into_iter().map(move |item| LinearIssueChange {
                    issue: issue.clone(),
                    changed_at: entry.created,
                    actor: entry.author.as_ref().map(JiraUser::snapshot),
                    kind: match item.field.as_str() {
                        "status" => LinearIssueChangeKind::StateChanged {
                            from: item.from_string,
                            to: item.to_string.unwrap_or_default(),
                        },
                        "assignee" => LinearIssueChangeKind::AssigneeChanged {
                            from: item.from_string,
                            to: item.to_string,
                        },
                        _ => LinearIssueChangeKind::Updated { field: item.field },
                    },
                })
            })
            .collect();
        Ok(ListRecentIssueChangesOutput {
            changes,
            next_cursor: (next < page.total).then(|| Cursor(next.to_string())),
        })
    }
}

pub async fn execute_tool(
    config: &TrackerConfig,
    operation: FugueToolOperation,
    arguments: &serde_json::Value,
) -> (bool, String) {
    let result = execute_typed_tool(config, operation, arguments).await;
    let key = arguments
        .get("idempotency_key")
        .and_then(serde_json::Value::as_str)
        .map(|value| IdempotencyKey(value.to_string()));
    let result = match result {
        Ok(output) => FugueToolResult::Ok {
            output,
            metadata: ToolExecutionMetadata::default(),
        },
        Err(error) => {
            let (code, retry) = match &error {
                FugueError::JiraApiStatus { status: 401 } => (
                    FugueToolErrorCode::AuthenticationFailed,
                    RetryAdvice::DoNotRetry,
                ),
                FugueError::JiraApiStatus { status: 403 } => (
                    FugueToolErrorCode::PermissionDenied,
                    RetryAdvice::DoNotRetry,
                ),
                FugueError::JiraApiStatus { status: 404 } => {
                    (FugueToolErrorCode::NotFound, RetryAdvice::DoNotRetry)
                }
                FugueError::JiraApiStatus { status: 429 } => (
                    FugueToolErrorCode::RateLimited,
                    RetryAdvice::RetryAfter { seconds: 60 },
                ),
                FugueError::JiraStaleIssue => (
                    FugueToolErrorCode::PreconditionFailed,
                    RetryAdvice::RefreshStateThenRetry,
                ),
                FugueError::JiraApiStatus { status: 500..=599 } => (
                    FugueToolErrorCode::BackendUnavailable,
                    RetryAdvice::RefreshStateThenRetry,
                ),
                FugueError::JiraRequest(_) => (
                    FugueToolErrorCode::ExecutionFailed,
                    RetryAdvice::RefreshStateThenRetry,
                ),
                _ => (
                    FugueToolErrorCode::PreconditionFailed,
                    RetryAdvice::DoNotRetry,
                ),
            };
            FugueToolResult::Err {
                error: FugueToolError {
                    operation,
                    code,
                    message: error.to_string(),
                    retry,
                    idempotency_key: key,
                },
                metadata: ToolExecutionMetadata::default(),
            }
        }
    };
    let is_error = matches!(result, FugueToolResult::Err { .. });
    match serde_json::to_string(&result) {
        Ok(content) => (is_error, content),
        Err(error) => (true, error.to_string()),
    }
}

async fn execute_typed_tool(
    config: &TrackerConfig,
    operation: FugueToolOperation,
    arguments: &serde_json::Value,
) -> Result<FugueToolOutput> {
    let client = JiraClient::from_config(config)?;
    fn parse<T: DeserializeOwned>(arguments: &serde_json::Value) -> Result<T> {
        serde_json::from_value(arguments.clone())
            .map_err(|error| invalid(format!("Invalid Jira tool arguments: {error}")))
    }
    match operation {
        FugueToolOperation::LinearInspectIssue => client
            .inspect_issue(parse(arguments)?)
            .await
            .map(FugueToolOutput::LinearInspectIssue),
        FugueToolOperation::LinearClaimIssue => client
            .claim_issue(parse(arguments)?)
            .await
            .map(FugueToolOutput::LinearClaimIssue),
        FugueToolOperation::LinearMoveIssue => client
            .move_issue(parse(arguments)?)
            .await
            .map(FugueToolOutput::LinearMoveIssue),
        FugueToolOperation::LinearUpsertWorkpad => client
            .upsert_workpad(parse(arguments)?)
            .await
            .map(FugueToolOutput::LinearUpsertWorkpad),
        FugueToolOperation::LinearAddComment => client
            .add_comment(parse(arguments)?)
            .await
            .map(FugueToolOutput::LinearAddComment),
        FugueToolOperation::LinearListProjectStates => client
            .list_project_states()
            .await
            .map(FugueToolOutput::LinearListProjectStates),
        FugueToolOperation::LinearListRecentIssueChanges => client
            .recent_issue_changes(parse(arguments)?)
            .await
            .map(FugueToolOutput::LinearListRecentIssueChanges),
        FugueToolOperation::LinearEnsureWorkflowStates => {
            let input: EnsureWorkflowStatesInput = parse(arguments)?;
            let states = client.list_project_states().await?.states;
            if !input.states.iter().all(|required| {
                states
                    .iter()
                    .any(|state| state.name.eq_ignore_ascii_case(&required.name))
            }) {
                return Err(invalid(
                    "Jira workflow statuses cannot be created by Fugue; ask the project administrator to configure the required workflow",
                ));
            }
            Ok(FugueToolOutput::LinearEnsureWorkflowStates(
                EnsureWorkflowStatesOutput {
                    existing: states.clone(),
                    states,
                    created: Vec::new(),
                },
            ))
        }
        _ => Err(invalid("Unsupported Jira tool operation")),
    }
}

impl JiraUser {
    fn id(&self) -> &str {
        match &self.identity {
            JiraUserIdentity::Cloud { account_id } => account_id,
            JiraUserIdentity::Server { name } => name,
        }
    }
}

fn find_workpad<'a>(
    comments: &'a [JiraComment],
    heading: &str,
    issue: &str,
) -> Result<Option<&'a JiraComment>> {
    let heading = crate::linear_tools::normalize_workpad_heading(heading, issue);
    let issue_heading = format!("## {issue} Workpad");
    let mut matching = comments.iter().filter(|comment| {
        comment.body.markdown().lines().any(|line| {
            let text = line.trim().trim_start_matches('#').trim();
            text.eq_ignore_ascii_case(heading.trim_start_matches('#').trim())
                || text.eq_ignore_ascii_case(issue_heading.trim_start_matches('#').trim())
        })
    });
    let found = matching.next();
    if matching.next().is_some() {
        return Err(invalid(
            "Multiple Jira Fugue Workpad comments exist; resolve duplicates before updating",
        ));
    }
    Ok(found)
}

#[derive(Deserialize)]
struct CommentPage {
    comments: Vec<JiraComment>,
    total: usize,
}
#[derive(Deserialize)]
struct JiraComment {
    id: String,
    body: JiraBody,
}
#[derive(Serialize)]
struct CommentBody {
    body: JiraBody,
}
#[derive(Deserialize)]
struct Transitions {
    transitions: Vec<JiraTransition>,
}
#[derive(Deserialize)]
struct JiraTransition {
    id: String,
    to: JiraStatus,
}
#[derive(Serialize)]
struct TransitionBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<&'a BTreeMap<String, Box<serde_json::value::RawValue>>>,
    transition: JiraId,
}
#[derive(Serialize)]
#[serde(untagged)]
enum Assignment<'a> {
    Cloud {
        #[serde(rename = "accountId")]
        account_id: &'a str,
    },
    Server {
        name: &'a str,
    },
}
#[derive(Deserialize)]
struct ProjectStatuses {
    statuses: Vec<JiraStatus>,
}
#[derive(Serialize)]
struct CreateIssueBody<'a> {
    fields: CreateIssueFields<'a>,
}
#[derive(Serialize)]
struct CreateIssueFields<'a> {
    #[serde(flatten)]
    additional: &'a BTreeMap<String, Box<serde_json::value::RawValue>>,
    project: ProjectKey<'a>,
    summary: &'a str,
    description: JiraBody,
    issuetype: IssueTypeName<'a>,
}
#[derive(Serialize)]
struct ProjectKey<'a> {
    key: &'a str,
}
#[derive(Serialize)]
struct IssueTypeName<'a> {
    name: &'a str,
}
#[derive(Deserialize)]
struct ChangelogPage {
    values: Vec<ChangelogEntry>,
    total: usize,
}
#[derive(Deserialize)]
struct ChangelogEntry {
    created: DateTime<Utc>,
    author: Option<JiraUser>,
    items: Vec<ChangelogItem>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChangelogItem {
    field: String,
    from_string: Option<String>,
    to_string: Option<String>,
}

fn issue_key(issue: &LinearIssueRef) -> &str {
    match issue {
        LinearIssueRef::Identifier { identifier } => &identifier.0,
        LinearIssueRef::Id { id } => &id.0,
    }
}

const ISSUE_FIELDS: &[&str] = &[
    "summary",
    "description",
    "status",
    "project",
    "assignee",
    "labels",
    "created",
    "updated",
    "priority",
    "issuelinks",
    "attachment",
];

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchRequest<'a> {
    jql: &'a str,
    fields: &'a [&'a str],
    max_results: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_at: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchPage {
    issues: Vec<JiraIssue>,
    total: Option<usize>,
    next_page_token: Option<String>,
    is_last: Option<bool>,
}

#[derive(Deserialize)]
struct JiraIssue {
    id: String,
    key: String,
    fields: JiraFields,
}

#[derive(Deserialize)]
struct JiraFields {
    summary: String,
    description: Option<JiraBody>,
    status: JiraStatus,
    project: JiraProject,
    assignee: Option<JiraUser>,
    #[serde(default)]
    labels: Vec<String>,
    created: Option<DateTime<Utc>>,
    updated: DateTime<Utc>,
    priority: Option<JiraPriority>,
    #[serde(default)]
    issuelinks: Vec<JiraIssueLink>,
    #[serde(default)]
    attachment: Vec<JiraAttachment>,
}

#[derive(Serialize, Deserialize)]
struct JiraId {
    id: String,
}
#[derive(Deserialize)]
struct JiraPriority {
    #[serde(deserialize_with = "deserialize_priority")]
    id: i64,
}
fn deserialize_priority<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<i64, D::Error> {
    let id = String::deserialize(deserializer)?;
    id.parse().map_err(serde::de::Error::custom)
}
#[derive(Deserialize)]
struct JiraProject {
    id: String,
    key: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JiraStatus {
    id: String,
    name: String,
    status_category: JiraStatusCategory,
}
#[derive(Deserialize)]
struct JiraStatusCategory {
    key: JiraStatusCategoryKey,
}
#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum JiraStatusCategoryKey {
    New,
    Indeterminate,
    Done,
}

impl JiraStatus {
    fn snapshot(&self, terminal: &[String]) -> LinearStateSnapshot {
        let state_type = if terminal
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&self.name))
            || self.status_category.key == JiraStatusCategoryKey::Done
        {
            LinearStateType::Completed
        } else if self.status_category.key == JiraStatusCategoryKey::New {
            LinearStateType::Unstarted
        } else {
            LinearStateType::Started
        };
        LinearStateSnapshot {
            id: LinearStateId(self.id.clone()),
            name: self.name.clone(),
            state_type,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JiraUser {
    #[serde(flatten)]
    identity: JiraUserIdentity,
    display_name: String,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum JiraUserIdentity {
    Cloud {
        #[serde(rename = "accountId")]
        account_id: String,
    },
    Server {
        name: String,
    },
}
impl JiraUser {
    fn snapshot(&self) -> LinearUserSnapshot {
        LinearUserSnapshot {
            id: LinearUserId(self.id().to_string()),
            name: self.display_name.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JiraIssueLink {
    #[serde(rename = "type")]
    link_type: JiraLinkType,
    inward_issue: Option<JiraLinkedIssue>,
}
#[derive(Deserialize)]
struct JiraLinkType {
    name: String,
}
#[derive(Deserialize)]
struct JiraLinkedIssue {
    id: String,
    key: String,
    fields: JiraLinkedFields,
}
#[derive(Deserialize)]
struct JiraLinkedFields {
    status: JiraStatus,
}
#[derive(Deserialize)]
struct JiraAttachment {
    id: String,
    filename: String,
    content: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum JiraBody {
    Text(String),
    Document(AdfDocument),
}
impl JiraBody {
    fn markdown(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Document(doc) => AdfNodes(&doc.content, "\n").to_string(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct AdfDocument {
    #[serde(rename = "type")]
    kind: String,
    version: u8,
    content: Vec<AdfNode>,
}
impl AdfDocument {
    fn from_markdown(text: &str) -> Self {
        Self {
            kind: "doc".to_string(),
            version: 1,
            content: text
                .split('\n')
                .map(|line| {
                    let level = line.chars().take_while(|c| *c == '#').count();
                    let heading =
                        level > 0 && level <= 6 && line.as_bytes().get(level) == Some(&b' ');
                    let text = if heading { &line[level + 1..] } else { line };
                    AdfNode {
                        kind: if heading {
                            AdfKind::Heading
                        } else {
                            AdfKind::Paragraph
                        },
                        text: None,
                        attrs: heading.then_some(AdfAttrs {
                            level: Some(level),
                            ..Default::default()
                        }),
                        marks: Vec::new(),
                        content: if text.is_empty() {
                            Vec::new()
                        } else {
                            vec![AdfNode {
                                kind: AdfKind::Text,
                                text: Some(text.to_string()),
                                content: Vec::new(),
                                marks: Vec::new(),
                                attrs: None,
                            }]
                        },
                    }
                })
                .collect(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum AdfKind {
    Text,
    Paragraph,
    Heading,
    HardBreak,
    BulletList,
    OrderedList,
    ListItem,
    CodeBlock,
    Blockquote,
    Mention,
    InlineCard,
    #[serde(other)]
    Unknown,
}
#[derive(Clone, Serialize, Deserialize)]
struct AdfNode {
    #[serde(rename = "type")]
    kind: AdfKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    content: Vec<AdfNode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    marks: Vec<AdfMark>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attrs: Option<AdfAttrs>,
}
#[derive(Clone, Serialize, Deserialize)]
struct AdfMark {
    #[serde(rename = "type")]
    kind: String,
    attrs: Option<AdfAttrs>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
struct AdfAttrs {
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    href: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
}
struct AdfNodes<'a>(&'a [AdfNode], &'static str);
impl std::fmt::Display for AdfNodes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, node) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(self.1)?;
            }
            write!(f, "{node}")?;
        }
        Ok(())
    }
}
impl std::fmt::Display for AdfNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AdfNodes(&self.content, "");
        match self.kind {
            AdfKind::Text => {
                let text = self.text.as_deref().unwrap_or_default();
                if let Some(href) = self
                    .marks
                    .iter()
                    .filter(|mark| mark.kind == "link")
                    .find_map(|mark| mark.attrs.as_ref()?.href.as_deref())
                {
                    write!(f, "[{text}]({href})")
                } else {
                    f.write_str(text)
                }
            }
            AdfKind::Heading => {
                let level = self
                    .attrs
                    .as_ref()
                    .and_then(|attrs| attrs.level)
                    .unwrap_or(2)
                    .clamp(1, 6);
                for _ in 0..level {
                    f.write_str("#")?;
                }
                write!(f, " {inner}")
            }
            AdfKind::HardBreak => f.write_str("\n"),
            AdfKind::BulletList | AdfKind::OrderedList => {
                for (index, node) in self.content.iter().enumerate() {
                    if index > 0 {
                        f.write_str("\n")?;
                    }
                    write!(f, "- {node}")?;
                }
                Ok(())
            }
            AdfKind::CodeBlock => write!(f, "```\n{inner}\n```"),
            AdfKind::Mention | AdfKind::InlineCard => f.write_str(
                self.attrs
                    .as_ref()
                    .and_then(|attrs| attrs.text.as_deref().or(attrs.url.as_deref()))
                    .unwrap_or_default(),
            ),
            AdfKind::ListItem => write!(f, "{}", AdfNodes(&self.content, "\n")),
            _ => write!(f, "{inner}"),
        }
    }
}

#[cfg(test)]
mod tests;
