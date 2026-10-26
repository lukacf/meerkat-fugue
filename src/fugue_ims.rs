use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use serde_json::Value;

use crate::config::TrackerConfig;
use crate::domain::{BlockerRef, Issue};
use crate::error::{FugueError, Result};
use crate::ims::{
    ImsCommentAttachmentSnapshot, ImsCommentId, ImsCommentSnapshot, ImsIssueId, ImsIssueKey,
    ImsIssueRef, ImsIssueSnapshot, ImsRepoIssueQuery, ImsRepoScope, ImsStageName,
    IssueManagementSystem,
};
use crate::tools::IdempotencyKey;
use crate::tools::{
    AddCommentInput, AddCommentOutput, EnsureWorkflowStatesInput, EnsureWorkflowStatesOutput,
    InspectIssueInput, InspectIssueOutput, LinearCommentAttachmentInput, LinearCommentId,
    LinearFileAttachmentSnapshot, LinearFileAttachmentSource, LinearIssueId, LinearIssueIdentifier,
    LinearIssueRef, LinearIssueSnapshot, LinearStateId, LinearStateRef, LinearStateSnapshot,
    LinearStateType, LinearTeamId, LinearWorkpadSnapshot, ListProjectStatesInput,
    ListProjectStatesOutput, MoveIssueInput, MoveIssueOutput, UpsertWorkpadInput,
    UpsertWorkpadOutput, render_comment_body_with_attachments,
};
use crate::tracker::IssueTracker;

#[derive(Debug, Clone)]
pub struct FugueImsStore {
    path: PathBuf,
    repo: ImsRepoScope,
    active_stages: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CreateFugueIssue {
    pub key: ImsIssueKey,
    pub repo: String,
    pub title: String,
    pub description: Option<String>,
    pub stage: ImsStageName,
    pub labels: Vec<String>,
}

pub fn create_issue_for_config(
    config: &TrackerConfig,
    issue: CreateFugueIssue,
) -> Result<ImsIssueSnapshot> {
    FugueImsStore::from_config(config)?.create_issue(issue)
}

impl FugueImsStore {
    pub fn from_config(config: &TrackerConfig) -> Result<Self> {
        if config.kind != "fugue-ims" {
            return Err(FugueError::UnsupportedTrackerKind(config.kind.clone()));
        }
        let repo = config
            .project_slug
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or(FugueError::MissingTrackerProjectSlug)?;
        let path = fugue_ims_path(&config.endpoint)?;
        let store = Self {
            path,
            repo: ImsRepoScope(repo.to_owned()),
            active_stages: config.active_states.clone(),
        };
        store.initialize()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn repository_scope(&self) -> &str {
        &self.repo.0
    }

    pub(crate) fn resolve_issue_identifier(
        &self,
        issue: &LinearIssueRef,
    ) -> Result<LinearIssueIdentifier> {
        self.resolve_linear_issue_ref(issue)
            .map(|issue| LinearIssueIdentifier(issue.identifier))
    }

    pub fn allocate_issue_key(&self, prefix: &str) -> Result<ImsIssueKey> {
        let sanitized_prefix = sanitize_issue_key_prefix(prefix);
        let now = Utc::now().to_rfc3339();
        let conn = self.connection()?;
        conn.execute(
            "insert into issue_key_allocations (prefix, allocated_at) values (?1, ?2)",
            params![sanitized_prefix, now],
        )
        .map_err(sqlite_error)?;
        let sequence = conn.last_insert_rowid();
        Ok(ImsIssueKey(format!("{sanitized_prefix}-{sequence}")))
    }

    pub fn create_issue(&self, issue: CreateFugueIssue) -> Result<ImsIssueSnapshot> {
        if issue.repo != self.repo.0 {
            return Err(FugueError::ResponseError(
                "issue repository does not match the configured tracker project".into(),
            ));
        }
        let now = Utc::now();
        let id = format!("fugue-ims:{}", issue.key.0);
        let conn = self.connection()?;
        let labels_json = serde_json::to_string(&issue.labels)
            .map_err(|err| FugueError::InvalidConfig(format!("encode fugue-ims labels: {err}")))?;
        let changed = conn.execute(
            "insert into issues \
             (id, key, repo, title, description, stage, branch_name, url, labels_json, created_at, updated_at) \
             values (?1, ?2, ?3, ?4, ?5, ?6, null, ?7, ?8, ?9, ?9) \
             on conflict(key) do update set \
             title = excluded.title, description = excluded.description, \
             stage = excluded.stage, updated_at = excluded.updated_at where issues.repo = excluded.repo",
            params![
                id,
                issue.key.0,
                issue.repo,
                issue.title,
                issue.description,
                issue.stage.0,
                format!("fugue-ims://{}", issue.key.0),
                labels_json,
                now.to_rfc3339(),
            ],
        )
        .map_err(sqlite_error)?;
        if changed == 0 {
            return Err(FugueError::ResponseError(
                "issue key is unavailable in the configured repository".into(),
            ));
        }
        self.snapshot_for_key(&issue.key)
    }

    pub fn update_comment(
        &self,
        comment_id: &ImsCommentId,
        body: &str,
    ) -> Result<ImsCommentSnapshot> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connection()?;
        let changed = conn
            .execute(
                "update comments set body_mark = ?1, updated_at = ?2 where id = ?3 and deleted_at is null and issue_key in (select key from issues where repo = ?4)",
                params![body, now, comment_id.0, self.repo.0],
            )
            .map_err(sqlite_error)?;
        if changed == 0 {
            return Err(FugueError::ResponseError(format!(
                "fugue-ims comment not found: {}",
                comment_id.0
            )));
        }
        Ok(ImsCommentSnapshot {
            id: comment_id.clone(),
            body_mark: body.to_string(),
            attachments: self.attachments_for_comment(comment_id)?,
        })
    }

    pub fn delete_comment(&self, comment_id: &ImsCommentId) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connection()?;
        let changed = conn
            .execute(
                "update comments set deleted_at = ?1, updated_at = ?1 where id = ?2 and deleted_at is null and issue_key in (select key from issues where repo = ?3)",
                params![now, comment_id.0, self.repo.0],
            )
            .map_err(sqlite_error)?;
        if changed == 0 {
            return Err(FugueError::ResponseError(format!(
                "fugue-ims comment not found: {}",
                comment_id.0
            )));
        }
        Ok(())
    }

    pub fn all_open_issues(&self) -> Result<Vec<Issue>> {
        self.issues_by_stage_filter(&[])
    }

    pub async fn inspect_issue_tool(&self, input: InspectIssueInput) -> Result<InspectIssueOutput> {
        let issue = self.resolve_linear_issue_ref(&input.issue)?;
        let workpad = self.workpad_snapshot(&issue.identifier, "## Fugue Workpad")?;
        let comments = self.comments_for_key(&ImsIssueKey(issue.identifier.clone()))?;
        let attachments = fugue_ims_comment_attachment_snapshots(&issue.identifier, &comments);
        Ok(InspectIssueOutput {
            issue: self.linear_snapshot(issue),
            workpad,
            attachments,
        })
    }

    pub async fn move_issue_tool(&self, input: MoveIssueInput) -> Result<MoveIssueOutput> {
        let before = self.resolve_linear_issue_ref(&input.issue)?;
        let target_stage = match input.state {
            LinearStateRef::Name { name } => name,
            LinearStateRef::Id { id } => id.0,
        };
        let moved = self
            .move_issue(
                &ImsIssueRef {
                    key: ImsIssueKey(before.identifier.clone()),
                },
                &ImsStageName(target_stage),
                input.idempotency_key,
            )
            .await?;
        let moved_stage = moved.stage.0;
        let changed = !before.state.eq_ignore_ascii_case(&moved_stage);
        Ok(MoveIssueOutput {
            issue: self.linear_snapshot(Issue {
                id: moved.id.0,
                identifier: moved.key.0,
                title: moved.title,
                description: moved.description,
                priority: None,
                state: moved_stage,
                branch_name: None,
                url: None,
                labels: Vec::new(),
                blocked_by: Vec::new(),
                attachments: Vec::new(),
                created_at: None,
                updated_at: Some(Utc::now()),
            }),
            changed,
            previous_state: Some(self.linear_state_snapshot(&before.state)),
        })
    }

    pub async fn upsert_workpad_tool(
        &self,
        input: UpsertWorkpadInput,
    ) -> Result<UpsertWorkpadOutput> {
        let issue = self.resolve_linear_issue_ref(&input.issue)?;
        let body = workpad_body(&input.heading, &input.body_markdown);
        if let Some(existing) = self.workpad_snapshot(&issue.identifier, &input.heading)? {
            let body =
                crate::deploy_gate::merge_preserved_deploy_evidence(&body, &existing.body_markdown);
            self.update_comment(&ImsCommentId(existing.comment_id.0.clone()), &body)?;
            return Ok(UpsertWorkpadOutput {
                comment_id: existing.comment_id,
                url: existing.url,
                created: false,
                updated: true,
            });
        }
        if !input.create_if_missing {
            return Err(FugueError::ResponseError(format!(
                "fugue-ims workpad missing for {}",
                issue.identifier
            )));
        }
        let comment = self
            .add_comment(
                &ImsIssueRef {
                    key: ImsIssueKey(issue.identifier.clone()),
                },
                &body,
            )
            .await?;
        Ok(UpsertWorkpadOutput {
            comment_id: LinearCommentId(comment.id.0.clone()),
            url: Some(format!(
                "fugue-ims://{}/comments/{}",
                issue.identifier, comment.id.0
            )),
            created: true,
            updated: false,
        })
    }

    pub async fn add_comment_tool(&self, input: AddCommentInput) -> Result<AddCommentOutput> {
        let issue = self.resolve_linear_issue_ref(&input.issue)?;
        let body = render_comment_body_with_attachments(&input.body_markdown, &input.attachments);
        let comment = self
            .add_comment_with_attachments(
                &ImsIssueRef {
                    key: ImsIssueKey(issue.identifier.clone()),
                },
                &body,
                input
                    .attachments
                    .iter()
                    .cloned()
                    .map(ims_attachment_from_comment_input)
                    .collect(),
            )
            .await?;
        let attachments =
            linear_snapshots_from_ims_comment_attachments(&comment.id, comment.attachments.clone());
        Ok(AddCommentOutput {
            comment_id: LinearCommentId(comment.id.0.clone()),
            url: Some(format!(
                "fugue-ims://{}/comments/{}",
                issue.identifier, comment.id.0
            )),
            attachments,
        })
    }

    pub fn list_project_states_tool(
        &self,
        _input: ListProjectStatesInput,
        config: &TrackerConfig,
    ) -> ListProjectStatesOutput {
        ListProjectStatesOutput {
            states: configured_state_snapshots(config),
        }
    }

    pub fn ensure_workflow_states_tool(
        &self,
        input: EnsureWorkflowStatesInput,
        config: &TrackerConfig,
    ) -> EnsureWorkflowStatesOutput {
        let mut states = configured_state_snapshots(config);
        let mut created = Vec::new();
        let mut existing = Vec::new();
        for required in input.states {
            let snapshot = LinearStateSnapshot {
                id: LinearStateId(format!("fugue-ims-state:{}", required.name)),
                name: required.name,
                state_type: required.state_type,
            };
            if states
                .iter()
                .any(|state| state.name.eq_ignore_ascii_case(&snapshot.name))
            {
                existing.push(snapshot);
            } else {
                states.push(snapshot.clone());
                created.push(snapshot);
            }
        }
        EnsureWorkflowStatesOutput {
            states,
            created,
            existing,
        }
    }

    fn initialize(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                FugueError::Workspace(format!(
                    "create fugue-ims storage directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let conn = self.connection()?;
        conn.execute_batch(
            "pragma journal_mode = wal;
             create table if not exists issues (
               id text primary key,
               key text not null unique,
               repo text not null,
               title text not null,
               description text,
               stage text not null,
               branch_name text,
               url text,
               labels_json text not null,
               created_at text not null,
               updated_at text not null
             );
             create table if not exists comments (
               id text primary key,
               issue_key text not null,
               body_mark text not null,
               created_at text not null,
               updated_at text not null,
               deleted_at text,
               foreign key(issue_key) references issues(key)
             );
             create table if not exists comment_attachments (
               comment_id text not null,
               position integer not null,
               title text,
               url text not null,
               path text,
               content_type text,
               byte_len integer not null,
               primary key(comment_id, position),
               foreign key(comment_id) references comments(id)
             );
             create index if not exists comments_issue_key_idx on comments(issue_key);
             create index if not exists comment_attachments_comment_idx on comment_attachments(comment_id, position);
             create index if not exists issues_repo_stage_idx on issues(repo, stage);
             create index if not exists issues_repo_stage_ci_created_key_idx on issues(repo, lower(stage), created_at, key);
             create index if not exists issues_stage_ci_created_key_idx on issues(lower(stage), created_at, key);
             create table if not exists issue_key_allocations (
               id integer primary key autoincrement,
               prefix text not null,
               allocated_at text not null
             );",
        )
        .map_err(sqlite_error)?;
        Ok(())
    }

    fn connection(&self) -> Result<Connection> {
        Connection::open(&self.path).map_err(sqlite_error)
    }

    fn issue_by_key(&self, key: &ImsIssueKey) -> Result<Option<Issue>> {
        let conn = self.connection()?;
        conn.query_row(
            "select id, key, title, description, stage, branch_name, url, labels_json, created_at, updated_at \
             from issues where key = ?1 and repo = ?2",
            params![key.0, self.repo.0],
            issue_from_row,
        )
        .optional()
        .map_err(sqlite_error)
    }

    fn resolve_linear_issue_ref(&self, issue_ref: &LinearIssueRef) -> Result<Issue> {
        match issue_ref {
            LinearIssueRef::Identifier { identifier } => self
                .issue_by_key(&ImsIssueKey(identifier.0.clone()))?
                .ok_or_else(|| {
                    FugueError::ResponseError(format!(
                        "fugue-ims issue not found: {}",
                        identifier.0
                    ))
                }),
            LinearIssueRef::Id { id } => {
                let conn = self.connection()?;
                conn.query_row(
                    "select id, key, title, description, stage, branch_name, url, labels_json, created_at, updated_at \
                     from issues where id = ?1 and repo = ?2",
                    params![id.0, self.repo.0],
                    issue_from_row,
                )
                .optional()
                .map_err(sqlite_error)?
                .ok_or_else(|| {
                    FugueError::ResponseError(format!("fugue-ims issue not found: {}", id.0))
                })
            }
        }
    }

    fn workpad_snapshot(
        &self,
        issue_identifier: &str,
        heading: &str,
    ) -> Result<Option<LinearWorkpadSnapshot>> {
        let comments = self.comments_for_key(&ImsIssueKey(issue_identifier.to_string()))?;
        let issue_heading = format!("## {issue_identifier} Workpad");
        Ok(comments.into_iter().find_map(|comment| {
            let first_line = comment.body_mark.lines().next().map(str::trim);
            if first_line == Some(heading) || first_line == Some(issue_heading.as_str()) {
                Some(LinearWorkpadSnapshot {
                    comment_id: LinearCommentId(comment.id.0.clone()),
                    heading: first_line.unwrap_or(heading).to_string(),
                    body_markdown: comment.body_mark,
                    url: Some(format!(
                        "fugue-ims://{issue_identifier}/comments/{}",
                        comment.id.0
                    )),
                })
            } else {
                None
            }
        }))
    }

    fn linear_snapshot(&self, issue: Issue) -> LinearIssueSnapshot {
        LinearIssueSnapshot {
            id: LinearIssueId(issue.id),
            identifier: LinearIssueIdentifier(issue.identifier),
            title: issue.title,
            description: issue.description,
            url: issue.url,
            state: self.linear_state_snapshot(&issue.state),
            assignee: None,
            branch_name: issue.branch_name.map(crate::tools::BranchName),
            labels: issue.labels,
            updated_at: issue.updated_at.unwrap_or_else(Utc::now),
            team_id: LinearTeamId("fugue-ims".to_string()),
        }
    }

    fn linear_state_snapshot(&self, name: &str) -> LinearStateSnapshot {
        LinearStateSnapshot {
            id: LinearStateId(format!("fugue-ims-state:{name}")),
            name: name.to_string(),
            state_type: state_type_for_name(name),
        }
    }

    pub fn comments_for_issue_identifier(
        &self,
        issue_identifier: &str,
    ) -> Result<Vec<ImsCommentSnapshot>> {
        self.comments_for_key(&ImsIssueKey(issue_identifier.to_string()))
    }

    fn comments_for_key(&self, key: &ImsIssueKey) -> Result<Vec<ImsCommentSnapshot>> {
        self.snapshot_for_key(key)?;
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare(
                "select id, body_mark from comments \
                 where issue_key = ?1 and deleted_at is null and issue_key in (select key from issues where repo = ?2) order by created_at asc, id asc",
            )
            .map_err(sqlite_error)?;
        stmt.query_map(params![key.0, self.repo.0], |row| {
            Ok(ImsCommentSnapshot {
                id: ImsCommentId(row.get(0)?),
                body_mark: row.get(1)?,
                attachments: Vec::new(),
            })
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)
        .and_then(|comments| self.attachments_for_comments(comments))
    }

    fn attachments_for_comments(
        &self,
        mut comments: Vec<ImsCommentSnapshot>,
    ) -> Result<Vec<ImsCommentSnapshot>> {
        for comment in &mut comments {
            comment.attachments = self.attachments_for_comment(&comment.id)?;
        }
        Ok(comments)
    }

    fn attachments_for_comment(
        &self,
        comment_id: &ImsCommentId,
    ) -> Result<Vec<ImsCommentAttachmentSnapshot>> {
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare(
                "select title, url, path, content_type, byte_len from comment_attachments \
                 where comment_id = ?1 and comment_id in (select comments.id from comments join issues on issues.key = comments.issue_key where issues.repo = ?2 and comments.deleted_at is null) order by position asc",
            )
            .map_err(sqlite_error)?;
        stmt.query_map(params![comment_id.0, self.repo.0], |row| {
            Ok(ImsCommentAttachmentSnapshot {
                title: row.get(0)?,
                url: row.get(1)?,
                path: row.get(2)?,
                content_type: row.get(3)?,
                byte_len: row.get::<_, i64>(4)?.max(0) as u64,
            })
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)
    }

    fn issues_by_stage_filter(&self, stages: &[String]) -> Result<Vec<Issue>> {
        let conn = self.connection()?;
        if !stages.is_empty() {
            let normalized_stages = stages
                .iter()
                .map(|stage| stage.trim().to_ascii_lowercase())
                .filter(|stage| !stage.is_empty())
                .collect::<Vec<_>>();
            if normalized_stages.is_empty() {
                return Ok(Vec::new());
            }
            let placeholders = (2..=normalized_stages.len() + 1)
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "select id, key, title, description, stage, branch_name, url, labels_json, created_at, updated_at \
                 from issues where repo = ?1 and lower(stage) in ({placeholders}) order by created_at asc, key asc",
            );
            let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
            return stmt
                .query_map(
                    params_from_iter(std::iter::once(&self.repo.0).chain(normalized_stages.iter())),
                    issue_from_row,
                )
                .map_err(sqlite_error)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(sqlite_error);
        }
        let mut stmt = conn
            .prepare(
                "select id, key, title, description, stage, branch_name, url, labels_json, created_at, updated_at \
                 from issues where repo = ?1 order by created_at asc, key asc",
            )
            .map_err(sqlite_error)?;
        let issues = stmt
            .query_map(params![self.repo.0], issue_from_row)
            .map_err(sqlite_error)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)?;
        // An empty stage filter includes terminal history for repository views.
        Ok(issues)
    }
}

#[async_trait]
impl IssueTracker for FugueImsStore {
    async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>> {
        self.fetch_issues_by_states(&self.active_stages).await
    }

    async fn fetch_issues_by_states(&self, states: &[String]) -> Result<Vec<Issue>> {
        self.issues_by_stage_filter(states)
    }

    async fn fetch_issue_states_by_ids(&self, issue_ids: &[String]) -> Result<Vec<Issue>> {
        if issue_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.connection()?;
        let placeholders = (2..=issue_ids.len() + 1)
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "select id, key, title, description, stage, branch_name, url, labels_json, created_at, updated_at from issues where repo = ?1 and id in ({placeholders}) order by created_at asc, key asc"
        );
        let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
        stmt.query_map(
            params_from_iter(std::iter::once(&self.repo.0).chain(issue_ids.iter())),
            issue_from_row,
        )
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)
    }

    async fn graphql(&self, _query: &str, _variables: Value) -> Result<Value> {
        Err(FugueError::ResponseError(
            "fugue-ims does not expose a GraphQL endpoint".to_string(),
        ))
    }
}

#[async_trait]
impl IssueManagementSystem for FugueImsStore {
    async fn list_repo_issues(&self, query: &ImsRepoIssueQuery) -> Result<Vec<ImsIssueSnapshot>> {
        if query.repo != self.repo {
            return Err(FugueError::ResponseError(
                "repository query does not match the configured tracker project".into(),
            ));
        }
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare(
                "select id, key, title, description, stage, branch_name, url, labels_json, created_at, updated_at \
                 from issues where repo = ?1 order by created_at asc, key asc",
            )
            .map_err(sqlite_error)?;
        let issues = stmt
            .query_map(params![query.repo.0], issue_from_row)
            .map_err(sqlite_error)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)?;
        Ok(issues.into_iter().map(ims_snapshot_from_issue).collect())
    }

    async fn issue(&self, issue: &ImsIssueRef) -> Result<ImsIssueSnapshot> {
        self.snapshot_for_key(&issue.key)
    }

    async fn comments(&self, issue: &ImsIssueRef) -> Result<Vec<ImsCommentSnapshot>> {
        self.comments_for_key(&issue.key)
    }

    async fn move_issue(
        &self,
        issue: &ImsIssueRef,
        stage: &ImsStageName,
        _idempotency_key: IdempotencyKey,
    ) -> Result<ImsIssueSnapshot> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connection()?;
        let changed = conn
            .execute(
                "update issues set stage = ?1, updated_at = ?2 where key = ?3 and repo = ?4",
                params![stage.0, now, issue.key.0, self.repo.0],
            )
            .map_err(sqlite_error)?;
        if changed == 0 {
            return Err(FugueError::ResponseError(format!(
                "fugue-ims issue not found: {}",
                issue.key.0
            )));
        }
        self.snapshot_for_key(&issue.key)
    }

    async fn add_comment(&self, issue: &ImsIssueRef, body: &str) -> Result<ImsCommentSnapshot> {
        self.add_comment_with_attachments(issue, body, Vec::new())
            .await
    }

    async fn add_comment_with_attachments(
        &self,
        issue: &ImsIssueRef,
        body: &str,
        attachments: Vec<ImsCommentAttachmentSnapshot>,
    ) -> Result<ImsCommentSnapshot> {
        let now = Utc::now().to_rfc3339();
        let id = format!("fugue-ims-comment:{}:{now}", issue.key.0);
        let mut conn = self.connection()?;
        let exists = self.issue_by_key(&issue.key)?.is_some();
        if !exists {
            return Err(FugueError::ResponseError(format!(
                "fugue-ims issue not found: {}",
                issue.key.0
            )));
        }
        let tx = conn.transaction().map_err(sqlite_error)?;
        let changed = tx.execute(
            "insert into comments (id, issue_key, body_mark, created_at, updated_at, deleted_at) \
             select ?1, ?2, ?3, ?4, ?4, null where exists (select 1 from issues where key = ?2 and repo = ?5)",
            params![id, issue.key.0, body, now, self.repo.0],
        )
        .map_err(sqlite_error)?;
        if changed == 0 {
            return Err(FugueError::ResponseError(
                "issue is unavailable in the configured repository".into(),
            ));
        }
        for (position, attachment) in attachments.iter().enumerate() {
            tx.execute(
                "insert into comment_attachments \
                 (comment_id, position, title, url, path, content_type, byte_len) \
                 values (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    id,
                    position as i64,
                    attachment.title,
                    attachment.url,
                    attachment.path,
                    attachment.content_type,
                    i64::try_from(attachment.byte_len).unwrap_or(i64::MAX),
                ],
            )
            .map_err(sqlite_error)?;
        }
        tx.commit().map_err(sqlite_error)?;
        Ok(ImsCommentSnapshot {
            id: ImsCommentId(id),
            body_mark: body.to_string(),
            attachments,
        })
    }
}

impl FugueImsStore {
    fn snapshot_for_key(&self, key: &ImsIssueKey) -> Result<ImsIssueSnapshot> {
        self.issue_by_key(key)?
            .map(ims_snapshot_from_issue)
            .ok_or_else(|| {
                FugueError::ResponseError(format!("fugue-ims issue not found: {}", key.0))
            })
    }
}

pub(crate) fn fugue_ims_path(endpoint: &str) -> Result<PathBuf> {
    endpoint
        .strip_prefix("sqlite://")
        .map(PathBuf::from)
        .or_else(|| endpoint.strip_prefix("file://").map(PathBuf::from))
        .or_else(|| (!endpoint.trim().is_empty()).then(|| PathBuf::from(endpoint)))
        .ok_or_else(|| {
            FugueError::InvalidConfig(
                "tracker.endpoint must point at a fugue-ims sqlite database".to_string(),
            )
        })
}

fn issue_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Issue> {
    let labels_json: String = row.get(7)?;
    let labels = serde_json::from_str::<Vec<String>>(&labels_json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(err))
    })?;
    Ok(Issue {
        id: row.get(0)?,
        identifier: row.get(1)?,
        title: row.get(2)?,
        description: row.get(3)?,
        priority: None,
        state: row.get(4)?,
        branch_name: row.get(5)?,
        url: row.get(6)?,
        labels,
        blocked_by: Vec::<BlockerRef>::new(),
        attachments: Vec::new(),
        created_at: parse_datetime(row.get::<_, String>(8)?.as_str()),
        updated_at: parse_datetime(row.get::<_, String>(9)?.as_str()),
    })
}

fn ims_snapshot_from_issue(issue: Issue) -> ImsIssueSnapshot {
    ImsIssueSnapshot {
        id: ImsIssueId(issue.id),
        key: ImsIssueKey(issue.identifier),
        title: issue.title,
        description: issue.description,
        stage: ImsStageName(issue.state),
    }
}

fn ims_attachment_from_comment_input(
    attachment: LinearCommentAttachmentInput,
) -> ImsCommentAttachmentSnapshot {
    ImsCommentAttachmentSnapshot {
        title: attachment.title,
        url: attachment.url,
        path: attachment.path,
        content_type: attachment.content_type,
        byte_len: attachment.byte_len,
    }
}

fn linear_snapshots_from_ims_comment_attachments(
    comment_id: &ImsCommentId,
    attachments: Vec<ImsCommentAttachmentSnapshot>,
) -> Vec<LinearFileAttachmentSnapshot> {
    attachments
        .into_iter()
        .map(|attachment| LinearFileAttachmentSnapshot {
            source: LinearFileAttachmentSource::Comment {
                comment_id: LinearCommentId(comment_id.0.clone()),
            },
            title: attachment.title,
            path: attachment.path.unwrap_or_else(|| attachment.url.clone()),
            url: attachment.url,
            content_type: attachment.content_type,
            byte_len: attachment.byte_len,
        })
        .collect()
}

fn fugue_ims_comment_attachment_snapshots(
    issue_identifier: &str,
    comments: &[ImsCommentSnapshot],
) -> Vec<LinearFileAttachmentSnapshot> {
    let mut snapshots = Vec::new();
    for comment in comments {
        if comment.attachments.is_empty() {
            snapshots.extend(legacy_fugue_ims_comment_attachment_snapshots(
                issue_identifier,
                comment,
            ));
        } else {
            snapshots.extend(linear_snapshots_from_ims_comment_attachments(
                &comment.id,
                comment.attachments.clone(),
            ));
        }
    }
    snapshots
}

fn legacy_fugue_ims_comment_attachment_snapshots(
    issue_identifier: &str,
    comment: &ImsCommentSnapshot,
) -> Vec<LinearFileAttachmentSnapshot> {
    let mut snapshots = Vec::new();
    for link in markdown_links(&comment.body_mark) {
        if !is_fugue_ims_attachment_url(issue_identifier, &link.url) {
            continue;
        }
        let path = fugue_ims_attachment_path(issue_identifier, &link.url)
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| link.url.clone());
        let byte_len = std::fs::metadata(&path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        snapshots.push(LinearFileAttachmentSnapshot {
            source: LinearFileAttachmentSource::Comment {
                comment_id: LinearCommentId(comment.id.0.clone()),
            },
            title: Some(link.title),
            url: link.url,
            path,
            content_type: None,
            byte_len,
        });
    }
    snapshots
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MarkdownLink {
    title: String,
    url: String,
}

fn markdown_links(body: &str) -> Vec<MarkdownLink> {
    let mut links = Vec::new();
    for line in body.lines() {
        let Some(open_label) = line.find('[') else {
            continue;
        };
        let Some(close_label_rel) = line[open_label + 1..].find(']') else {
            continue;
        };
        let close_label = open_label + 1 + close_label_rel;
        let after_label = &line[close_label + 1..];
        let Some(open_url_rel) = after_label.find('(') else {
            continue;
        };
        let open_url = close_label + 1 + open_url_rel;
        let Some(close_url_rel) = line[open_url + 1..].find(')') else {
            continue;
        };
        let close_url = open_url + 1 + close_url_rel;
        let title = line[open_label + 1..close_label]
            .replace("\\[", "[")
            .replace("\\]", "]");
        let url = line[open_url + 1..close_url].to_string();
        links.push(MarkdownLink { title, url });
    }
    links
}

fn is_fugue_ims_attachment_url(issue_identifier: &str, url: &str) -> bool {
    let issue_prefix = format!(
        "/api/v1/issues/{}/attachments/",
        percent_encode_path_segment(issue_identifier)
    );
    url.starts_with(&issue_prefix)
}

fn fugue_ims_attachment_path(issue_identifier: &str, url: &str) -> Option<PathBuf> {
    let file_name = url.rsplit('/').next()?;
    let decoded = percent_decode_path_segment(file_name)?;
    Some(
        std::env::current_dir()
            .ok()?
            .join(".fugue")
            .join("issue-attachments")
            .join(sanitize_attachment_component(issue_identifier))
            .join(sanitize_attachment_component(&decoded)),
    )
}

fn sanitize_attachment_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let trimmed = sanitized.trim_matches(['.', '-']).to_string();
    if trimmed.is_empty() {
        "attachment".to_string()
    } else {
        trimmed
    }
}

fn percent_encode_path_segment(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_') {
                vec![byte as char]
            } else {
                format!("%{byte:02X}").chars().collect()
            }
        })
        .collect()
}

fn percent_decode_path_segment(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hi = hex_value(*bytes.get(index + 1)?)?;
            let lo = hex_value(*bytes.get(index + 2)?)?;
            decoded.push((hi << 4) | lo);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|datetime| datetime.with_timezone(&Utc))
}

fn configured_state_snapshots(config: &TrackerConfig) -> Vec<LinearStateSnapshot> {
    let mut names = Vec::<String>::new();
    for name in config
        .intake_states
        .iter()
        .chain(config.in_flight_states.iter())
        .chain(config.active_states.iter())
        .chain(config.terminal_states.iter())
    {
        if !names
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(name))
        {
            names.push(name.clone());
        }
    }
    names
        .into_iter()
        .map(|name| LinearStateSnapshot {
            id: LinearStateId(format!("fugue-ims-state:{name}")),
            state_type: state_type_for_configured_name(config, &name),
            name,
        })
        .collect()
}

fn state_type_for_configured_name(config: &TrackerConfig, name: &str) -> LinearStateType {
    if name.eq_ignore_ascii_case("Backlog") {
        return LinearStateType::Backlog;
    }
    if config
        .terminal_states
        .iter()
        .any(|terminal| terminal.eq_ignore_ascii_case(name))
    {
        return terminal_state_type_for_name(name);
    }
    if config
        .intake_states
        .iter()
        .any(|intake| intake.eq_ignore_ascii_case(name))
    {
        return LinearStateType::Unstarted;
    }
    LinearStateType::Started
}

fn state_type_for_name(name: &str) -> LinearStateType {
    match name.to_ascii_lowercase().as_str() {
        "backlog" => LinearStateType::Backlog,
        "todo" | "rework" => LinearStateType::Unstarted,
        "done" => LinearStateType::Completed,
        "canceled" | "cancelled" => LinearStateType::Canceled,
        "duplicate" => LinearStateType::Duplicate,
        _ => LinearStateType::Started,
    }
}

fn terminal_state_type_for_name(name: &str) -> LinearStateType {
    match name.to_ascii_lowercase().as_str() {
        "canceled" | "cancelled" => LinearStateType::Canceled,
        "duplicate" => LinearStateType::Duplicate,
        "done" => LinearStateType::Completed,
        _ => LinearStateType::Completed,
    }
}

fn workpad_body(heading: &str, body_markdown: &str) -> String {
    let body = body_markdown.trim();
    if body.starts_with(heading) {
        body.to_string()
    } else if body.is_empty() {
        heading.to_string()
    } else {
        format!("{heading}\n\n{body}")
    }
}

fn sqlite_error(error: rusqlite::Error) -> FugueError {
    FugueError::ResponseError(format!("fugue-ims sqlite error: {error}"))
}

pub fn fugue_ims_tracker_config(path: &Path) -> TrackerConfig {
    TrackerConfig {
        jira: None,
        kind: "fugue-ims".to_string(),
        endpoint: path.display().to_string(),
        api_key: None,
        project_slug: Some("fugue-ims".to_string()),
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
            "Rework".to_string(),
        ],
        attachment_root: None,
        max_attachment_files: 20,
        max_attachment_bytes: 25 * 1024 * 1024,
        max_attachment_total_bytes: 100 * 1024 * 1024,
        terminal_states: vec![
            "Done".to_string(),
            "Canceled".to_string(),
            "Duplicate".to_string(),
        ],
    }
}

fn sanitize_issue_key_prefix(prefix: &str) -> String {
    let sanitized = prefix
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .find(|part| !part.is_empty())
        .map(|part| part.to_ascii_uppercase())
        .unwrap_or_else(|| "FUG".to_string());
    if sanitized.is_empty() {
        "FUG".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ims::ImsRepoScope;

    #[test]
    fn sqlite_ims_exposes_linear_compatible_state_types_from_config() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = fugue_ims_tracker_config(&temp.path().join("ims.sqlite"));
        config.intake_states = vec!["Ready".to_string(), "Rework".to_string()];
        config.in_flight_states = vec!["Build".to_string()];
        config.active_states = vec![
            "Ready".to_string(),
            "Rework".to_string(),
            "Build".to_string(),
            "Deploy".to_string(),
        ];
        config.terminal_states = vec![
            "Shipped".to_string(),
            "Canceled".to_string(),
            "Duplicate".to_string(),
        ];

        let states = configured_state_snapshots(&config)
            .into_iter()
            .map(|state| (state.name, state.state_type))
            .collect::<Vec<_>>();

        assert!(states.contains(&("Ready".to_string(), LinearStateType::Unstarted)));
        assert!(states.contains(&("Rework".to_string(), LinearStateType::Unstarted)));
        assert!(states.contains(&("Build".to_string(), LinearStateType::Started)));
        assert!(states.contains(&("Deploy".to_string(), LinearStateType::Started)));
        assert!(states.contains(&("Shipped".to_string(), LinearStateType::Completed)));
        assert!(states.contains(&("Canceled".to_string(), LinearStateType::Canceled)));
        assert!(states.contains(&("Duplicate".to_string(), LinearStateType::Duplicate)));
    }

    #[tokio::test]
    async fn sqlite_ims_persists_issue_stage_and_comments() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = fugue_ims_tracker_config(&temp.path().join("ims.sqlite"));
        config.project_slug = Some("owner/repo".into());
        let store = FugueImsStore::from_config(&config).expect("store");
        store
            .create_issue(CreateFugueIssue {
                key: ImsIssueKey("FUG-1".to_string()),
                repo: "owner/repo".to_string(),
                title: "Do the thing".to_string(),
                description: Some("Body".to_string()),
                stage: ImsStageName("Todo".to_string()),
                labels: Vec::new(),
            })
            .expect("issue");

        let issue_ref = ImsIssueRef {
            key: ImsIssueKey("FUG-1".to_string()),
        };
        let moved = store
            .move_issue(
                &issue_ref,
                &ImsStageName("Planning".to_string()),
                IdempotencyKey("move-1".to_string()),
            )
            .await
            .expect("move");
        assert_eq!(moved.stage, ImsStageName("Planning".to_string()));

        let comment = store
            .add_comment(&issue_ref, "hello")
            .await
            .expect("comment");
        assert_eq!(comment.body_mark, "hello");
        store
            .update_comment(&comment.id, "hello again")
            .expect("update");
        assert_eq!(
            store.comments(&issue_ref).await.expect("comments")[0].body_mark,
            "hello again"
        );
        store.delete_comment(&comment.id).expect("delete");
        assert!(
            store
                .comments(&issue_ref)
                .await
                .expect("comments")
                .is_empty()
        );

        let scoped = store
            .list_repo_issues(&ImsRepoIssueQuery {
                repo: ImsRepoScope("owner/repo".to_string()),
            })
            .await
            .expect("repo issues");
        assert_eq!(scoped.len(), 1);
    }

    #[tokio::test]
    async fn sqlite_ims_add_comment_accepts_and_inspects_comment_attachments() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = fugue_ims_tracker_config(&temp.path().join("ims.sqlite"));
        config.project_slug = Some("owner/repo".into());
        let store = FugueImsStore::from_config(&config).expect("store");
        store
            .create_issue(CreateFugueIssue {
                key: ImsIssueKey("FUG-2".to_string()),
                repo: "owner/repo".to_string(),
                title: "Inspect comment attachments".to_string(),
                description: None,
                stage: ImsStageName("Todo".to_string()),
                labels: Vec::new(),
            })
            .expect("issue");

        let attachment = LinearCommentAttachmentInput {
            title: Some("trace log.txt".to_string()),
            url: "/api/v1/issues/FUG-2/attachments/001-trace-log.txt".to_string(),
            path: None,
            content_type: Some("text/plain".to_string()),
            byte_len: 16,
        };
        let added = store
            .add_comment_tool(AddCommentInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("FUG-2".to_string()),
                },
                body_markdown: "Please inspect the trace.".to_string(),
                attachments: vec![attachment],
            })
            .await
            .expect("comment with attachment");
        assert_eq!(added.attachments.len(), 1);

        let inspected = store
            .inspect_issue_tool(InspectIssueInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("FUG-2".to_string()),
                },
            })
            .await
            .expect("inspect issue");
        assert_eq!(inspected.attachments.len(), 1);
        assert_eq!(
            inspected.attachments[0].title.as_deref(),
            Some("trace log.txt")
        );
        assert_eq!(
            inspected.attachments[0].content_type.as_deref(),
            Some("text/plain")
        );
        assert_eq!(inspected.attachments[0].byte_len, 16);
        assert!(matches!(
            inspected.attachments[0].source,
            LinearFileAttachmentSource::Comment { .. }
        ));
        assert!(
            store
                .comments(&ImsIssueRef {
                    key: ImsIssueKey("FUG-2".to_string())
                })
                .await
                .expect("comments")[0]
                .body_mark
                .contains("Attached files:")
        );
    }

    #[test]
    fn sqlite_ims_stage_filter_uses_case_insensitive_stage_index() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = fugue_ims_tracker_config(&temp.path().join("ims.sqlite"));
        config.project_slug = Some("owner/repo".into());
        let store = FugueImsStore::from_config(&config).expect("store");
        let conn = store.connection().expect("connection");
        let mut stmt = conn
            .prepare(
                "explain query plan \
                 select id, key, title, description, stage, branch_name, url, labels_json, created_at, updated_at \
                 from issues where repo = ?1 and lower(stage) in (?2, ?3) order by created_at asc, key asc",
            )
            .expect("plan");
        let details = stmt
            .query_map(params!["owner/repo", "todo", "active"], |row| {
                row.get::<_, String>(3)
            })
            .expect("plan rows")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("details")
            .join("\n");
        assert!(
            details.contains("issues_repo_stage_ci_created_key_idx"),
            "stage filter should use the case-insensitive stage index; plan was {details}"
        );
    }

    #[test]
    fn sqlite_ims_stage_filter_does_not_materialize_off_lane_history() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = fugue_ims_tracker_config(&temp.path().join("ims.sqlite"));
        config.project_slug = Some("owner/repo".into());
        let store = FugueImsStore::from_config(&config).expect("store");
        for index in 0..1_500 {
            store
                .create_issue(CreateFugueIssue {
                    key: ImsIssueKey(format!("HIST-{index}")),
                    repo: "owner/repo".to_string(),
                    title: format!("Historical issue {index}"),
                    description: Some("not on the current board".to_string()),
                    stage: ImsStageName("Done".to_string()),
                    labels: Vec::new(),
                })
                .expect("historical issue");
        }
        for (key, stage) in [("FUG-10", "todo"), ("FUG-11", "ACTIVE")] {
            store
                .create_issue(CreateFugueIssue {
                    key: ImsIssueKey(key.to_string()),
                    repo: "owner/repo".to_string(),
                    title: format!("Visible {key}"),
                    description: None,
                    stage: ImsStageName(stage.to_string()),
                    labels: Vec::new(),
                })
                .expect("visible issue");
        }

        let started = std::time::Instant::now();
        let issues = store
            .issues_by_stage_filter(&["Todo".to_string(), "active".to_string()])
            .expect("filtered issues");
        let elapsed = started.elapsed();
        eprintln!(
            "sqlite IMS visible-stage query over 1,500 off-lane historical issues completed in {elapsed:?}"
        );

        assert_eq!(
            issues
                .iter()
                .map(|issue| issue.identifier.as_str())
                .collect::<Vec<_>>(),
            vec!["FUG-10", "FUG-11"]
        );
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "filtered stage query should stay under 0.5s for seeded history; elapsed {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn sqlite_ims_backs_linear_shaped_tool_surface() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = fugue_ims_tracker_config(&temp.path().join("ims.sqlite"));
        config.project_slug = Some("owner/repo".into());
        let store = FugueImsStore::from_config(&config).expect("store");
        store
            .create_issue(CreateFugueIssue {
                key: ImsIssueKey("FUG-2".to_string()),
                repo: "owner/repo".to_string(),
                title: "Use tool surface".to_string(),
                description: None,
                stage: ImsStageName("Todo".to_string()),
                labels: vec!["kapellmeister-safe".to_string()],
            })
            .expect("issue");

        let (is_error, body) = crate::linear_tools::execute_linear_fugue_tool(
            &config,
            "linear_move_issue",
            &serde_json::json!({
                "issue": {"kind": "identifier", "identifier": "FUG-2"},
                "state": {"kind": "name", "name": "Planning"},
                "expected_updated_at": null,
                "idempotency_key": "move"
            }),
        )
        .await;
        assert!(!is_error, "{body}");
        assert!(body.contains("Planning"));

        let (is_error, body) = crate::linear_tools::execute_linear_fugue_tool(
            &config,
            "linear_upsert_workpad",
            &serde_json::json!({
                "issue": {"kind": "identifier", "identifier": "FUG-2"},
                "heading": "## Fugue Workpad",
                "body_markdown": "Tool-backed workpad",
                "create_if_missing": true,
                "idempotency_key": "workpad"
            }),
        )
        .await;
        assert!(!is_error, "{body}");

        let inspected = store
            .inspect_issue_tool(InspectIssueInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("FUG-2".to_string()),
                },
            })
            .await
            .expect("inspect");
        assert_eq!(inspected.issue.state.name, "Planning");
        assert!(
            inspected
                .workpad
                .as_ref()
                .is_some_and(|workpad| workpad.body_markdown.contains("Tool-backed workpad"))
        );
    }
    #[tokio::test]
    async fn shared_database_preserves_repository_scope_for_reads_and_writes() {
        let root = tempfile::tempdir().unwrap();
        let mut config_a = fugue_ims_tracker_config(&root.path().join("shared.sqlite3"));
        config_a.project_slug = Some("owner/one".into());
        let mut config_b = config_a.clone();
        config_b.project_slug = Some("owner/two".into());
        let one = FugueImsStore::from_config(&config_a).unwrap();
        let two = FugueImsStore::from_config(&config_b).unwrap();
        let create = |key: &str, repo: &str| CreateFugueIssue {
            key: ImsIssueKey(key.into()),
            repo: repo.into(),
            title: key.into(),
            description: None,
            stage: ImsStageName("Todo".into()),
            labels: Vec::new(),
        };
        one.create_issue(create("ONE-1", "owner/one")).unwrap();
        let foreign = two.create_issue(create("TWO-1", "owner/two")).unwrap();
        let foreign_ref = ImsIssueRef {
            key: foreign.key.clone(),
        };
        let comment = two.add_comment(&foreign_ref, "private note").await.unwrap();

        assert_eq!(
            one.all_open_issues().unwrap().len(),
            1,
            "board must be scoped"
        );
        assert_eq!(one.fetch_candidate_issues().await.unwrap().len(), 1);
        assert!(
            one.fetch_issue_states_by_ids(std::slice::from_ref(&foreign.id.0))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            one.list_repo_issues(&ImsRepoIssueQuery {
                repo: ImsRepoScope("owner/two".into())
            })
            .await
            .is_err()
        );
        for issue in [
            LinearIssueRef::Id {
                id: LinearIssueId(foreign.id.0.clone()),
            },
            LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier(foreign.key.0.clone()),
            },
        ] {
            assert!(
                one.inspect_issue_tool(InspectIssueInput {
                    issue: issue.clone()
                })
                .await
                .is_err()
            );
            assert!(
                crate::workgraph_gate::canonical_issue_identifier(&config_a, &issue)
                    .await
                    .is_err()
            );
        }
        assert!(one.issue(&foreign_ref).await.is_err());
        assert!(one.comments(&foreign_ref).await.is_err());
        assert!(
            one.add_comment(&foreign_ref, "must not write")
                .await
                .is_err()
        );
        assert!(one.update_comment(&comment.id, "must not update").is_err());
        assert!(one.delete_comment(&comment.id).is_err());
        assert!(
            one.move_issue(
                &foreign_ref,
                &ImsStageName("Done".into()),
                IdempotencyKey("foreign".into())
            )
            .await
            .is_err()
        );
        assert!(
            one.create_issue(create("TWO-1", "owner/one")).is_err(),
            "upsert cannot transfer ownership"
        );
        assert!(
            one.create_issue(create("TWO-2", "owner/two")).is_err(),
            "create cannot choose another scope"
        );

        assert_eq!(two.issue(&foreign_ref).await.unwrap(), foreign);
        assert_eq!(two.comments(&foreign_ref).await.unwrap(), vec![comment]);
        assert_eq!(two.all_open_issues().unwrap().len(), 1);
        let local = ImsIssueRef {
            key: ImsIssueKey("ONE-1".into()),
        };
        one.move_issue(
            &local,
            &ImsStageName("Active".into()),
            IdempotencyKey("local".into()),
        )
        .await
        .unwrap();
        assert_eq!(one.issue(&local).await.unwrap().stage.0, "Active");
        let own_id = one.issue(&local).await.unwrap().id.0;
        one.move_issue(
            &local,
            &ImsStageName("Done".into()),
            IdempotencyKey("done".into()),
        )
        .await
        .unwrap();
        let observed = one
            .fetch_issue_states_by_ids(&[own_id, foreign.id.0])
            .await
            .unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0].state, "Done",
            "reconciliation must observe local terminal states"
        );
        assert_eq!(
            one.all_open_issues().unwrap().len(),
            1,
            "terminal history remains repository-scoped"
        );
    }
    #[test]
    fn repository_scope_is_required_before_creating_storage() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("not-created.sqlite3");
        let mut config = fugue_ims_tracker_config(&path);
        for scope in [None, Some("  ".to_owned())] {
            config.project_slug = scope;
            assert!(matches!(
                FugueImsStore::from_config(&config),
                Err(FugueError::MissingTrackerProjectSlug)
            ));
            assert!(!path.exists());
        }
    }
}
