use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::config::TrackerConfig;
use crate::error::{FugueError, LinearGraphqlFailure, Result};
use crate::fugue_ims::FugueImsStore;
use crate::ims::{
    ImsCommentAttachmentSnapshot, ImsCommentId, ImsCommentSnapshot, ImsIssueId, ImsIssueKey,
    ImsIssueRef, ImsIssueSnapshot, ImsRepoIssueQuery, ImsStageName, IssueManagementSystem,
};
use crate::linear_lane_state::{LinearCommentSnapshot, record_internal_lane_change_from_env};
use crate::tools::{
    AddCommentInput, AddCommentOutput, BranchName, ClaimIssueInput, ClaimIssueOutput, Cursor,
    EnsureWorkflowState, EnsureWorkflowStatesInput, EnsureWorkflowStatesOutput, FugueToolError,
    FugueToolErrorCode, FugueToolOperation, FugueToolOutput, FugueToolResult, IdempotencyKey,
    InspectIssueInput, InspectIssueOutput, LinearCommentAttachmentInput, LinearCommentId,
    LinearFileAttachmentSnapshot, LinearFileAttachmentSource, LinearIssueChange,
    LinearIssueChangeKind, LinearIssueCommentSnapshot, LinearIssueId, LinearIssueIdentifier,
    LinearIssueRef, LinearIssueSnapshot, LinearStateId, LinearStateRef, LinearStateSnapshot,
    LinearStateType, LinearTeamId, LinearUserId, LinearUserRef, LinearUserSnapshot,
    LinearWorkpadSnapshot, ListProjectStatesInput, ListProjectStatesOutput,
    ListRecentIssueChangesInput, ListRecentIssueChangesOutput, MoveIssueInput, MoveIssueOutput,
    RetryAdvice, ToolExecutionMetadata, UpsertWorkpadInput, UpsertWorkpadOutput,
    render_comment_body_with_attachments,
};
use crate::tracker::{IssueTracker, LinearClient, is_allowed_attachment_download_url};

const DEFAULT_WORKPAD_HEADING: &str = "## Fugue Workpad";
const LINEAR_ATTACHMENT_DIR_ENV: &str = "FUGUE_LINEAR_ATTACHMENT_DIR";
pub const LINEAR_TERMINAL_STATES_ENV: &str = "FUGUE_LINEAR_TERMINAL_STATES";
pub const LINEAR_REVIEW_HANDOFF_STATES_ENV: &str = "FUGUE_LINEAR_REVIEW_HANDOFF_STATES";
pub const LINEAR_ACTION_STATES_ENV: &str = "FUGUE_LINEAR_ACTION_STATES";

pub struct LinearToolExecutor<'a> {
    client: &'a LinearClient,
    review_handoff_states: Vec<String>,
}

impl<'a> LinearToolExecutor<'a> {
    pub fn new(client: &'a LinearClient) -> Self {
        Self {
            client,
            review_handoff_states: review_handoff_states_from_env(),
        }
    }

    pub fn with_review_handoff_states(
        client: &'a LinearClient,
        review_handoff_states: Vec<String>,
    ) -> Self {
        Self {
            client,
            review_handoff_states,
        }
    }

    pub async fn resolve_team_id(&self, selector: &str) -> LinearToolResultData<LinearTeamId> {
        if selector.trim().is_empty() {
            return Err(LinearToolFailure::new(
                FugueToolErrorCode::InvalidInput,
                "linear team selector must be non-empty",
                RetryAdvice::DoNotRetry,
                None,
            ));
        }
        let response = self
            .client
            .graphql(TEAM_BY_SELECTOR_QUERY, json!({ "id": selector }))
            .await?;
        let data: TeamBySelectorResponse = decode_data(response)?;
        data.team.map(|team| LinearTeamId(team.id)).ok_or_else(|| {
            LinearToolFailure::new(
                FugueToolErrorCode::NotFound,
                format!("Linear team {selector:?} was not found"),
                RetryAdvice::DoNotRetry,
                None,
            )
        })
    }

    pub async fn inspect_issue(
        &self,
        input: InspectIssueInput,
    ) -> LinearToolResultData<InspectIssueOutput> {
        let issue = self
            .fetch_issue(&input.issue, CommentFetch::First100)
            .await?;
        let workpad = workpad_snapshot_for_issue(
            &issue.comments,
            DEFAULT_WORKPAD_HEADING,
            &issue.issue.identifier.0,
        );
        let attachments = download_issue_file_attachments(self.client, &issue).await?;
        Ok(InspectIssueOutput {
            issue: issue.issue,
            workpad,
            attachments,
        })
    }

    pub(crate) async fn resolve_issue_identifier(
        &self,
        issue: &LinearIssueRef,
    ) -> LinearToolResultData<LinearIssueIdentifier> {
        self.fetch_issue(issue, CommentFetch::None)
            .await
            .map(|issue| issue.issue.identifier)
    }

    pub async fn issue_comment_snapshots(
        &self,
        issue_ref: &LinearIssueRef,
    ) -> LinearToolResultData<Vec<LinearCommentSnapshot>> {
        let issue = self.fetch_issue(issue_ref, CommentFetch::First100).await?;
        Ok(issue
            .comments
            .into_iter()
            .map(|comment| LinearCommentSnapshot {
                id: LinearCommentId(comment.id),
                body_mark: stable_comment_body_mark(&comment.body),
                body_markdown: comment.body,
            })
            .collect())
    }

    pub async fn issue_comments(
        &self,
        issue_ref: &LinearIssueRef,
    ) -> LinearToolResultData<Vec<LinearIssueCommentSnapshot>> {
        let issue = self.fetch_issue(issue_ref, CommentFetch::First100).await?;
        Ok(issue
            .comments
            .into_iter()
            .map(LinearIssueCommentSnapshot::from)
            .collect())
    }

    pub async fn workpad_body(
        &self,
        issue: &LinearIssueRef,
        heading: &str,
    ) -> LinearToolResultData<Option<String>> {
        let heading = if heading.trim().is_empty() {
            DEFAULT_WORKPAD_HEADING
        } else {
            heading.trim()
        };
        let issue = self.fetch_issue(issue, CommentFetch::First100).await?;
        Ok(
            find_workpad_comment_for_issue(&issue.comments, heading, &issue.issue.identifier.0)
                .map(|comment| comment.body.clone()),
        )
    }

    pub async fn claim_issue(
        &self,
        input: ClaimIssueInput,
    ) -> LinearToolResultData<ClaimIssueOutput> {
        let current = self.fetch_issue(&input.issue, CommentFetch::None).await?;
        let previous_assignee = current.issue.assignee.clone();
        let assignee = self.resolve_user(&input.assignee).await?;
        let state_id = match &input.target_state {
            Some(state) => Some(
                self.resolve_state_for_issue(&current.issue, state)
                    .await?
                    .id
                    .0,
            ),
            None => None,
        };
        let assignee_changed = current
            .issue
            .assignee
            .as_ref()
            .map(|user| user.id.0.as_str())
            != Some(assignee.id.0.as_str());
        let state_changed = match (&input.target_state, &state_id) {
            (Some(_), Some(state_id)) => current.issue.state.id.0 != *state_id,
            _ => false,
        };
        if !assignee_changed && !state_changed {
            return Ok(ClaimIssueOutput {
                issue: current.issue,
                claimed: false,
                previous_assignee,
            });
        }

        let mut update = serde_json::Map::new();
        update.insert("assigneeId".to_string(), json!(assignee.id.0));
        if let Some(state_id) = state_id {
            update.insert("stateId".to_string(), json!(state_id));
        }
        let issue = self
            .update_issue(&current.issue.id.0, Value::Object(update))
            .await?;
        Ok(ClaimIssueOutput {
            issue,
            claimed: true,
            previous_assignee,
        })
    }

    pub async fn move_issue(&self, input: MoveIssueInput) -> LinearToolResultData<MoveIssueOutput> {
        let current = self
            .fetch_issue(&input.issue, CommentFetch::First100)
            .await?;
        if let Some(expected) = input.expected_updated_at
            && current.issue.updated_at != expected
        {
            return Err(LinearToolFailure::new(
                FugueToolErrorCode::PreconditionFailed,
                "issue updated_at did not match expected_updated_at",
                RetryAdvice::RefreshStateThenRetry,
                Some(input.idempotency_key),
            ));
        }
        let target = self
            .resolve_state_for_issue(&current.issue, &input.state)
            .await?;
        let current_is_terminal = linear_state_is_terminal(&current.issue.state);
        let target_is_terminal = linear_state_is_terminal(&target);
        if current_is_terminal && !target_is_terminal {
            return Err(LinearToolFailure::new(
                FugueToolErrorCode::PreconditionFailed,
                format!(
                    "refusing to move terminal issue {} from {} to nonterminal state {}; terminal issues require explicit operator/Kapellmeister restart through a control lane",
                    current.issue.identifier.0, current.issue.state.name, target.name
                ),
                RetryAdvice::DoNotRetry,
                Some(input.idempotency_key),
            ));
        }
        if let Some(finding) = unresolved_rework_finding_blocks_handoff_move(
            &current.issue.state,
            &target,
            &current.comments,
            &self.review_handoff_states,
        ) {
            return Err(LinearToolFailure::new(
                FugueToolErrorCode::PreconditionFailed,
                format!(
                    "refusing to move {} from Rework to {}; unresolved operator review finding must be addressed first: {}",
                    current.issue.identifier.0, target.name, finding
                ),
                RetryAdvice::RefreshStateThenRetry,
                Some(input.idempotency_key),
            ));
        }
        if current.issue.state.id == target.id {
            if let Err(error) = record_internal_lane_change_from_env(
                &current.issue.identifier.0,
                &current.issue.state.name,
            ) {
                tracing::warn!(
                    error = %error,
                    issue_identifier = %current.issue.identifier.0,
                    lane = %current.issue.state.name,
                    "failed to record internal Linear lane no-op"
                );
            }
            return Ok(MoveIssueOutput {
                issue: current.issue,
                changed: false,
                previous_state: None,
            });
        }
        let previous_state = current.issue.state.clone();
        let issue = self
            .update_issue(&current.issue.id.0, json!({ "stateId": target.id.0 }))
            .await?;
        if let Err(error) =
            record_internal_lane_change_from_env(&issue.identifier.0, &issue.state.name)
        {
            tracing::warn!(
                error = %error,
                issue_identifier = %issue.identifier.0,
                lane = %issue.state.name,
                "failed to record internal Linear lane change"
            );
        }
        Ok(MoveIssueOutput {
            issue,
            changed: true,
            previous_state: Some(previous_state),
        })
    }

    pub async fn upsert_workpad(
        &self,
        input: UpsertWorkpadInput,
    ) -> LinearToolResultData<UpsertWorkpadOutput> {
        let issue = self
            .fetch_issue(&input.issue, CommentFetch::First100)
            .await?;
        let heading = normalize_workpad_heading(&input.heading, &issue.issue.identifier.0);
        let rendered_body = workpad_body(&heading, &input.body_markdown);
        if let Some(comment) =
            find_workpad_comment_for_issue(&issue.comments, &heading, &issue.issue.identifier.0)
        {
            let full_body = crate::review_findings::merge_preserved_review_findings(
                &rendered_body,
                Some(&comment.body),
            );
            let full_body =
                crate::deploy_gate::merge_preserved_deploy_evidence(&full_body, &comment.body);
            if comment.body == full_body {
                return Ok(UpsertWorkpadOutput {
                    comment_id: LinearCommentId(comment.id.clone()),
                    url: comment.url.clone(),
                    created: false,
                    updated: false,
                });
            }
            let updated = self.update_comment(&comment.id, &full_body).await?;
            return Ok(UpsertWorkpadOutput {
                comment_id: LinearCommentId(updated.id),
                url: updated.url,
                created: false,
                updated: true,
            });
        }
        if !input.create_if_missing {
            return Err(LinearToolFailure::new(
                FugueToolErrorCode::NotFound,
                "workpad comment not found and create_if_missing is false",
                RetryAdvice::DoNotRetry,
                Some(input.idempotency_key),
            ));
        }
        let created = self
            .create_comment(&issue.issue.id.0, &rendered_body)
            .await?;
        Ok(UpsertWorkpadOutput {
            comment_id: LinearCommentId(created.id),
            url: created.url,
            created: true,
            updated: false,
        })
    }

    pub async fn add_comment(
        &self,
        input: AddCommentInput,
    ) -> LinearToolResultData<AddCommentOutput> {
        let issue_id = self.resolve_issue_id(&input.issue).await?;
        let body = render_comment_body_with_attachments(&input.body_markdown, &input.attachments);
        let comment = self.create_comment(&issue_id, &body).await?;
        let attachments = comment_attachment_snapshots(&comment.id, input.attachments);
        Ok(AddCommentOutput {
            comment_id: LinearCommentId(comment.id),
            url: comment.url,
            attachments,
        })
    }

    pub async fn list_project_states(
        &self,
        input: ListProjectStatesInput,
    ) -> LinearToolResultData<ListProjectStatesOutput> {
        let variables = list_project_states_variables(&input)?;
        let response = self
            .client
            .graphql(LIST_PROJECT_STATES_QUERY, variables)
            .await?;
        let data: WorkflowStatesResponse = decode_data(response)?;
        Ok(ListProjectStatesOutput {
            states: data
                .workflow_states
                .nodes
                .into_iter()
                .map(LinearStateSnapshot::from)
                .collect(),
        })
    }

    pub async fn ensure_workflow_states(
        &self,
        input: EnsureWorkflowStatesInput,
    ) -> LinearToolResultData<EnsureWorkflowStatesOutput> {
        if input.states.is_empty() {
            return Err(LinearToolFailure::new(
                FugueToolErrorCode::InvalidInput,
                "linear.ensure_workflow_states requires at least one state",
                RetryAdvice::DoNotRetry,
                Some(input.idempotency_key),
            ));
        }
        let existing = self
            .list_project_states(ListProjectStatesInput {
                team_id: Some(input.team_id.clone()),
                project_id: None,
            })
            .await?
            .states;
        let mut output_states = existing.clone();
        let mut created = Vec::new();
        let mut already_present = Vec::new();
        for required in input.states {
            if let Some(found) = existing_state_by_name(&output_states, &required.name) {
                already_present.push(found.clone());
                continue;
            }
            let state = self
                .create_workflow_state(&input.team_id, &required, output_states.len())
                .await?;
            output_states.push(state.clone());
            created.push(state);
        }
        Ok(EnsureWorkflowStatesOutput {
            states: output_states,
            created,
            existing: already_present,
        })
    }

    async fn create_workflow_state(
        &self,
        team_id: &LinearTeamId,
        state: &EnsureWorkflowState,
        position_index: usize,
    ) -> LinearToolResultData<LinearStateSnapshot> {
        let response = self
            .client
            .graphql(
                CREATE_WORKFLOW_STATE_QUERY,
                json!({
                    "input": {
                        "teamId": team_id.0,
                        "name": state.name,
                        "type": linear_state_type_input(state.state_type),
                        "color": linear_state_type_color(state.state_type),
                        "position": (position_index as f64 + 1.0) * 100.0,
                    }
                }),
            )
            .await?;
        let data: WorkflowStateCreateResponse = decode_data(response)?;
        if !data.workflow_state_create.success {
            return Err(LinearToolFailure::new(
                FugueToolErrorCode::ExecutionFailed,
                "workflowStateCreate returned success=false",
                RetryAdvice::DoNotRetry,
                None,
            ));
        }
        data.workflow_state_create
            .workflow_state
            .map(LinearStateSnapshot::from)
            .ok_or_else(|| {
                LinearToolFailure::new(
                    FugueToolErrorCode::ExecutionFailed,
                    "workflowStateCreate did not return a workflowState",
                    RetryAdvice::DoNotRetry,
                    None,
                )
            })
    }

    pub async fn list_recent_issue_changes(
        &self,
        input: ListRecentIssueChangesInput,
    ) -> LinearToolResultData<ListRecentIssueChangesOutput> {
        let limit = i64::from(input.limit.clamp(1, 100));
        let issue_id = match &input.issue {
            Some(issue) => Some(self.resolve_issue_id(issue).await?),
            None => None,
        };
        let (query, variables) = recent_comments_request(
            self.client.project_slug(),
            issue_id,
            limit,
            input.cursor.map(|cursor| cursor.0),
        );
        let response = self.client.graphql(query, variables).await?;
        let data: CommentsResponse = decode_data(response)?;
        for comment in &data.comments.nodes {
            self.guard_project(comment.issue.project.as_ref())?;
        }
        let changes = data
            .comments
            .nodes
            .into_iter()
            .filter(|comment| input.since.is_none_or(|since| comment.created_at >= since))
            .map(|comment| LinearIssueChange {
                issue: LinearIssueRef::Id {
                    id: LinearIssueId(comment.issue.id),
                },
                changed_at: comment.created_at,
                actor: comment.user.map(LinearUserSnapshot::from),
                kind: LinearIssueChangeKind::Commented {
                    comment_id: LinearCommentId(comment.id),
                },
            })
            .collect();
        Ok(ListRecentIssueChangesOutput {
            changes,
            next_cursor: data.comments.page_info.end_cursor.map(Cursor),
        })
    }

    async fn fetch_issue(
        &self,
        issue_ref: &LinearIssueRef,
        comments: CommentFetch,
    ) -> LinearToolResultData<IssueWithComments> {
        let response = self
            .client
            .graphql(
                match comments {
                    CommentFetch::None => INSPECT_ISSUE_QUERY,
                    CommentFetch::First100 => INSPECT_ISSUE_WITH_COMMENTS_QUERY,
                },
                json!({ "id": issue_ref.lookup_value() }),
            )
            .await?;
        let data: IssueResponse = decode_data(response)?;
        let issue = data.issue.ok_or_else(|| {
            LinearToolFailure::new(
                FugueToolErrorCode::NotFound,
                "Linear issue not found",
                RetryAdvice::DoNotRetry,
                None,
            )
        })?;
        self.guard_project(issue.project.as_ref())?;
        Ok(IssueWithComments::from(issue))
    }

    fn guard_project(&self, project: Option<&IssueProjectNode>) -> LinearToolResultData<()> {
        if project.is_some_and(|project| project.slug_id == self.client.project_slug()) {
            Ok(())
        } else {
            Err(LinearToolFailure::new(
                FugueToolErrorCode::NotFound,
                "Linear issue not found in the configured project",
                RetryAdvice::DoNotRetry,
                None,
            ))
        }
    }

    async fn resolve_issue_id(&self, issue_ref: &LinearIssueRef) -> LinearToolResultData<String> {
        Ok(self
            .fetch_issue(issue_ref, CommentFetch::None)
            .await?
            .issue
            .id
            .0)
    }

    async fn resolve_user(
        &self,
        user_ref: &LinearUserRef,
    ) -> LinearToolResultData<LinearUserSnapshot> {
        match user_ref {
            LinearUserRef::Id { id } => Ok(LinearUserSnapshot {
                id: id.clone(),
                name: String::new(),
            }),
            LinearUserRef::Me => {
                let response = self.client.graphql(VIEWER_QUERY, json!({})).await?;
                let data: ViewerResponse = decode_data(response)?;
                Ok(data.viewer.into())
            }
        }
    }

    async fn resolve_state_for_issue(
        &self,
        issue: &LinearIssueSnapshot,
        state_ref: &LinearStateRef,
    ) -> LinearToolResultData<LinearStateSnapshot> {
        match state_ref {
            LinearStateRef::Id { id } => Ok(LinearStateSnapshot {
                id: id.clone(),
                name: String::new(),
                state_type: LinearStateType::Started,
            }),
            LinearStateRef::Name { name } if issue.state.name == *name => Ok(issue.state.clone()),
            LinearStateRef::Name { name } => {
                let states = self
                    .list_project_states(ListProjectStatesInput {
                        team_id: Some(issue.team_id.clone()),
                        project_id: None,
                    })
                    .await?
                    .states;
                states
                    .into_iter()
                    .find(|state| state.name == *name)
                    .ok_or_else(|| {
                        LinearToolFailure::new(
                            FugueToolErrorCode::NotFound,
                            format!("Linear state not found: {name}"),
                            RetryAdvice::DoNotRetry,
                            None,
                        )
                    })
            }
        }
    }

    async fn update_issue(
        &self,
        issue_id: &str,
        input: Value,
    ) -> LinearToolResultData<LinearIssueSnapshot> {
        let response = self
            .client
            .graphql(
                UPDATE_ISSUE_QUERY,
                json!({ "id": issue_id, "input": input }),
            )
            .await?;
        let data: IssueUpdateResponse = decode_data(response)?;
        if !data.issue_update.success {
            return Err(LinearToolFailure::new(
                FugueToolErrorCode::ExecutionFailed,
                "issueUpdate returned success=false",
                RetryAdvice::RefreshStateThenRetry,
                None,
            ));
        }
        let issue = data.issue_update.issue.ok_or_else(|| {
            LinearToolFailure::new(
                FugueToolErrorCode::ExecutionFailed,
                "issueUpdate returned no issue",
                RetryAdvice::RefreshStateThenRetry,
                None,
            )
        })?;
        self.guard_project(issue.project.as_ref())?;
        Ok(LinearIssueSnapshot::from(issue))
    }

    async fn create_comment(
        &self,
        issue_id: &str,
        body: &str,
    ) -> LinearToolResultData<CommentNode> {
        let response = self
            .client
            .graphql(
                CREATE_COMMENT_QUERY,
                json!({ "input": { "issueId": issue_id, "body": body } }),
            )
            .await?;
        let data: CommentCreateResponse = decode_data(response)?;
        payload_comment(
            data.comment_create.success,
            data.comment_create.comment,
            "commentCreate",
        )
    }

    async fn update_comment(
        &self,
        comment_id: &str,
        body: &str,
    ) -> LinearToolResultData<CommentNode> {
        let response = self
            .client
            .graphql(
                UPDATE_COMMENT_QUERY,
                json!({ "id": comment_id, "input": { "body": body } }),
            )
            .await?;
        let data: CommentUpdateResponse = decode_data(response)?;
        payload_comment(
            data.comment_update.success,
            data.comment_update.comment,
            "commentUpdate",
        )
    }
}

#[async_trait]
impl IssueManagementSystem for LinearToolExecutor<'_> {
    async fn list_repo_issues(&self, _query: &ImsRepoIssueQuery) -> Result<Vec<ImsIssueSnapshot>> {
        let issues = self.client.fetch_open_project_issues().await?;
        Ok(issues
            .into_iter()
            .map(|issue| ImsIssueSnapshot {
                id: ImsIssueId(issue.id),
                key: ImsIssueKey(issue.identifier),
                title: issue.title,
                description: issue.description,
                stage: ImsStageName(issue.state),
            })
            .collect())
    }

    async fn issue(&self, issue: &ImsIssueRef) -> Result<ImsIssueSnapshot> {
        let output = self
            .inspect_issue(InspectIssueInput {
                issue: linear_issue_ref_from_ims(issue),
            })
            .await
            .map_err(|error| {
                linear_tool_failure_into_fugue_error(error, FugueToolOperation::LinearInspectIssue)
            })?;
        Ok(ims_issue_snapshot_from_linear(output.issue))
    }

    async fn comments(&self, issue: &ImsIssueRef) -> Result<Vec<ImsCommentSnapshot>> {
        let comments = self
            .issue_comment_snapshots(&linear_issue_ref_from_ims(issue))
            .await
            .map_err(|error| {
                linear_tool_failure_into_fugue_error(error, FugueToolOperation::LinearInspectIssue)
            })?;
        Ok(comments
            .into_iter()
            .map(|comment| ImsCommentSnapshot {
                id: ImsCommentId(comment.id.0),
                body_mark: comment.body_mark,
                attachments: Vec::new(),
            })
            .collect())
    }

    async fn move_issue(
        &self,
        issue: &ImsIssueRef,
        stage: &ImsStageName,
        idempotency_key: IdempotencyKey,
    ) -> Result<ImsIssueSnapshot> {
        let output = self
            .move_issue(MoveIssueInput {
                issue: linear_issue_ref_from_ims(issue),
                state: LinearStateRef::Name {
                    name: stage.0.clone(),
                },
                expected_updated_at: None,
                idempotency_key,
            })
            .await
            .map_err(|error| {
                linear_tool_failure_into_fugue_error(error, FugueToolOperation::LinearMoveIssue)
            })?;
        Ok(ims_issue_snapshot_from_linear(output.issue))
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
        let linear_attachments: Vec<LinearCommentAttachmentInput> = attachments
            .iter()
            .cloned()
            .map(linear_comment_attachment_input_from_ims)
            .collect();
        let body_mark = render_comment_body_with_attachments(body, &linear_attachments);
        let output = self
            .add_comment(AddCommentInput {
                issue: linear_issue_ref_from_ims(issue),
                body_markdown: body.to_string(),
                attachments: linear_attachments,
            })
            .await
            .map_err(|error| {
                linear_tool_failure_into_fugue_error(error, FugueToolOperation::LinearAddComment)
            })?;
        Ok(ImsCommentSnapshot {
            id: ImsCommentId(output.comment_id.0),
            body_mark: stable_comment_body_mark(&body_mark),
            attachments,
        })
    }
}

fn linear_comment_attachment_input_from_ims(
    attachment: ImsCommentAttachmentSnapshot,
) -> LinearCommentAttachmentInput {
    LinearCommentAttachmentInput {
        title: attachment.title,
        url: attachment.url,
        path: attachment.path,
        content_type: attachment.content_type,
        byte_len: attachment.byte_len,
    }
}

fn linear_issue_ref_from_ims(issue: &ImsIssueRef) -> LinearIssueRef {
    LinearIssueRef::Identifier {
        identifier: LinearIssueIdentifier(issue.key.0.clone()),
    }
}

fn ims_issue_snapshot_from_linear(issue: LinearIssueSnapshot) -> ImsIssueSnapshot {
    ImsIssueSnapshot {
        id: ImsIssueId(issue.id.0),
        key: ImsIssueKey(issue.identifier.0),
        title: issue.title,
        description: issue.description,
        stage: ImsStageName(issue.state.name),
    }
}

fn linear_tool_failure_into_fugue_error(
    error: LinearToolFailure,
    operation: FugueToolOperation,
) -> FugueError {
    let tool_error = error.into_error(operation);
    FugueError::ResponseError(tool_error.message)
}

type LinearToolResultData<T> = std::result::Result<T, LinearToolFailure>;

#[derive(Debug, Clone)]
pub struct LinearToolFailure {
    code: FugueToolErrorCode,
    message: String,
    retry: RetryAdvice,
    idempotency_key: Option<IdempotencyKey>,
}

impl LinearToolFailure {
    pub fn new(
        code: FugueToolErrorCode,
        message: impl Into<String>,
        retry: RetryAdvice,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            retry,
            idempotency_key,
        }
    }

    pub fn into_error(self, operation: FugueToolOperation) -> FugueToolError {
        FugueToolError {
            operation,
            code: self.code,
            message: self.message,
            retry: self.retry,
            idempotency_key: self.idempotency_key,
        }
    }
}

impl From<FugueError> for LinearToolFailure {
    fn from(error: FugueError) -> Self {
        let (code, retry) = match &error {
            FugueError::MissingTrackerApiKey => (
                FugueToolErrorCode::AuthenticationFailed,
                RetryAdvice::DoNotRetry,
            ),
            FugueError::MissingTrackerProjectSlug | FugueError::InvalidConfig(_) => {
                (FugueToolErrorCode::InvalidInput, RetryAdvice::DoNotRetry)
            }
            FugueError::LinearApiRequest(_) | FugueError::LinearApiStatus(_) => (
                FugueToolErrorCode::BackendUnavailable,
                RetryAdvice::SafeToRetry,
            ),
            FugueError::LinearGraphqlErrors(kind) => classify_graphql_errors(*kind),
            FugueError::LinearUnknownPayload(_) => {
                (FugueToolErrorCode::ExecutionFailed, RetryAdvice::DoNotRetry)
            }
            _ => (FugueToolErrorCode::ExecutionFailed, RetryAdvice::DoNotRetry),
        };
        Self::new(code, error.to_string(), retry, None)
    }
}

pub fn is_linear_fugue_tool_name(name: &str) -> bool {
    matches!(
        name.parse::<FugueToolOperation>(),
        Ok(FugueToolOperation::LinearInspectIssue
            | FugueToolOperation::LinearClaimIssue
            | FugueToolOperation::LinearMoveIssue
            | FugueToolOperation::LinearUpsertWorkpad
            | FugueToolOperation::LinearListProjectStates
            | FugueToolOperation::LinearEnsureWorkflowStates
            | FugueToolOperation::LinearListRecentIssueChanges)
    )
}

pub async fn execute_linear_fugue_tool(
    config: &TrackerConfig,
    name: &str,
    arguments: &Value,
) -> (bool, String) {
    execute_linear_fugue_tool_with_review_handoff_states(config, name, arguments, &[]).await
}

pub async fn execute_linear_fugue_tool_with_review_handoff_states(
    config: &TrackerConfig,
    name: &str,
    arguments: &Value,
    review_handoff_states: &[String],
) -> (bool, String) {
    let operation = match name.parse::<FugueToolOperation>() {
        Ok(operation) => operation,
        Err(err) => {
            return raw_tool_error(
                name,
                FugueToolErrorCode::UnsupportedOperation,
                err.to_string(),
            );
        }
    };
    if config.kind == "fugue-ims" {
        return execute_fugue_ims_linear_tool(config, operation, name, arguments).await;
    }
    if config.kind == "jira" {
        return crate::jira::execute_tool(config, operation, arguments).await;
    }
    let client = match LinearClient::from_config(config) {
        Ok(client) => client,
        Err(err) => {
            let failure = LinearToolFailure::from(err).into_error(operation);
            return tool_result_to_callback(FugueToolResult::Err {
                error: failure,
                metadata: ToolExecutionMetadata::default(),
            });
        }
    };
    let executor = if review_handoff_states.is_empty() {
        LinearToolExecutor::new(&client)
    } else {
        LinearToolExecutor::with_review_handoff_states(&client, review_handoff_states.to_vec())
    };
    let result = match operation {
        FugueToolOperation::LinearInspectIssue => match parse(arguments).map_err(with_key(None)) {
            Ok(input) => executor
                .inspect_issue(input)
                .await
                .map(FugueToolOutput::LinearInspectIssue),
            Err(err) => Err(err),
        },
        FugueToolOperation::LinearClaimIssue => {
            let idempotency_key = idempotency_key(arguments);
            match parse(arguments).map_err(with_key(idempotency_key)) {
                Ok(input) => executor
                    .claim_issue(input)
                    .await
                    .map(FugueToolOutput::LinearClaimIssue),
                Err(err) => Err(err),
            }
        }
        FugueToolOperation::LinearMoveIssue => {
            let idempotency_key = idempotency_key(arguments);
            match parse(arguments).map_err(with_key(idempotency_key)) {
                Ok(input) => executor
                    .move_issue(input)
                    .await
                    .map(FugueToolOutput::LinearMoveIssue),
                Err(err) => Err(err),
            }
        }
        FugueToolOperation::LinearUpsertWorkpad => {
            let idempotency_key = idempotency_key(arguments);
            match parse(arguments).map_err(with_key(idempotency_key)) {
                Ok(input) => executor
                    .upsert_workpad(input)
                    .await
                    .map(FugueToolOutput::LinearUpsertWorkpad),
                Err(err) => Err(err),
            }
        }
        FugueToolOperation::LinearAddComment => match parse(arguments).map_err(with_key(None)) {
            Ok(input) => executor
                .add_comment(input)
                .await
                .map(FugueToolOutput::LinearAddComment),
            Err(err) => Err(err),
        },
        FugueToolOperation::LinearListProjectStates => {
            match parse(arguments).map_err(with_key(None)) {
                Ok(input) => executor
                    .list_project_states(input)
                    .await
                    .map(FugueToolOutput::LinearListProjectStates),
                Err(err) => Err(err),
            }
        }
        FugueToolOperation::LinearEnsureWorkflowStates => {
            let idempotency_key = idempotency_key(arguments);
            match parse(arguments).map_err(with_key(idempotency_key)) {
                Ok(input) => executor
                    .ensure_workflow_states(input)
                    .await
                    .map(FugueToolOutput::LinearEnsureWorkflowStates),
                Err(err) => Err(err),
            }
        }
        FugueToolOperation::LinearListRecentIssueChanges => {
            match parse(arguments).map_err(with_key(None)) {
                Ok(input) => executor
                    .list_recent_issue_changes(input)
                    .await
                    .map(FugueToolOutput::LinearListRecentIssueChanges),
                Err(err) => Err(err),
            }
        }
        _ => Err(LinearToolFailure::new(
            FugueToolErrorCode::UnsupportedOperation,
            format!("unsupported Linear Fugue tool: {name}"),
            RetryAdvice::DoNotRetry,
            None,
        )),
    };
    match result {
        Ok(output) => tool_result_to_callback(FugueToolResult::Ok {
            output,
            metadata: ToolExecutionMetadata::default(),
        }),
        Err(error) => tool_result_to_callback(FugueToolResult::Err {
            error: error.into_error(operation),
            metadata: ToolExecutionMetadata::default(),
        }),
    }
}

async fn execute_fugue_ims_linear_tool(
    config: &TrackerConfig,
    operation: FugueToolOperation,
    name: &str,
    arguments: &Value,
) -> (bool, String) {
    let store = match FugueImsStore::from_config(config) {
        Ok(store) => store,
        Err(err) => {
            return tool_result_to_callback(FugueToolResult::Err {
                error: LinearToolFailure::from(err).into_error(operation),
                metadata: ToolExecutionMetadata::default(),
            });
        }
    };
    let result = match operation {
        FugueToolOperation::LinearInspectIssue => match parse(arguments).map_err(with_key(None)) {
            Ok(input) => store
                .inspect_issue_tool(input)
                .await
                .map(FugueToolOutput::LinearInspectIssue)
                .map_err(LinearToolFailure::from),
            Err(err) => Err(err),
        },
        FugueToolOperation::LinearMoveIssue => {
            let idempotency_key = idempotency_key(arguments);
            match parse(arguments).map_err(with_key(idempotency_key)) {
                Ok(input) => store
                    .move_issue_tool(input)
                    .await
                    .map(FugueToolOutput::LinearMoveIssue)
                    .map_err(LinearToolFailure::from),
                Err(err) => Err(err),
            }
        }
        FugueToolOperation::LinearUpsertWorkpad => {
            let idempotency_key = idempotency_key(arguments);
            match parse(arguments).map_err(with_key(idempotency_key)) {
                Ok(input) => store
                    .upsert_workpad_tool(input)
                    .await
                    .map(FugueToolOutput::LinearUpsertWorkpad)
                    .map_err(LinearToolFailure::from),
                Err(err) => Err(err),
            }
        }
        FugueToolOperation::LinearAddComment => match parse(arguments).map_err(with_key(None)) {
            Ok(input) => store
                .add_comment_tool(input)
                .await
                .map(FugueToolOutput::LinearAddComment)
                .map_err(LinearToolFailure::from),
            Err(err) => Err(err),
        },
        FugueToolOperation::LinearListProjectStates => {
            match parse(arguments).map_err(with_key(None)) {
                Ok(input) => Ok(FugueToolOutput::LinearListProjectStates(
                    store.list_project_states_tool(input, config),
                )),
                Err(err) => Err(err),
            }
        }
        FugueToolOperation::LinearEnsureWorkflowStates => {
            match parse(arguments).map_err(with_key(idempotency_key(arguments))) {
                Ok(input) => Ok(FugueToolOutput::LinearEnsureWorkflowStates(
                    store.ensure_workflow_states_tool(input, config),
                )),
                Err(err) => Err(err),
            }
        }
        FugueToolOperation::LinearListRecentIssueChanges => Ok(
            FugueToolOutput::LinearListRecentIssueChanges(ListRecentIssueChangesOutput {
                changes: Vec::new(),
                next_cursor: None,
            }),
        ),
        _ => Err(LinearToolFailure::new(
            FugueToolErrorCode::UnsupportedOperation,
            format!("unsupported Fugue IMS Linear-shaped tool: {name}"),
            RetryAdvice::DoNotRetry,
            None,
        )),
    };
    match result {
        Ok(output) => tool_result_to_callback(FugueToolResult::Ok {
            output,
            metadata: ToolExecutionMetadata::default(),
        }),
        Err(error) => tool_result_to_callback(FugueToolResult::Err {
            error: error.into_error(operation),
            metadata: ToolExecutionMetadata::default(),
        }),
    }
}

fn tool_result_to_callback(result: FugueToolResult) -> (bool, String) {
    let is_error = matches!(result, FugueToolResult::Err { .. });
    let content = serde_json::to_string(&result)
        .unwrap_or_else(|err| json!({ "error": err.to_string() }).to_string());
    (is_error, content)
}

fn raw_tool_error(name: &str, code: FugueToolErrorCode, message: String) -> (bool, String) {
    let operation = name
        .parse()
        .unwrap_or(FugueToolOperation::LinearInspectIssue);
    tool_result_to_callback(FugueToolResult::Err {
        error: FugueToolError {
            operation,
            code,
            message,
            retry: RetryAdvice::DoNotRetry,
            idempotency_key: None,
        },
        metadata: ToolExecutionMetadata::default(),
    })
}

fn parse<T>(arguments: &Value) -> std::result::Result<T, serde_json::Error>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(arguments.clone())
}

fn with_key(key: Option<IdempotencyKey>) -> impl FnOnce(serde_json::Error) -> LinearToolFailure {
    move |error| {
        LinearToolFailure::new(
            FugueToolErrorCode::InvalidInput,
            error.to_string(),
            RetryAdvice::DoNotRetry,
            key,
        )
    }
}

fn idempotency_key(arguments: &Value) -> Option<IdempotencyKey> {
    arguments
        .get("idempotency_key")
        .and_then(Value::as_str)
        .map(|value| IdempotencyKey(value.to_string()))
}

fn decode_data<T>(response: Value) -> LinearToolResultData<T>
where
    T: for<'de> Deserialize<'de>,
{
    let data = response.get("data").cloned().ok_or_else(|| {
        LinearToolFailure::new(
            FugueToolErrorCode::ExecutionFailed,
            "Linear response missing data",
            RetryAdvice::DoNotRetry,
            None,
        )
    })?;
    serde_json::from_value(data).map_err(|err| {
        LinearToolFailure::new(
            FugueToolErrorCode::ExecutionFailed,
            format!("Linear response did not match expected shape: {err}"),
            RetryAdvice::DoNotRetry,
            None,
        )
    })
}

fn classify_graphql_errors(kind: LinearGraphqlFailure) -> (FugueToolErrorCode, RetryAdvice) {
    match kind {
        LinearGraphqlFailure::Authentication => (
            FugueToolErrorCode::AuthenticationFailed,
            RetryAdvice::DoNotRetry,
        ),
        LinearGraphqlFailure::Permission => (
            FugueToolErrorCode::PermissionDenied,
            RetryAdvice::DoNotRetry,
        ),
        LinearGraphqlFailure::NotFound => (FugueToolErrorCode::NotFound, RetryAdvice::DoNotRetry),
        LinearGraphqlFailure::RateLimited => (
            FugueToolErrorCode::RateLimited,
            RetryAdvice::RetryAfter { seconds: 60 },
        ),
        LinearGraphqlFailure::Conflict => (
            FugueToolErrorCode::Conflict,
            RetryAdvice::RefreshStateThenRetry,
        ),
        LinearGraphqlFailure::Other => {
            (FugueToolErrorCode::ExecutionFailed, RetryAdvice::DoNotRetry)
        }
    }
}

fn payload_comment(
    success: bool,
    comment: Option<CommentNode>,
    operation: &str,
) -> LinearToolResultData<CommentNode> {
    if !success {
        return Err(LinearToolFailure::new(
            FugueToolErrorCode::ExecutionFailed,
            format!("{operation} returned success=false"),
            RetryAdvice::RefreshStateThenRetry,
            None,
        ));
    }
    comment.ok_or_else(|| {
        LinearToolFailure::new(
            FugueToolErrorCode::ExecutionFailed,
            format!("{operation} returned no comment"),
            RetryAdvice::RefreshStateThenRetry,
            None,
        )
    })
}

fn comment_attachment_snapshots(
    comment_id: &str,
    attachments: Vec<LinearCommentAttachmentInput>,
) -> Vec<LinearFileAttachmentSnapshot> {
    attachments
        .into_iter()
        .map(|attachment| LinearFileAttachmentSnapshot {
            source: LinearFileAttachmentSource::Comment {
                comment_id: LinearCommentId(comment_id.to_string()),
            },
            title: attachment.title,
            path: attachment.path.unwrap_or_else(|| attachment.url.clone()),
            url: attachment.url,
            content_type: attachment.content_type,
            byte_len: attachment.byte_len,
        })
        .collect()
}

#[derive(Debug, Clone)]
struct AttachmentCandidate {
    source: LinearFileAttachmentSource,
    title: Option<String>,
    url: String,
}

async fn download_issue_file_attachments(
    client: &LinearClient,
    issue: &IssueWithComments,
) -> LinearToolResultData<Vec<LinearFileAttachmentSnapshot>> {
    let candidates = issue_file_attachment_candidates(issue);
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let storage = client.attachment_root();
    let root = match storage {
        Some(root) => root.join("attachments"),
        None => linear_attachment_root_dir()?,
    }
    .join(sanitize_path_component(&issue.issue.identifier.0));
    tokio::fs::create_dir_all(&root).await.map_err(|err| {
        LinearToolFailure::new(
            FugueToolErrorCode::ExecutionFailed,
            format!(
                "failed to create Linear attachment directory {}: {err}",
                root.display()
            ),
            RetryAdvice::RefreshStateThenRetry,
            None,
        )
    })?;

    let mut snapshots = Vec::with_capacity(candidates.len());
    let mut budget = client.attachment_download_limits().budget();
    for (index, candidate) in candidates.into_iter().enumerate() {
        if let Some(snapshot) =
            fugue_hosted_attachment_snapshot(storage, &issue.issue.identifier.0, &candidate)
        {
            snapshots.push(snapshot);
            continue;
        }
        let path = root.join(download_filename(index, &candidate));
        let downloaded = client
            .download_file_to_path(&candidate.url, &path, &mut budget)
            .await?;
        snapshots.push(LinearFileAttachmentSnapshot {
            source: candidate.source,
            title: candidate.title,
            url: candidate.url,
            path: downloaded.path.display().to_string(),
            content_type: downloaded.content_type,
            byte_len: downloaded.byte_len,
        });
    }
    Ok(snapshots)
}

fn issue_file_attachment_candidates(issue: &IssueWithComments) -> Vec<AttachmentCandidate> {
    let mut seen = BTreeSet::new();
    let mut candidates = Vec::new();

    for attachment in &issue.attachments {
        push_attachment_candidate(
            &mut candidates,
            &mut seen,
            AttachmentCandidate {
                source: LinearFileAttachmentSource::IssueAttachment {
                    attachment_id: crate::tools::LinearAttachmentId(attachment.id.clone()),
                },
                title: attachment.title.clone(),
                url: attachment.url.clone(),
            },
        );
    }

    if let Some(description) = &issue.issue.description {
        for link in extract_linear_file_links(description) {
            push_attachment_candidate(
                &mut candidates,
                &mut seen,
                AttachmentCandidate {
                    source: LinearFileAttachmentSource::IssueDescription,
                    title: link.title,
                    url: link.url,
                },
            );
        }
    }

    for comment in &issue.comments {
        for link in extract_linear_file_links(&comment.body) {
            push_attachment_candidate(
                &mut candidates,
                &mut seen,
                AttachmentCandidate {
                    source: LinearFileAttachmentSource::Comment {
                        comment_id: LinearCommentId(comment.id.clone()),
                    },
                    title: link.title,
                    url: link.url,
                },
            );
        }
    }

    candidates
}

fn push_attachment_candidate(
    candidates: &mut Vec<AttachmentCandidate>,
    seen: &mut BTreeSet<String>,
    candidate: AttachmentCandidate,
) {
    if !is_linear_file_url(&candidate.url) {
        return;
    }
    let key = strip_url_fragment(&candidate.url).to_ascii_lowercase();
    if seen.insert(key) {
        candidates.push(candidate);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MarkdownFileLink {
    title: Option<String>,
    url: String,
}

fn extract_linear_file_links(markdown: &str) -> Vec<MarkdownFileLink> {
    let mut links = Vec::new();
    let bytes = markdown.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if !markdown.is_char_boundary(index) {
            index += 1;
            continue;
        }
        if bytes[index] == b']'
            && index + 1 < bytes.len()
            && bytes[index + 1] == b'('
            && let Some(label_start) = markdown[..index].rfind('[')
        {
            let label = markdown[label_start + 1..index].trim();
            let url_start = index + 2;
            if let Some(relative_end) = markdown[url_start..].find(')') {
                let url_end = url_start + relative_end;
                let url = trim_markdown_url(&markdown[url_start..url_end]);
                if is_linear_file_url(url) {
                    links.push(MarkdownFileLink {
                        title: (!label.is_empty()).then(|| label.to_string()),
                        url: url.to_string(),
                    });
                }
                index = url_end + 1;
                continue;
            }
        }

        if markdown[index..].starts_with("https://") {
            let url_end = markdown[index..]
                .find(|character: char| {
                    character.is_whitespace() || character == ')' || character == '<'
                })
                .map(|offset| index + offset)
                .unwrap_or(markdown.len());
            let url = trim_markdown_url(&markdown[index..url_end]);
            if is_linear_file_url(url) {
                links.push(MarkdownFileLink {
                    title: None,
                    url: url.to_string(),
                });
            }
            index = url_end;
            continue;
        }

        index += 1;
    }
    links
}

fn trim_markdown_url(url: &str) -> &str {
    url.trim()
        .trim_matches('<')
        .trim_matches('>')
        .trim_end_matches(['.', ',', ';', ':'])
}

fn is_linear_file_url(url: &str) -> bool {
    is_allowed_attachment_download_url(url) || parse_fugue_hosted_attachment_url(url).is_some()
}

fn fugue_hosted_attachment_snapshot(
    storage: Option<&Path>,
    issue_identifier: &str,
    candidate: &AttachmentCandidate,
) -> Option<LinearFileAttachmentSnapshot> {
    let (_url_issue_identifier, file_name) = parse_fugue_hosted_attachment_url(&candidate.url)?;
    let root = match storage {
        Some(root) => root.to_path_buf(),
        None => std::env::current_dir().ok()?.join(".fugue"),
    };
    let path = root
        .join("issue-attachments")
        .join(sanitize_path_component(issue_identifier))
        .join(file_name);
    let byte_len = std::fs::metadata(&path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    Some(LinearFileAttachmentSnapshot {
        source: candidate.source.clone(),
        title: candidate.title.clone(),
        url: candidate.url.clone(),
        path: path.display().to_string(),
        content_type: None,
        byte_len,
    })
}

fn parse_fugue_hosted_attachment_url(url: &str) -> Option<(String, String)> {
    let without_fragment = strip_url_fragment(url);
    let without_query = without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment);
    let parts: Vec<_> = without_query.split('/').collect();
    if parts.len() != 7
        || !parts[0].is_empty()
        || parts[1] != "api"
        || parts[2] != "v1"
        || parts[3] != "issues"
        || parts[5] != "attachments"
    {
        return None;
    }
    let issue_identifier = parts[4];
    let file_name = parts[6];
    if issue_identifier.is_empty()
        || file_name.is_empty()
        || issue_identifier.contains("..")
        || file_name.contains("..")
        || file_name != sanitize_path_component(file_name)
    {
        return None;
    }
    Some((issue_identifier.to_string(), file_name.to_string()))
}

fn linear_attachment_root_dir() -> LinearToolResultData<PathBuf> {
    if let Some(path) = std::env::var_os(LINEAR_ATTACHMENT_DIR_ENV).map(PathBuf::from) {
        return Ok(path);
    }
    let cwd = std::env::current_dir().map_err(|err| {
        LinearToolFailure::new(
            FugueToolErrorCode::ExecutionFailed,
            format!(
                "failed to derive workspace-local Linear attachment directory from current directory; set {LINEAR_ATTACHMENT_DIR_ENV}: {err}"
            ),
            RetryAdvice::RefreshStateThenRetry,
            None,
        )
    })?;
    Ok(cwd.join(".fugue").join("attachments"))
}

fn download_filename(index: usize, candidate: &AttachmentCandidate) -> String {
    let url_name = filename_from_url(&candidate.url);
    let title_name = candidate
        .title
        .as_deref()
        .map(sanitize_path_component)
        .filter(|name| !name.is_empty());
    let mut name = title_name
        .or_else(|| url_name.clone())
        .unwrap_or_else(|| "attachment".to_string());
    if Path::new(&name).extension().is_none()
        && let Some(extension) = url_name
            .as_deref()
            .and_then(|url_name| Path::new(url_name).extension())
            .and_then(|extension| extension.to_str())
            .filter(|extension| !extension.is_empty())
    {
        name.push('.');
        name.push_str(extension);
    }
    format!("{:03}-{name}", index + 1)
}

fn filename_from_url(url: &str) -> Option<String> {
    let without_fragment = strip_url_fragment(url);
    let without_query = without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment);
    without_query
        .rsplit('/')
        .next()
        .map(sanitize_path_component)
        .filter(|name| !name.is_empty())
}

fn strip_url_fragment(url: &str) -> &str {
    url.split('#').next().unwrap_or(url)
}

fn sanitize_path_component(value: &str) -> String {
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

#[derive(Debug, Clone, Copy)]
enum CommentFetch {
    None,
    First100,
}

fn workpad_body(heading: &str, body_markdown: &str) -> String {
    let body = body_markdown.trim();
    if body.is_empty() {
        return heading.to_string();
    }
    if let Some((first_line, rest)) = body.split_once('\n') {
        if heading_text_matches(first_line, heading) {
            let rest = rest.trim_start_matches('\n');
            if rest.is_empty() {
                heading.to_string()
            } else {
                format!("{heading}\n\n{rest}")
            }
        } else {
            format!("{heading}\n\n{body}")
        }
    } else if heading_text_matches(body, heading) {
        heading.to_string()
    } else {
        format!("{heading}\n\n{body}")
    }
}

pub(crate) fn normalize_workpad_heading(heading: &str, issue_identifier: &str) -> String {
    let trimmed = heading.trim();
    if trimmed.is_empty() {
        return DEFAULT_WORKPAD_HEADING.to_string();
    }
    let text = markdown_heading_text(trimmed);
    if text.eq_ignore_ascii_case(markdown_heading_text(DEFAULT_WORKPAD_HEADING)) {
        return DEFAULT_WORKPAD_HEADING.to_string();
    }
    let issue_heading = format!("## {issue_identifier} Workpad");
    if text.eq_ignore_ascii_case(markdown_heading_text(&issue_heading)) {
        return issue_heading;
    }
    if trimmed.starts_with('#') {
        trimmed.to_string()
    } else {
        format!("## {trimmed}")
    }
}

#[cfg(test)]
fn find_workpad_comment<'a>(comments: &'a [CommentNode], heading: &str) -> Option<&'a CommentNode> {
    find_workpad_comment_by_headings(comments, &[heading])
}

fn find_workpad_comment_for_issue<'a>(
    comments: &'a [CommentNode],
    heading: &str,
    issue_identifier: &str,
) -> Option<&'a CommentNode> {
    let issue_heading = format!("## {issue_identifier} Workpad");
    let heading = normalize_workpad_heading(heading, issue_identifier);
    find_workpad_comment_by_headings(comments, &[heading.as_str(), issue_heading.as_str()])
}

fn workpad_snapshot_for_issue(
    comments: &[CommentNode],
    heading: &str,
    issue_identifier: &str,
) -> Option<LinearWorkpadSnapshot> {
    find_workpad_comment_for_issue(comments, heading, issue_identifier).map(|comment| {
        LinearWorkpadSnapshot {
            comment_id: LinearCommentId(comment.id.clone()),
            heading: workpad_heading_from_body(&comment.body, heading, issue_identifier),
            body_markdown: comment.body.clone(),
            url: comment.url.clone(),
        }
    })
}

fn workpad_heading_from_body(body: &str, heading: &str, issue_identifier: &str) -> String {
    let issue_heading = format!("## {issue_identifier} Workpad");
    let heading = normalize_workpad_heading(heading, issue_identifier);
    body.lines()
        .map(str::trim)
        .find(|line| {
            heading_text_matches(line, &heading) || heading_text_matches(line, &issue_heading)
        })
        .map(|line| {
            if heading_text_matches(line, &issue_heading) {
                issue_heading.clone()
            } else {
                heading.clone()
            }
        })
        .unwrap_or(heading)
        .to_string()
}

fn find_workpad_comment_by_headings<'a>(
    comments: &'a [CommentNode],
    headings: &[&str],
) -> Option<&'a CommentNode> {
    comments
        .iter()
        .filter(|comment| {
            comment.body.lines().any(|line| {
                headings
                    .iter()
                    .any(|heading| heading_text_matches(line, heading))
            })
        })
        .max_by_key(|comment| {
            let has_exact_heading = comment.body.lines().any(|line| {
                let trimmed = line.trim();
                headings.contains(&trimmed)
            });
            (workpad_comment_rank(&comment.body), has_exact_heading)
        })
}

fn heading_text_matches(line: &str, heading: &str) -> bool {
    markdown_heading_text(line).eq_ignore_ascii_case(markdown_heading_text(heading))
}

fn markdown_heading_text(line: &str) -> &str {
    line.trim().trim_start_matches('#').trim()
}

fn workpad_comment_rank(body: &str) -> u8 {
    let has_issue_specific_section = [
        "### Plan",
        "### Implementation plan",
        "### Protected behavior",
        "### Acceptance criteria",
        "### Acceptance checklist",
    ]
    .iter()
    .any(|section| body.contains(section));
    let has_issue_plan = has_issue_specific_section
        && (body.contains("- [ ]") || body.contains("- [x]") || body.contains("- [X]"))
        && !body.contains("Pending planner-authored issue-specific checkbox plan");
    let has_handoff = body.contains("/pull/")
        || body.contains("PR #")
        || body.contains("### Pull request or Handoff\n- [x]");
    match (has_issue_plan, has_handoff) {
        (_, true) => 3,
        (true, false) => 2,
        (false, false) if body.contains("Pending planner-authored") => 0,
        (false, false) => 1,
    }
}

impl LinearIssueRef {
    fn lookup_value(&self) -> &str {
        match self {
            LinearIssueRef::Id { id } => &id.0,
            LinearIssueRef::Identifier { identifier } => &identifier.0,
        }
    }
}

#[derive(Debug, Deserialize)]
struct IssueResponse {
    issue: Option<IssueNode>,
}

#[derive(Debug, Deserialize)]
struct ViewerResponse {
    viewer: UserNode,
}

#[derive(Debug, Deserialize)]
struct TeamBySelectorResponse {
    team: Option<TeamNode>,
}

#[derive(Debug, Deserialize)]
struct WorkflowStatesResponse {
    #[serde(rename = "workflowStates")]
    workflow_states: Nodes<StateNode>,
}

#[derive(Debug, Deserialize)]
struct WorkflowStateCreateResponse {
    #[serde(rename = "workflowStateCreate")]
    workflow_state_create: WorkflowStatePayload,
}

#[derive(Debug, Deserialize)]
struct WorkflowStatePayload {
    success: bool,
    #[serde(rename = "workflowState")]
    workflow_state: Option<StateNode>,
}

#[derive(Debug, Deserialize)]
struct CommentsResponse {
    comments: Nodes<CommentChangeNode>,
}

#[derive(Debug, Deserialize)]
struct IssueUpdateResponse {
    #[serde(rename = "issueUpdate")]
    issue_update: IssuePayload,
}

#[derive(Debug, Deserialize)]
struct CommentCreateResponse {
    #[serde(rename = "commentCreate")]
    comment_create: CommentPayload,
}

#[derive(Debug, Deserialize)]
struct CommentUpdateResponse {
    #[serde(rename = "commentUpdate")]
    comment_update: CommentPayload,
}

#[derive(Debug, Deserialize)]
struct IssuePayload {
    success: bool,
    issue: Option<IssueNode>,
}

#[derive(Debug, Deserialize)]
struct CommentPayload {
    success: bool,
    comment: Option<CommentNode>,
}

#[derive(Debug, Deserialize)]
struct Nodes<T> {
    nodes: Vec<T>,
    #[serde(rename = "pageInfo", default)]
    page_info: PageInfo,
}

#[derive(Debug, Default, Deserialize)]
struct PageInfo {
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IssueNode {
    id: String,
    identifier: String,
    title: String,
    description: Option<String>,
    url: Option<String>,
    #[serde(rename = "branchName")]
    branch_name: Option<String>,
    #[serde(rename = "updatedAt")]
    updated_at: DateTime<Utc>,
    state: StateNode,
    team: TeamNode,
    project: Option<IssueProjectNode>,
    assignee: Option<UserNode>,
    labels: Nodes<LabelNode>,
    #[serde(default)]
    attachments: Option<Nodes<AttachmentNode>>,
    comments: Option<Nodes<CommentNode>>,
}

#[derive(Debug, Deserialize)]
struct IssueProjectNode {
    #[serde(rename = "slugId")]
    slug_id: String,
}

#[derive(Debug, Deserialize)]
struct TeamNode {
    id: String,
}

#[derive(Debug, Deserialize)]
struct StateNode {
    id: String,
    name: String,
    #[serde(rename = "type")]
    state_type: LinearStateTypeWire,
}

#[derive(Debug, Deserialize)]
struct UserNode {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct LabelNode {
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CommentNode {
    id: String,
    body: String,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct AttachmentNode {
    id: String,
    title: Option<String>,
    url: String,
}

fn stable_comment_body_mark(body: &str) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in body.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}:{}", body.len())
}

#[derive(Debug, Deserialize)]
struct CommentChangeNode {
    id: String,
    #[serde(rename = "createdAt")]
    created_at: DateTime<Utc>,
    issue: CommentIssueNode,
    user: Option<UserNode>,
}

#[derive(Debug, Deserialize)]
struct CommentIssueNode {
    id: String,
    project: Option<IssueProjectNode>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum LinearStateTypeWire {
    Backlog,
    Unstarted,
    Started,
    Completed,
    Canceled,
    Duplicate,
}

struct IssueWithComments {
    issue: LinearIssueSnapshot,
    comments: Vec<CommentNode>,
    attachments: Vec<AttachmentNode>,
}

impl From<IssueNode> for IssueWithComments {
    fn from(node: IssueNode) -> Self {
        let IssueNode {
            id,
            identifier,
            title,
            description,
            url,
            branch_name,
            updated_at,
            state,
            team,
            project,
            assignee,
            labels,
            attachments,
            comments,
        } = node;
        let comments = comments.map(|comments| comments.nodes).unwrap_or_default();
        let attachments = attachments
            .map(|attachments| attachments.nodes)
            .unwrap_or_default();
        let issue_node = IssueNode {
            id,
            identifier,
            title,
            description,
            url,
            branch_name,
            updated_at,
            state,
            team,
            project,
            assignee,
            labels,
            attachments: None,
            comments: None,
        };
        Self {
            issue: LinearIssueSnapshot::from(issue_node),
            comments,
            attachments,
        }
    }
}

fn unresolved_rework_finding_blocks_handoff_move(
    current: &LinearStateSnapshot,
    target: &LinearStateSnapshot,
    comments: &[CommentNode],
    review_handoff_states: &[String],
) -> Option<String> {
    if !current.name.eq_ignore_ascii_case("Rework")
        || !linear_state_is_review_handoff(target, review_handoff_states)
    {
        return None;
    }
    comments.iter().find_map(|comment| {
        crate::review_findings::extract_unresolved_operator_review_note(&comment.body)
    })
}

impl From<CommentNode> for LinearIssueCommentSnapshot {
    fn from(node: CommentNode) -> Self {
        Self {
            comment_id: LinearCommentId(node.id),
            body_markdown: node.body,
            url: node.url,
        }
    }
}

impl From<IssueNode> for LinearIssueSnapshot {
    fn from(node: IssueNode) -> Self {
        Self {
            id: LinearIssueId(node.id),
            identifier: LinearIssueIdentifier(node.identifier),
            title: node.title,
            description: node.description,
            url: node.url,
            state: node.state.into(),
            assignee: node.assignee.map(LinearUserSnapshot::from),
            branch_name: node.branch_name.map(BranchName),
            labels: node
                .labels
                .nodes
                .into_iter()
                .map(|label| label.name)
                .collect(),
            updated_at: node.updated_at,
            team_id: LinearTeamId(node.team.id),
        }
    }
}

impl From<StateNode> for LinearStateSnapshot {
    fn from(node: StateNode) -> Self {
        Self {
            id: LinearStateId(node.id),
            name: node.name,
            state_type: node.state_type.into(),
        }
    }
}

impl From<LinearStateTypeWire> for LinearStateType {
    fn from(value: LinearStateTypeWire) -> Self {
        match value {
            LinearStateTypeWire::Backlog => Self::Backlog,
            LinearStateTypeWire::Unstarted => Self::Unstarted,
            LinearStateTypeWire::Started => Self::Started,
            LinearStateTypeWire::Completed => Self::Completed,
            LinearStateTypeWire::Canceled => Self::Canceled,
            LinearStateTypeWire::Duplicate => Self::Duplicate,
        }
    }
}

impl From<UserNode> for LinearUserSnapshot {
    fn from(node: UserNode) -> Self {
        Self {
            id: LinearUserId(node.id),
            name: node.name,
        }
    }
}

const INSPECT_ISSUE_QUERY: &str = r#"
query FugueInspectIssue($id: String!) {
  issue(id: $id) {
    id identifier title description url branchName updatedAt
    state { id name type }
    team { id }
    project { slugId }
    assignee { id name }
    labels { nodes { name } }
  }
}
"#;

const INSPECT_ISSUE_WITH_COMMENTS_QUERY: &str = r#"
query FugueInspectIssueWithComments($id: String!) {
  issue(id: $id) {
    id identifier title description url branchName updatedAt
    state { id name type }
    team { id }
    project { slugId }
    assignee { id name }
    labels { nodes { name } }
    comments(first: 100) { nodes { id body url } }
    attachments(first: 100) { nodes { id title url } }
  }
}
"#;

const VIEWER_QUERY: &str = r#"
query FugueViewer { viewer { id name } }
"#;

const TEAM_BY_SELECTOR_QUERY: &str = r#"
query FugueTeamBySelector($id: String!) {
  team(id: $id) { id }
}
"#;

const LIST_PROJECT_STATES_QUERY: &str = r#"
query FugueProjectStates($teamId: ID, $first: Int!) {
  workflowStates(
    filter: { team: { id: { eq: $teamId } } }
    first: $first
  ) {
    nodes { id name type }
  }
}
"#;

const CREATE_WORKFLOW_STATE_QUERY: &str = r#"
mutation FugueCreateWorkflowState($input: WorkflowStateCreateInput!) {
  workflowStateCreate(input: $input) {
    success
    workflowState { id name type }
  }
}
"#;

const UPDATE_ISSUE_QUERY: &str = r#"
mutation FugueUpdateIssue($id: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $id, input: $input) {
    success
    issue {
      id identifier title description url branchName updatedAt
      state { id name type }
      team { id }
      project { slugId }
      assignee { id name }
      labels { nodes { name } }
    }
  }
}
"#;

const CREATE_COMMENT_QUERY: &str = r#"
mutation FugueCreateComment($input: CommentCreateInput!) {
  commentCreate(input: $input) { success comment { id body url } }
}
"#;

const UPDATE_COMMENT_QUERY: &str = r#"
mutation FugueUpdateComment($id: String!, $input: CommentUpdateInput!) {
  commentUpdate(id: $id, input: $input, skipEditedAt: true) { success comment { id body url } }
}
"#;

fn list_project_states_variables(input: &ListProjectStatesInput) -> LinearToolResultData<Value> {
    if let Some(project_id) = &input.project_id {
        return Err(LinearToolFailure::new(
            FugueToolErrorCode::InvalidInput,
            format!(
                "project_id is not supported for linear.list_project_states; provide team_id only: {}",
                project_id.0
            ),
            RetryAdvice::DoNotRetry,
            None,
        ));
    }
    Ok(json!({
        "teamId": input.team_id.as_ref().map(|id| id.0.as_str()),
        "first": 100,
    }))
}

fn existing_state_by_name<'a>(
    states: &'a [LinearStateSnapshot],
    name: &str,
) -> Option<&'a LinearStateSnapshot> {
    let normalized = crate::domain::normalize_state(name);
    states
        .iter()
        .find(|state| crate::domain::normalize_state(&state.name) == normalized)
}

fn linear_state_type_input(state_type: LinearStateType) -> &'static str {
    match state_type {
        LinearStateType::Backlog => "backlog",
        LinearStateType::Unstarted => "unstarted",
        LinearStateType::Started => "started",
        LinearStateType::Completed => "completed",
        LinearStateType::Canceled => "canceled",
        LinearStateType::Duplicate => "duplicate",
    }
}

fn linear_state_type_color(state_type: LinearStateType) -> &'static str {
    match state_type {
        LinearStateType::Backlog => "#bec2c8",
        LinearStateType::Unstarted => "#f2c94c",
        LinearStateType::Started => "#5e6ad2",
        LinearStateType::Completed => "#27ae60",
        LinearStateType::Canceled => "#95a2b3",
        LinearStateType::Duplicate => "#95a2b3",
    }
}

fn linear_state_is_terminal(state: &LinearStateSnapshot) -> bool {
    matches!(
        state.state_type,
        LinearStateType::Completed | LinearStateType::Canceled | LinearStateType::Duplicate
    ) || std::env::var(LINEAR_TERMINAL_STATES_ENV)
        .ok()
        .is_some_and(|states| {
            states
                .split(',')
                .map(str::trim)
                .any(|candidate| candidate.eq_ignore_ascii_case(&state.name))
        })
}

fn linear_state_is_review_handoff(
    state: &LinearStateSnapshot,
    review_handoff_states: &[String],
) -> bool {
    linear_state_is_terminal(state)
        || review_handoff_states
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&state.name))
}

fn review_handoff_states_from_env() -> Vec<String> {
    std::env::var(LINEAR_REVIEW_HANDOFF_STATES_ENV)
        .ok()
        .map(|states| {
            states
                .split(',')
                .map(str::trim)
                .filter(|state| !state.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn recent_comments_request(
    project_slug: &str,
    issue_id: Option<String>,
    limit: i64,
    cursor: Option<String>,
) -> (&'static str, Value) {
    match issue_id {
        Some(issue_id) => (
            LIST_RECENT_COMMENTS_FOR_ISSUE_QUERY,
            json!({
                "issueId": issue_id,
                "projectSlug": project_slug,
                "first": limit,
                "after": cursor,
            }),
        ),
        None => (
            LIST_RECENT_COMMENTS_QUERY,
            json!({
                "projectSlug": project_slug,
                "first": limit,
                "after": cursor,
            }),
        ),
    }
}

const LIST_RECENT_COMMENTS_QUERY: &str = r#"
query FugueRecentComments($projectSlug: String!, $first: Int!, $after: String) {
  comments(filter: { issue: { project: { slugId: { eq: $projectSlug } } } }, first: $first, after: $after) {
    nodes { id createdAt issue { id project { slugId } } user { id name } }
    pageInfo { endCursor }
  }
}
"#;

const LIST_RECENT_COMMENTS_FOR_ISSUE_QUERY: &str = r#"
query FugueRecentCommentsForIssue($projectSlug: String!, $issueId: ID!, $first: Int!, $after: String) {
  comments(filter: { issue: { project: { slugId: { eq: $projectSlug } }, id: { eq: $issueId } } }, first: $first, after: $after) {
    nodes { id createdAt issue { id project { slugId } } user { id name } }
    pageInfo { endCursor }
  }
}
"#;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn linear_comment_feed_is_project_filtered_and_refuses_foreign_results() {
        use crate::jira::tests::{mock_jira as mock_http, reply};
        for project in [Some("own-project"), Some("foreign-project"), None] {
            let response = json!({"data":{"comments":{"nodes":[{
                "id":"comment-1", "createdAt":"2026-01-01T00:00:00Z",
                "issue":{"id":"issue-1", "project":project.map(|slug|json!({"slugId":slug}))},
                "user":{"id":"user-1", "name":"Private actor"}
            }],"pageInfo":{"endCursor":null}}}});
            let mock = mock_http(vec![reply("POST", "/graphql", response)]).await;
            let config = scoped_linear_fixture(&format!("{}/graphql", mock.endpoint));
            let (failed, content) = execute_linear_fugue_tool(
                &config,
                "linear_list_recent_issue_changes",
                &json!({"limit":10}),
            )
            .await;
            assert_eq!(failed, project != Some("own-project"), "{content}");
            if failed {
                assert!(!content.contains("Private actor"));
            }
            let requests = mock.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].1["variables"]["projectSlug"], "own-project");
            assert!(
                requests[0].1["query"]
                    .as_str()
                    .unwrap()
                    .contains("project: { slugId: { eq: $projectSlug } }")
            );
        }
    }

    #[tokio::test]
    async fn tracker_boundary_partial_linear_errors_do_not_disclose_provider_data() {
        use crate::jira::tests::{mock_jira as mock_http, reply};
        let mut partial = linear_scope_response(Some("other-project"));
        partial["errors"] = json!([{"message":"Issue not found: private-message-sentinel", "path":["private-path-sentinel"], "extensions":{"code":"ENTITY_NOT_FOUND","private":"private-extension-sentinel"}}]);
        let mock = mock_http(vec![reply("POST", "/graphql", partial)]).await;
        let config = scoped_linear_fixture(&format!("{}/graphql", mock.endpoint));
        let (failed, content) = execute_linear_fugue_tool(
            &config,
            "linear_inspect_issue",
            &json!({"issue":{"kind":"id","id":"issue-1"}}),
        )
        .await;
        assert!(failed);
        for private in [
            "Private issue",
            "other-project",
            "private-message-sentinel",
            "private-path-sentinel",
            "private-extension-sentinel",
        ] {
            assert!(
                !content.contains(private),
                "error exposed {private}: {content}"
            );
        }
        let result: FugueToolResult = serde_json::from_str(&content).unwrap();
        assert!(
            matches!(result, FugueToolResult::Err {error,..} if error.code == FugueToolErrorCode::NotFound)
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tracker_boundary_linear_error_categories_survive_without_raw_provider_messages() {
        use crate::jira::tests::{mock_jira as mock_http, reply};
        for (error, expected, retry) in [
            (
                json!({"message":"private provider details", "extensions":{"code":"RATELIMITED"}}),
                FugueToolErrorCode::RateLimited,
                RetryAdvice::RetryAfter { seconds: 60 },
            ),
            (
                json!({"message":"Authentication failed for private user"}),
                FugueToolErrorCode::AuthenticationFailed,
                RetryAdvice::DoNotRetry,
            ),
            (
                json!({"message":"Permission missing on private team"}),
                FugueToolErrorCode::PermissionDenied,
                RetryAdvice::DoNotRetry,
            ),
            (
                json!({"message":"Conflict with private issue"}),
                FugueToolErrorCode::Conflict,
                RetryAdvice::RefreshStateThenRetry,
            ),
            (
                json!({"message":"private unrecognized failure"}),
                FugueToolErrorCode::ExecutionFailed,
                RetryAdvice::DoNotRetry,
            ),
        ] {
            let status = if expected == FugueToolErrorCode::RateLimited {
                axum::http::StatusCode::BAD_REQUEST
            } else {
                axum::http::StatusCode::OK
            };
            let mock =
                mock_http(vec![reply(
                "POST",
                "/graphql",
                json!({"errors":[error],"data":{"issue":{"title":"private foreign title"}}}),
            ).with_status(status)])
                .await;
            let config = scoped_linear_fixture(&format!("{}/graphql", mock.endpoint));
            let (failed, content) = execute_linear_fugue_tool(
                &config,
                "linear_inspect_issue",
                &json!({"issue":{"kind":"id","id":"issue-1"}}),
            )
            .await;
            assert!(failed);
            assert!(!content.contains("private"), "{content}");
            let result: FugueToolResult = serde_json::from_str(&content).unwrap();
            let FugueToolResult::Err { error, .. } = result else {
                panic!("expected failure")
            };
            assert_eq!(error.code, expected);
            assert_eq!(error.retry, retry);
        }
    }

    #[tokio::test]
    async fn tracker_boundary_raw_graphql_errors_are_safe_json() {
        use crate::jira::tests::{mock_jira as mock_http, reply};
        let mock = mock_http(vec![reply("POST", "/graphql", json!({"errors":[{"message":"private diagnostic", "extensions":{"code":"RATELIMITED"}}],"data":{"viewer":{"email":"private contact"}}}))]).await;
        let config = scoped_linear_fixture(&format!("{}/graphql", mock.endpoint));
        let (failed, content) = crate::tracker::execute_linear_graphql_tool(
            &config,
            &json!({"query":"query { viewer { id } }"}),
        )
        .await;
        assert!(failed);
        assert!(!content.contains("private"));
        let payload: Value = serde_json::from_str(&content).unwrap();
        assert!(payload["error"].as_str().unwrap().contains("rate limit"));
    }

    #[tokio::test]
    async fn linear_bulk_issue_lookup_keeps_project_filter_on_wire() {
        use crate::jira::tests::{mock_jira as mock_http, reply};
        let mock = mock_http(vec![reply(
            "POST",
            "/graphql",
            json!({"data":{"issues":{"nodes":[]}}}),
        )])
        .await;
        let config = scoped_linear_fixture(&format!("{}/graphql", mock.endpoint));
        let client = LinearClient::from_config(&config).unwrap();
        assert!(
            client
                .fetch_issue_states_by_ids(&["foreign-issue".into()])
                .await
                .unwrap()
                .is_empty()
        );
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].1["variables"]["projectSlug"], "own-project");
        assert_eq!(requests[0].1["variables"]["ids"], json!(["foreign-issue"]));
        assert!(
            requests[0].1["query"]
                .as_str()
                .unwrap()
                .contains("project: { slugId: { eq: $projectSlug } }")
        );
    }

    fn scoped_linear_fixture(endpoint: &str) -> TrackerConfig {
        let definition = crate::config::WorkflowDefinition {
            config: json!({"tracker": {"kind":"linear", "endpoint":endpoint,
                "api_key":"fixture", "project_slug":"own-project"}})
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "fixture".into(),
        };
        crate::config::ServiceConfig::from_definition(
            PathBuf::from("/tmp/fugue-scope-fixture.yaml"),
            &definition,
        )
        .unwrap()
        .tracker
    }

    fn linear_scope_response(project: Option<&str>) -> Value {
        json!({"data":{
            "issue": {"id":"issue-1", "identifier":"TEST-1", "title":"Private issue",
                "description":null, "url":null, "branchName":null, "updatedAt":"2026-01-01T00:00:00Z",
                "state":{"id":"active", "name":"Active", "type":"started"},
                "team":{"id":"team"}, "project":project.map(|slug|json!({"slugId":slug})),
                "assignee":{"id":"operator", "name":"Operator"}, "labels":{"nodes":[]},
                "comments":{"nodes":[]}, "attachments":{"nodes":[]}},
            "commentCreate":{"success":true, "comment":{"id":"comment-1", "body":"note", "url":null}}
        }})
    }

    #[tokio::test]
    async fn linear_project_scope_precedes_inspection_or_comment_mutation_by_key_and_id() {
        use crate::jira::tests::{mock_jira as mock_http, reply};
        for project in [Some("other-project"), None] {
            for issue in [
                json!({"kind":"id","id":"issue-1"}),
                json!({"kind":"identifier","identifier":"TEST-1"}),
            ] {
                for tool in [
                    "linear_add_comment",
                    "linear_inspect_issue",
                    "linear_claim_issue",
                    "linear_move_issue",
                    "linear_upsert_workpad",
                ] {
                    let mock = mock_http(vec![
                        reply("POST", "/graphql", linear_scope_response(project)),
                        reply("POST", "/graphql", linear_scope_response(project)),
                    ])
                    .await;
                    let config = scoped_linear_fixture(&format!("{}/graphql", mock.endpoint));
                    let input = json!({"issue":issue, "body_markdown":"note", "heading":"## Fugue Workpad",
                        "create_if_missing":true, "assignee":{"kind":"id", "id":"operator"},
                        "state":{"kind":"name","name":"Active"}, "idempotency_key":"scope"});
                    let (failed, content) = execute_linear_fugue_tool(&config, tool, &input).await;
                    assert!(failed, "{tool}: {content}");
                    let result: FugueToolResult = serde_json::from_str(&content).unwrap();
                    assert!(
                        matches!(result, FugueToolResult::Err { error, .. } if error.code == FugueToolErrorCode::NotFound),
                        "{tool}: {content}"
                    );
                    let requests = mock.requests.lock().unwrap();
                    assert_eq!(
                        requests.len(),
                        1,
                        "only the membership lookup may reach Linear"
                    );
                    assert!(
                        requests[0].1["query"]
                            .as_str()
                            .unwrap()
                            .trim_start()
                            .starts_with("query ")
                    );
                }
            }
        }
        let mock = mock_http(
            (0..3)
                .map(|_| {
                    reply(
                        "POST",
                        "/graphql",
                        linear_scope_response(Some("own-project")),
                    )
                })
                .collect(),
        )
        .await;
        let config = scoped_linear_fixture(&format!("{}/graphql", mock.endpoint));
        let input = json!({"issue":{"kind":"id","id":"issue-1"}, "body_markdown":"note"});
        for tool in ["linear_inspect_issue", "linear_add_comment"] {
            let (failed, content) = execute_linear_fugue_tool(&config, tool, &input).await;
            assert!(!failed, "{content}");
        }
        assert!(mock.responses.lock().unwrap().is_empty());
    }

    use super::*;
    use serde_json::json;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        key: &'static str,
        original: Option<String>,
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let lock = ENV_LOCK.lock().expect("env test lock");
            let original = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self {
                key,
                original,
                _lock: lock,
            }
        }

        fn unset(key: &'static str) -> Self {
            let lock = ENV_LOCK.lock().expect("env test lock");
            let original = std::env::var(key).ok();
            unsafe {
                std::env::remove_var(key);
            }
            Self {
                key,
                original,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(original) = &self.original {
                    std::env::set_var(self.key, original);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    #[test]
    fn provider_safe_linear_names_route_to_typed_operations() {
        for contract in crate::tools::TOOL_CATALOG
            .iter()
            .filter(|contract| contract.namespace == crate::tools::ToolNamespace::Linear)
        {
            let name = contract.operation.provider_callback_name();

            if contract.operation == FugueToolOperation::LinearAddComment {
                assert!(
                    !is_linear_fugue_tool_name(name),
                    "{name} is intentionally kept off the agent-facing Linear MCP surface; agents must update the canonical Workpad instead"
                );
                continue;
            }
            assert!(
                is_linear_fugue_tool_name(name),
                "{name} should route as Linear"
            );
            assert_eq!(
                name.parse::<FugueToolOperation>().ok(),
                Some(contract.operation)
            );
        }
    }

    #[tokio::test]
    async fn unsupported_provider_tool_names_return_structured_errors() {
        let config = TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: vec![],
            in_flight_states: vec![],
            active_states: vec![],
            attachment_root: None,
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: vec![],
        };
        let (is_error, content) =
            execute_linear_fugue_tool(&config, "linear_delete_issue", &json!({})).await;

        assert!(is_error);
        let result: FugueToolResult =
            serde_json::from_str(&content).expect("structured tool error");
        match result {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.code, FugueToolErrorCode::UnsupportedOperation);
                assert_eq!(error.operation, FugueToolOperation::LinearInspectIssue);
                assert!(error.message.contains("linear_delete_issue"));
            }
            FugueToolResult::Ok { .. } => panic!("expected unsupported operation error"),
        }
    }

    #[test]
    fn comment_queries_request_urls_for_human_handoffs() {
        assert!(CREATE_COMMENT_QUERY.contains("id body url"));
        assert!(UPDATE_COMMENT_QUERY.contains("id body url"));
        assert!(INSPECT_ISSUE_WITH_COMMENTS_QUERY.contains("id body url"));
    }

    #[test]
    fn comment_payload_decodes_url() {
        let data: CommentCreateResponse = serde_json::from_value(json!({
            "commentCreate": {
                "success": true,
                "comment": {
                    "id": "comment-1",
                    "body": "hello",
                    "url": "https://linear.app/comment-1"
                }
            }
        }))
        .expect("comment response should decode");
        let comment = payload_comment(
            data.comment_create.success,
            data.comment_create.comment,
            "commentCreate",
        )
        .expect("comment payload should be present");
        assert_eq!(comment.url.as_deref(), Some("https://linear.app/comment-1"));
    }

    #[test]
    fn list_project_states_rejects_project_scope() {
        let err = list_project_states_variables(&ListProjectStatesInput {
            team_id: None,
            project_id: Some(crate::tools::LinearProjectId("project-1".to_string())),
        })
        .expect_err("project_id should be explicitly rejected");
        assert_eq!(err.code, FugueToolErrorCode::InvalidInput);
        assert_eq!(err.retry, RetryAdvice::DoNotRetry);
        assert!(err.message.contains("project_id"));
    }

    #[test]
    fn list_project_states_accepts_team_scope() {
        let variables = list_project_states_variables(&ListProjectStatesInput {
            team_id: Some(LinearTeamId("team-1".to_string())),
            project_id: None,
        })
        .expect("team scope should be accepted");
        assert_eq!(variables.get("teamId"), Some(&json!("team-1")));
        assert_eq!(variables.get("first"), Some(&json!(100)));
    }

    #[test]
    fn workflow_state_provisioning_uses_typed_linear_state_shapes() {
        assert!(CREATE_WORKFLOW_STATE_QUERY.contains("workflowStateCreate"));
        assert_eq!(linear_state_type_input(LinearStateType::Backlog), "backlog");
        assert_eq!(
            linear_state_type_input(LinearStateType::Unstarted),
            "unstarted"
        );
        assert_eq!(linear_state_type_input(LinearStateType::Started), "started");
        assert_eq!(
            linear_state_type_input(LinearStateType::Completed),
            "completed"
        );
        assert_eq!(
            linear_state_type_input(LinearStateType::Canceled),
            "canceled"
        );
        assert_eq!(
            linear_state_type_input(LinearStateType::Duplicate),
            "duplicate"
        );

        let existing = vec![LinearStateSnapshot {
            id: LinearStateId("state-1".to_string()),
            name: "Reviewing".to_string(),
            state_type: LinearStateType::Started,
        }];
        let found = existing_state_by_name(&existing, "reviewing")
            .expect("state lookup should normalize names");
        assert_eq!(found.id, LinearStateId("state-1".to_string()));
    }

    #[test]
    fn linear_state_terminal_guard_uses_type_and_configured_names() {
        let completed = LinearStateSnapshot {
            id: LinearStateId("done".to_string()),
            name: "Done".to_string(),
            state_type: LinearStateType::Completed,
        };
        let started = LinearStateSnapshot {
            id: LinearStateId("review-ready".to_string()),
            name: "Ready for review".to_string(),
            state_type: LinearStateType::Started,
        };
        let duplicate = LinearStateSnapshot {
            id: LinearStateId("duplicate".to_string()),
            name: "Duplicate".to_string(),
            state_type: LinearStateType::Duplicate,
        };
        let _guard = EnvGuard::set(LINEAR_TERMINAL_STATES_ENV, "Ready for review,Blocked");

        assert!(linear_state_is_terminal(&completed));
        assert!(linear_state_is_terminal(&duplicate));
        assert!(linear_state_is_terminal(&started));
    }

    #[test]
    fn unresolved_operator_review_comment_blocks_rework_to_review_ready_move() {
        let current = LinearStateSnapshot {
            id: LinearStateId("rework".to_string()),
            name: "Rework".to_string(),
            state_type: LinearStateType::Started,
        };
        let target = LinearStateSnapshot {
            id: LinearStateId("review-ready".to_string()),
            name: "Ready for review".to_string(),
            state_type: LinearStateType::Started,
        };
        let comments = vec![CommentNode {
            id: "comment-1".to_string(),
            body: "Operator review for PR #186: not accepted yet. Blocking rework: derive lanes from config.".to_string(),
            url: None,
        }];
        let _guard = EnvGuard::set(LINEAR_REVIEW_HANDOFF_STATES_ENV, "Ready for review,Blocked");

        let finding = unresolved_rework_finding_blocks_handoff_move(
            &current,
            &target,
            &comments,
            &["Ready for review".to_string(), "Blocked".to_string()],
        )
        .unwrap();

        assert!(finding.contains("Operator review"));
    }

    #[test]
    fn unresolved_operator_review_comment_allows_rework_to_planning_move() {
        let current = LinearStateSnapshot {
            id: LinearStateId("rework".to_string()),
            name: "Rework".to_string(),
            state_type: LinearStateType::Started,
        };
        let target = LinearStateSnapshot {
            id: LinearStateId("planning".to_string()),
            name: "Planning".to_string(),
            state_type: LinearStateType::Started,
        };
        let comments = vec![CommentNode {
            id: "comment-1".to_string(),
            body: "Operator review for PR #186: not accepted yet. Blocking rework: derive lanes from config.".to_string(),
            url: None,
        }];
        let _guard = EnvGuard::set(LINEAR_TERMINAL_STATES_ENV, "Ready for review,Blocked");

        assert!(
            unresolved_rework_finding_blocks_handoff_move(
                &current,
                &target,
                &comments,
                &["Ready for review".to_string(), "Blocked".to_string()],
            )
            .is_none()
        );
    }

    #[test]
    fn workpad_upsert_body_adds_single_heading() {
        assert_eq!(
            workpad_body("## Fugue Workpad", "### Plan\n- [ ] Do it"),
            "## Fugue Workpad\n\n### Plan\n- [ ] Do it"
        );
        assert_eq!(
            workpad_body("## Fugue Workpad", "## Fugue Workpad\n\n### Plan"),
            "## Fugue Workpad\n\n### Plan"
        );
        assert_eq!(
            workpad_body("## Fugue Workpad", "Fugue Workpad\n\n### Plan"),
            "## Fugue Workpad\n\n### Plan"
        );
    }

    #[test]
    fn workpad_heading_normalization_keeps_single_canonical_workpad_namespace() {
        assert_eq!(
            normalize_workpad_heading("Fugue Workpad", "LUC-392"),
            "## Fugue Workpad"
        );
        assert_eq!(
            normalize_workpad_heading("# Fugue Workpad", "LUC-392"),
            "## Fugue Workpad"
        );
        assert_eq!(
            normalize_workpad_heading("LUC-392 Workpad", "LUC-392"),
            "## LUC-392 Workpad"
        );
    }

    #[test]
    fn workpad_duplicate_prevention_selects_existing_heading() {
        let comments = vec![
            CommentNode {
                id: "regular".to_string(),
                body: "hello".to_string(),
                url: None,
            },
            CommentNode {
                id: "workpad".to_string(),
                body: "## Fugue Workpad\n\nold".to_string(),
                url: None,
            },
        ];
        let selected = find_workpad_comment(&comments, "## Fugue Workpad");
        assert_eq!(selected.map(|comment| comment.id.as_str()), Some("workpad"));
    }

    #[test]
    fn workpad_duplicate_prevention_prefers_issue_specific_plan_over_placeholder() {
        let comments = vec![
            CommentNode {
                id: "placeholder".to_string(),
                body: "## Fugue Workpad\n\n### Plan\n- [ ] Pending planner-authored issue-specific checkbox plan.\n\n### Progress\n- [x] Issue mob started.".to_string(),
                url: None,
            },
            CommentNode {
                id: "planner".to_string(),
                body: "## Fugue Workpad\n\n### Plan\n- [x] Read issue.\n- [ ] Implement feature.\n\n### Progress\n- [x] Planner wrote plan.".to_string(),
                url: None,
            },
        ];

        let selected = find_workpad_comment(&comments, "## Fugue Workpad");

        assert_eq!(selected.map(|comment| comment.id.as_str()), Some("planner"));
    }

    #[test]
    fn workpad_lookup_accepts_issue_specific_heading() {
        let comments = vec![CommentNode {
            id: "issue-workpad".to_string(),
            body: "## LUC-392 Workpad\n\n### Plan\n- [x] Keep the issue-specific plan.".to_string(),
            url: None,
        }];

        let selected = find_workpad_comment_for_issue(&comments, "## Fugue Workpad", "LUC-392");

        assert_eq!(
            selected.map(|comment| comment.id.as_str()),
            Some("issue-workpad")
        );
    }

    #[test]
    fn workpad_lookup_treats_plain_and_markdown_fugue_headings_as_same_workpad() {
        let comments = vec![CommentNode {
            id: "canonical-workpad".to_string(),
            body: "## Fugue Workpad\n\n### Plan\n- [x] Existing canonical plan.".to_string(),
            url: None,
        }];

        let selected = find_workpad_comment_for_issue(&comments, "Fugue Workpad", "LUC-392");

        assert_eq!(
            selected.map(|comment| comment.id.as_str()),
            Some("canonical-workpad")
        );
    }

    #[test]
    fn workpad_lookup_prefers_exact_markdown_heading_when_duplicate_plain_heading_exists() {
        let comments = vec![
            CommentNode {
                id: "plain-workpad".to_string(),
                body: "Fugue Workpad\n\n### Plan\n- [x] Existing plain plan.".to_string(),
                url: None,
            },
            CommentNode {
                id: "canonical-workpad".to_string(),
                body: "## Fugue Workpad\n\n### Plan\n- [x] Existing canonical plan.".to_string(),
                url: None,
            },
        ];

        let selected = find_workpad_comment_for_issue(&comments, "Fugue Workpad", "LUC-392");

        assert_eq!(
            selected.map(|comment| comment.id.as_str()),
            Some("canonical-workpad")
        );
    }

    #[test]
    fn workpad_snapshot_returns_default_workpad_body() {
        let comments = vec![CommentNode {
            id: "workpad".to_string(),
            body: "## Fugue Workpad\n\n### Plan\n- [x] Read the issue.\n- [ ] Implement."
                .to_string(),
            url: Some("https://linear.app/comment/workpad".to_string()),
        }];

        let snapshot = workpad_snapshot_for_issue(&comments, "## Fugue Workpad", "LUC-392")
            .expect("default workpad should be returned");

        assert_eq!(snapshot.comment_id, LinearCommentId("workpad".to_string()));
        assert_eq!(snapshot.heading, "## Fugue Workpad");
        assert!(snapshot.body_markdown.contains("### Plan"));
        assert_eq!(
            snapshot.url.as_deref(),
            Some("https://linear.app/comment/workpad")
        );
    }

    #[test]
    fn workpad_snapshot_returns_issue_specific_workpad_body() {
        let comments = vec![CommentNode {
            id: "issue-workpad".to_string(),
            body: "LUC-397 Workpad\n\n## LUC-397 Workpad\n\n### Plan\n- [x] Keep the issue-specific plan.".to_string(),
            url: None,
        }];

        let snapshot = workpad_snapshot_for_issue(&comments, "## Fugue Workpad", "LUC-397")
            .expect("issue-specific workpad should be returned");

        assert_eq!(
            snapshot.comment_id,
            LinearCommentId("issue-workpad".to_string())
        );
        assert_eq!(snapshot.heading, "## LUC-397 Workpad");
        assert!(
            snapshot
                .body_markdown
                .contains("Keep the issue-specific plan")
        );
    }

    #[test]
    fn inspect_issue_output_serializes_workpad_body() {
        let output = InspectIssueOutput {
            issue: LinearIssueSnapshot {
                id: LinearIssueId("issue-1".to_string()),
                identifier: LinearIssueIdentifier("LUC-423".to_string()),
                title: "Clean console integration".to_string(),
                description: None,
                url: None,
                state: LinearStateSnapshot {
                    id: LinearStateId("state-1".to_string()),
                    name: "Planning".to_string(),
                    state_type: LinearStateType::Started,
                },
                assignee: None,
                branch_name: None,
                labels: Vec::new(),
                updated_at: DateTime::<Utc>::UNIX_EPOCH,
                team_id: LinearTeamId("team-1".to_string()),
            },
            workpad: Some(LinearWorkpadSnapshot {
                comment_id: LinearCommentId("comment-1".to_string()),
                heading: "## Fugue Workpad".to_string(),
                body_markdown: "## Fugue Workpad\n\n### Plan\n- [ ] Do the work.".to_string(),
                url: None,
            }),
            attachments: vec![LinearFileAttachmentSnapshot {
                source: LinearFileAttachmentSource::IssueDescription,
                title: Some("input.png".to_string()),
                url: "https://uploads.linear.app/input.png".to_string(),
                path: "/workspace/LUC-423/.fugue/attachments/LUC-423/001-input.png".to_string(),
                content_type: Some("image/png".to_string()),
                byte_len: 12,
            }],
        };

        let value = serde_json::to_value(output).expect("output serializes");

        assert_eq!(
            value.pointer("/workpad/comment_id").and_then(Value::as_str),
            Some("comment-1")
        );
        assert_eq!(
            value
                .pointer("/workpad/body_markdown")
                .and_then(Value::as_str),
            Some("## Fugue Workpad\n\n### Plan\n- [ ] Do the work.")
        );
        assert_eq!(
            value.pointer("/attachments/0/path").and_then(Value::as_str),
            Some("/workspace/LUC-423/.fugue/attachments/LUC-423/001-input.png")
        );
    }

    #[test]
    fn markdown_linear_file_links_are_extracted_from_links_and_raw_urls() {
        let links = extract_linear_file_links(
            "Please inspect ![screenshot](https://uploads.linear.app/team/file.png) and https://uploads.linear.app/team/log.txt.",
        );

        assert_eq!(links.len(), 2);
        assert_eq!(links[0].title.as_deref(), Some("screenshot"));
        assert_eq!(links[0].url, "https://uploads.linear.app/team/file.png");
        assert_eq!(links[1].title, None);
        assert_eq!(links[1].url, "https://uploads.linear.app/team/log.txt");
    }

    #[test]
    fn issue_file_attachment_candidates_deduplicate_linear_uploads() {
        let issue = IssueWithComments {
            issue: LinearIssueSnapshot {
                id: LinearIssueId("issue-1".to_string()),
                identifier: LinearIssueIdentifier("LUC-631".to_string()),
                title: "Attachments".to_string(),
                description: Some("![same](https://uploads.linear.app/team/same.png)".to_string()),
                url: None,
                state: LinearStateSnapshot {
                    id: LinearStateId("state-1".to_string()),
                    name: "Todo".to_string(),
                    state_type: LinearStateType::Unstarted,
                },
                assignee: None,
                branch_name: None,
                labels: Vec::new(),
                updated_at: DateTime::<Utc>::UNIX_EPOCH,
                team_id: LinearTeamId("team-1".to_string()),
            },
            comments: vec![CommentNode {
                id: "comment-1".to_string(),
                body: "duplicate https://uploads.linear.app/team/same.png".to_string(),
                url: None,
            }],
            attachments: vec![AttachmentNode {
                id: "attachment-1".to_string(),
                title: Some("same.png".to_string()),
                url: "https://uploads.linear.app/team/same.png".to_string(),
            }],
        };

        let candidates = issue_file_attachment_candidates(&issue);

        assert_eq!(candidates.len(), 1);
        assert!(matches!(
            candidates[0].source,
            LinearFileAttachmentSource::IssueAttachment { .. }
        ));
    }

    fn hosted_attachment_issue() -> IssueWithComments {
        IssueWithComments {
            issue: LinearIssueSnapshot {
                id: LinearIssueId("issue-1".to_string()),
                identifier: LinearIssueIdentifier("LUC-653".to_string()),
                title: "Attachments".to_string(),
                description: None,
                url: None,
                state: LinearStateSnapshot {
                    id: LinearStateId("state-1".to_string()),
                    name: "Active".to_string(),
                    state_type: LinearStateType::Started,
                },
                assignee: None,
                branch_name: None,
                labels: Vec::new(),
                updated_at: DateTime::<Utc>::UNIX_EPOCH,
                team_id: LinearTeamId("team-1".to_string()),
            },
            comments: vec![CommentNode {
                id: "comment-1".to_string(),
                body: "Attached files:\n- [trace.txt](/api/v1/issues/LUC-653/attachments/001-trace.txt) (16 bytes)".to_string(),
                url: None,
            }],
            attachments: Vec::new(),
        }
    }

    #[test]
    fn issue_file_attachment_candidates_include_fugue_hosted_comment_links() {
        let issue = hosted_attachment_issue();

        let candidates = issue_file_attachment_candidates(&issue);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].title.as_deref(), Some("trace.txt"));
        assert_eq!(
            candidates[0].url,
            "/api/v1/issues/LUC-653/attachments/001-trace.txt"
        );
        assert!(matches!(
            candidates[0].source,
            LinearFileAttachmentSource::Comment { ref comment_id }
                if comment_id.0 == "comment-1"
        ));
        assert!(parse_fugue_hosted_attachment_url(&candidates[0].url).is_some());
    }

    #[tokio::test]
    async fn managed_linear_attachments_use_each_repositorys_storage() {
        let root = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        for name in ["first", "second"] {
            let state = root.path().join(name);
            let definition = crate::config::WorkflowDefinition {
                config: json!({
                    "tracker": {"kind":"linear", "api_key":"test-token", "project_slug":"test"},
                    "meerkat": {"repository_runtime":{"repository":name, "state_path":state}}
                })
                .as_object()
                .unwrap()
                .clone(),
                prompt_template: "test".into(),
            };
            let config = crate::config::ServiceConfig::from_definition(
                state.join("generation/fugue.yaml"),
                &definition,
            )
            .unwrap();
            let expected = config
                .attachment_storage_root()
                .join("issue-attachments/LUC-653/001-trace.txt");
            std::fs::create_dir_all(expected.parent().unwrap()).unwrap();
            std::fs::write(&expected, name).unwrap();
            let client = LinearClient::from_config(&config.tracker).unwrap();
            let attachments = download_issue_file_attachments(&client, &hosted_attachment_issue())
                .await
                .unwrap();
            assert_eq!(Path::new(&attachments[0].path), expected);
            assert_eq!(std::fs::read_to_string(&attachments[0].path).unwrap(), name);
            paths.push(attachments[0].path.clone());
        }
        assert_ne!(paths[0], paths[1]);
    }

    #[test]
    fn issue_file_attachment_candidates_ignore_non_file_link_attachments() {
        let issue = IssueWithComments {
            issue: LinearIssueSnapshot {
                id: LinearIssueId("issue-1".to_string()),
                identifier: LinearIssueIdentifier("LUC-641".to_string()),
                title: "Deploy".to_string(),
                description: None,
                url: None,
                state: LinearStateSnapshot {
                    id: LinearStateId("state-1".to_string()),
                    name: "Deploy".to_string(),
                    state_type: LinearStateType::Started,
                },
                assignee: None,
                branch_name: None,
                labels: Vec::new(),
                updated_at: DateTime::<Utc>::UNIX_EPOCH,
                team_id: LinearTeamId("team-1".to_string()),
            },
            comments: Vec::new(),
            attachments: vec![AttachmentNode {
                id: "attachment-1".to_string(),
                title: Some("PR #222".to_string()),
                url: "https://github.com/lukacf/meerkat-fugue/pull/222".to_string(),
            }],
        };

        assert!(issue_file_attachment_candidates(&issue).is_empty());
    }

    #[test]
    fn linear_attachment_root_defaults_to_workspace_local_current_dir() {
        let _guard = EnvGuard::unset(LINEAR_ATTACHMENT_DIR_ENV);
        let root = linear_attachment_root_dir().expect("attachment root");
        assert_eq!(
            root,
            std::env::current_dir()
                .expect("current dir")
                .join(".fugue")
                .join("attachments")
        );
        assert!(!root.starts_with(std::env::temp_dir().join("fugue-linear-attachments")));
    }

    #[test]
    fn linear_attachment_root_honors_explicit_workspace_env() {
        let workspace = tempfile::tempdir().expect("workspace");
        let expected = workspace.path().join(".fugue/attachments");
        let _guard = EnvGuard::set(
            LINEAR_ATTACHMENT_DIR_ENV,
            expected.to_str().expect("utf-8 path"),
        );
        assert_eq!(
            linear_attachment_root_dir().expect("attachment root"),
            expected
        );
    }

    #[test]
    fn workpad_lookup_accepts_title_line_before_issue_specific_heading() {
        let comments = vec![CommentNode {
            id: "issue-workpad".to_string(),
            body: "LUC-397 Workpad\n\n## LUC-397 Workpad\n\n### Plan\n- [x] Keep the issue-specific plan.".to_string(),
            url: None,
        }];

        let selected = find_workpad_comment_for_issue(&comments, "## Fugue Workpad", "LUC-397");

        assert_eq!(
            selected.map(|comment| comment.id.as_str()),
            Some("issue-workpad")
        );
    }

    #[test]
    fn workpad_lookup_prefers_handoff_evidence_across_compatible_headings() {
        let comments = vec![
            CommentNode {
                id: "placeholder".to_string(),
                body: "## Fugue Workpad\n\n### Plan\n- [ ] Pending planner-authored issue-specific checkbox plan.".to_string(),
                url: None,
            },
            CommentNode {
                id: "issue-workpad".to_string(),
                body: "## LUC-392 Workpad\n\n### Plan\n- [x] Plan.\n\n### Pull request or Handoff\n- [x] PR #111.".to_string(),
                url: None,
            },
        ];

        let selected = find_workpad_comment_for_issue(&comments, "## Fugue Workpad", "LUC-392");

        assert_eq!(
            selected.map(|comment| comment.id.as_str()),
            Some("issue-workpad")
        );
    }

    #[test]
    fn already_in_state_move_is_detectable_by_state_id() {
        let issue_state = LinearStateSnapshot {
            id: LinearStateId("state-1".to_string()),
            name: "In Progress".to_string(),
            state_type: LinearStateType::Started,
        };
        let target_state = LinearStateSnapshot {
            id: LinearStateId("state-1".to_string()),
            name: "In Progress".to_string(),
            state_type: LinearStateType::Started,
        };
        assert_eq!(issue_state.id, target_state.id);
    }

    #[test]
    fn recent_comments_query_stays_project_scoped_without_an_issue() {
        let (query, variables) = recent_comments_request("project", None, 25, None);

        assert!(query.contains("project: { slugId: { eq: $projectSlug } }"));
        assert_eq!(variables["projectSlug"], "project");
        assert_eq!(variables.get("first"), Some(&json!(25)));
        assert_eq!(variables.get("issueId"), None);
    }

    #[test]
    fn recent_comments_query_filters_when_issue_is_present() {
        let (query, variables) = recent_comments_request(
            "project",
            Some("issue-1".to_string()),
            25,
            Some("cursor-1".to_string()),
        );

        assert!(query.contains("filter: { issue:"));
        assert!(query.contains("project: { slugId: { eq: $projectSlug } }"));
        assert_eq!(variables["projectSlug"], "project");
        assert_eq!(variables.get("issueId"), Some(&json!("issue-1")));
        assert_eq!(variables.get("after"), Some(&json!("cursor-1")));
    }

    #[test]
    fn structured_linear_errors_are_classified() {
        let (code, retry) = classify_graphql_errors(LinearGraphqlFailure::NotFound);
        assert_eq!(code, FugueToolErrorCode::NotFound);
        assert_eq!(retry, RetryAdvice::DoNotRetry);
    }
}
