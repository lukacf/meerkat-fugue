use std::path::Path;

use crate::config::{
    LinearRuntimeConfig, MaterializedMobTemplate, NativeLinearProvisioningMode, ServiceConfig,
    TrackerConfig,
};
use crate::control_plane::{
    ControlPlaneEffect, HostControlObservation, RuntimePresence, reduce_comment_observation,
    reduce_open_issue_without_runtime, reduce_stage_observation,
};
use crate::domain::{Issue, normalize_state, sanitize_workspace_key};
use crate::error::{FugueError, Result};
use crate::fugue_ims::{CreateFugueIssue, FugueImsStore};
use crate::ims::ImsStageName;
use crate::kapellmeister::{
    CursorPartition, DurableCursorKey, DurableCursorPosition, DurableCursorSource,
    KapellmeisterRunningMobObservation, KapellmeisterShadowSignal,
};
use crate::linear_lane_state::{
    LINEAR_LANE_STATE_PATH_ENV, LinearCommentSnapshot, LinearLaneState,
    lane_state_path_for_workflow, legacy_linear_lane_repair, record_internal_lane_change_from_env,
};
pub use crate::linear_lane_state::{
    LinearCommentChange as IssueCommentChange, LinearCommentObservation as IssueCommentObservation,
    LinearCommentState as IssueCommentState, LinearLaneClassification as IssueLaneClassification,
    LinearLaneObservation as IssueLaneObservation, LinearLaneState as IssueLaneState,
};
use crate::linear_projection::{
    active_projection_phase_labels, evaluate_linear_projection, projection_lane_for_node,
    projection_phase_label_for_node,
};
use crate::linear_tools::{LINEAR_TERMINAL_STATES_ENV, LinearToolExecutor};
use crate::tools::{
    AddCommentInput, AddCommentOutput, Cursor, EnsureWorkflowState, EnsureWorkflowStatesInput,
    FugueToolError, FugueToolErrorCode, FugueToolOperation, FugueToolOutput, FugueToolResult,
    IdempotencyKey, InspectIssueInput, InspectIssueOutput, LinearCommentId,
    LinearIssueCommentSnapshot, LinearIssueIdentifier, LinearIssueRef, LinearStateRef,
    ListProjectStatesInput, MobEventSnapshot, MobRunStatus, MoveIssueInput, MoveIssueOutput,
    RetryAdvice, ToolExecutionMetadata, UpsertWorkpadInput, UpsertWorkpadOutput,
};
use crate::tracker::{IssueTracker, LinearClient};
use chrono::{DateTime, Utc};

pub fn terminal_states_env_name() -> &'static str {
    LINEAR_TERMINAL_STATES_ENV
}

pub fn lane_state_path_env_name() -> &'static str {
    LINEAR_LANE_STATE_PATH_ENV
}

pub fn record_internal_lane_from_env(identifier: &str, lane: &str) -> Result<()> {
    record_internal_lane_change_from_env(identifier, lane).map(|_| ())
}

pub fn record_internal_lane_for_workflow(
    workflow_dir: &Path,
    identifier: &str,
    lane: &str,
) -> Result<()> {
    let path = lane_state_path_for_workflow(workflow_dir);
    let mut state = LinearLaneState::read(&path)?;
    state.observe_lane(identifier, lane);
    state.write(&path)
}

pub fn issue_lane_state_path_for_workflow(workflow_dir: &Path) -> std::path::PathBuf {
    crate::linear_lane_state::lane_state_path_for_workflow(workflow_dir)
}

pub fn issue_comment_state_path_for_workflow(workflow_dir: &Path) -> std::path::PathBuf {
    crate::linear_lane_state::comment_state_path_for_workflow(workflow_dir)
}

pub fn classify_issue_lane(config: &ServiceConfig, state: &str) -> IssueLaneClassification {
    crate::linear_lane_state::classify_linear_lane(config, state)
}

pub fn issue_ref_for_lane_observation(identifier: String) -> LinearIssueRef {
    crate::linear_lane_state::issue_ref_for_lane_observation(identifier)
}

pub fn state_ref_for_observed_lane(lane: &str) -> LinearStateRef {
    crate::linear_lane_state::state_ref_for_lane(lane.to_string())
}

pub fn evaluate_issue_projection(
    template: &MaterializedMobTemplate,
    status: MobRunStatus,
    events: &[MobEventSnapshot],
) -> Option<String> {
    let projection = template.linear_projection.as_ref()?;
    evaluate_linear_projection(projection, status, events)
}

pub fn projection_lane_for_template_node(
    template: &MaterializedMobTemplate,
    node: &str,
) -> Option<String> {
    let projection = template.linear_projection.as_ref()?;
    projection_lane_for_node(projection, node).map(ToString::to_string)
}

pub fn projection_phase_label_for_template_node(
    template: &MaterializedMobTemplate,
    node: &str,
) -> Option<String> {
    let projection = template.linear_projection.as_ref()?;
    projection_phase_label_for_node(projection, node).map(ToString::to_string)
}

pub fn active_projection_phase_labels_for_template(
    template: &MaterializedMobTemplate,
    events: &[MobEventSnapshot],
) -> Vec<String> {
    template
        .linear_projection
        .as_ref()
        .map(|projection| active_projection_phase_labels(projection, events))
        .unwrap_or_default()
}

pub fn projected_terminal_lane_for_template_status(
    template: &MaterializedMobTemplate,
    status: MobRunStatus,
) -> Option<String> {
    let projection = template.linear_projection.as_ref()?;
    evaluate_linear_projection(projection, status, &[])
}

#[derive(Debug, Clone)]
pub struct IssueLaneRuntimeObservation {
    pub issue: Issue,
    pub running_mob: Option<KapellmeisterRunningMobObservation>,
}

pub fn reconcile_issue_lane_observation_state(
    workflow_dir: &Path,
    config: &ServiceConfig,
    observations: Vec<IssueLaneRuntimeObservation>,
) -> Result<Vec<KapellmeisterShadowSignal>> {
    let path = issue_lane_state_path_for_workflow(workflow_dir);
    let mut state = IssueLaneState::read(&path)?;
    let mut signals = Vec::new();
    for observation in observations {
        signals.extend(issue_lane_control_plane_signals(
            config,
            &observation.issue,
            state.observe_issue(&observation.issue),
            observation.running_mob,
        ));
    }
    state.write(&path)?;
    Ok(signals)
}

pub(crate) fn issue_lane_control_plane_signals(
    config: &ServiceConfig,
    issue: &Issue,
    observation: IssueLaneObservation,
    running_mob: Option<KapellmeisterRunningMobObservation>,
) -> Vec<KapellmeisterShadowSignal> {
    let classification = classify_issue_lane(config, &issue.state);
    let observed_at = issue
        .updated_at
        .or(issue.created_at)
        .unwrap_or_else(Utc::now);
    let runtime_presence = if running_mob.is_some() {
        RuntimePresence::Present
    } else {
        RuntimePresence::Absent
    };
    let effects = match observation {
        IssueLaneObservation::Unchanged => reduce_open_issue_without_runtime(
            issue.identifier.clone(),
            issue.state.clone(),
            classification,
            runtime_presence,
        ),
        observation => reduce_stage_observation(observation, classification, runtime_presence),
    };
    let mut signals = Vec::new();
    if let Some(repair) = legacy_linear_lane_repair(config, &issue.state) {
        signals.push(
            KapellmeisterShadowSignal::legacy_linear_lane_repair_suggested(
                observed_at,
                Some(issue_lane_observation_cursor(
                    &issue.identifier,
                    &format!("legacy-repair:{}", repair.source_lane),
                    observed_at,
                )),
                issue_ref_for_lane_observation(issue.identifier.clone()),
                repair,
            ),
        );
    }
    signals.extend(effects.into_iter().filter_map(|effect| {
        issue_control_plane_effect_to_signal(effect, observed_at, running_mob.clone())
    }));
    signals
}

pub(crate) fn issue_comment_control_plane_signals(
    issue: &Issue,
    changes: Vec<IssueCommentChange>,
    observed_at: DateTime<Utc>,
) -> Vec<KapellmeisterShadowSignal> {
    reduce_comment_observation(
        issue.identifier.clone(),
        IssueCommentObservation::Changed { changes },
    )
    .into_iter()
    .filter_map(|effect| issue_control_plane_effect_to_signal(effect, observed_at, None))
    .collect()
}

fn issue_control_plane_effect_to_signal(
    effect: ControlPlaneEffect<LinearCommentId>,
    observed_at: DateTime<Utc>,
    running_mob: Option<KapellmeisterRunningMobObservation>,
) -> Option<KapellmeisterShadowSignal> {
    let ControlPlaneEffect::NotifyKapellmeister(observation) = effect;
    match observation {
        HostControlObservation::IssueFirstSeen {
            issue,
            lane,
            classification,
            ..
        } => Some(KapellmeisterShadowSignal::linear_issue_first_seen(
            observed_at,
            Some(issue_lane_observation_cursor(&issue, &lane, observed_at)),
            issue_ref_for_lane_observation(issue),
            state_ref_for_observed_lane(&lane),
            classification,
            running_mob,
        )),
        HostControlObservation::IssueStageChanged {
            issue,
            from,
            to,
            classification,
            ..
        } => Some(KapellmeisterShadowSignal::linear_lane_changed(
            observed_at,
            Some(issue_lane_observation_cursor(&issue, &to, observed_at)),
            issue_ref_for_lane_observation(issue),
            state_ref_for_observed_lane(&from),
            state_ref_for_observed_lane(&to),
            classification,
            running_mob,
        )),
        HostControlObservation::OpenIssueWithoutRuntime {
            issue,
            lane,
            classification,
        } => {
            let observed_at = Utc::now();
            Some(KapellmeisterShadowSignal::open_issue_without_runtime(
                observed_at,
                Some(open_issue_without_runtime_observation_cursor(
                    &issue,
                    &lane,
                    observed_at,
                )),
                issue_ref_for_lane_observation(issue),
                state_ref_for_observed_lane(&lane),
                classification,
            ))
        }
        HostControlObservation::IssueCommentAdded {
            issue,
            comment_id,
            body_markdown,
        } => {
            let operator_review_finding =
                crate::review_findings::extract_unresolved_operator_review_note(&body_markdown);
            Some(KapellmeisterShadowSignal::linear_issue_commented(
                observed_at,
                Some(issue_comment_observation_cursor(
                    &issue,
                    "added",
                    &comment_id,
                    observed_at,
                )),
                issue_ref_for_identifier(&issue),
                comment_id,
                Some(body_markdown),
                operator_review_finding,
                None,
            ))
        }
        HostControlObservation::IssueCommentChanged {
            issue,
            comment_id,
            body_markdown,
        } => {
            let operator_review_finding =
                crate::review_findings::extract_unresolved_operator_review_note(&body_markdown);
            Some(KapellmeisterShadowSignal::linear_issue_comment_changed(
                observed_at,
                Some(issue_comment_observation_cursor(
                    &issue,
                    "changed",
                    &comment_id,
                    observed_at,
                )),
                issue_ref_for_identifier(&issue),
                comment_id,
                Some(body_markdown),
                operator_review_finding,
                None,
            ))
        }
        HostControlObservation::IssueCommentDeleted { issue, comment_id } => {
            Some(KapellmeisterShadowSignal::linear_issue_comment_deleted(
                observed_at,
                Some(issue_comment_observation_cursor(
                    &issue,
                    "deleted",
                    &comment_id,
                    observed_at,
                )),
                issue_ref_for_identifier(&issue),
                comment_id,
                None,
            ))
        }
    }
}

fn issue_lane_observation_cursor(
    identifier: &str,
    lane: &str,
    observed_at: DateTime<Utc>,
) -> DurableCursorPosition {
    DurableCursorPosition {
        key: DurableCursorKey {
            source: DurableCursorSource::LinearIssueChanges,
            partition: CursorPartition(sanitize_workspace_key(identifier)),
        },
        cursor: Cursor(format!("{identifier}:lane:{}", normalize_state(lane))),
        observed_at,
    }
}

fn open_issue_without_runtime_observation_cursor(
    identifier: &str,
    lane: &str,
    observed_at: DateTime<Utc>,
) -> DurableCursorPosition {
    let bucket = observed_at.timestamp().div_euclid(300);
    DurableCursorPosition {
        key: DurableCursorKey {
            source: DurableCursorSource::LinearIssueChanges,
            partition: CursorPartition(sanitize_workspace_key(identifier)),
        },
        cursor: Cursor(format!(
            "{identifier}:open_without_runtime:{}:{bucket}",
            normalize_state(lane)
        )),
        observed_at,
    }
}

fn issue_comment_observation_cursor(
    identifier: &str,
    action: &str,
    comment_id: &LinearCommentId,
    observed_at: DateTime<Utc>,
) -> DurableCursorPosition {
    DurableCursorPosition {
        key: DurableCursorKey {
            source: DurableCursorSource::LinearIssueChanges,
            partition: CursorPartition(sanitize_workspace_key(identifier)),
        },
        cursor: Cursor(format!("{identifier}:comment:{action}:{}", comment_id.0)),
        observed_at,
    }
}

pub enum IssueAdapter {
    Linear(LinearClient),
    FugueIms(FugueImsStore),
    Jira(Box<crate::jira::JiraClient>),
}

fn fugue_ims_issue_key_prefix(config: &TrackerConfig) -> String {
    config
        .project_slug
        .as_deref()
        .and_then(|slug| {
            slug.split(|ch: char| !ch.is_ascii_alphanumeric())
                .find(|part| !part.is_empty())
        })
        .map(|part| part.to_ascii_uppercase())
        .unwrap_or_else(|| "FUG".to_string())
}

impl IssueAdapter {
    pub fn from_config(config: &TrackerConfig) -> Result<Self> {
        match config.kind.as_str() {
            "jira" => crate::jira::JiraClient::from_config(config)
                .map(|client| Self::Jira(Box::new(client))),
            "fugue-ims" => FugueImsStore::from_config(config).map(Self::FugueIms),
            "linear" => LinearClient::from_config(config).map(Self::Linear),
            kind => Err(FugueError::UnsupportedTrackerKind(kind.to_string())),
        }
    }

    pub async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>> {
        match self {
            Self::Linear(client) => client.fetch_candidate_issues().await,
            Self::Jira(client) => client.fetch_candidate_issues().await,
            Self::FugueIms(store) => store.fetch_candidate_issues().await,
        }
    }

    pub async fn fetch_open_project_issues(&self) -> Result<Vec<Issue>> {
        match self {
            Self::Linear(client) => client.fetch_open_project_issues().await,
            Self::Jira(client) => client.fetch_open_project_issues().await,
            Self::FugueIms(store) => store.all_open_issues(),
        }
    }

    pub async fn fetch_issues_by_states(&self, states: &[String]) -> Result<Vec<Issue>> {
        match self {
            Self::Linear(client) => client.fetch_issues_by_states(states).await,
            Self::Jira(client) => client.fetch_issues_by_states(states).await,
            Self::FugueIms(store) => store.fetch_issues_by_states(states).await,
        }
    }

    pub async fn fetch_issue_board_issues_by_states(
        &self,
        states: &[String],
    ) -> Result<Vec<Issue>> {
        match self {
            Self::Linear(client) => client.fetch_board_issues_by_states(states).await,
            Self::Jira(client) => client.fetch_issues_by_states(states).await,
            Self::FugueIms(store) => store.fetch_issues_by_states(states).await,
        }
    }

    pub async fn fetch_issue_states_by_ids(&self, issue_ids: &[String]) -> Result<Vec<Issue>> {
        match self {
            Self::Linear(client) => client.fetch_issue_states_by_ids(issue_ids).await,
            Self::Jira(client) => client.fetch_issue_states_by_ids(issue_ids).await,
            Self::FugueIms(store) => store.fetch_issue_states_by_ids(issue_ids).await,
        }
    }

    pub async fn materialize_issue_attachments(
        &self,
        issue: &Issue,
        workspace_path: &Path,
    ) -> Result<Issue> {
        match self {
            Self::Linear(client) => {
                client
                    .materialize_issue_attachments(issue, workspace_path)
                    .await
            }
            Self::FugueIms(_) => Ok(issue.clone()),
            Self::Jira(client) => {
                client
                    .materialize_issue_attachments(issue, workspace_path)
                    .await
            }
        }
    }

    pub async fn create_issue(
        &self,
        tracker_config: &TrackerConfig,
        title: &str,
        description: Option<&str>,
        lane: &str,
    ) -> Result<Issue> {
        match self {
            Self::Jira(client) => client.create_issue(title, description, lane).await,
            Self::Linear(client) => {
                client
                    .create_issue_in_project(title, description, lane)
                    .await
            }
            Self::FugueIms(store) => {
                let key = store.allocate_issue_key(&fugue_ims_issue_key_prefix(tracker_config))?;
                let snapshot = store.create_issue(CreateFugueIssue {
                    key,
                    repo: store.repository_scope().to_owned(),
                    title: title.to_string(),
                    description: description.map(ToString::to_string),
                    stage: ImsStageName(lane.to_string()),
                    labels: Vec::new(),
                })?;
                Ok(Issue {
                    id: snapshot.id.0,
                    identifier: snapshot.key.0,
                    title: snapshot.title,
                    description: snapshot.description,
                    priority: None,
                    state: snapshot.stage.0,
                    branch_name: None,
                    url: None,
                    labels: Vec::new(),
                    blocked_by: Vec::new(),
                    attachments: Vec::new(),
                    created_at: Some(Utc::now()),
                    updated_at: Some(Utc::now()),
                })
            }
        }
    }

    /// Resolve a reference within this tracker scope without fetching comments
    /// or downloading attachments as a side effect of the access check.
    pub(crate) async fn resolve_issue_identifier(
        &self,
        issue: &LinearIssueRef,
    ) -> Result<LinearIssueIdentifier> {
        match self {
            Self::Jira(client) => client.resolve_issue_identifier(issue).await,
            Self::FugueIms(store) => store.resolve_issue_identifier(issue),
            Self::Linear(client) => LinearToolExecutor::new(client)
                .resolve_issue_identifier(issue)
                .await
                .map_err(|err| {
                    linear_tool_error(err.into_error(FugueToolOperation::LinearInspectIssue))
                }),
        }
    }

    pub async fn inspect_issue(&self, input: InspectIssueInput) -> Result<InspectIssueOutput> {
        match self {
            Self::Jira(client) => client.inspect_issue(input).await,
            Self::Linear(client) => LinearToolExecutor::new(client)
                .inspect_issue(input)
                .await
                .map_err(|err| {
                    linear_tool_error(err.into_error(FugueToolOperation::LinearInspectIssue))
                }),
            Self::FugueIms(store) => store.inspect_issue_tool(input).await,
        }
    }

    pub async fn workpad_body(
        &self,
        issue: &LinearIssueRef,
        heading: &str,
    ) -> Result<Option<String>> {
        match self {
            Self::Jira(client) => client.workpad_body(issue, heading).await,
            Self::Linear(client) => LinearToolExecutor::new(client)
                .workpad_body(issue, heading)
                .await
                .map_err(|err| {
                    linear_tool_error(err.into_error(FugueToolOperation::LinearInspectIssue))
                }),
            Self::FugueIms(store) => store
                .inspect_issue_tool(InspectIssueInput {
                    issue: issue.clone(),
                })
                .await
                .map(|output| output.workpad.map(|workpad| workpad.body_markdown)),
        }
    }

    pub async fn issue_comments(
        &self,
        issue: &LinearIssueRef,
    ) -> Result<Vec<LinearIssueCommentSnapshot>> {
        match self {
            Self::Jira(client) => client.issue_comments(issue).await,
            Self::Linear(client) => LinearToolExecutor::new(client)
                .issue_comments(issue)
                .await
                .map_err(|err| {
                    linear_tool_error(err.into_error(FugueToolOperation::LinearInspectIssue))
                }),
            Self::FugueIms(store) => {
                let inspected = store
                    .inspect_issue_tool(InspectIssueInput {
                        issue: issue.clone(),
                    })
                    .await?;
                store
                    .comments_for_issue_identifier(&inspected.issue.identifier.0)
                    .map(|comments| {
                        comments
                            .into_iter()
                            .map(|comment| LinearIssueCommentSnapshot {
                                comment_id: LinearCommentId(comment.id.0.clone()),
                                body_markdown: comment.body_mark,
                                url: Some(format!(
                                    "fugue-ims://{}/comments/{}",
                                    inspected.issue.identifier.0, comment.id.0
                                )),
                            })
                            .collect()
                    })
            }
        }
    }

    pub async fn issue_comment_snapshots(
        &self,
        issue: &LinearIssueRef,
    ) -> Result<Vec<LinearCommentSnapshot>> {
        match self {
            Self::Jira(client) => Ok(client
                .issue_comments(issue)
                .await?
                .into_iter()
                .map(|comment| LinearCommentSnapshot {
                    id: comment.comment_id,
                    body_mark: comment.body_markdown.clone(),
                    body_markdown: comment.body_markdown,
                })
                .collect()),
            Self::Linear(client) => LinearToolExecutor::new(client)
                .issue_comment_snapshots(issue)
                .await
                .map_err(|err| {
                    linear_tool_error(err.into_error(FugueToolOperation::LinearInspectIssue))
                }),
            Self::FugueIms(store) => store
                .inspect_issue_tool(InspectIssueInput {
                    issue: issue.clone(),
                })
                .await
                .and_then(|output| store.comments_for_issue_identifier(&output.issue.identifier.0))
                .map(|comments| {
                    comments
                        .into_iter()
                        .map(|comment| LinearCommentSnapshot {
                            id: crate::tools::LinearCommentId(comment.id.0),
                            body_mark: comment.body_mark.clone(),
                            body_markdown: comment.body_mark,
                        })
                        .collect()
                }),
        }
    }

    pub async fn reconcile_issue_comment_observation_state(
        &self,
        workflow_dir: &Path,
        issues: &[Issue],
    ) -> Result<Vec<KapellmeisterShadowSignal>> {
        let path = issue_comment_state_path_for_workflow(workflow_dir);
        let mut state = IssueCommentState::read(&path)?;
        let mut signals = Vec::new();
        for issue in issues {
            let comments = self
                .issue_comment_snapshots(&issue_ref_for_identifier(&issue.identifier))
                .await?;
            if let IssueCommentObservation::Changed { changes } =
                state.observe_issue_comments(&issue.identifier, comments)
            {
                let observed_at = issue.updated_at.unwrap_or_else(Utc::now);
                signals.extend(issue_comment_control_plane_signals(
                    issue,
                    changes,
                    observed_at,
                ));
            }
        }
        state.write(&path)?;
        Ok(signals)
    }

    pub async fn add_comment(&self, input: AddCommentInput) -> Result<AddCommentOutput> {
        match self {
            Self::Jira(client) => client.add_comment(input).await,
            Self::Linear(client) => LinearToolExecutor::new(client)
                .add_comment(input)
                .await
                .map_err(|err| {
                    linear_tool_error(err.into_error(FugueToolOperation::LinearAddComment))
                }),
            Self::FugueIms(store) => store.add_comment_tool(input).await,
        }
    }

    pub async fn move_issue(&self, input: MoveIssueInput) -> Result<MoveIssueOutput> {
        match self {
            Self::Jira(client) => client.move_issue(input).await,
            Self::Linear(client) => LinearToolExecutor::new(client)
                .move_issue(input)
                .await
                .map_err(|err| {
                    linear_tool_error(err.into_error(FugueToolOperation::LinearMoveIssue))
                }),
            Self::FugueIms(store) => store.move_issue_tool(input).await,
        }
    }

    /// An authenticated board action is explicit operator restart intent.
    /// Agent tool dispatch must continue using `move_issue`.
    pub(crate) async fn move_issue_as_operator(
        &self,
        input: MoveIssueInput,
    ) -> Result<MoveIssueOutput> {
        match self {
            Self::Jira(client) => client.move_issue_as_operator(input).await,
            _ => self.move_issue(input).await,
        }
    }

    pub async fn upsert_workpad(&self, input: UpsertWorkpadInput) -> Result<UpsertWorkpadOutput> {
        match self {
            Self::Jira(client) => client.upsert_workpad(input).await,
            Self::Linear(client) => LinearToolExecutor::new(client)
                .upsert_workpad(input)
                .await
                .map_err(|err| {
                    linear_tool_error(err.into_error(FugueToolOperation::LinearUpsertWorkpad))
                }),
            Self::FugueIms(store) => store.upsert_workpad_tool(input).await,
        }
    }

    pub async fn ensure_projection_lanes(
        &self,
        tracker_config: &TrackerConfig,
        runtime: &LinearRuntimeConfig,
    ) -> Result<()> {
        if runtime.required_states.is_empty() {
            return Ok(());
        }
        // Built-in ticket states are configuration, not a remotely provisioned
        // workflow. Native config validation already checks their roles and
        // types; there is no external team or workflow to look up.
        if matches!(self, Self::FugueIms(_)) {
            return Ok(());
        }
        if let Self::Jira(client) = self {
            if runtime.provisioning.mode == NativeLinearProvisioningMode::CreateMissing {
                return Err(FugueError::InvalidConfig("Jira workflows must be provisioned by an administrator; use provisioning.mode=validate".to_string()));
            }
            let states = client.list_project_states().await?.states;
            let missing: Vec<_> = runtime
                .required_states
                .iter()
                .filter(|required| {
                    !states
                        .iter()
                        .any(|state| state.name.eq_ignore_ascii_case(&required.name))
                })
                .map(|required| required.name.as_str())
                .collect();
            return if missing.is_empty() {
                Ok(())
            } else {
                Err(FugueError::InvalidConfig(format!(
                    "Jira projection preflight missing statuses: {missing:?}"
                )))
            };
        }
        let Some(team_selector) = runtime.team.as_deref() else {
            if runtime.provisioning.mode == NativeLinearProvisioningMode::CreateMissing {
                return Err(FugueError::InvalidConfig(
                    "linear.provisioning.mode=create_missing requires linear.team".to_string(),
                ));
            }
            return Err(FugueError::InvalidConfig(
                "linear.provisioning.mode=validate requires linear.team so Fugue can validate required projected workflow states"
                    .to_string(),
            ));
        };

        let states = match self {
            Self::Jira(client) => client.list_project_states().await?.states,
            Self::Linear(client) => {
                let executor = LinearToolExecutor::new(client);
                let team_id = executor.resolve_team_id(team_selector).await.map_err(|err| {
                    FugueError::InvalidConfig(format!(
                        "Linear projection lane preflight could not resolve team {team_selector:?}: {}",
                        err.into_error(FugueToolOperation::LinearListProjectStates).message
                    ))
                })?;
                match runtime.provisioning.mode {
                    NativeLinearProvisioningMode::Validate => executor
                        .list_project_states(ListProjectStatesInput {
                            team_id: Some(team_id),
                            project_id: None,
                        })
                        .await
                        .map_err(|err| {
                            FugueError::InvalidConfig(format!(
                                "Linear projection lane preflight could not list workflow states: {}",
                                err.into_error(FugueToolOperation::LinearListProjectStates).message
                            ))
                        })?
                        .states,
                    NativeLinearProvisioningMode::CreateMissing => executor
                        .ensure_workflow_states(EnsureWorkflowStatesInput {
                            team_id,
                            states: runtime
                                .required_states
                                .iter()
                                .map(|state| EnsureWorkflowState {
                                    name: state.name.clone(),
                                    state_type: state.state_type,
                                })
                                .collect(),
                            idempotency_key: IdempotencyKey(
                                "linear-projection-lane-preflight".to_string(),
                            ),
                        })
                        .await
                        .map_err(|err| {
                            FugueError::InvalidConfig(format!(
                                "Linear projection lane provisioning failed: {}",
                                err.into_error(FugueToolOperation::LinearEnsureWorkflowStates)
                                    .message
                            ))
                        })?
                        .states,
                }
            }
            Self::FugueIms(store) => match runtime.provisioning.mode {
                NativeLinearProvisioningMode::Validate => {
                    store
                        .list_project_states_tool(
                            ListProjectStatesInput {
                                team_id: None,
                                project_id: None,
                            },
                            tracker_config,
                        )
                        .states
                }
                NativeLinearProvisioningMode::CreateMissing => {
                    store
                        .ensure_workflow_states_tool(
                            EnsureWorkflowStatesInput {
                                team_id: crate::tools::LinearTeamId(team_selector.to_string()),
                                states: runtime
                                    .required_states
                                    .iter()
                                    .map(|state| EnsureWorkflowState {
                                        name: state.name.clone(),
                                        state_type: state.state_type,
                                    })
                                    .collect(),
                                idempotency_key: IdempotencyKey(
                                    "linear-projection-lane-preflight".to_string(),
                                ),
                            },
                            tracker_config,
                        )
                        .states
                }
            },
        };
        let missing = missing_required_states(&runtime.required_states, &states);
        if !missing.is_empty() {
            return Err(FugueError::InvalidConfig(format!(
                "Linear projection lane preflight missing workflow states: {}",
                missing.join(", ")
            )));
        }
        Ok(())
    }

    pub async fn upsert_workpad_tool_result(&self, input: UpsertWorkpadInput) -> FugueToolResult {
        match self.upsert_workpad(input).await {
            Ok(output) => FugueToolResult::Ok {
                output: FugueToolOutput::LinearUpsertWorkpad(output),
                metadata: ToolExecutionMetadata::default(),
            },
            Err(error) => FugueToolResult::Err {
                error: FugueToolError {
                    operation: FugueToolOperation::LinearUpsertWorkpad,
                    code: FugueToolErrorCode::ExecutionFailed,
                    message: error.to_string(),
                    retry: RetryAdvice::DoNotRetry,
                    idempotency_key: None,
                },
                metadata: ToolExecutionMetadata::default(),
            },
        }
    }
}

pub fn issue_ref_for_identifier(identifier: &str) -> LinearIssueRef {
    LinearIssueRef::Identifier {
        identifier: LinearIssueIdentifier(identifier.to_string()),
    }
}

pub fn missing_required_states(
    required: &[crate::config::LinearRequiredState],
    existing: &[crate::tools::LinearStateSnapshot],
) -> Vec<String> {
    required
        .iter()
        .filter(|required| {
            !existing.iter().any(|state| {
                state.name.eq_ignore_ascii_case(&required.name)
                    && state.state_type == required.state_type
            })
        })
        .map(|state| format!("{} ({:?})", state.name, state.state_type))
        .collect()
}

fn linear_tool_error(error: FugueToolError) -> FugueError {
    FugueError::ResponseError(error.message)
}

#[cfg(test)]
mod issue_creation_tests {
    use super::*;
    use crate::config::{ServiceConfig, WorkflowDefinition};
    use serde_json::json;

    fn fugue_ims_config(path: &std::path::Path) -> TrackerConfig {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "fugue-ims",
                    "endpoint": format!("sqlite://{}", path.display()),
                    "project_slug": "owner/repo",
                    "intake_states": ["Ready Queue"],
                    "active_states": ["Ready Queue", "Planning", "Implementing", "Reviewing"],
                    "terminal_states": ["Done", "Canceled"]
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        ServiceConfig::from_definition(std::path::PathBuf::from("/tmp/WORKFLOW.md"), &definition)
            .unwrap()
            .tracker
    }

    #[tokio::test]
    async fn tracker_boundary_padded_builtin_scope_can_create_and_read_its_issue() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = fugue_ims_config(&temp.path().join("ims.sqlite"));
        config.project_slug = Some("  owner/repo  ".into());
        let adapter = IssueAdapter::from_config(&config).unwrap();
        let issue = adapter
            .create_issue(&config, "Normalized scope", None, "Ready Queue")
            .await
            .unwrap();
        let read = adapter
            .inspect_issue(InspectIssueInput {
                issue: issue_ref_for_identifier(&issue.identifier),
            })
            .await
            .unwrap();
        assert_eq!(read.issue.title, "Normalized scope");
        config.project_slug = Some("owner/other".into());
        assert!(
            IssueAdapter::from_config(&config)
                .unwrap()
                .inspect_issue(InspectIssueInput {
                    issue: issue_ref_for_identifier(&issue.identifier)
                })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn issue_adapter_creates_fugue_ims_issues_in_configured_lanes_with_unique_keys() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = fugue_ims_config(&temp.path().join("ims.sqlite"));
        let adapter = IssueAdapter::from_config(&config).expect("adapter");

        let icebox = adapter
            .create_issue(&config, "Icebox issue", Some("Icebox body"), "Icebox")
            .await
            .expect("icebox create");
        let ready = adapter
            .create_issue(&config, "Ready issue", Some("Ready body"), "Ready Queue")
            .await
            .expect("ready create");

        assert_ne!(icebox.identifier, ready.identifier);
        assert_eq!(icebox.state, "Icebox");
        assert_eq!(icebox.title, "Icebox issue");
        assert_eq!(icebox.description.as_deref(), Some("Icebox body"));
        assert_eq!(ready.state, "Ready Queue");
        assert_eq!(ready.title, "Ready issue");

        let lanes = vec!["Icebox".to_string(), "Ready Queue".to_string()];
        let issues = adapter
            .fetch_issues_by_states(&lanes)
            .await
            .expect("issues");
        assert!(
            issues
                .iter()
                .any(|issue| issue.identifier == icebox.identifier)
        );
        assert!(
            issues
                .iter()
                .any(|issue| issue.identifier == ready.identifier)
        );
    }
}
