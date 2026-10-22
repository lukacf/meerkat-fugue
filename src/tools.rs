//! Typed contracts for host-executed Fugue tools.
//!
//! Stage 2A is contract-only: this module defines the catalog, typed request
//! and response payloads, idempotency fields for retryable mutations, and
//! structured errors for future tool hosts.  It is intentionally not wired into
//! the current v0 worker backend.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Cursor(pub String);

macro_rules! string_newtype {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub String);
    };
}

string_newtype!(LinearIssueId);
string_newtype!(LinearIssueIdentifier);
string_newtype!(LinearProjectId);
string_newtype!(LinearStateId);
string_newtype!(LinearTeamId);
string_newtype!(LinearUserId);
string_newtype!(LinearCommentId);
string_newtype!(LinearAttachmentId);
string_newtype!(GitHubOwner);
string_newtype!(GitHubRepoName);
string_newtype!(GitSha);
string_newtype!(GitRef);
string_newtype!(BranchName);
string_newtype!(WorkspaceKey);
string_newtype!(MobRunId);
string_newtype!(MobTemplateId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct GitHubPullRequestNumber(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FugueToolOperation {
    LinearInspectIssue,
    LinearClaimIssue,
    LinearMoveIssue,
    LinearUpsertWorkpad,
    LinearAddComment,
    LinearListProjectStates,
    LinearEnsureWorkflowStates,
    LinearListRecentIssueChanges,
    GitHubInspectRepo,
    GitHubInspectPr,
    GitHubCreatePr,
    GitHubCheckCi,
    GitHubMergePr,
    GitHubLinkIssue,
    WorkspaceCreateWorkspace,
    WorkspaceInspectWorkspace,
    WorkspaceCleanWorkspace,
    WorkspaceRunRepositorySetup,
    MobListRuns,
    MobInspectRun,
    MobSpawnOrResumeExecution,
    MobCancelExecution,
    MobObserveEvents,
}

impl FugueToolOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LinearInspectIssue => "linear.inspect_issue",
            Self::LinearClaimIssue => "linear.claim_issue",
            Self::LinearMoveIssue => "linear.move_issue",
            Self::LinearUpsertWorkpad => "linear.upsert_workpad",
            Self::LinearAddComment => "linear.add_comment",
            Self::LinearListProjectStates => "linear.list_project_states",
            Self::LinearEnsureWorkflowStates => "linear.ensure_workflow_states",
            Self::LinearListRecentIssueChanges => "linear.list_recent_issue_changes",
            Self::GitHubInspectRepo => "github.inspect_repo",
            Self::GitHubInspectPr => "github.inspect_pr",
            Self::GitHubCreatePr => "github.create_pr",
            Self::GitHubCheckCi => "github.check_ci",
            Self::GitHubMergePr => "github.merge_pr",
            Self::GitHubLinkIssue => "github.link_issue",
            Self::WorkspaceCreateWorkspace => "workspace.create_workspace",
            Self::WorkspaceInspectWorkspace => "workspace.inspect_workspace",
            Self::WorkspaceCleanWorkspace => "workspace.clean_workspace",
            Self::WorkspaceRunRepositorySetup => "workspace.run_repository_setup",
            Self::MobListRuns => "mob.list_runs",
            Self::MobInspectRun => "mob.inspect_run",
            Self::MobSpawnOrResumeExecution => "mob.spawn_or_resume_execution",
            Self::MobCancelExecution => "mob.cancel_execution",
            Self::MobObserveEvents => "mob.observe_events",
        }
    }

    pub const fn provider_callback_name(self) -> &'static str {
        match self {
            Self::LinearInspectIssue => "linear_inspect_issue",
            Self::LinearClaimIssue => "linear_claim_issue",
            Self::LinearMoveIssue => "linear_move_issue",
            Self::LinearUpsertWorkpad => "linear_upsert_workpad",
            Self::LinearAddComment => "linear_add_comment",
            Self::LinearListProjectStates => "linear_list_project_states",
            Self::LinearEnsureWorkflowStates => "linear_ensure_workflow_states",
            Self::LinearListRecentIssueChanges => "linear_list_recent_issue_changes",
            Self::GitHubInspectRepo => "github_inspect_repo",
            Self::GitHubInspectPr => "github_inspect_pr",
            Self::GitHubCreatePr => "github_create_pr",
            Self::GitHubCheckCi => "github_check_ci",
            Self::GitHubMergePr => "github_merge_pr",
            Self::GitHubLinkIssue => "github_link_issue",
            Self::WorkspaceCreateWorkspace => "workspace_create_workspace",
            Self::WorkspaceInspectWorkspace => "workspace_inspect_workspace",
            Self::WorkspaceCleanWorkspace => "workspace_clean_workspace",
            Self::WorkspaceRunRepositorySetup => "workspace_run_repository_setup",
            Self::MobListRuns => "mob_list_runs",
            Self::MobInspectRun => "mob_inspect_run",
            Self::MobSpawnOrResumeExecution => "mob_spawn_or_resume_execution",
            Self::MobCancelExecution => "mob_cancel_execution",
            Self::MobObserveEvents => "mob_observe_events",
        }
    }

    pub const fn mutates(self) -> bool {
        matches!(
            self,
            Self::LinearClaimIssue
                | Self::LinearMoveIssue
                | Self::LinearUpsertWorkpad
                | Self::LinearAddComment
                | Self::LinearEnsureWorkflowStates
                | Self::GitHubCreatePr
                | Self::GitHubMergePr
                | Self::GitHubLinkIssue
                | Self::WorkspaceCreateWorkspace
                | Self::WorkspaceCleanWorkspace
                | Self::WorkspaceRunRepositorySetup
                | Self::MobSpawnOrResumeExecution
                | Self::MobCancelExecution
        )
    }

    pub const fn idempotency(self) -> IdempotencyRequirement {
        match self {
            Self::LinearAddComment => IdempotencyRequirement::NonIdempotentMutation,
            _ if self.mutates() => IdempotencyRequirement::RequiredForMutation,
            _ => IdempotencyRequirement::NotRequiredReadOnly,
        }
    }
}

impl fmt::Display for FugueToolOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for FugueToolOperation {
    type Err = UnsupportedOperation;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "linear.inspect_issue" => Self::LinearInspectIssue,
            "linear.claim_issue" => Self::LinearClaimIssue,
            "linear.move_issue" => Self::LinearMoveIssue,
            "linear.upsert_workpad" => Self::LinearUpsertWorkpad,
            "linear.add_comment" => Self::LinearAddComment,
            "linear.list_project_states" => Self::LinearListProjectStates,
            "linear.ensure_workflow_states" => Self::LinearEnsureWorkflowStates,
            "linear.list_recent_issue_changes" => Self::LinearListRecentIssueChanges,
            "github.inspect_repo" => Self::GitHubInspectRepo,
            "github.inspect_pr" => Self::GitHubInspectPr,
            "github.create_pr" => Self::GitHubCreatePr,
            "github.check_ci" => Self::GitHubCheckCi,
            "github.merge_pr" => Self::GitHubMergePr,
            "github.link_issue" => Self::GitHubLinkIssue,
            "workspace.create_workspace" => Self::WorkspaceCreateWorkspace,
            "workspace.inspect_workspace" => Self::WorkspaceInspectWorkspace,
            "workspace.clean_workspace" => Self::WorkspaceCleanWorkspace,
            "workspace.run_repository_setup" => Self::WorkspaceRunRepositorySetup,
            "mob.list_runs" => Self::MobListRuns,
            "mob.inspect_run" => Self::MobInspectRun,
            "mob.spawn_or_resume_execution" => Self::MobSpawnOrResumeExecution,
            "mob.cancel_execution" => Self::MobCancelExecution,
            "mob.observe_events" => Self::MobObserveEvents,
            "linear_inspect_issue" => Self::LinearInspectIssue,
            "linear_claim_issue" => Self::LinearClaimIssue,
            "linear_move_issue" => Self::LinearMoveIssue,
            "linear_upsert_workpad" => Self::LinearUpsertWorkpad,
            "linear_add_comment" => Self::LinearAddComment,
            "linear_list_project_states" => Self::LinearListProjectStates,
            "linear_ensure_workflow_states" => Self::LinearEnsureWorkflowStates,
            "linear_list_recent_issue_changes" => Self::LinearListRecentIssueChanges,
            "github_inspect_repo" => Self::GitHubInspectRepo,
            "github_inspect_pr" => Self::GitHubInspectPr,
            "github_create_pr" => Self::GitHubCreatePr,
            "github_check_ci" => Self::GitHubCheckCi,
            "github_merge_pr" => Self::GitHubMergePr,
            "github_link_issue" => Self::GitHubLinkIssue,
            "workspace_create_workspace" => Self::WorkspaceCreateWorkspace,
            "workspace_inspect_workspace" => Self::WorkspaceInspectWorkspace,
            "workspace_clean_workspace" => Self::WorkspaceCleanWorkspace,
            "workspace_run_repository_setup" => Self::WorkspaceRunRepositorySetup,
            "mob_list_runs" => Self::MobListRuns,
            "mob_inspect_run" => Self::MobInspectRun,
            "mob_spawn_or_resume_execution" => Self::MobSpawnOrResumeExecution,
            "mob_cancel_execution" => Self::MobCancelExecution,
            "mob_observe_events" => Self::MobObserveEvents,
            _ => {
                return Err(UnsupportedOperation {
                    operation: value.to_string(),
                });
            }
        })
    }
}

impl Serialize for FugueToolOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for FugueToolOperation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedOperation {
    pub operation: String,
}

impl fmt::Display for UnsupportedOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unsupported Fugue tool operation: {}",
            self.operation
        )
    }
}

impl std::error::Error for UnsupportedOperation {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyRequirement {
    NotRequiredReadOnly,
    RequiredForMutation,
    NonIdempotentMutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolNamespace {
    Linear,
    Github,
    Workspace,
    Mob,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolContract {
    pub operation: FugueToolOperation,
    pub namespace: ToolNamespace,
    pub mutates: bool,
    pub idempotency: IdempotencyRequirement,
}

impl ToolContract {
    pub const fn new(operation: FugueToolOperation, namespace: ToolNamespace) -> Self {
        Self {
            operation,
            namespace,
            mutates: operation.mutates(),
            idempotency: operation.idempotency(),
        }
    }
}

pub const TOOL_CATALOG: &[ToolContract] = &[
    ToolContract::new(
        FugueToolOperation::LinearInspectIssue,
        ToolNamespace::Linear,
    ),
    ToolContract::new(FugueToolOperation::LinearClaimIssue, ToolNamespace::Linear),
    ToolContract::new(FugueToolOperation::LinearMoveIssue, ToolNamespace::Linear),
    ToolContract::new(
        FugueToolOperation::LinearUpsertWorkpad,
        ToolNamespace::Linear,
    ),
    ToolContract::new(FugueToolOperation::LinearAddComment, ToolNamespace::Linear),
    ToolContract::new(
        FugueToolOperation::LinearListProjectStates,
        ToolNamespace::Linear,
    ),
    ToolContract::new(
        FugueToolOperation::LinearEnsureWorkflowStates,
        ToolNamespace::Linear,
    ),
    ToolContract::new(
        FugueToolOperation::LinearListRecentIssueChanges,
        ToolNamespace::Linear,
    ),
    ToolContract::new(FugueToolOperation::GitHubInspectRepo, ToolNamespace::Github),
    ToolContract::new(FugueToolOperation::GitHubInspectPr, ToolNamespace::Github),
    ToolContract::new(FugueToolOperation::GitHubCreatePr, ToolNamespace::Github),
    ToolContract::new(FugueToolOperation::GitHubCheckCi, ToolNamespace::Github),
    ToolContract::new(FugueToolOperation::GitHubMergePr, ToolNamespace::Github),
    ToolContract::new(FugueToolOperation::GitHubLinkIssue, ToolNamespace::Github),
    ToolContract::new(
        FugueToolOperation::WorkspaceCreateWorkspace,
        ToolNamespace::Workspace,
    ),
    ToolContract::new(
        FugueToolOperation::WorkspaceInspectWorkspace,
        ToolNamespace::Workspace,
    ),
    ToolContract::new(
        FugueToolOperation::WorkspaceCleanWorkspace,
        ToolNamespace::Workspace,
    ),
    ToolContract::new(
        FugueToolOperation::WorkspaceRunRepositorySetup,
        ToolNamespace::Workspace,
    ),
    ToolContract::new(FugueToolOperation::MobListRuns, ToolNamespace::Mob),
    ToolContract::new(FugueToolOperation::MobInspectRun, ToolNamespace::Mob),
    ToolContract::new(
        FugueToolOperation::MobSpawnOrResumeExecution,
        ToolNamespace::Mob,
    ),
    ToolContract::new(FugueToolOperation::MobCancelExecution, ToolNamespace::Mob),
    ToolContract::new(FugueToolOperation::MobObserveEvents, ToolNamespace::Mob),
];

pub fn tool_contract(operation: FugueToolOperation) -> Option<&'static ToolContract> {
    TOOL_CATALOG
        .iter()
        .find(|contract| contract.operation == operation)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LinearIssueRef {
    Id { id: LinearIssueId },
    Identifier { identifier: LinearIssueIdentifier },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LinearStateRef {
    Id { id: LinearStateId },
    Name { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LinearUserRef {
    Id { id: LinearUserId },
    Me,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHubRepoRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<GitHubHost>,
    pub owner: GitHubOwner,
    pub name: GitHubRepoName,
}

/// Validated GitHub Enterprise hostname, optionally including a port.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct GitHubHost(String);

impl TryFrom<String> for GitHubHost {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let url = reqwest::Url::parse(&format!("https://{value}"))
            .map_err(|_| "GitHub host must be a hostname, optionally with a port".to_string())?;
        if value.is_empty()
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_alphanumeric() && !b".-:[]".contains(&byte))
            || value.contains(['/', '?', '#', '@', '\\'])
            || value.chars().any(char::is_whitespace)
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err("GitHub host must not contain a URL, path, or credentials".to_string());
        }
        Ok(Self(value.to_ascii_lowercase()))
    }
}

impl From<GitHubHost> for String {
    fn from(host: GitHubHost) -> Self {
        host.0
    }
}

impl fmt::Display for GitHubHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl GitHubRepoRef {
    pub fn hostname(&self) -> &str {
        self.host
            .as_ref()
            .map(|host| host.0.as_str())
            .unwrap_or("github.com")
    }
}

impl fmt::Display for GitHubRepoRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(host) = &self.host {
            write!(formatter, "{host}/")?;
        }
        write!(formatter, "{}/{}", self.owner.0, self.name.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GitHubPullRequestRef {
    Number { number: GitHubPullRequestNumber },
    Branch { head_branch: BranchName },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "input")]
pub enum FugueToolInput {
    #[serde(rename = "linear.inspect_issue")]
    LinearInspectIssue(InspectIssueInput),
    #[serde(rename = "linear.claim_issue")]
    LinearClaimIssue(ClaimIssueInput),
    #[serde(rename = "linear.move_issue")]
    LinearMoveIssue(MoveIssueInput),
    #[serde(rename = "linear.upsert_workpad")]
    LinearUpsertWorkpad(UpsertWorkpadInput),
    #[serde(rename = "linear.add_comment")]
    LinearAddComment(AddCommentInput),
    #[serde(rename = "linear.list_project_states")]
    LinearListProjectStates(ListProjectStatesInput),
    #[serde(rename = "linear.ensure_workflow_states")]
    LinearEnsureWorkflowStates(EnsureWorkflowStatesInput),
    #[serde(rename = "linear.list_recent_issue_changes")]
    LinearListRecentIssueChanges(ListRecentIssueChangesInput),
    #[serde(rename = "github.inspect_repo")]
    GitHubInspectRepo(InspectRepoInput),
    #[serde(rename = "github.inspect_pr")]
    GitHubInspectPr(InspectPrInput),
    #[serde(rename = "github.create_pr")]
    GitHubCreatePr(CreatePrInput),
    #[serde(rename = "github.check_ci")]
    GitHubCheckCi(CheckCiInput),
    #[serde(rename = "github.merge_pr")]
    GitHubMergePr(MergePrInput),
    #[serde(rename = "github.link_issue")]
    GitHubLinkIssue(LinkIssueInput),
    #[serde(rename = "workspace.create_workspace")]
    WorkspaceCreateWorkspace(CreateWorkspaceInput),
    #[serde(rename = "workspace.inspect_workspace")]
    WorkspaceInspectWorkspace(InspectWorkspaceInput),
    #[serde(rename = "workspace.clean_workspace")]
    WorkspaceCleanWorkspace(CleanWorkspaceInput),
    #[serde(rename = "workspace.run_repository_setup")]
    WorkspaceRunRepositorySetup(RunRepositorySetupInput),
    #[serde(rename = "mob.list_runs")]
    MobListRuns(ListRunsInput),
    #[serde(rename = "mob.inspect_run")]
    MobInspectRun(InspectRunInput),
    #[serde(rename = "mob.spawn_or_resume_execution")]
    MobSpawnOrResumeExecution(SpawnOrResumeExecutionInput),
    #[serde(rename = "mob.cancel_execution")]
    MobCancelExecution(CancelExecutionInput),
    #[serde(rename = "mob.observe_events")]
    MobObserveEvents(ObserveEventsInput),
}

impl FugueToolInput {
    pub const fn operation(&self) -> FugueToolOperation {
        match self {
            Self::LinearInspectIssue(_) => FugueToolOperation::LinearInspectIssue,
            Self::LinearClaimIssue(_) => FugueToolOperation::LinearClaimIssue,
            Self::LinearMoveIssue(_) => FugueToolOperation::LinearMoveIssue,
            Self::LinearUpsertWorkpad(_) => FugueToolOperation::LinearUpsertWorkpad,
            Self::LinearAddComment(_) => FugueToolOperation::LinearAddComment,
            Self::LinearListProjectStates(_) => FugueToolOperation::LinearListProjectStates,
            Self::LinearEnsureWorkflowStates(_) => FugueToolOperation::LinearEnsureWorkflowStates,
            Self::LinearListRecentIssueChanges(_) => {
                FugueToolOperation::LinearListRecentIssueChanges
            }
            Self::GitHubInspectRepo(_) => FugueToolOperation::GitHubInspectRepo,
            Self::GitHubInspectPr(_) => FugueToolOperation::GitHubInspectPr,
            Self::GitHubCreatePr(_) => FugueToolOperation::GitHubCreatePr,
            Self::GitHubCheckCi(_) => FugueToolOperation::GitHubCheckCi,
            Self::GitHubMergePr(_) => FugueToolOperation::GitHubMergePr,
            Self::GitHubLinkIssue(_) => FugueToolOperation::GitHubLinkIssue,
            Self::WorkspaceCreateWorkspace(_) => FugueToolOperation::WorkspaceCreateWorkspace,
            Self::WorkspaceInspectWorkspace(_) => FugueToolOperation::WorkspaceInspectWorkspace,
            Self::WorkspaceCleanWorkspace(_) => FugueToolOperation::WorkspaceCleanWorkspace,
            Self::WorkspaceRunRepositorySetup(_) => FugueToolOperation::WorkspaceRunRepositorySetup,
            Self::MobListRuns(_) => FugueToolOperation::MobListRuns,
            Self::MobInspectRun(_) => FugueToolOperation::MobInspectRun,
            Self::MobSpawnOrResumeExecution(_) => FugueToolOperation::MobSpawnOrResumeExecution,
            Self::MobCancelExecution(_) => FugueToolOperation::MobCancelExecution,
            Self::MobObserveEvents(_) => FugueToolOperation::MobObserveEvents,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "output")]
pub enum FugueToolOutput {
    #[serde(rename = "linear.inspect_issue")]
    LinearInspectIssue(InspectIssueOutput),
    #[serde(rename = "linear.claim_issue")]
    LinearClaimIssue(ClaimIssueOutput),
    #[serde(rename = "linear.move_issue")]
    LinearMoveIssue(MoveIssueOutput),
    #[serde(rename = "linear.upsert_workpad")]
    LinearUpsertWorkpad(UpsertWorkpadOutput),
    #[serde(rename = "linear.add_comment")]
    LinearAddComment(AddCommentOutput),
    #[serde(rename = "linear.list_project_states")]
    LinearListProjectStates(ListProjectStatesOutput),
    #[serde(rename = "linear.ensure_workflow_states")]
    LinearEnsureWorkflowStates(EnsureWorkflowStatesOutput),
    #[serde(rename = "linear.list_recent_issue_changes")]
    LinearListRecentIssueChanges(ListRecentIssueChangesOutput),
    #[serde(rename = "github.inspect_repo")]
    GitHubInspectRepo(InspectRepoOutput),
    #[serde(rename = "github.inspect_pr")]
    GitHubInspectPr(InspectPrOutput),
    #[serde(rename = "github.create_pr")]
    GitHubCreatePr(CreatePrOutput),
    #[serde(rename = "github.check_ci")]
    GitHubCheckCi(CheckCiOutput),
    #[serde(rename = "github.merge_pr")]
    GitHubMergePr(MergePrOutput),
    #[serde(rename = "github.link_issue")]
    GitHubLinkIssue(LinkIssueOutput),
    #[serde(rename = "workspace.create_workspace")]
    WorkspaceCreateWorkspace(CreateWorkspaceOutput),
    #[serde(rename = "workspace.inspect_workspace")]
    WorkspaceInspectWorkspace(InspectWorkspaceOutput),
    #[serde(rename = "workspace.clean_workspace")]
    WorkspaceCleanWorkspace(CleanWorkspaceOutput),
    #[serde(rename = "workspace.run_repository_setup")]
    WorkspaceRunRepositorySetup(RunRepositorySetupOutput),
    #[serde(rename = "mob.list_runs")]
    MobListRuns(ListRunsOutput),
    #[serde(rename = "mob.inspect_run")]
    MobInspectRun(InspectRunOutput),
    #[serde(rename = "mob.spawn_or_resume_execution")]
    MobSpawnOrResumeExecution(SpawnOrResumeExecutionOutput),
    #[serde(rename = "mob.cancel_execution")]
    MobCancelExecution(CancelExecutionOutput),
    #[serde(rename = "mob.observe_events")]
    MobObserveEvents(ObserveEventsOutput),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectIssueInput {
    pub issue: LinearIssueRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectIssueOutput {
    pub issue: LinearIssueSnapshot,
    pub workpad: Option<LinearWorkpadSnapshot>,
    #[serde(default)]
    pub attachments: Vec<LinearFileAttachmentSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinearFileAttachmentSnapshot {
    pub source: LinearFileAttachmentSource,
    pub title: Option<String>,
    pub url: String,
    pub path: String,
    pub content_type: Option<String>,
    pub byte_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LinearFileAttachmentSource {
    IssueAttachment { attachment_id: LinearAttachmentId },
    IssueDescription,
    Comment { comment_id: LinearCommentId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinearWorkpadSnapshot {
    pub comment_id: LinearCommentId,
    pub heading: String,
    pub body_markdown: String,
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinearIssueCommentSnapshot {
    pub comment_id: LinearCommentId,
    pub body_markdown: String,
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimIssueInput {
    pub issue: LinearIssueRef,
    pub assignee: LinearUserRef,
    pub target_state: Option<LinearStateRef>,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimIssueOutput {
    pub issue: LinearIssueSnapshot,
    pub claimed: bool,
    pub previous_assignee: Option<LinearUserSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveIssueInput {
    pub issue: LinearIssueRef,
    pub state: LinearStateRef,
    pub expected_updated_at: Option<DateTime<Utc>>,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveIssueOutput {
    pub issue: LinearIssueSnapshot,
    pub changed: bool,
    pub previous_state: Option<LinearStateSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpsertWorkpadInput {
    pub issue: LinearIssueRef,
    pub heading: String,
    pub body_markdown: String,
    pub create_if_missing: bool,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpsertWorkpadOutput {
    pub comment_id: LinearCommentId,
    pub url: Option<String>,
    pub created: bool,
    pub updated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddCommentInput {
    pub issue: LinearIssueRef,
    pub body_markdown: String,
    #[serde(default)]
    pub attachments: Vec<LinearCommentAttachmentInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddCommentOutput {
    pub comment_id: LinearCommentId,
    pub url: Option<String>,
    #[serde(default)]
    pub attachments: Vec<LinearFileAttachmentSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinearCommentAttachmentInput {
    pub title: Option<String>,
    pub url: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    pub byte_len: u64,
}

pub fn render_comment_body_with_attachments(
    body_markdown: &str,
    attachments: &[LinearCommentAttachmentInput],
) -> String {
    if attachments.is_empty() {
        return body_markdown.to_string();
    }

    let mut body = body_markdown.trim_end().to_string();
    if !body.is_empty() {
        body.push_str("\n\n");
    }
    body.push_str("Attached files:\n");
    for attachment in attachments {
        let title = attachment
            .title
            .as_deref()
            .filter(|title| !title.trim().is_empty())
            .unwrap_or("attachment")
            .replace('[', "\\[")
            .replace(']', "\\]");
        body.push_str(&format!(
            "- [{}]({}) ({} bytes)\n",
            title, attachment.url, attachment.byte_len
        ));
    }
    body
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListProjectStatesInput {
    pub team_id: Option<LinearTeamId>,
    pub project_id: Option<LinearProjectId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListProjectStatesOutput {
    pub states: Vec<LinearStateSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnsureWorkflowStatesInput {
    pub team_id: LinearTeamId,
    pub states: Vec<EnsureWorkflowState>,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnsureWorkflowState {
    pub name: String,
    pub state_type: LinearStateType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnsureWorkflowStatesOutput {
    pub states: Vec<LinearStateSnapshot>,
    pub created: Vec<LinearStateSnapshot>,
    pub existing: Vec<LinearStateSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRecentIssueChangesInput {
    pub issue: Option<LinearIssueRef>,
    pub since: Option<DateTime<Utc>>,
    pub cursor: Option<Cursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRecentIssueChangesOutput {
    pub changes: Vec<LinearIssueChange>,
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinearIssueSnapshot {
    pub id: LinearIssueId,
    pub identifier: LinearIssueIdentifier,
    pub title: String,
    pub description: Option<String>,
    pub url: Option<String>,
    pub state: LinearStateSnapshot,
    pub assignee: Option<LinearUserSnapshot>,
    pub branch_name: Option<BranchName>,
    pub labels: Vec<String>,
    pub updated_at: DateTime<Utc>,
    pub team_id: LinearTeamId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinearStateSnapshot {
    pub id: LinearStateId,
    pub name: String,
    pub state_type: LinearStateType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinearStateType {
    Backlog,
    Unstarted,
    Started,
    Completed,
    Canceled,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinearUserSnapshot {
    pub id: LinearUserId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinearIssueChange {
    pub issue: LinearIssueRef,
    pub changed_at: DateTime<Utc>,
    pub actor: Option<LinearUserSnapshot>,
    pub kind: LinearIssueChangeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LinearIssueChangeKind {
    Created,
    StateChanged {
        from: Option<String>,
        to: String,
    },
    AssigneeChanged {
        from: Option<String>,
        to: Option<String>,
    },
    Commented {
        comment_id: LinearCommentId,
    },
    Updated {
        field: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectRepoInput {
    pub repo: GitHubRepoRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectRepoOutput {
    pub repo: GitHubRepoSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectPrInput {
    pub repo: GitHubRepoRef,
    pub pull_request: GitHubPullRequestRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectPrOutput {
    pub pull_request: GitHubPullRequestSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatePrInput {
    pub repo: GitHubRepoRef,
    pub title: String,
    pub body_markdown: String,
    pub head_branch: BranchName,
    pub base_branch: BranchName,
    pub draft: bool,
    pub existing_pr_match: ExistingPrMatch,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExistingPrMatch {
    HeadBranch,
    TitleAndHeadBranch,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatePrOutput {
    pub pull_request: GitHubPullRequestSnapshot,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckCiInput {
    pub repo: GitHubRepoRef,
    pub git_ref: GitRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckCiOutput {
    pub status: CiStatus,
    pub checks: Vec<GitHubCheckSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergePrInput {
    pub repo: GitHubRepoRef,
    pub pull_request: GitHubPullRequestRef,
    pub method: MergeMethod,
    pub expected_head_sha: Option<GitSha>,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeMethod {
    Merge,
    Squash,
    Rebase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergePrOutput {
    pub pull_request: GitHubPullRequestSnapshot,
    pub merged: bool,
    pub merge_commit_sha: Option<GitSha>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkIssueInput {
    pub repo: GitHubRepoRef,
    pub pull_request: GitHubPullRequestRef,
    pub issue: LinearIssueRef,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkIssueOutput {
    pub linked: bool,
    pub pull_request: GitHubPullRequestSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHubRepoSnapshot {
    pub repo: GitHubRepoRef,
    pub default_branch: BranchName,
    pub clone_url: String,
    pub html_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHubPullRequestSnapshot {
    pub repo: GitHubRepoRef,
    pub number: GitHubPullRequestNumber,
    pub title: String,
    pub url: String,
    pub state: PullRequestState,
    pub draft: bool,
    pub head_branch: BranchName,
    pub base_branch: BranchName,
    pub head_sha: GitSha,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PullRequestState {
    Open,
    Closed,
    Merged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHubCheckSnapshot {
    pub name: String,
    pub status: CiStatus,
    pub details_url: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CiStatus {
    /// The host reported neither checks nor commit statuses. This is not success.
    NoChecks,
    Pending,
    Success,
    Failure,
    Error,
    Skipped,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWorkspaceInput {
    pub repo: GitHubRepoRef,
    pub issue: LinearIssueRef,
    pub base_ref: GitRef,
    pub workspace_key: WorkspaceKey,
    pub reuse_existing: bool,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWorkspaceOutput {
    pub workspace: WorkspaceSnapshot,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectWorkspaceInput {
    pub workspace_key: WorkspaceKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectWorkspaceOutput {
    pub workspace: Option<WorkspaceSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanWorkspaceInput {
    pub workspace_key: WorkspaceKey,
    pub require_clean_git: bool,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanWorkspaceOutput {
    pub cleaned: bool,
    pub workspace_key: WorkspaceKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRepositorySetupInput {
    pub workspace_key: WorkspaceKey,
    pub setup_profile: Option<String>,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRepositorySetupOutput {
    pub workspace_key: WorkspaceKey,
    pub status: RepositorySetupStatus,
    pub exit_code: Option<i32>,
    pub log_excerpt: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositorySetupStatus {
    Succeeded,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshot {
    pub workspace_key: WorkspaceKey,
    pub path: String,
    pub repo: GitHubRepoRef,
    pub current_branch: Option<BranchName>,
    pub head_sha: Option<GitSha>,
    pub dirty: bool,
    pub exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRunsInput {
    pub issue: Option<LinearIssueRef>,
    pub status: Option<MobRunStatus>,
    pub cursor: Option<Cursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRunsOutput {
    pub runs: Vec<MobRunSnapshot>,
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectRunInput {
    pub run_id: MobRunId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectRunOutput {
    pub run: MobRunSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnOrResumeExecutionInput {
    pub issue: LinearIssueRef,
    pub workspace_key: WorkspaceKey,
    pub template_id: MobTemplateId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnOrResumeExecutionOutput {
    pub run: MobRunSnapshot,
    pub spawned: bool,
    pub resumed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelExecutionInput {
    pub run_id: MobRunId,
    pub reason: String,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelExecutionOutput {
    pub run: MobRunSnapshot,
    pub cancelled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveEventsInput {
    pub run_id: MobRunId,
    pub cursor: Option<Cursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveEventsOutput {
    pub events: Vec<MobEventSnapshot>,
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobRunSnapshot {
    pub run_id: MobRunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta_flow_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<String>,
    pub issue: Option<LinearIssueRef>,
    pub workspace_key: Option<WorkspaceKey>,
    pub template_id: Option<MobTemplateId>,
    pub status: MobRunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobRunStatus {
    Pending,
    Running,
    WaitingForInput,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobEventSnapshot {
    pub sequence: u64,
    pub occurred_at: DateTime<Utc>,
    pub kind: MobEventKind,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MobEventKind {
    RunStarted,
    MemberStarted {
        member: String,
    },
    MemberCompleted {
        member: String,
    },
    MemberKickoffUpdated {
        member: String,
        phase: String,
    },
    ToolCallStarted {
        operation: FugueToolOperation,
    },
    ToolCallCompleted {
        operation: FugueToolOperation,
    },
    RunCompleted {
        status: MobRunStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_output: Option<Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "retain the typed application contract without incidental boxing"
)]
pub enum FugueToolResult {
    Ok {
        output: FugueToolOutput,
        metadata: ToolExecutionMetadata,
    },
    Err {
        error: FugueToolError,
        metadata: ToolExecutionMetadata,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ToolExecutionMetadata {
    pub request_id: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ToolUsageMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolUsageMetadata {
    pub provider: Option<String>,
    pub billable_units: Option<u64>,
    pub rate_limit_remaining: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FugueToolError {
    pub operation: FugueToolOperation,
    pub code: FugueToolErrorCode,
    pub message: String,
    pub retry: RetryAdvice,
    pub idempotency_key: Option<IdempotencyKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FugueToolErrorCode {
    UnsupportedOperation,
    InvalidInput,
    NotFound,
    Conflict,
    PreconditionFailed,
    AuthenticationFailed,
    PermissionDenied,
    RateLimited,
    BackendUnavailable,
    Timeout,
    ExecutionFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetryAdvice {
    SafeToRetry,
    RetryAfter { seconds: u64 },
    RefreshStateThenRetry,
    DoNotRetry,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn repository_hosts_reject_urls_credentials_and_paths() {
        for host in [
            "",
            "https://git.example.com",
            "user@git.example.com",
            "git.example.com/other",
            "git.example.com?query",
            "git.example.com#fragment",
            "git.example.com\\other",
            "git example.com",
        ] {
            assert!(
                GitHubHost::try_from(host.to_string()).is_err(),
                "accepted {host:?}"
            );
        }
        let repo: GitHubRepoRef = serde_json::from_value(
            json!({"host":"GIT.EXAMPLE.COM:8443","owner":"acme","name":"app"}),
        )
        .expect("repo");
        assert_eq!(repo.to_string(), "git.example.com:8443/acme/app");
    }

    #[test]
    fn operation_names_round_trip_as_catalog_strings() {
        assert_eq!(
            FugueToolOperation::GitHubCreatePr.to_string(),
            "github.create_pr"
        );
        assert_eq!(
            "mob.spawn_or_resume_execution"
                .parse::<FugueToolOperation>()
                .ok(),
            Some(FugueToolOperation::MobSpawnOrResumeExecution)
        );
        assert!("github.delete_repo".parse::<FugueToolOperation>().is_err());
    }

    #[test]
    fn catalog_marks_mutations_as_idempotent() {
        let create_pr = tool_contract(FugueToolOperation::GitHubCreatePr)
            .expect("catalog should include github.create_pr");
        assert!(create_pr.mutates);
        assert_eq!(
            create_pr.idempotency,
            IdempotencyRequirement::RequiredForMutation
        );

        let inspect_repo = tool_contract(FugueToolOperation::GitHubInspectRepo)
            .expect("catalog should include github.inspect_repo");
        assert!(!inspect_repo.mutates);
        assert_eq!(
            inspect_repo.idempotency,
            IdempotencyRequirement::NotRequiredReadOnly
        );
    }

    #[test]
    fn render_comment_body_with_attachments_preserves_body_and_links_files() {
        let rendered = render_comment_body_with_attachments(
            "Please inspect this trace.",
            &[LinearCommentAttachmentInput {
                title: Some("trace [redacted].txt".to_string()),
                url: "/api/v1/issues/FUG-2/attachments/001-trace.txt".to_string(),
                path: None,
                content_type: Some("text/plain".to_string()),
                byte_len: 16,
            }],
        );

        assert!(rendered.starts_with("Please inspect this trace.\n\nAttached files:"));
        assert!(rendered.contains("[trace \\[redacted\\].txt]"));
        assert!(rendered.contains("/api/v1/issues/FUG-2/attachments/001-trace.txt"));
        assert!(rendered.contains("16 bytes"));
    }

    #[test]
    fn linear_add_comment_contract_is_non_idempotent() {
        let add_comment = tool_contract(FugueToolOperation::LinearAddComment)
            .expect("catalog should include linear.add_comment");
        assert!(add_comment.mutates);
        assert_eq!(
            add_comment.idempotency,
            IdempotencyRequirement::NonIdempotentMutation
        );

        let value = json!({
            "operation": "linear.add_comment",
            "input": {
                "issue": { "kind": "identifier", "identifier": "LUC-136" },
                "body_markdown": "note"
            }
        });
        assert!(serde_json::from_value::<FugueToolInput>(value).is_ok());
    }

    #[test]
    fn serializes_tagged_tool_input_shape() {
        let input = FugueToolInput::LinearClaimIssue(ClaimIssueInput {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-136".to_string()),
            },
            assignee: LinearUserRef::Me,
            target_state: Some(LinearStateRef::Name {
                name: "In Progress".to_string(),
            }),
            idempotency_key: IdempotencyKey("claim:LUC-136".to_string()),
        });

        let value = serde_json::to_value(&input).expect("tool input serializes");
        assert_eq!(
            value,
            json!({
                "operation": "linear.claim_issue",
                "input": {
                    "issue": { "kind": "identifier", "identifier": "LUC-136" },
                    "assignee": { "kind": "me" },
                    "target_state": { "kind": "name", "name": "In Progress" },
                    "idempotency_key": "claim:LUC-136"
                }
            })
        );
        let round_trip: FugueToolInput = serde_json::from_value(value).expect("tool input parses");
        assert_eq!(round_trip.operation(), FugueToolOperation::LinearClaimIssue);
        assert_eq!(round_trip, input);
    }

    #[test]
    fn rejects_unsupported_tagged_operation() {
        let value = json!({
            "operation": "linear.delete_issue",
            "input": { "issue": { "kind": "identifier", "identifier": "LUC-136" } }
        });
        assert!(serde_json::from_value::<FugueToolInput>(value).is_err());
    }

    #[test]
    fn rejects_invalid_typed_enum_values() {
        let value = json!({
            "operation": "github.merge_pr",
            "input": {
                "repo": { "owner": "lukacf", "name": "meerkat-fugue" },
                "pull_request": { "kind": "number", "number": 1 },
                "method": "teleport",
                "expected_head_sha": null,
                "idempotency_key": "merge:1"
            }
        });
        assert!(serde_json::from_value::<FugueToolInput>(value).is_err());
    }

    #[test]
    fn structured_error_keeps_usage_metadata_out_of_domain_output() {
        let error = FugueToolResult::Err {
            error: FugueToolError {
                operation: FugueToolOperation::WorkspaceRunRepositorySetup,
                code: FugueToolErrorCode::BackendUnavailable,
                message: "setup runner unavailable".to_string(),
                retry: RetryAdvice::SafeToRetry,
                idempotency_key: Some(IdempotencyKey("setup:LUC-136".to_string())),
            },
            metadata: ToolExecutionMetadata {
                request_id: Some("req-1".to_string()),
                usage: Some(ToolUsageMetadata {
                    provider: Some("github".to_string()),
                    billable_units: Some(1),
                    rate_limit_remaining: Some(4999),
                }),
                ..ToolExecutionMetadata::default()
            },
        };

        let value = serde_json::to_value(error).expect("tool result serializes");
        assert_eq!(value["status"], "err");
        assert_eq!(value["metadata"]["usage"]["provider"], "github");
        assert!(value["error"].get("usage").is_none());
    }
}
