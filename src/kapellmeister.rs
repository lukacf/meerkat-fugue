//! Typed Stage 3A Kapellmeister shadow-mode contracts.
//!
//! These contracts are intentionally inert: they model normalized signals,
//! read-only authority, recommendations, durable cursors, and comparison
//! outcomes without granting Kapellmeister production decision authority.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tracing::{info, warn};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

use crate::config::{KapellmeisterShadowModeConfig, MeerkatConfig, TrackerConfig};
use crate::error::{FugueError, Result};
use crate::final_pr_gate::FinalPrGateStartRequest;
use crate::independent_review::IndependentReviewStartRequest;
use crate::linear_lane_state::LegacyLinearLaneRepair;
use crate::linear_lane_state::LinearLaneClassification;

use crate::tools::{
    AddCommentInput, AddCommentOutput, BranchName, CheckCiInput, CheckCiOutput, CiStatus, Cursor,
    FugueToolOperation, FugueToolOutput, FugueToolResult, GitHubCheckSnapshot, GitHubOwner,
    GitHubPullRequestNumber, GitHubPullRequestSnapshot, GitHubRepoName, GitHubRepoRef, GitRef,
    GitSha, IdempotencyKey, LinearCommentId, LinearIssueChange, LinearIssueChangeKind,
    LinearIssueId, LinearIssueIdentifier, LinearIssueRef, LinearStateId, LinearStateRef,
    LinearUserRef, LinearUserSnapshot, MergePrInput, MobRunId, MobRunStatus, MobTemplateId,
    PullRequestState, UpsertWorkpadInput, WorkspaceKey, render_comment_body_with_attachments,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShadowSignalId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShadowRecommendationId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShadowDecisionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CursorPartition(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterShadowSignal {
    pub id: ShadowSignalId,
    pub observed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_cursor: Option<DurableCursorPosition>,
    pub kind: KapellmeisterShadowSignalKind,
}

impl KapellmeisterShadowSignal {
    pub fn new(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        kind: KapellmeisterShadowSignalKind,
    ) -> Self {
        let id = stable_signal_id(&source_cursor, &kind);
        Self {
            id,
            observed_at,
            source_cursor,
            kind,
        }
    }

    pub fn linear_issue_moved(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        from_state: Option<LinearStateRef>,
        to_state: LinearStateRef,
        actor: Option<LinearUserSnapshot>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::LinearIssueMoved {
                issue,
                from_state,
                to_state,
                actor,
            },
        )
    }

    pub fn linear_lane_changed(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        from_state: LinearStateRef,
        to_state: LinearStateRef,
        classification: LinearLaneClassification,
        running_mob: Option<KapellmeisterRunningMobObservation>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::LinearLaneChanged {
                issue,
                from_state,
                to_state,
                classification,
                running_mob,
            },
        )
    }

    pub fn linear_issue_first_seen(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        lane: LinearStateRef,
        classification: LinearLaneClassification,
        running_mob: Option<KapellmeisterRunningMobObservation>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::LinearIssueFirstSeen {
                issue,
                lane,
                classification,
                running_mob,
            },
        )
    }

    pub fn runtime_recovered(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        run_id: MobRunId,
        linear_lane: LinearStateRef,
        projected_lane: Option<LinearStateRef>,
        workspace: Option<KapellmeisterWorkspaceEvidence>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::RuntimeRecovered {
                issue,
                run_id,
                linear_lane,
                projected_lane,
                workspace,
            },
        )
    }

    pub fn open_issue_without_runtime(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        lane: LinearStateRef,
        classification: LinearLaneClassification,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::OpenIssueWithoutRuntime {
                issue,
                lane,
                classification,
            },
        )
    }

    pub fn legacy_linear_lane_repair_suggested(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        repair: LegacyLinearLaneRepair,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::LegacyLinearLaneRepairSuggested { issue, repair },
        )
    }

    pub fn linear_issue_commented(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        comment_id: LinearCommentId,
        body_markdown: Option<String>,
        operator_review_finding: Option<String>,
        actor: Option<LinearUserSnapshot>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::LinearIssueCommented {
                issue,
                comment_id,
                body_markdown,
                operator_review_finding,
                actor,
            },
        )
    }

    pub fn linear_issue_comment_changed(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        comment_id: LinearCommentId,
        body_markdown: Option<String>,
        operator_review_finding: Option<String>,
        actor: Option<LinearUserSnapshot>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::LinearIssueCommentChanged {
                issue,
                comment_id,
                body_markdown,
                operator_review_finding,
                actor,
            },
        )
    }

    pub fn linear_issue_comment_deleted(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        comment_id: LinearCommentId,
        actor: Option<LinearUserSnapshot>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::LinearIssueCommentDeleted {
                issue,
                comment_id,
                actor,
            },
        )
    }

    pub fn pull_request_opened(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        repo: GitHubRepoRef,
        pull_request: GitHubPullRequestSnapshot,
        actor: Option<String>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::PullRequestOpened {
                repo,
                pull_request,
                actor,
            },
        )
    }

    pub fn pull_request_merged(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        repo: GitHubRepoRef,
        pull_request: GitHubPullRequestSnapshot,
        merge_commit_sha: Option<GitSha>,
        actor: Option<String>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::PullRequestMerged {
                repo,
                pull_request,
                merge_commit_sha,
                actor,
            },
        )
    }

    pub fn ci_changed(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        repo: GitHubRepoRef,
        git_ref: GitRef,
        status: CiStatus,
        checks: Vec<GitHubCheckSnapshot>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::CiChanged {
                repo,
                git_ref,
                status,
                checks,
            },
        )
    }

    pub fn worker_started(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
        attempt: u32,
        session_id: Option<String>,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::WorkerStarted {
                issue,
                workspace_key,
                attempt,
                session_id,
            },
        )
    }

    pub fn worker_finished(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
        outcome: ShadowWorkerOutcome,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::WorkerFinished {
                issue,
                workspace_key,
                outcome,
            },
        )
    }

    pub fn worker_failed(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
        failure: ShadowWorkerFailure,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::WorkerFailed {
                issue,
                workspace_key,
                failure,
            },
        )
    }

    pub fn final_pr_gate_requested(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        pull_request: GitHubPullRequestSnapshot,
        start_request: FinalPrGateStartRequest,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::FinalPrGateRequested {
                issue,
                pull_request,
                start_request,
            },
        )
    }

    pub fn pr_handoff_monitor_changed(
        observed_at: DateTime<Utc>,
        source_cursor: Option<DurableCursorPosition>,
        issue: LinearIssueRef,
        pull_request: GitHubPullRequestSnapshot,
        ci_status: Option<CiStatus>,
        decision: PrHandoffMonitorDecision,
        detail: String,
    ) -> Self {
        Self::new(
            observed_at,
            source_cursor,
            KapellmeisterShadowSignalKind::PrHandoffMonitorChanged {
                issue,
                pull_request,
                ci_status,
                decision,
                detail,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterRunningMobObservation {
    pub run_id: MobRunId,
    pub status: MobRunStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phase_labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterWorkspaceEvidence {
    pub path: String,
    pub branch: Option<BranchName>,
    pub head_sha: Option<GitSha>,
    pub upstream_branch: Option<BranchName>,
    pub upstream_head_sha: Option<GitSha>,
    pub dirty: bool,
}

fn stable_signal_id(
    source_cursor: &Option<DurableCursorPosition>,
    kind: &KapellmeisterShadowSignalKind,
) -> ShadowSignalId {
    let cursor_fingerprint = source_cursor
        .as_ref()
        .map(|cursor| format!("{:?}|{}", cursor.key, cursor.cursor.0))
        .unwrap_or_default();
    let mut hash = 0xcbf29ce484222325u64;
    for byte in format!("{cursor_fingerprint}|{kind:?}").bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    ShadowSignalId(format!("sig-{hash:016x}"))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "retain the typed application contract without incidental boxing"
)]
pub enum KapellmeisterShadowSignalKind {
    LinearIssueMoved {
        issue: LinearIssueRef,
        from_state: Option<LinearStateRef>,
        to_state: LinearStateRef,
        actor: Option<LinearUserSnapshot>,
    },
    LinearLaneChanged {
        issue: LinearIssueRef,
        from_state: LinearStateRef,
        to_state: LinearStateRef,
        classification: LinearLaneClassification,
        running_mob: Option<KapellmeisterRunningMobObservation>,
    },
    LinearIssueFirstSeen {
        issue: LinearIssueRef,
        lane: LinearStateRef,
        classification: LinearLaneClassification,
        running_mob: Option<KapellmeisterRunningMobObservation>,
    },
    RuntimeRecovered {
        issue: LinearIssueRef,
        run_id: MobRunId,
        linear_lane: LinearStateRef,
        projected_lane: Option<LinearStateRef>,
        workspace: Option<KapellmeisterWorkspaceEvidence>,
    },
    OpenIssueWithoutRuntime {
        issue: LinearIssueRef,
        lane: LinearStateRef,
        classification: LinearLaneClassification,
    },
    LegacyLinearLaneRepairSuggested {
        issue: LinearIssueRef,
        repair: LegacyLinearLaneRepair,
    },
    LinearIssueCommented {
        issue: LinearIssueRef,
        comment_id: LinearCommentId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body_markdown: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operator_review_finding: Option<String>,
        actor: Option<LinearUserSnapshot>,
    },
    LinearIssueCommentChanged {
        issue: LinearIssueRef,
        comment_id: LinearCommentId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body_markdown: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operator_review_finding: Option<String>,
        actor: Option<LinearUserSnapshot>,
    },
    LinearIssueCommentDeleted {
        issue: LinearIssueRef,
        comment_id: LinearCommentId,
        actor: Option<LinearUserSnapshot>,
    },
    PullRequestOpened {
        repo: GitHubRepoRef,
        pull_request: GitHubPullRequestSnapshot,
        actor: Option<String>,
    },
    PullRequestMerged {
        repo: GitHubRepoRef,
        pull_request: GitHubPullRequestSnapshot,
        merge_commit_sha: Option<GitSha>,
        actor: Option<String>,
    },
    CiChanged {
        repo: GitHubRepoRef,
        git_ref: GitRef,
        status: CiStatus,
        checks: Vec<GitHubCheckSnapshot>,
    },
    WorkerStarted {
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
        attempt: u32,
        session_id: Option<String>,
    },
    WorkerFinished {
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
        outcome: ShadowWorkerOutcome,
    },
    WorkerFailed {
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
        failure: ShadowWorkerFailure,
    },
    FinalPrGateRequested {
        issue: LinearIssueRef,
        pull_request: GitHubPullRequestSnapshot,
        start_request: FinalPrGateStartRequest,
    },
    PrHandoffMonitorChanged {
        issue: LinearIssueRef,
        pull_request: GitHubPullRequestSnapshot,
        ci_status: Option<CiStatus>,
        decision: PrHandoffMonitorDecision,
        detail: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrHandoffMonitorDecision {
    CiPending,
    ReadyForDone,
    Replan,
    Merged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowWorkerOutcome {
    Succeeded,
    CompletedWithTerminalIssueState,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowWorkerFailure {
    pub reason: ShadowWorkerFailureReason,
    pub message: Option<String>,
    pub retry_scheduled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowWorkerFailureReason {
    WorkerError,
    TimedOut,
    Stalled,
    ReconciliationCancelled,
    InfrastructureUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterSignalDeliveryBatch {
    pub signals: Vec<KapellmeisterShadowSignal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workpad_preview: Option<KapellmeisterWorkpadPreview>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterSignalDeliveryReceipt {
    pub delivered_signal_ids: Vec<ShadowSignalId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observations: Vec<KapellmeisterShadowObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommendations: Vec<KapellmeisterShadowRecommendation>,
}

impl KapellmeisterSignalDeliveryReceipt {
    pub fn delivered_all(signals: &[KapellmeisterShadowSignal]) -> Self {
        Self {
            delivered_signal_ids: signals.iter().map(|signal| signal.id.clone()).collect(),
            observations: Vec::new(),
            recommendations: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterSignalDeliveryOutcome {
    pub session_available: bool,
    pub delivered_signal_ids: Vec<ShadowSignalId>,
    pub skipped_duplicate_signal_ids: Vec<ShadowSignalId>,
    pub durable_cursors: Vec<DurableCursorPosition>,
    pub recommendations: Vec<KapellmeisterShadowRecommendation>,
    pub decision_comparisons: Vec<KapellmeisterDecisionComparison>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterShadowObservation {
    pub signal_id: ShadowSignalId,
    pub observed_at: DateTime<Utc>,
    pub status: KapellmeisterShadowObservationStatus,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KapellmeisterShadowObservationStatus {
    Delivered,
    DuplicateSkipped,
    MutationDisabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterWorkpadPreview {
    pub issue: LinearIssueRef,
    pub heading: String,
    pub body_markdown: String,
    pub blocked_operation: FugueToolOperation,
    pub mutation_allowed: bool,
}

pub fn shadow_signals_from_linear_change(
    change: LinearIssueChange,
) -> Vec<KapellmeisterShadowSignal> {
    let observed_at = change.changed_at;
    let issue = change.issue;
    let actor = change.actor;
    match change.kind {
        LinearIssueChangeKind::StateChanged { from, to } => {
            vec![KapellmeisterShadowSignal::linear_issue_moved(
                observed_at,
                Some(linear_cursor(
                    &issue,
                    format!("state:{from:?}:{to}"),
                    observed_at,
                )),
                issue,
                from.map(|name| LinearStateRef::Name { name }),
                LinearStateRef::Name { name: to },
                actor,
            )]
        }
        LinearIssueChangeKind::Commented { comment_id } => {
            let cursor = linear_cursor(&issue, format!("comment:{}", comment_id.0), observed_at);
            vec![KapellmeisterShadowSignal::linear_issue_commented(
                observed_at,
                Some(cursor),
                issue,
                comment_id,
                None,
                None,
                actor,
            )]
        }
        LinearIssueChangeKind::Created
        | LinearIssueChangeKind::AssigneeChanged { .. }
        | LinearIssueChangeKind::Updated { .. } => Vec::new(),
    }
}

pub fn shadow_signals_from_tool_result(result: &FugueToolResult) -> Vec<KapellmeisterShadowSignal> {
    let FugueToolResult::Ok { output, metadata } = result else {
        return Vec::new();
    };
    let observed_at = metadata
        .completed_at
        .or(metadata.started_at)
        .unwrap_or_else(Utc::now);
    match output {
        FugueToolOutput::LinearMoveIssue(output) if output.changed => {
            let issue = LinearIssueRef::Id {
                id: output.issue.id.clone(),
            };
            vec![KapellmeisterShadowSignal::linear_issue_moved(
                observed_at,
                Some(linear_cursor(
                    &issue,
                    format!("move:{}:{}", output.issue.id.0, output.issue.updated_at),
                    observed_at,
                )),
                issue,
                output.previous_state.clone().map(LinearStateRef::from),
                LinearStateRef::from(output.issue.state.clone()),
                None,
            )]
        }
        FugueToolOutput::LinearClaimIssue(output) if output.claimed => {
            let issue = LinearIssueRef::Id {
                id: output.issue.id.clone(),
            };
            vec![KapellmeisterShadowSignal::linear_issue_moved(
                observed_at,
                Some(linear_cursor(
                    &issue,
                    format!("claim:{}:{}", output.issue.id.0, output.issue.updated_at),
                    observed_at,
                )),
                issue,
                None,
                LinearStateRef::from(output.issue.state.clone()),
                output.issue.assignee.clone(),
            )]
        }
        FugueToolOutput::LinearAddComment(_) => Vec::new(),
        FugueToolOutput::LinearListRecentIssueChanges(output) => output
            .changes
            .iter()
            .cloned()
            .flat_map(shadow_signals_from_linear_change)
            .collect(),
        FugueToolOutput::GitHubCreatePr(output) if output.created => {
            let pull_request = output.pull_request.clone();
            vec![KapellmeisterShadowSignal::pull_request_opened(
                observed_at,
                Some(github_pr_cursor(&pull_request, "opened", observed_at)),
                pull_request.repo.clone(),
                pull_request,
                None,
            )]
        }
        FugueToolOutput::GitHubMergePr(output) if output.merged => {
            let pull_request = output.pull_request.clone();
            vec![KapellmeisterShadowSignal::pull_request_merged(
                observed_at,
                Some(github_pr_cursor(&pull_request, "merged", observed_at)),
                pull_request.repo.clone(),
                pull_request,
                output.merge_commit_sha.clone(),
                None,
            )]
        }
        FugueToolOutput::GitHubInspectPr(output)
            if output.pull_request.state == PullRequestState::Merged =>
        {
            let pull_request = output.pull_request.clone();
            vec![KapellmeisterShadowSignal::pull_request_merged(
                observed_at,
                Some(github_pr_cursor(&pull_request, "merged", observed_at)),
                pull_request.repo.clone(),
                pull_request,
                None,
                None,
            )]
        }
        FugueToolOutput::GitHubCheckCi(CheckCiOutput { status, checks }) => {
            let repo = GitHubRepoRef {
                host: None,
                owner: GitHubOwner("unknown".to_string()),
                name: GitHubRepoName("unknown".to_string()),
            };
            vec![KapellmeisterShadowSignal::ci_changed(
                observed_at,
                Some(DurableCursorPosition {
                    key: DurableCursorKey {
                        source: DurableCursorSource::GithubCi,
                        partition: github_repo_partition(&repo),
                    },
                    cursor: Cursor(format!("ci:{status:?}:{}", checks.len())),
                    observed_at,
                }),
                repo,
                GitRef("unknown".to_string()),
                *status,
                checks.clone(),
            )]
        }
        _ => Vec::new(),
    }
}

pub fn shadow_signals_from_add_comment_result(
    input: &AddCommentInput,
    output: &AddCommentOutput,
    observed_at: DateTime<Utc>,
) -> Vec<KapellmeisterShadowSignal> {
    let body_markdown =
        render_comment_body_with_attachments(&input.body_markdown, &input.attachments);
    vec![KapellmeisterShadowSignal::linear_issue_commented(
        observed_at,
        Some(linear_cursor(
            &input.issue,
            format!("comment:{}", output.comment_id.0),
            observed_at,
        )),
        input.issue.clone(),
        output.comment_id.clone(),
        Some(body_markdown.clone()),
        crate::review_findings::extract_unresolved_operator_review_note(&body_markdown),
        None,
    )]
}

pub fn shadow_signals_from_check_ci_result(
    input: &CheckCiInput,
    output: &CheckCiOutput,
    observed_at: DateTime<Utc>,
) -> Vec<KapellmeisterShadowSignal> {
    vec![KapellmeisterShadowSignal::ci_changed(
        observed_at,
        Some(DurableCursorPosition {
            key: DurableCursorKey {
                source: DurableCursorSource::GithubCi,
                partition: github_repo_partition(&input.repo),
            },
            cursor: Cursor(format!(
                "ci:{}:{:?}:{}",
                input.git_ref.0,
                output.status,
                output.checks.len()
            )),
            observed_at,
        }),
        input.repo.clone(),
        input.git_ref.clone(),
        output.status,
        output.checks.clone(),
    )]
}

impl From<crate::tools::LinearStateSnapshot> for LinearStateRef {
    fn from(value: crate::tools::LinearStateSnapshot) -> Self {
        LinearStateRef::Id { id: value.id }
    }
}

fn linear_cursor(
    issue: &LinearIssueRef,
    suffix: String,
    observed_at: DateTime<Utc>,
) -> DurableCursorPosition {
    DurableCursorPosition {
        key: DurableCursorKey {
            source: DurableCursorSource::LinearIssueChanges,
            partition: match issue {
                LinearIssueRef::Id { id } => CursorPartition(id.0.clone()),
                LinearIssueRef::Identifier { identifier } => CursorPartition(identifier.0.clone()),
            },
        },
        cursor: Cursor(format!("linear:{suffix}")),
        observed_at,
    }
}

fn github_repo_partition(repo: &GitHubRepoRef) -> CursorPartition {
    CursorPartition(repo.to_string())
}

fn github_pr_cursor(
    pull_request: &GitHubPullRequestSnapshot,
    event: &str,
    observed_at: DateTime<Utc>,
) -> DurableCursorPosition {
    DurableCursorPosition {
        key: DurableCursorKey {
            source: DurableCursorSource::GithubPullRequests,
            partition: github_repo_partition(&pull_request.repo),
        },
        cursor: Cursor(format!(
            "github:pr:{}:{event}:{}",
            pull_request.number.0, pull_request.head_sha.0
        )),
        observed_at,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowDeliveryOutcomeLog {
    pub completed: bool,
}

pub fn spawn_shadow_signal_delivery<F>(delivery: F) -> JoinHandle<ShadowDeliveryOutcomeLog>
where
    F: std::future::Future<Output = Result<KapellmeisterSignalDeliveryOutcome>> + Send + 'static,
{
    tokio::spawn(async move {
        match delivery.await {
            Ok(outcome) => {
                info!(?outcome, "kapellmeister shadow signal delivery observed");
                ShadowDeliveryOutcomeLog { completed: true }
            }
            Err(err) => {
                warn!(error = %err, "kapellmeister shadow signal delivery failed");
                ShadowDeliveryOutcomeLog { completed: false }
            }
        }
    })
}

pub fn write_disabled_workpad_preview(
    authority: KapellmeisterToolAuthority,
    input: UpsertWorkpadInput,
) -> Result<KapellmeisterWorkpadPreview> {
    if authority.permits(FugueToolOperation::LinearUpsertWorkpad) {
        return Err(FugueError::InvalidConfig(
            "kapellmeister shadow workpad preview cannot permit Linear mutations".to_string(),
        ));
    }
    Ok(KapellmeisterWorkpadPreview {
        issue: input.issue,
        heading: input.heading,
        body_markdown: input.body_markdown,
        blocked_operation: FugueToolOperation::LinearUpsertWorkpad,
        mutation_allowed: false,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterShadowRecommendationResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommendations: Vec<KapellmeisterShadowRecommendation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterShadowRecommendation {
    pub id: ShadowRecommendationId,
    pub generated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub based_on_signals: Vec<ShadowSignalId>,
    pub action: KapellmeisterRecommendedAction,
    pub confidence: RecommendationConfidence,
    pub rationale: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum KapellmeisterRecommendedAction {
    Noop,
    DispatchWorker {
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
    },
    DeferIssue {
        issue: LinearIssueRef,
        reason: String,
    },
    RetryWorker {
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
        after_seconds: u64,
    },
    TerminateWorker {
        issue: LinearIssueRef,
        reason: String,
    },
    MoveLinearIssue {
        issue: LinearIssueRef,
        target_state: LinearStateRef,
    },
    OpenPullRequest {
        repo: GitHubRepoRef,
        title: String,
        head_branch: BranchName,
        base_branch: BranchName,
        draft: bool,
    },
    MergePullRequest {
        repo: GitHubRepoRef,
        number: GitHubPullRequestNumber,
        expected_head_sha: Option<GitSha>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationConfidence {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterDecisionComparison {
    pub recommendation_id: ShadowRecommendationId,
    pub rust_decision: RustAuthoritativeDecision,
    pub outcome: DecisionComparisonOutcome,
    pub compared_at: DateTime<Utc>,
    pub notes: Option<String>,
}

impl KapellmeisterDecisionComparison {
    pub fn compare(
        recommendation: &KapellmeisterShadowRecommendation,
        rust_decision: RustAuthoritativeDecision,
        compared_at: DateTime<Utc>,
    ) -> Self {
        let (outcome, notes) =
            compare_recommendation_to_rust_decision(&recommendation.action, &rust_decision.action);
        Self {
            recommendation_id: recommendation.id.clone(),
            rust_decision,
            outcome,
            compared_at,
            notes,
        }
    }
}

fn compare_recommendation_to_rust_decision(
    recommendation: &KapellmeisterRecommendedAction,
    rust_decision: &RustAuthoritativeAction,
) -> (DecisionComparisonOutcome, Option<String>) {
    match (recommendation, rust_decision) {
        (KapellmeisterRecommendedAction::Noop, _) => (
            DecisionComparisonOutcome::ShadowOnly,
            Some("Kapellmeister recommended no production action".to_string()),
        ),
        (
            KapellmeisterRecommendedAction::DispatchWorker {
                issue: recommended_issue,
                workspace_key: recommended_workspace,
            },
            RustAuthoritativeAction::DispatchWorker {
                issue: rust_issue,
                workspace_key: rust_workspace,
            },
        ) => same_or_different(
            recommended_issue == rust_issue && recommended_workspace == rust_workspace,
            "dispatch worker recommendation matched Rust dispatch",
            "dispatch worker recommendation differed from Rust dispatch",
        ),
        (
            KapellmeisterRecommendedAction::RetryWorker {
                issue: recommended_issue,
                workspace_key: recommended_workspace,
                after_seconds: recommended_after,
            },
            RustAuthoritativeAction::RetryWorker {
                issue: rust_issue,
                workspace_key: rust_workspace,
                after_seconds: rust_after,
            },
        ) => same_or_different(
            recommended_issue == rust_issue
                && recommended_workspace == rust_workspace
                && recommended_after == rust_after,
            "retry recommendation matched Rust retry schedule",
            "retry recommendation differed from Rust retry schedule",
        ),
        (
            KapellmeisterRecommendedAction::TerminateWorker {
                issue: recommended_issue,
                reason: recommended_reason,
            },
            RustAuthoritativeAction::TerminateWorker {
                issue: rust_issue,
                reason: rust_reason,
            },
        ) => same_or_different(
            recommended_issue == rust_issue && recommended_reason == rust_reason,
            "termination recommendation matched Rust termination",
            "termination recommendation differed from Rust termination",
        ),
        (
            KapellmeisterRecommendedAction::DeferIssue {
                issue: recommended_issue,
                ..
            },
            RustAuthoritativeAction::SkipDispatch {
                issue: rust_issue, ..
            },
        ) => same_or_different(
            recommended_issue == rust_issue,
            "defer recommendation matched Rust skip dispatch",
            "defer recommendation differed from Rust skip dispatch",
        ),
        (KapellmeisterRecommendedAction::DeferIssue { issue, .. }, _) => (
            DecisionComparisonOutcome::Disagrees,
            Some(format!(
                "Kapellmeister recommended deferring {issue:?}, but Rust chose a different action"
            )),
        ),
        (
            KapellmeisterRecommendedAction::MoveLinearIssue { .. }
            | KapellmeisterRecommendedAction::OpenPullRequest { .. }
            | KapellmeisterRecommendedAction::MergePullRequest { .. },
            _,
        ) => (
            DecisionComparisonOutcome::NotComparable,
            Some(
                "recommendation targets a tool-level mutation outside Rust scheduler comparison"
                    .to_string(),
            ),
        ),
        (_, RustAuthoritativeAction::ContinueWorker { .. }) => (
            DecisionComparisonOutcome::RustOnly,
            Some(
                "Rust continued an existing worker without a comparable shadow action".to_string(),
            ),
        ),
        (_, RustAuthoritativeAction::SkipDispatch { .. }) => (
            DecisionComparisonOutcome::Disagrees,
            Some(
                "Rust skipped dispatch while Kapellmeister recommended a different action"
                    .to_string(),
            ),
        ),
        _ => (
            DecisionComparisonOutcome::Disagrees,
            Some(
                "Kapellmeister recommendation differed from Rust authoritative action".to_string(),
            ),
        ),
    }
}

fn same_or_different(
    same: bool,
    agree_note: &str,
    disagree_note: &str,
) -> (DecisionComparisonOutcome, Option<String>) {
    if same {
        (
            DecisionComparisonOutcome::Agrees,
            Some(agree_note.to_string()),
        )
    } else {
        (
            DecisionComparisonOutcome::Disagrees,
            Some(disagree_note.to_string()),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RustAuthoritativeDecision {
    pub id: ShadowDecisionId,
    pub decided_at: DateTime<Utc>,
    pub action: RustAuthoritativeAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RustAuthoritativeAction {
    DispatchWorker {
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
    },
    SkipDispatch {
        issue: LinearIssueRef,
        reason: String,
    },
    RetryWorker {
        issue: LinearIssueRef,
        workspace_key: WorkspaceKey,
        after_seconds: u64,
    },
    TerminateWorker {
        issue: LinearIssueRef,
        reason: String,
    },
    ContinueWorker {
        issue: LinearIssueRef,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionComparisonOutcome {
    Agrees,
    Disagrees,
    NotComparable,
    ShadowOnly,
    RustOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DurableCursorKey {
    pub source: DurableCursorSource,
    pub partition: CursorPartition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableCursorSource {
    LinearIssueChanges,
    GithubPullRequests,
    GithubCi,
    WorkerEvents,
    KapellmeisterRecommendations,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableCursorPosition {
    pub key: DurableCursorKey,
    pub cursor: Cursor,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KapellmeisterAuthorityMode {
    ShadowOnly,
    GuardedLabeled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KapellmeisterAuthorityConfig {
    pub mode: KapellmeisterAuthorityMode,
    #[serde(default)]
    pub safe_selectors: KapellmeisterSafeSelectors,
}

impl Default for KapellmeisterAuthorityConfig {
    fn default() -> Self {
        Self {
            mode: KapellmeisterAuthorityMode::ShadowOnly,
            safe_selectors: KapellmeisterSafeSelectors::default(),
        }
    }
}

impl KapellmeisterAuthorityConfig {
    pub fn validate(&self) -> Result<()> {
        match self.mode {
            KapellmeisterAuthorityMode::ShadowOnly => Ok(()),
            KapellmeisterAuthorityMode::GuardedLabeled => self.safe_selectors.validate_guarded(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KapellmeisterSafeSelectors {
    pub repositories: Vec<GitHubRepoRef>,
    pub linear_labels: Vec<String>,
}

impl KapellmeisterSafeSelectors {
    pub fn validate_guarded(&self) -> Result<()> {
        if self.repositories.is_empty() && self.linear_labels.is_empty() {
            return Err(FugueError::InvalidConfig(
                "kapellmeister.authority_mode guarded_labeled requires at least one explicit repository or Linear label selector".to_string(),
            ));
        }
        for repo in &self.repositories {
            validate_non_wildcard_selector(
                &repo.owner.0,
                "kapellmeister.safe_selectors.repositories.owner",
            )?;
            validate_non_wildcard_selector(
                &repo.name.0,
                "kapellmeister.safe_selectors.repositories.name",
            )?;
        }
        for label in &self.linear_labels {
            validate_non_wildcard_selector(label, "kapellmeister.safe_selectors.linear_labels")?;
        }
        Ok(())
    }
}

fn validate_non_wildcard_selector(value: &str, field: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "*" || trimmed.eq_ignore_ascii_case("all") {
        return Err(FugueError::InvalidConfig(format!(
            "{field} must be an explicit narrow selector"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterToolAuthority {
    pub mode: KapellmeisterAuthorityMode,
    pub read_only_tools: bool,
    #[serde(default)]
    pub safe_selectors: KapellmeisterSafeSelectors,
}

impl Default for KapellmeisterToolAuthority {
    fn default() -> Self {
        Self {
            mode: KapellmeisterAuthorityMode::ShadowOnly,
            read_only_tools: true,
            safe_selectors: KapellmeisterSafeSelectors::default(),
        }
    }
}

impl KapellmeisterToolAuthority {
    pub fn from_config(config: KapellmeisterAuthorityConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            read_only_tools: matches!(config.mode, KapellmeisterAuthorityMode::ShadowOnly),
            mode: config.mode,
            safe_selectors: config.safe_selectors,
        })
    }

    pub fn permits(&self, operation: FugueToolOperation) -> bool {
        match self.mode {
            KapellmeisterAuthorityMode::ShadowOnly => self.read_only_tools && !operation.mutates(),
            KapellmeisterAuthorityMode::GuardedLabeled => matches!(
                operation,
                FugueToolOperation::LinearInspectIssue
                    | FugueToolOperation::LinearListProjectStates
                    | FugueToolOperation::LinearListRecentIssueChanges
                    | FugueToolOperation::LinearClaimIssue
                    | FugueToolOperation::LinearMoveIssue
                    | FugueToolOperation::LinearUpsertWorkpad
                    | FugueToolOperation::MobSpawnOrResumeExecution
                    | FugueToolOperation::GitHubMergePr
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterDecisionRequest {
    pub request_id: ShadowDecisionId,
    pub requested_at: DateTime<Utc>,
    pub authority: KapellmeisterAuthorityConfig,
    pub context: KapellmeisterDecisionContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_action: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KapellmeisterDecisionContext {
    pub issue: Option<LinearIssueRef>,
    pub repository: Option<GitHubRepoRef>,
    pub linear_labels: Vec<String>,
    pub workspace_key: Option<WorkspaceKey>,
    pub observations: Vec<KapellmeisterHostObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KapellmeisterHostObservation {
    LinearIssueObserved {
        issue: LinearIssueRef,
        state: LinearStateRef,
        labels: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterDecisionResponse {
    pub request_id: ShadowDecisionId,
    pub decided_at: DateTime<Utc>,
    pub decision: KapellmeisterHostDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
}

#[async_trait]
pub trait KapellmeisterDecisionRuntime {
    async fn decide(
        &mut self,
        request: &KapellmeisterDecisionRequest,
    ) -> Result<KapellmeisterDecisionResponse>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum KapellmeisterHostDecision {
    Claim {
        input: crate::tools::ClaimIssueInput,
    },
    Move {
        input: crate::tools::MoveIssueInput,
    },
    WorkpadUpdate {
        input: UpsertWorkpadInput,
    },
    WorkerStart {
        input: crate::tools::SpawnOrResumeExecutionInput,
    },
    FinalPrGateStart {
        request: FinalPrGateStartRequest,
    },
    IndependentReviewStart {
        request: IndependentReviewStartRequest,
    },
    Merge {
        input: MergePrInput,
    },
    ReviewRequest {
        issue: LinearIssueRef,
        reason: String,
    },
    Stop {
        reason: String,
    },
    Noop {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterRepoScope {
    pub workspace_key: WorkspaceKey,
    pub root: PathBuf,
}

impl KapellmeisterRepoScope {
    pub fn new(workspace_key: WorkspaceKey, root: PathBuf) -> Self {
        Self {
            workspace_key,
            root,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterShadowSessionState {
    pub repo_scope: KapellmeisterRepoScope,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub durable_cursors: Vec<DurableCursorPosition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delivered_signal_ids: Vec<ShadowSignalId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shadow_observations: Vec<KapellmeisterShadowObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommendations: Vec<KapellmeisterShadowRecommendation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decision_comparisons: Vec<KapellmeisterDecisionComparison>,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resumed_at: Option<DateTime<Utc>>,
    pub authority: KapellmeisterToolAuthority,
}

impl KapellmeisterShadowSessionState {
    fn from_start(
        repo_scope: KapellmeisterRepoScope,
        authority: KapellmeisterToolAuthority,
        started: KapellmeisterShadowSessionStart,
    ) -> Self {
        Self {
            repo_scope,
            session_id: started.session_id,
            session_ref: started.session_ref,
            stream_id: started.stream_id,
            durable_cursors: started.durable_cursors,
            delivered_signal_ids: Vec::new(),
            shadow_observations: Vec::new(),
            recommendations: Vec::new(),
            decision_comparisons: Vec::new(),
            started_at: Utc::now(),
            resumed_at: None,
            authority,
        }
    }

    fn apply_resume(&mut self, resumed: KapellmeisterShadowSessionStart) {
        self.session_id = resumed.session_id;
        self.session_ref = resumed.session_ref.or_else(|| self.session_ref.clone());
        self.stream_id = resumed.stream_id;
        if !resumed.durable_cursors.is_empty() {
            self.durable_cursors = resumed.durable_cursors;
        }
        self.resumed_at = Some(Utc::now());
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KapellmeisterShadowInspection {
    pub enabled: bool,
    pub authority: KapellmeisterToolAuthority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resumed_at: Option<DateTime<Utc>>,
    pub durable_cursor_count: usize,
    pub delivered_signal_count: usize,
    pub observation_count: usize,
    pub recommendation_count: usize,
    pub comparison_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_comparison: Option<KapellmeisterDecisionComparison>,
    pub health: KapellmeisterShadowHealth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KapellmeisterShadowHealth {
    Disabled,
    NoState,
    Observing,
    HasRecommendations,
    HasComparisons,
}

impl KapellmeisterShadowInspection {
    pub fn disabled(authority: KapellmeisterToolAuthority) -> Self {
        Self {
            enabled: false,
            authority,
            state_path: None,
            session_id: None,
            session_ref: None,
            resumed_at: None,
            durable_cursor_count: 0,
            delivered_signal_count: 0,
            observation_count: 0,
            recommendation_count: 0,
            comparison_count: 0,
            last_comparison: None,
            health: KapellmeisterShadowHealth::Disabled,
        }
    }

    pub fn no_state(path: PathBuf, authority: KapellmeisterToolAuthority) -> Self {
        Self {
            enabled: true,
            authority,
            state_path: Some(path),
            session_id: None,
            session_ref: None,
            resumed_at: None,
            durable_cursor_count: 0,
            delivered_signal_count: 0,
            observation_count: 0,
            recommendation_count: 0,
            comparison_count: 0,
            last_comparison: None,
            health: KapellmeisterShadowHealth::NoState,
        }
    }

    pub fn from_state(path: PathBuf, state: &KapellmeisterShadowSessionState) -> Self {
        let health = if !state.decision_comparisons.is_empty() {
            KapellmeisterShadowHealth::HasComparisons
        } else if !state.recommendations.is_empty() {
            KapellmeisterShadowHealth::HasRecommendations
        } else {
            KapellmeisterShadowHealth::Observing
        };
        Self {
            enabled: true,
            authority: state.authority.clone(),
            state_path: Some(path),
            session_id: Some(state.session_id.clone()),
            session_ref: state.session_ref.clone(),
            resumed_at: state.resumed_at,
            durable_cursor_count: state.durable_cursors.len(),
            delivered_signal_count: state.delivered_signal_ids.len(),
            observation_count: state.shadow_observations.len(),
            recommendation_count: state.recommendations.len(),
            comparison_count: state.decision_comparisons.len(),
            last_comparison: state.decision_comparisons.last().cloned(),
            health,
        }
    }
}

pub fn inspect_shadow_state(
    config: &KapellmeisterShadowModeConfig,
) -> Result<KapellmeisterShadowInspection> {
    if !config.enabled {
        return Ok(KapellmeisterShadowInspection::disabled(
            config.authority.clone(),
        ));
    }
    match read_shadow_state(&config.state_path)? {
        Some(state) => Ok(KapellmeisterShadowInspection::from_state(
            config.state_path.clone(),
            &state,
        )),
        None => Ok(KapellmeisterShadowInspection::no_state(
            config.state_path.clone(),
            config.authority.clone(),
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KapellmeisterShadowSessionRequest {
    pub repo_scope: KapellmeisterRepoScope,
    pub prompt: String,
    pub authority: KapellmeisterToolAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KapellmeisterShadowSessionStart {
    pub session_id: String,
    pub session_ref: Option<String>,
    pub stream_id: Option<String>,
    pub durable_cursors: Vec<DurableCursorPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KapellmeisterShadowHostStatus {
    Disabled,
    Started(KapellmeisterShadowSessionState),
    Resumed(KapellmeisterShadowSessionState),
}

#[async_trait]
pub trait KapellmeisterShadowRuntime {
    async fn start_shadow_session(
        &mut self,
        request: &KapellmeisterShadowSessionRequest,
    ) -> Result<KapellmeisterShadowSessionStart>;

    async fn resume_shadow_session(
        &mut self,
        existing: &KapellmeisterShadowSessionState,
        request: &KapellmeisterShadowSessionRequest,
    ) -> Result<KapellmeisterShadowSessionStart>;

    async fn deliver_shadow_signals(
        &mut self,
        _existing: &KapellmeisterShadowSessionState,
        batch: &KapellmeisterSignalDeliveryBatch,
    ) -> Result<KapellmeisterSignalDeliveryReceipt> {
        Ok(KapellmeisterSignalDeliveryReceipt::delivered_all(
            &batch.signals,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct KapellmeisterShadowHost {
    config: KapellmeisterShadowModeConfig,
    repo_scope: KapellmeisterRepoScope,
}

impl KapellmeisterShadowHost {
    pub fn new(config: KapellmeisterShadowModeConfig, repo_scope: KapellmeisterRepoScope) -> Self {
        Self { config, repo_scope }
    }

    pub async fn reconcile<R: KapellmeisterShadowRuntime + Send>(
        &self,
        runtime: &mut R,
    ) -> Result<KapellmeisterShadowHostStatus> {
        if !self.config.enabled {
            return Ok(KapellmeisterShadowHostStatus::Disabled);
        }
        let request = KapellmeisterShadowSessionRequest {
            repo_scope: self.repo_scope.clone(),
            prompt: self.config.prompt.clone(),
            authority: self.config.authority.clone(),
        };
        if let Some(mut existing) = read_shadow_state(&self.config.state_path)?
            && existing.repo_scope == self.repo_scope
            && !existing.session_id.trim().is_empty()
        {
            let resumed = runtime.resume_shadow_session(&existing, &request).await?;
            existing.apply_resume(resumed);
            existing.authority = self.config.authority.clone();
            write_shadow_state(&self.config.state_path, &existing)?;
            return Ok(KapellmeisterShadowHostStatus::Resumed(existing));
        }
        let started = runtime.start_shadow_session(&request).await?;
        let state = KapellmeisterShadowSessionState::from_start(
            self.repo_scope.clone(),
            self.config.authority.clone(),
            started,
        );
        write_shadow_state(&self.config.state_path, &state)?;
        Ok(KapellmeisterShadowHostStatus::Started(state))
    }

    pub async fn deliver_signals<R: KapellmeisterShadowRuntime + Send>(
        &self,
        runtime: &mut R,
        signals: Vec<KapellmeisterShadowSignal>,
        workpad_preview: Option<KapellmeisterWorkpadPreview>,
    ) -> Result<KapellmeisterSignalDeliveryOutcome> {
        if !self.config.enabled {
            return Ok(KapellmeisterSignalDeliveryOutcome {
                session_available: false,
                delivered_signal_ids: Vec::new(),
                skipped_duplicate_signal_ids: signals.into_iter().map(|signal| signal.id).collect(),
                durable_cursors: Vec::new(),
                recommendations: Vec::new(),
                decision_comparisons: Vec::new(),
            });
        }
        let Some(mut existing) = read_shadow_state(&self.config.state_path)? else {
            return Ok(KapellmeisterSignalDeliveryOutcome {
                session_available: false,
                delivered_signal_ids: Vec::new(),
                skipped_duplicate_signal_ids: signals.into_iter().map(|signal| signal.id).collect(),
                durable_cursors: Vec::new(),
                recommendations: Vec::new(),
                decision_comparisons: Vec::new(),
            });
        };
        let mut skipped_duplicate_signal_ids = Vec::new();
        let mut pending = Vec::new();
        for signal in signals {
            if existing.delivered_signal_ids.contains(&signal.id) {
                existing
                    .shadow_observations
                    .push(KapellmeisterShadowObservation {
                        signal_id: signal.id.clone(),
                        observed_at: Utc::now(),
                        status: KapellmeisterShadowObservationStatus::DuplicateSkipped,
                        note: Some(
                            "signal id was already delivered to Kapellmeister shadow".to_string(),
                        ),
                    });
                skipped_duplicate_signal_ids.push(signal.id);
            } else {
                pending.push(signal);
            }
        }
        if pending.is_empty() {
            truncate_observations(&mut existing.shadow_observations);
            write_shadow_state(&self.config.state_path, &existing)?;
            return Ok(KapellmeisterSignalDeliveryOutcome {
                session_available: true,
                delivered_signal_ids: Vec::new(),
                skipped_duplicate_signal_ids,
                durable_cursors: existing.durable_cursors,
                recommendations: Vec::new(),
                decision_comparisons: Vec::new(),
            });
        }
        let batch = KapellmeisterSignalDeliveryBatch {
            signals: pending,
            workpad_preview,
        };
        let receipt = match runtime.deliver_shadow_signals(&existing, &batch).await {
            Ok(receipt) => receipt,
            Err(err) if is_rkat_session_not_found(&err) => {
                warn!(
                    session_id = %existing.session_id,
                    "kapellmeister shadow delivery session missing; starting a fresh session and retrying delivery once"
                );
                let request = KapellmeisterShadowSessionRequest {
                    repo_scope: self.repo_scope.clone(),
                    prompt: self.config.prompt.clone(),
                    authority: self.config.authority.clone(),
                };
                let previous = existing.clone();
                let started = runtime.start_shadow_session(&request).await?;
                let mut recovered = KapellmeisterShadowSessionState::from_start(
                    self.repo_scope.clone(),
                    self.config.authority.clone(),
                    started,
                );
                for cursor in previous.durable_cursors {
                    advance_cursor(&mut recovered.durable_cursors, cursor);
                }
                recovered.delivered_signal_ids = previous.delivered_signal_ids;
                recovered.shadow_observations = previous.shadow_observations;
                recovered.recommendations = previous.recommendations;
                recovered.decision_comparisons = previous.decision_comparisons;
                existing = recovered;
                write_shadow_state(&self.config.state_path, &existing)?;
                runtime.deliver_shadow_signals(&existing, &batch).await?
            }
            Err(err) => return Err(err),
        };
        let mut delivered_signal_ids = Vec::new();
        for signal_id in &receipt.delivered_signal_ids {
            if !existing.delivered_signal_ids.contains(signal_id) {
                existing.delivered_signal_ids.push(signal_id.clone());
            }
            delivered_signal_ids.push(signal_id.clone());
        }
        let delivered_lookup = delivered_signal_ids.iter().collect::<Vec<_>>();
        for signal in &batch.signals {
            if delivered_lookup.iter().any(|id| **id == signal.id) {
                if let Some(cursor) = &signal.source_cursor {
                    advance_cursor(&mut existing.durable_cursors, cursor.clone());
                }
                let observation = KapellmeisterShadowObservation {
                    signal_id: signal.id.clone(),
                    observed_at: Utc::now(),
                    status: KapellmeisterShadowObservationStatus::Delivered,
                    note: Some(
                        "shadow observation recorded without production mutation".to_string(),
                    ),
                };
                info!(signal_id = %signal.id.0, "kapellmeister shadow signal delivered");
                existing.shadow_observations.push(observation);
            }
        }
        existing.shadow_observations.extend(receipt.observations);
        let recommendations = receipt.recommendations;
        let comparisons =
            compare_recommendations_with_delivered_signals(&recommendations, &batch.signals);
        existing.recommendations.extend(recommendations.clone());
        existing.decision_comparisons.extend(comparisons.clone());
        truncate_recommendations(&mut existing.recommendations);
        truncate_comparisons(&mut existing.decision_comparisons);
        truncate_observations(&mut existing.shadow_observations);
        write_shadow_state(&self.config.state_path, &existing)?;
        Ok(KapellmeisterSignalDeliveryOutcome {
            session_available: true,
            delivered_signal_ids,
            skipped_duplicate_signal_ids,
            durable_cursors: existing.durable_cursors,
            recommendations,
            decision_comparisons: comparisons,
        })
    }
}

fn compare_recommendations_with_delivered_signals(
    recommendations: &[KapellmeisterShadowRecommendation],
    signals: &[KapellmeisterShadowSignal],
) -> Vec<KapellmeisterDecisionComparison> {
    recommendations
        .iter()
        .filter_map(|recommendation| {
            rust_decision_from_recommendation_signals(recommendation, signals).map(|decision| {
                KapellmeisterDecisionComparison::compare(recommendation, decision, Utc::now())
            })
        })
        .collect()
}

fn rust_decision_from_recommendation_signals(
    recommendation: &KapellmeisterShadowRecommendation,
    signals: &[KapellmeisterShadowSignal],
) -> Option<RustAuthoritativeDecision> {
    recommendation
        .based_on_signals
        .iter()
        .filter_map(|signal_id| signals.iter().find(|signal| signal.id == *signal_id))
        .filter_map(rust_decision_from_signal)
        .next()
}

fn rust_decision_from_signal(
    signal: &KapellmeisterShadowSignal,
) -> Option<RustAuthoritativeDecision> {
    match &signal.kind {
        KapellmeisterShadowSignalKind::WorkerStarted {
            issue,
            workspace_key,
            ..
        } => Some(RustAuthoritativeDecision {
            id: ShadowDecisionId(format!("rust-{}", signal.id.0)),
            decided_at: signal.observed_at,
            action: RustAuthoritativeAction::DispatchWorker {
                issue: issue.clone(),
                workspace_key: workspace_key.clone(),
            },
        }),
        KapellmeisterShadowSignalKind::WorkerFailed {
            issue,
            workspace_key,
            failure,
        } if failure.retry_scheduled => Some(RustAuthoritativeDecision {
            id: ShadowDecisionId(format!("rust-{}", signal.id.0)),
            decided_at: signal.observed_at,
            action: RustAuthoritativeAction::RetryWorker {
                issue: issue.clone(),
                workspace_key: workspace_key.clone(),
                after_seconds: 0,
            },
        }),
        KapellmeisterShadowSignalKind::WorkerFailed { issue, failure, .. } => {
            Some(RustAuthoritativeDecision {
                id: ShadowDecisionId(format!("rust-{}", signal.id.0)),
                decided_at: signal.observed_at,
                action: RustAuthoritativeAction::TerminateWorker {
                    issue: issue.clone(),
                    reason: format!("worker failed without retry: {:?}", failure.reason),
                },
            })
        }
        KapellmeisterShadowSignalKind::WorkerFinished { issue, outcome, .. } => {
            matches!(outcome, ShadowWorkerOutcome::Succeeded).then(|| RustAuthoritativeDecision {
                id: ShadowDecisionId(format!("rust-{}", signal.id.0)),
                decided_at: signal.observed_at,
                action: RustAuthoritativeAction::ContinueWorker {
                    issue: issue.clone(),
                },
            })
        }
        _ => None,
    }
}

fn advance_cursor(cursors: &mut Vec<DurableCursorPosition>, candidate: DurableCursorPosition) {
    let mut by_key = cursors
        .drain(..)
        .map(|cursor| (cursor.key.clone(), cursor))
        .collect::<BTreeMap<_, _>>();
    by_key
        .entry(candidate.key.clone())
        .and_modify(|current| {
            if candidate.observed_at >= current.observed_at {
                *current = candidate.clone();
            }
        })
        .or_insert(candidate);
    *cursors = by_key.into_values().collect();
}

fn truncate_observations(observations: &mut Vec<KapellmeisterShadowObservation>) {
    const MAX_OBSERVATIONS: usize = 200;
    if observations.len() > MAX_OBSERVATIONS {
        let excess = observations.len() - MAX_OBSERVATIONS;
        observations.drain(0..excess);
    }
}

fn truncate_recommendations(recommendations: &mut Vec<KapellmeisterShadowRecommendation>) {
    const MAX_RECOMMENDATIONS: usize = 200;
    if recommendations.len() > MAX_RECOMMENDATIONS {
        let excess = recommendations.len() - MAX_RECOMMENDATIONS;
        recommendations.drain(0..excess);
    }
}

fn truncate_comparisons(comparisons: &mut Vec<KapellmeisterDecisionComparison>) {
    const MAX_COMPARISONS: usize = 200;
    if comparisons.len() > MAX_COMPARISONS {
        let excess = comparisons.len() - MAX_COMPARISONS;
        comparisons.drain(0..excess);
    }
}

pub struct RkatKapellmeisterShadowRuntime {
    meerkat: MeerkatConfig,
    tracker: TrackerConfig,
    workspace: PathBuf,
}

impl RkatKapellmeisterShadowRuntime {
    pub fn new(meerkat: MeerkatConfig, tracker: TrackerConfig, workspace: PathBuf) -> Self {
        Self {
            meerkat,
            tracker,
            workspace,
        }
    }

    async fn with_client(&self) -> Result<KapellmeisterRpcClient> {
        KapellmeisterRpcClient::start(&self.meerkat, &self.workspace).await
    }
}

pub async fn send_kapellmeister_console_turn(
    meerkat: &MeerkatConfig,
    workspace: &Path,
    session_id: &str,
    content: &str,
    state_path: Option<&Path>,
) -> Result<Value> {
    let mut client = KapellmeisterRpcClient::start(meerkat, workspace).await?;
    let shadow_meerkat = read_only_shadow_meerkat_config(meerkat);
    let params =
        shadow_meerkat.turn_start_params(session_id.to_string(), content.to_string(), false, true);
    let result = match client
        .call(
            "turn/start",
            params,
            Duration::from_millis(shadow_meerkat.turn_timeout_ms),
        )
        .await
    {
        Ok(turn_result) => Ok(turn_result),
        Err(error) if is_rkat_session_not_found(&error) => {
            warn!(
                session_id,
                "Kapellmeister console session missing; creating a fresh one-shot console session"
            );
            let mut params = shadow_meerkat.create_session_params(content.to_string(), None);
            if let Some(object) = params.as_object_mut() {
                object.insert(
                    "initial_turn".to_string(),
                    Value::String("run_immediately".to_string()),
                );
                object.insert(
                    "peer_meta".to_string(),
                    json!({
                        "fugue_role": "kapellmeister_console",
                        "workspace_key": "repo",
                        "authority": "shadow_only"
                    }),
                );
            }
            let created = client
                .call(
                    "session/create",
                    params,
                    Duration::from_millis(shadow_meerkat.turn_timeout_ms),
                )
                .await?;
            if let Some(path) = state_path
                && let Some(new_session_id) = created.get("session_id").and_then(Value::as_str)
                && let Some(mut state) = read_shadow_state(path)?
            {
                state.session_id = new_session_id.to_string();
                state.session_ref = created
                    .get("session_ref")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .or(state.session_ref);
                state.stream_id = None;
                state.resumed_at = Some(Utc::now());
                write_shadow_state(path, &state)?;
            }
            Ok(created)
        }
        Err(error) => Err(error),
    };
    client.stop().await;
    result
}

pub async fn read_kapellmeister_session_history(
    meerkat: &MeerkatConfig,
    workspace: &Path,
    session_id: &str,
    offset: usize,
    limit: Option<usize>,
) -> Result<Value> {
    let mut client = KapellmeisterRpcClient::start(meerkat, workspace).await?;
    let result = client
        .call(
            "session/history",
            json!({
                "session_id": session_id,
                "offset": offset,
                "limit": limit,
            }),
            Duration::from_millis(meerkat.read_timeout_ms),
        )
        .await;
    client.stop().await;
    result
}

#[async_trait]
impl KapellmeisterDecisionRuntime for RkatKapellmeisterShadowRuntime {
    async fn decide(
        &mut self,
        request: &KapellmeisterDecisionRequest,
    ) -> Result<KapellmeisterDecisionResponse> {
        let mut client = self.with_client().await?;
        if !client
            .methods
            .iter()
            .any(|method| method == "session/create")
            || !client.methods.iter().any(|method| method == "turn/start")
        {
            return Err(FugueError::ResponseError(
                "rkat-rpc missing required session/create or turn/start for Kapellmeister decision"
                    .to_string(),
            ));
        }
        let (decision_meerkat, params) =
            kapellmeister_decision_session_params(&self.meerkat, request)?;
        let turn_result = client
            .call(
                "session/create",
                params,
                Duration::from_millis(decision_meerkat.turn_timeout_ms),
            )
            .await?;
        client.stop().await;
        parse_decision_response_for_request(&turn_result, &request.request_id)
    }
}

#[async_trait]
impl KapellmeisterShadowRuntime for RkatKapellmeisterShadowRuntime {
    async fn start_shadow_session(
        &mut self,
        request: &KapellmeisterShadowSessionRequest,
    ) -> Result<KapellmeisterShadowSessionStart> {
        let mut client = self.with_client().await?;
        let shadow_meerkat = read_only_shadow_meerkat_config(&self.meerkat);
        let mut params = shadow_meerkat.create_session_params(
            request.prompt.clone(),
            inert_shadow_external_tools(&self.tracker),
        );
        if let Some(object) = params.as_object_mut() {
            object.insert(
                "peer_meta".to_string(),
                json!({
                    "fugue_role": "kapellmeister_shadow",
                    "workspace_key": request.repo_scope.workspace_key.0,
                    "authority": "shadow_only"
                }),
            );
        }
        let created = client
            .call(
                "session/create",
                params,
                Duration::from_millis(shadow_meerkat.read_timeout_ms),
            )
            .await?;
        let session_id = string_field(&created, "session_id")?;
        let session_ref = created
            .get("session_ref")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        client.stop().await;
        Ok(KapellmeisterShadowSessionStart {
            session_id,
            session_ref,
            stream_id: None,
            durable_cursors: Vec::new(),
        })
    }

    async fn resume_shadow_session(
        &mut self,
        existing: &KapellmeisterShadowSessionState,
        request: &KapellmeisterShadowSessionRequest,
    ) -> Result<KapellmeisterShadowSessionStart> {
        let mut client = self.with_client().await?;
        if client.methods.iter().any(|method| method == "session/read") {
            let read_result = client
                .call(
                    "session/read",
                    json!({ "session_id": existing.session_id }),
                    Duration::from_millis(self.meerkat.read_timeout_ms),
                )
                .await;
            match read_result {
                Ok(_) => {
                    client.stop().await;
                    return Ok(KapellmeisterShadowSessionStart {
                        session_id: existing.session_id.clone(),
                        session_ref: existing.session_ref.clone(),
                        stream_id: None,
                        durable_cursors: existing.durable_cursors.clone(),
                    });
                }
                Err(err) if is_rkat_session_not_found(&err) => {
                    warn!(
                        session_id = %existing.session_id,
                        "kapellmeister shadow session missing from Meerkat runtime; starting a fresh session"
                    );
                    client.stop().await;
                    return self.start_shadow_session(request).await;
                }
                Err(err) => {
                    client.stop().await;
                    return Err(err);
                }
            }
        }
        client.stop().await;
        Ok(KapellmeisterShadowSessionStart {
            session_id: existing.session_id.clone(),
            session_ref: existing.session_ref.clone(),
            stream_id: None,
            durable_cursors: existing.durable_cursors.clone(),
        })
    }

    async fn deliver_shadow_signals(
        &mut self,
        existing: &KapellmeisterShadowSessionState,
        batch: &KapellmeisterSignalDeliveryBatch,
    ) -> Result<KapellmeisterSignalDeliveryReceipt> {
        let mut client = self.with_client().await?;
        if !client.methods.iter().any(|method| method == "turn/start") {
            return Err(FugueError::ResponseError(
                "rkat-rpc missing required turn/start for shadow signal delivery".to_string(),
            ));
        }
        let shadow_meerkat = read_only_shadow_meerkat_config(&self.meerkat);
        let prompt = kapellmeister_recommendation_prompt(batch)?;
        let params = shadow_meerkat.turn_start_params(
            existing.session_id.clone(),
            prompt.clone(),
            false,
            true,
        );
        let turn_result = match client
            .call(
                "turn/start",
                params,
                Duration::from_millis(shadow_meerkat.turn_timeout_ms),
            )
            .await
        {
            Ok(turn_result) => turn_result,
            Err(err) if is_rkat_session_not_found(&err) => {
                warn!(
                    session_id = %existing.session_id,
                    "kapellmeister shadow delivery session missing inside runtime; using one-shot session/create delivery"
                );
                let mut params = shadow_meerkat
                    .create_session_params(prompt, inert_shadow_external_tools(&self.tracker));
                if let Some(object) = params.as_object_mut() {
                    object.insert(
                        "initial_turn".to_string(),
                        Value::String("run_immediately".to_string()),
                    );
                    object.insert(
                        "peer_meta".to_string(),
                        json!({
                            "fugue_role": "kapellmeister_shadow_delivery",
                            "workspace_key": existing.repo_scope.workspace_key.0,
                            "authority": "shadow_only"
                        }),
                    );
                }
                client
                    .call(
                        "session/create",
                        params,
                        Duration::from_millis(shadow_meerkat.turn_timeout_ms),
                    )
                    .await?
            }
            Err(err) => {
                client.stop().await;
                return Err(err);
            }
        };
        client.stop().await;
        kapellmeister_signal_delivery_receipt_from_turn_result(batch, &turn_result)
    }
}

fn is_rkat_session_not_found(error: &FugueError) -> bool {
    matches!(error, FugueError::ResponseError(message) if
        message.to_ascii_lowercase().contains("session not found")
    )
}

struct KapellmeisterRpcClient {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: u64,
    methods: Vec<String>,
    stopped: bool,
}

impl KapellmeisterRpcClient {
    async fn start(meerkat: &MeerkatConfig, workspace: &Path) -> Result<Self> {
        let command_line = meerkat.command_line();
        let mut child = Command::new("bash")
            .arg("-lc")
            .arg(&command_line)
            .current_dir(workspace)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|err| FugueError::MeerkatRpcNotFound(format!("{command_line}: {err}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| FugueError::ProcessExit("rkat-rpc stdin unavailable".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| FugueError::ProcessExit("rkat-rpc stdout unavailable".to_string()))?;
        let mut client = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            next_id: 1,
            methods: Vec::new(),
            stopped: false,
        };
        let init = client
            .call(
                "initialize",
                json!({}),
                Duration::from_millis(meerkat.read_timeout_ms),
            )
            .await?;
        client.methods = init
            .get("methods")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                FugueError::ResponseError("initialize result did not include methods".to_string())
            })?
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect();
        if !client
            .methods
            .iter()
            .any(|method| method == "session/create")
        {
            return Err(FugueError::ResponseError(
                "rkat-rpc missing required session/create".to_string(),
            ));
        }
        Ok(client)
    }

    async fn call(&mut self, method: &str, params: Value, timeout: Duration) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let mut encoded = serde_json::to_vec(&request)
            .map_err(|err| FugueError::ResponseError(err.to_string()))?;
        encoded.push(b'\n');
        self.stdin
            .write_all(&encoded)
            .await
            .map_err(|err| FugueError::ProcessExit(format!("write failed: {err}")))?;
        self.stdin
            .flush()
            .await
            .map_err(|err| FugueError::ProcessExit(format!("flush failed: {err}")))?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or_else(|| {
                    FugueError::ResponseTimeout(format!("{method} timed out after {timeout:?}"))
                })?;
            let line = tokio::time::timeout(remaining, self.stdout.next_line())
                .await
                .map_err(|_| {
                    FugueError::ResponseTimeout(format!("{method} timed out after {timeout:?}"))
                })?
                .map_err(|err| FugueError::ProcessExit(format!("read failed: {err}")))?
                .ok_or_else(|| FugueError::ProcessExit("rkat-rpc stdout closed".to_string()))?;
            let value: Value = serde_json::from_str(&line)
                .map_err(|err| FugueError::ResponseError(format!("malformed response: {err}")))?;
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = value.get("error") {
                return Err(FugueError::ResponseError(error.to_string()));
            }
            return value
                .get("result")
                .cloned()
                .ok_or_else(|| FugueError::ResponseError(format!("{method} missing result")));
        }
    }

    async fn stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

impl Drop for KapellmeisterRpcClient {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.child.start_kill();
        }
    }
}

fn string_field(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| FugueError::ResponseError(format!("response missing {field}")))
}

fn kapellmeister_recommendation_prompt(batch: &KapellmeisterSignalDeliveryBatch) -> Result<String> {
    let payload = serde_json::to_string_pretty(batch)
        .map_err(|err| FugueError::ResponseError(err.to_string()))?;
    Ok(format!(
        "Observe these normalized Fugue signals in shadow mode only. \
         Do not mutate Linear, GitHub, workspaces, or production state. \
         Return only a JSON object matching this shape: \
         {{\"recommendations\":[KapellmeisterShadowRecommendation,...]}}. \
         Each recommendation must use the typed snake_case action variants from the payload contract, \
         include based_on_signals ids when it responds to a delivered signal, and remain advisory evidence only.\n\n{payload}"
    ))
}

fn kapellmeister_decision_prompt(request: &KapellmeisterDecisionRequest) -> Result<String> {
    let payload = serde_json::to_string_pretty(request)
        .map_err(|err| FugueError::ResponseError(err.to_string()))?;
    Ok(format!(
        "You are Kapellmeister selecting the next guarded Fugue host action for one safe selected issue. \
         Return only a JSON object matching KapellmeisterDecisionResponse. \
         Choose exactly one typed host decision: claim, move, workpad_update, worker_start, final_pr_gate_start, independent_review_start, review_request, stop, or noop. \
         If requested_action asks for a Linear lifecycle projection, choose a move decision for the named issue and target lane unless the action is unsafe. \
         The guarded host executor will validate selectors, issue scope, allowed tools, and idempotency before applying your decision. \
         You have Meerkat mob tooling available for repo-level delegation semantics: delegate/mob_create on the agent side and mob_run_flow inside mobs. \
         Treat context.observations as Fugue host observations, not host-owned lifecycle decisions. \
         For a newly observed configured intake or remediation issue, choose worker_start when the next canonical step should be to start or resume the configured issue mob; the issue mob planner owns the detailed Workpad plan after worker_start. Use the configured intake and running projection lanes; once the mob accepts the signal, the issue lead should project work to the configured running lane and record detailed phase as labels/evidence/runtime state. \
         If an observation reports the configured running lane, treat it as resumable active work and choose worker_start unless a terminal/pause/blocker observation makes that unsafe. \
         Never choose noop merely because you are waiting for an external flow step; Kapellmeister owns the repo-level decision to start, resume, replan, pause, or ask for human help. \
         If an observation reports a human lane change, decide whether it means pause, cancel, replan, or resume before selecting a host command. \
         Use workpad_update only for repo-agent evidence such as PR, check, merge, deploy, or reviewer feedback handoff; do not perform planner/coder/reviewer issue work directly in the repo decision turn. \
         If you choose worker_start, authorize the host mechanism to start the configured native issue mob; do not sequence planner/coder/reviewer turns yourself. If an observation or signal contains a FinalPrGateStartRequest, choose final_pr_gate_start with that exact typed request so the guarded Kapellmeister host executor starts or resumes the repo-level final gate; do not merge or run final reviewers directly in this decision turn. If an observation reports the configured Review action lane and you have PR evidence, choose independent_review_start with the exact typed IndependentReviewStartRequest so the host starts the review orchestrator mob; the review orchestrator chooses and spawns exactly three reviewers. If an observation reports the configured Deploy lane and you have verified PR, CI, WorkGraph, and deploy policy evidence, use the fugue_run_deploy tool rather than worker_start.\n\n{payload}"
    ))
}

fn kapellmeister_signal_delivery_receipt_from_turn_result(
    batch: &KapellmeisterSignalDeliveryBatch,
    turn_result: &Value,
) -> Result<KapellmeisterSignalDeliveryReceipt> {
    let response = parse_shadow_recommendation_response(turn_result)?;
    let mut receipt = KapellmeisterSignalDeliveryReceipt::delivered_all(&batch.signals);
    receipt.recommendations = response.recommendations;
    Ok(receipt)
}

fn parse_shadow_recommendation_response(
    value: &Value,
) -> Result<KapellmeisterShadowRecommendationResponse> {
    for candidate in shadow_recommendation_response_candidates(value) {
        if let Ok(response) =
            serde_json::from_value::<KapellmeisterShadowRecommendationResponse>(candidate.clone())
        {
            return Ok(response);
        }
        if let Some(response) = parse_flexible_shadow_recommendation_response(&candidate)? {
            return Ok(response);
        }
    }
    Err(FugueError::ResponseError(format!(
        "turn/start did not return typed kapellmeister recommendations: {value}"
    )))
}

fn parse_flexible_shadow_recommendation_response(
    value: &Value,
) -> Result<Option<KapellmeisterShadowRecommendationResponse>> {
    let Some(items) = value.get("recommendations").and_then(Value::as_array) else {
        return Ok(None);
    };
    let recommendations = items
        .iter()
        .enumerate()
        .map(|(index, item)| parse_flexible_shadow_recommendation(index, item))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(KapellmeisterShadowRecommendationResponse {
        recommendations,
    }))
}

fn parse_flexible_shadow_recommendation(
    index: usize,
    value: &Value,
) -> Result<KapellmeisterShadowRecommendation> {
    let object = value.as_object().ok_or_else(|| {
        FugueError::ResponseError("Kapellmeister recommendation must be object".to_string())
    })?;
    let based_on_signals = object
        .get("based_on_signals")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|id| ShadowSignalId(id.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let id = flexible_string(value, &["id", "recommendation_id"])
        .map(ShadowRecommendationId)
        .or_else(|| {
            based_on_signals
                .first()
                .map(|signal_id| ShadowRecommendationId(format!("rec-{}", signal_id.0)))
        })
        .unwrap_or_else(|| ShadowRecommendationId(format!("rec-flexible-{index}")));
    let generated_at = object
        .get("generated_at")
        .and_then(Value::as_str)
        .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);
    let confidence = object
        .get("confidence")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or(RecommendationConfidence::Medium);
    let action = parse_flexible_recommended_action(object.get("action").unwrap_or(value), value)?;
    Ok(KapellmeisterShadowRecommendation {
        id,
        generated_at,
        based_on_signals,
        action,
        confidence,
        rationale: flexible_reason(value, "Kapellmeister recommendation"),
    })
}

fn parse_flexible_recommended_action(
    action_value: &Value,
    recommendation_value: &Value,
) -> Result<KapellmeisterRecommendedAction> {
    if let Ok(action) =
        serde_json::from_value::<KapellmeisterRecommendedAction>(action_value.clone())
    {
        return Ok(action);
    }
    let action_kind = action_value
        .get("action")
        .or_else(|| action_value.get("type"))
        .or_else(|| action_value.get("kind"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            FugueError::ResponseError(
                "Kapellmeister recommendation action missing action/type/kind".to_string(),
            )
        })?;
    let payload = action_value.get("input").unwrap_or(action_value);
    let issue_value = payload
        .get("issue")
        .or_else(|| recommendation_value.get("issue"));
    let workspace_key = || {
        flexible_string(payload, &["workspace_key"])
            .or_else(|| flexible_string(recommendation_value, &["workspace_key"]))
            .map(WorkspaceKey)
            .ok_or_else(|| {
                FugueError::ResponseError(
                    "Kapellmeister worker recommendation missing workspace_key".to_string(),
                )
            })
    };
    match action_kind {
        "noop" | "no_op" => Ok(KapellmeisterRecommendedAction::Noop),
        "dispatch_worker" | "worker_start" => Ok(KapellmeisterRecommendedAction::DispatchWorker {
            issue: flexible_issue_ref(issue_value)?,
            workspace_key: workspace_key()?,
        }),
        "retry_worker" => Ok(KapellmeisterRecommendedAction::RetryWorker {
            issue: flexible_issue_ref(issue_value)?,
            workspace_key: workspace_key()?,
            after_seconds: payload
                .get("after_seconds")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        }),
        "defer_issue" => Ok(KapellmeisterRecommendedAction::DeferIssue {
            issue: flexible_issue_ref(issue_value)?,
            reason: flexible_reason(payload, "Kapellmeister recommended deferring issue"),
        }),
        "terminate_worker" => Ok(KapellmeisterRecommendedAction::TerminateWorker {
            issue: flexible_issue_ref(issue_value)?,
            reason: flexible_reason(payload, "Kapellmeister recommended terminating worker"),
        }),
        "move_linear_issue" | "move" => Ok(KapellmeisterRecommendedAction::MoveLinearIssue {
            issue: flexible_issue_ref(issue_value)?,
            target_state: payload
                .get("target_state")
                .or_else(|| payload.get("state"))
                .map(flexible_state_ref)
                .transpose()?
                .ok_or_else(|| {
                    FugueError::ResponseError(
                        "Kapellmeister move recommendation missing target_state".to_string(),
                    )
                })?,
        }),
        other => Err(FugueError::ResponseError(format!(
            "unsupported Kapellmeister recommendation action {other}"
        ))),
    }
}

#[cfg(test)]
fn parse_decision_response(value: &Value) -> Result<KapellmeisterDecisionResponse> {
    parse_decision_response_inner(value, None)
}

fn parse_decision_response_for_request(
    value: &Value,
    request_id: &ShadowDecisionId,
) -> Result<KapellmeisterDecisionResponse> {
    parse_decision_response_inner(value, Some(request_id))
}

fn parse_decision_response_inner(
    value: &Value,
    fallback_request_id: Option<&ShadowDecisionId>,
) -> Result<KapellmeisterDecisionResponse> {
    let mut last_flexible_error = None;
    for candidate in decision_response_candidates(value) {
        if let Ok(response) =
            serde_json::from_value::<KapellmeisterDecisionResponse>(candidate.clone())
        {
            return Ok(response);
        }
        match parse_flexible_decision_response(&candidate, fallback_request_id) {
            Ok(Some(response)) => return Ok(response),
            Ok(None) => {}
            Err(err) => last_flexible_error = Some(err.to_string()),
        }
    }
    let detail = last_flexible_error
        .map(|err| format!("; last flexible parser error: {err}"))
        .unwrap_or_default();
    Err(FugueError::ResponseError(format!(
        "Kapellmeister decision turn did not return typed decision response: {value}{detail}"
    )))
}

#[derive(Debug, Deserialize)]
struct FlexibleDecisionResponse {
    #[serde(default)]
    request_id: Option<ShadowDecisionId>,
    #[serde(default)]
    decided_at: Option<DateTime<Utc>>,
    decision: Value,
    #[serde(default)]
    rationale: Option<String>,
}

fn parse_flexible_decision_response(
    value: &Value,
    fallback_request_id: Option<&ShadowDecisionId>,
) -> Result<Option<KapellmeisterDecisionResponse>> {
    let envelope = match serde_json::from_value::<FlexibleDecisionResponse>(value.clone()) {
        Ok(envelope) => envelope,
        Err(_) => return Ok(None),
    };
    let Some(request_id) = envelope.request_id.or_else(|| fallback_request_id.cloned()) else {
        return Ok(None);
    };
    let decision = parse_flexible_host_decision(&request_id, &envelope.decision)?;
    Ok(Some(KapellmeisterDecisionResponse {
        request_id,
        decided_at: envelope.decided_at.unwrap_or_else(Utc::now),
        decision,
        rationale: envelope.rationale,
    }))
}

fn parse_flexible_host_decision(
    request_id: &ShadowDecisionId,
    value: &Value,
) -> Result<KapellmeisterHostDecision> {
    if let Ok(decision) = serde_json::from_value::<KapellmeisterHostDecision>(value.clone()) {
        return Ok(decision);
    }

    let object = value.as_object().ok_or_else(|| {
        FugueError::ResponseError(
            "Kapellmeister flexible decision must be a JSON object".to_string(),
        )
    })?;
    let kind = object
        .get("type")
        .or_else(|| object.get("kind"))
        .or_else(|| object.get("decision"))
        .or_else(|| object.get("action"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            FugueError::ResponseError(
                "Kapellmeister flexible decision missing type/kind/decision/action".to_string(),
            )
        })?;

    let payload = object.get("input").unwrap_or(value);
    match kind {
        "claim" => parse_flexible_claim_decision(request_id, payload),
        "move" => parse_flexible_move_decision(request_id, payload),
        "workpad_update" => parse_flexible_workpad_decision(request_id, payload),
        "worker_start" => parse_flexible_worker_start_decision(request_id, payload),
        "final_pr_gate_start" => parse_flexible_final_pr_gate_start_decision(payload),
        "independent_review_start" => parse_flexible_independent_review_start_decision(payload),
        "review_request" => parse_flexible_review_request_decision(payload),
        "stop" => Ok(KapellmeisterHostDecision::Stop {
            reason: flexible_reason(payload, "Kapellmeister requested stop"),
        }),
        "noop" => Ok(KapellmeisterHostDecision::Noop {
            reason: flexible_reason(payload, "Kapellmeister requested no-op"),
        }),
        other => Err(FugueError::ResponseError(format!(
            "unsupported Kapellmeister flexible decision type {other}"
        ))),
    }
}

fn parse_flexible_claim_decision(
    request_id: &ShadowDecisionId,
    value: &Value,
) -> Result<KapellmeisterHostDecision> {
    if let Ok(input) = serde_json::from_value(value.clone()) {
        return Ok(KapellmeisterHostDecision::Claim { input });
    }
    let issue = flexible_issue_ref(value.get("issue"))?;
    let assignee = value
        .get("assignee")
        .map(flexible_user_ref)
        .transpose()?
        .unwrap_or(LinearUserRef::Me);
    let target_state = value.get("target_state").or_else(|| value.get("state"));
    Ok(KapellmeisterHostDecision::Claim {
        input: crate::tools::ClaimIssueInput {
            issue,
            assignee,
            target_state: target_state.map(flexible_state_ref).transpose()?,
            idempotency_key: flexible_idempotency_key(value, "kapellmeister-claim", request_id),
        },
    })
}

fn parse_flexible_move_decision(
    request_id: &ShadowDecisionId,
    value: &Value,
) -> Result<KapellmeisterHostDecision> {
    if let Ok(input) = serde_json::from_value(value.clone()) {
        return Ok(KapellmeisterHostDecision::Move { input });
    }
    let state = value
        .get("state")
        .or_else(|| value.get("target_state"))
        .map(flexible_state_ref)
        .transpose()?
        .ok_or_else(|| {
            FugueError::ResponseError("Kapellmeister move decision missing state".to_string())
        })?;
    Ok(KapellmeisterHostDecision::Move {
        input: crate::tools::MoveIssueInput {
            issue: flexible_issue_ref(value.get("issue"))?,
            state,
            expected_updated_at: value
                .get("expected_updated_at")
                .map(|updated| serde_json::from_value(updated.clone()))
                .transpose()
                .map_err(|err| FugueError::ResponseError(err.to_string()))?,
            idempotency_key: flexible_idempotency_key(value, "kapellmeister-move", request_id),
        },
    })
}

fn parse_flexible_workpad_decision(
    request_id: &ShadowDecisionId,
    value: &Value,
) -> Result<KapellmeisterHostDecision> {
    if let Ok(input) = serde_json::from_value(value.clone()) {
        return Ok(KapellmeisterHostDecision::WorkpadUpdate { input });
    }
    let body_markdown = flexible_string(value, &["body_markdown", "content", "markdown", "text"])
        .ok_or_else(|| {
        FugueError::ResponseError(
            "Kapellmeister workpad_update missing body_markdown/content".to_string(),
        )
    })?;
    Ok(KapellmeisterHostDecision::WorkpadUpdate {
        input: UpsertWorkpadInput {
            issue: flexible_issue_ref(value.get("issue"))?,
            heading: flexible_string(value, &["heading"])
                .unwrap_or_else(|| "## Fugue Workpad".to_string()),
            body_markdown,
            create_if_missing: value
                .get("create_if_missing")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            idempotency_key: flexible_idempotency_key(value, "kapellmeister-workpad", request_id),
        },
    })
}

fn parse_flexible_worker_start_decision(
    request_id: &ShadowDecisionId,
    value: &Value,
) -> Result<KapellmeisterHostDecision> {
    if let Ok(input) = serde_json::from_value(value.clone()) {
        return Ok(KapellmeisterHostDecision::WorkerStart { input });
    }
    let issue = flexible_issue_ref(value.get("issue"))?;
    let workspace_key = flexible_string(value, &["workspace_key"])
        .or_else(|| issue_identifier_string(&issue))
        .ok_or_else(|| {
            FugueError::ResponseError(
                "Kapellmeister worker_start missing workspace_key".to_string(),
            )
        })?;
    Ok(KapellmeisterHostDecision::WorkerStart {
        input: crate::tools::SpawnOrResumeExecutionInput {
            issue,
            workspace_key: WorkspaceKey(workspace_key),
            template_id: MobTemplateId(
                flexible_string(value, &["template_id"])
                    .unwrap_or_else(|| "implementation_mob".to_string()),
            ),
            worktree_path: flexible_string(value, &["worktree_path"]),
            instructions: flexible_string(value, &["instructions"]),
            idempotency_key: flexible_idempotency_key(
                value,
                "kapellmeister-worker-start",
                request_id,
            ),
        },
    })
}

fn parse_flexible_final_pr_gate_start_decision(value: &Value) -> Result<KapellmeisterHostDecision> {
    if let Ok(request) = serde_json::from_value::<FinalPrGateStartRequest>(value.clone()) {
        return Ok(KapellmeisterHostDecision::FinalPrGateStart { request });
    }
    if let Some(request_value) = value.get("request").or_else(|| value.get("start_request")) {
        let request = serde_json::from_value::<FinalPrGateStartRequest>(request_value.clone())
            .map_err(|err| FugueError::ResponseError(err.to_string()))?;
        return Ok(KapellmeisterHostDecision::FinalPrGateStart { request });
    }
    Err(FugueError::ResponseError(
        "Kapellmeister final_pr_gate_start decision missing typed request".to_string(),
    ))
}

fn parse_flexible_independent_review_start_decision(
    value: &Value,
) -> Result<KapellmeisterHostDecision> {
    if let Ok(request) = serde_json::from_value::<IndependentReviewStartRequest>(value.clone()) {
        return Ok(KapellmeisterHostDecision::IndependentReviewStart { request });
    }
    if let Some(request_value) = value.get("request").or_else(|| value.get("start_request")) {
        let request =
            serde_json::from_value::<IndependentReviewStartRequest>(request_value.clone())
                .map_err(|err| FugueError::ResponseError(err.to_string()))?;
        return Ok(KapellmeisterHostDecision::IndependentReviewStart { request });
    }
    Err(FugueError::ResponseError(
        "Kapellmeister independent_review_start decision missing typed request".to_string(),
    ))
}

fn parse_flexible_review_request_decision(value: &Value) -> Result<KapellmeisterHostDecision> {
    Ok(KapellmeisterHostDecision::ReviewRequest {
        issue: flexible_issue_ref(value.get("issue"))?,
        reason: flexible_reason(value, "Kapellmeister requested review"),
    })
}

fn flexible_issue_ref(value: Option<&Value>) -> Result<LinearIssueRef> {
    let value = value.ok_or_else(|| {
        FugueError::ResponseError("Kapellmeister decision missing issue".to_string())
    })?;
    if let Ok(issue) = serde_json::from_value::<LinearIssueRef>(value.clone()) {
        return Ok(issue);
    }
    if let Some(identifier) = value.as_str() {
        return Ok(LinearIssueRef::Identifier {
            identifier: LinearIssueIdentifier(identifier.to_string()),
        });
    }
    let object = value.as_object().ok_or_else(|| {
        FugueError::ResponseError("Kapellmeister issue reference must be object/string".to_string())
    })?;
    if let Some(identifier) = object.get("identifier").and_then(Value::as_str) {
        return Ok(LinearIssueRef::Identifier {
            identifier: LinearIssueIdentifier(identifier.to_string()),
        });
    }
    if let Some(identifier) = object.get("key").and_then(Value::as_str) {
        return Ok(LinearIssueRef::Identifier {
            identifier: LinearIssueIdentifier(identifier.to_string()),
        });
    }
    if let Some(id) = object.get("id").and_then(Value::as_str) {
        return Ok(LinearIssueRef::Id {
            id: LinearIssueId(id.to_string()),
        });
    }
    Err(FugueError::ResponseError(
        "Kapellmeister issue reference missing identifier/id".to_string(),
    ))
}

fn flexible_state_ref(value: &Value) -> Result<LinearStateRef> {
    if let Ok(state) = serde_json::from_value::<LinearStateRef>(value.clone()) {
        return Ok(state);
    }
    if let Some(name) = value.as_str() {
        return Ok(LinearStateRef::Name {
            name: name.to_string(),
        });
    }
    let object = value.as_object().ok_or_else(|| {
        FugueError::ResponseError("Kapellmeister state reference must be object/string".to_string())
    })?;
    if let Some(name) = object.get("name").and_then(Value::as_str) {
        return Ok(LinearStateRef::Name {
            name: name.to_string(),
        });
    }
    if let Some(id) = object.get("id").and_then(Value::as_str) {
        return Ok(LinearStateRef::Id {
            id: LinearStateId(id.to_string()),
        });
    }
    Err(FugueError::ResponseError(
        "Kapellmeister state reference missing name/id".to_string(),
    ))
}

fn flexible_user_ref(value: &Value) -> Result<LinearUserRef> {
    if let Ok(user) = serde_json::from_value::<LinearUserRef>(value.clone()) {
        return Ok(user);
    }
    if value
        .as_str()
        .is_some_and(|user| user.eq_ignore_ascii_case("me"))
    {
        return Ok(LinearUserRef::Me);
    }
    if let Some(id) = value.as_str() {
        return Ok(LinearUserRef::Id {
            id: crate::tools::LinearUserId(id.to_string()),
        });
    }
    let object = value.as_object().ok_or_else(|| {
        FugueError::ResponseError("Kapellmeister user reference must be object/string".to_string())
    })?;
    if object
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind.eq_ignore_ascii_case("me"))
    {
        return Ok(LinearUserRef::Me);
    }
    if let Some(id) = object.get("id").and_then(Value::as_str) {
        return Ok(LinearUserRef::Id {
            id: crate::tools::LinearUserId(id.to_string()),
        });
    }
    Err(FugueError::ResponseError(
        "Kapellmeister user reference missing id".to_string(),
    ))
}

fn flexible_string(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .map(ToString::to_string)
}

fn flexible_reason(value: &Value, fallback: &str) -> String {
    flexible_string(value, &["reason", "rationale", "message"])
        .unwrap_or_else(|| fallback.to_string())
}

fn flexible_idempotency_key(
    value: &Value,
    prefix: &str,
    request_id: &ShadowDecisionId,
) -> IdempotencyKey {
    flexible_string(value, &["idempotency_key"])
        .map(IdempotencyKey)
        .unwrap_or_else(|| IdempotencyKey(format!("{prefix}:{}", request_id.0)))
}

fn issue_identifier_string(issue: &LinearIssueRef) -> Option<String> {
    match issue {
        LinearIssueRef::Identifier { identifier } => Some(identifier.0.clone()),
        LinearIssueRef::Id { .. } => None,
    }
}

fn decision_response_candidates(value: &Value) -> Vec<Value> {
    let mut candidates = Vec::new();
    collect_decision_response_candidates(value, &mut candidates);
    candidates
}

fn collect_decision_response_candidates(value: &Value, candidates: &mut Vec<Value>) {
    if candidates.len() >= 32 {
        return;
    }
    if value.get("decision").is_some() {
        candidates.push(value.clone());
    }
    if let Some(text) = value.as_str() {
        if let Some(parsed) = parse_json_object_from_text(text) {
            collect_decision_response_candidates(&parsed, candidates);
        }
        return;
    }
    match value {
        Value::Array(items) => {
            for item in items {
                collect_decision_response_candidates(item, candidates);
            }
        }
        Value::Object(object) => {
            for key in [
                "structured_output",
                "output",
                "response",
                "message",
                "content",
                "text",
                "data",
            ] {
                if let Some(nested) = object.get(key) {
                    collect_decision_response_candidates(nested, candidates);
                }
            }
            if let Some(content) = object.get("content")
                && let Some(items) = content.as_array()
            {
                for item in items {
                    if let Some(text) = item.get("text") {
                        collect_decision_response_candidates(text, candidates);
                    }
                    if let Some(json) = item.get("json") {
                        collect_decision_response_candidates(json, candidates);
                    }
                }
            }
        }
        _ => {}
    }
}

fn shadow_recommendation_response_candidates(value: &Value) -> Vec<Value> {
    let mut candidates = Vec::new();
    collect_shadow_recommendation_response_candidates(value, &mut candidates);
    candidates
}

fn collect_shadow_recommendation_response_candidates(value: &Value, candidates: &mut Vec<Value>) {
    if candidates.len() >= 32 {
        return;
    }
    if value.get("recommendations").is_some() {
        candidates.push(value.clone());
    }
    if let Some(text) = value.as_str() {
        if let Some(parsed) = parse_json_object_from_text(text) {
            collect_shadow_recommendation_response_candidates(&parsed, candidates);
        }
        return;
    }
    match value {
        Value::Array(items) => {
            for item in items {
                collect_shadow_recommendation_response_candidates(item, candidates);
            }
        }
        Value::Object(object) => {
            for key in [
                "structured_output",
                "output",
                "response",
                "message",
                "content",
                "text",
                "data",
            ] {
                if let Some(nested) = object.get(key) {
                    collect_shadow_recommendation_response_candidates(nested, candidates);
                }
            }
            if let Some(content) = object.get("content")
                && let Some(items) = content.as_array()
            {
                for item in items {
                    if let Some(text) = item.get("text") {
                        collect_shadow_recommendation_response_candidates(text, candidates);
                    }
                    if let Some(json) = item.get("json") {
                        collect_shadow_recommendation_response_candidates(json, candidates);
                    }
                }
            }
        }
        _ => {}
    }
}

fn parse_json_object_from_text(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed)
        && value.is_object()
    {
        return Some(value);
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if start >= end {
        return None;
    }
    serde_json::from_str::<Value>(&trimmed[start..=end])
        .ok()
        .filter(Value::is_object)
}

fn read_only_shadow_meerkat_config(meerkat: &MeerkatConfig) -> MeerkatConfig {
    let mut shadow = meerkat.clone();
    shadow.enable_shell = Some(false);
    shadow.enable_builtins = Some(false);
    shadow.enable_mob = Some(false);
    shadow.flow_tool_overlay = None;
    shadow
}

fn decision_safe_meerkat_config(meerkat: &MeerkatConfig) -> MeerkatConfig {
    let mut decision = read_only_shadow_meerkat_config(meerkat);
    decision.enable_mob = Some(true);
    decision.enable_memory = Some(false);
    decision.shell_env = None;
    decision
}

fn kapellmeister_decision_session_params(
    meerkat: &MeerkatConfig,
    request: &KapellmeisterDecisionRequest,
) -> Result<(MeerkatConfig, Value)> {
    let decision_meerkat = decision_safe_meerkat_config(meerkat);
    let prompt = kapellmeister_decision_prompt(request)?;
    let mut params = decision_meerkat.create_session_params(prompt, None);
    if let Some(object) = params.as_object_mut() {
        object.insert(
            "peer_meta".to_string(),
            json!({
                "fugue_role": "kapellmeister_dispatch_policy",
                "authority": request.authority.mode,
                "request_id": request.request_id.0
            }),
        );
        object.insert(
            "initial_turn".to_string(),
            Value::String("run_immediately".to_string()),
        );
    }
    Ok((decision_meerkat, params))
}

fn inert_shadow_external_tools(_config: &TrackerConfig) -> Option<Vec<Value>> {
    None
}

pub fn read_shadow_state(path: &Path) -> Result<Option<KapellmeisterShadowSessionState>> {
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(path).map_err(|err| {
        FugueError::InvalidConfig(format!(
            "failed to read kapellmeister shadow state {}: {err}",
            path.display()
        ))
    })?;
    let state = serde_json::from_str(&contents).map_err(|err| {
        FugueError::InvalidConfig(format!(
            "failed to parse kapellmeister shadow state {}: {err}",
            path.display()
        ))
    })?;
    Ok(Some(state))
}

pub fn write_shadow_state(path: &Path, state: &KapellmeisterShadowSessionState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            FugueError::InvalidConfig(format!(
                "failed to create kapellmeister shadow state dir {}: {err}",
                parent.display()
            ))
        })?;
    }
    let contents = serde_json::to_string_pretty(state)
        .map_err(|err| FugueError::InvalidConfig(err.to_string()))?;
    fs::write(path, contents).map_err(|err| {
        FugueError::InvalidConfig(format!(
            "failed to write kapellmeister shadow state {}: {err}",
            path.display()
        ))
    })
}

pub fn read_only_shadow_tools() -> Vec<Value> {
    [
        FugueToolOperation::LinearInspectIssue,
        FugueToolOperation::LinearListProjectStates,
        FugueToolOperation::LinearListRecentIssueChanges,
    ]
    .into_iter()
    .filter(|operation| KapellmeisterToolAuthority::default().permits(*operation))
    .map(read_only_tool_schema)
    .collect()
}

fn read_only_tool_schema(operation: FugueToolOperation) -> Value {
    match operation {
        FugueToolOperation::LinearInspectIssue => json!({
            "name": operation.provider_callback_name(),
            "description": "Inspect a Linear issue through Fugue's typed Linear tool layer, including the current Fugue Workpad comment body when present.",
            "input_schema": {
                "type": "object",
                "properties": { "issue": { "type": "object" } },
                "required": ["issue"],
                "additionalProperties": false
            }
        }),
        FugueToolOperation::LinearListProjectStates => json!({
            "name": operation.provider_callback_name(),
            "description": "List Linear workflow states for a team/project scope.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "team_id": { "type": ["string", "null"] },
                    "project_id": { "type": ["string", "null"] }
                },
                "additionalProperties": false
            }
        }),
        FugueToolOperation::LinearListRecentIssueChanges => json!({
            "name": operation.provider_callback_name(),
            "description": "List recent issue comment changes exposed by available Linear API surfaces.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "issue": { "type": ["object", "null"] },
                    "since": { "type": ["string", "null"], "format": "date-time" },
                    "cursor": { "type": ["string", "null"] },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100 }
                },
                "required": ["limit"],
                "additionalProperties": false
            }
        }),
        _ => json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_cursors_include_enterprise_host_identity() {
        let public: GitHubRepoRef =
            serde_json::from_value(json!({"owner":"example","name":"app"})).expect("public repo");
        let enterprise: GitHubRepoRef = serde_json::from_value(
            json!({"host":"git.example.com","owner":"example","name":"app"}),
        )
        .expect("enterprise repo");
        assert_ne!(
            github_repo_partition(&public),
            github_repo_partition(&enterprise)
        );
    }
    use crate::tools::{
        AddCommentOutput, BranchName, ClaimIssueInput, CreatePrOutput, GitHubOwner, GitHubRepoName,
        IdempotencyKey, LinearIssueId, LinearIssueIdentifier, LinearIssueSnapshot, LinearStateId,
        LinearStateSnapshot, LinearStateType, LinearTeamId, LinearUserRef,
        ListRecentIssueChangesOutput, MobTemplateId, MoveIssueInput, MoveIssueOutput,
        SpawnOrResumeExecutionInput, ToolExecutionMetadata,
    };

    fn issue_ref() -> LinearIssueRef {
        LinearIssueRef::Identifier {
            identifier: LinearIssueIdentifier("LUC-146".to_string()),
        }
    }

    fn repo_ref() -> GitHubRepoRef {
        GitHubRepoRef {
            host: None,
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        }
    }

    fn decision_request_for_test(request_id: &str) -> KapellmeisterDecisionRequest {
        KapellmeisterDecisionRequest {
            request_id: ShadowDecisionId(request_id.to_string()),
            requested_at: "2026-04-29T22:15:00Z".parse().unwrap(),
            authority: KapellmeisterAuthorityConfig {
                mode: KapellmeisterAuthorityMode::GuardedLabeled,
                safe_selectors: KapellmeisterSafeSelectors {
                    repositories: vec![repo_ref()],
                    linear_labels: vec!["kapellmeister-safe".to_string()],
                },
            },
            context: KapellmeisterDecisionContext {
                issue: Some(issue_ref()),
                repository: Some(repo_ref()),
                linear_labels: vec!["kapellmeister-safe".to_string()],
                workspace_key: Some(WorkspaceKey("LUC-146".to_string())),
                observations: Vec::new(),
            },
            requested_action: Some("test decision".to_string()),
        }
    }

    fn meerkat_config_with_mutating_capabilities() -> MeerkatConfig {
        MeerkatConfig {
            runtime_connections: None,
            repository_runtime: None,
            command: "rkat-rpc".to_string(),
            realm: None,
            instance: None,
            realm_backend: None,
            state_root: None,
            context_root: None,
            user_config_root: None,
            auth_binding: None,
            model: Some("test-model".to_string()),
            provider: None,
            max_tokens: None,
            system_prompt: None,
            output_schema: None,
            structured_output_retries: None,
            hooks_override: None,
            provider_params: None,
            enable_shell: Some(true),
            enable_builtins: Some(true),
            enable_mob: Some(true),
            enable_memory: Some(true),
            keep_alive: false,
            max_sessions: 4096,
            comms_name: None,
            peer_meta: None,
            budget_limits: None,
            preload_skills: None,
            skill_refs: None,
            labels: None,
            additional_instructions: None,
            app_context: None,
            shell_env: Some(BTreeMap::from([("DANGEROUS".to_string(), "1".to_string())])),
            flow_tool_overlay: Some(serde_json::Map::from_iter([(
                "dangerous_tool".to_string(),
                json!({"enabled": true}),
            )])),
            first_turn_activation_prompt: "Begin.".to_string(),
            turn_timeout_ms: 3_600_000,
            read_timeout_ms: 5_000,
            stall_timeout_ms: 300_000,
        }
    }

    #[tokio::test]
    async fn kapellmeister_rpc_client_skips_session_events_until_matching_response()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let script = temp.path().join("fake-rpc.py");
        std::fs::write(
            &script,
            r#"
import json
import sys

for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    request_id = request.get("id")
    if method == "initialize":
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"methods": ["initialized", "session/create", "turn/start"]}
        }), flush=True)
    elif method == "session/create":
        print(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/event",
            "params": {"event": {"type": "run_started"}}
        }), flush=True)
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"session_id": "session-after-event"}
        }), flush=True)
    else:
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "method not found"}
        }), flush=True)
"#,
        )?;
        let mut meerkat = meerkat_config_with_mutating_capabilities();
        meerkat.command = format!("python3 {}", script.display());
        meerkat.read_timeout_ms = 1_000;
        let mut client = KapellmeisterRpcClient::start(&meerkat, temp.path()).await?;
        let result = client
            .call("session/create", json!({}), Duration::from_secs(2))
            .await?;
        client.stop().await;

        assert_eq!(result["session_id"], "session-after-event");
        Ok(())
    }

    #[test]
    fn shadow_signal_round_trips_as_typed_variant()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let signal = KapellmeisterShadowSignal {
            id: ShadowSignalId("sig-1".to_string()),
            observed_at: "2026-04-29T20:00:00Z".parse()?,
            source_cursor: Some(DurableCursorPosition {
                key: DurableCursorKey {
                    source: DurableCursorSource::LinearIssueChanges,
                    partition: CursorPartition("LUC".to_string()),
                },
                cursor: Cursor("cursor-1".to_string()),
                observed_at: "2026-04-29T20:00:00Z".parse()?,
            }),
            kind: KapellmeisterShadowSignalKind::LinearIssueMoved {
                issue: issue_ref(),
                from_state: None,
                to_state: LinearStateRef::Id {
                    id: LinearStateId("state-in-progress".to_string()),
                },
                actor: None,
            },
        };

        let encoded = serde_json::to_string(&signal)?;
        let decoded: KapellmeisterShadowSignal = serde_json::from_str(&encoded)?;

        assert_eq!(decoded, signal);
        assert!(encoded.contains("linear_issue_moved"));
        Ok(())
    }

    #[test]
    fn recommendation_and_comparison_contract_are_typed()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let recommendation = KapellmeisterShadowRecommendation {
            id: ShadowRecommendationId("rec-1".to_string()),
            generated_at: "2026-04-29T20:01:00Z".parse()?,
            based_on_signals: vec![ShadowSignalId("sig-1".to_string())],
            action: KapellmeisterRecommendedAction::OpenPullRequest {
                repo: repo_ref(),
                title: "Stage 3A contracts".to_string(),
                head_branch: BranchName("codex/LUC-146".to_string()),
                base_branch: BranchName("main".to_string()),
                draft: false,
            },
            confidence: RecommendationConfidence::Medium,
            rationale: "Worker completed with a branch ready for review.".to_string(),
        };
        let encoded = serde_json::to_string(&recommendation)?;
        let decoded: KapellmeisterShadowRecommendation = serde_json::from_str(&encoded)?;
        assert_eq!(decoded, recommendation);

        let comparison = KapellmeisterDecisionComparison {
            recommendation_id: recommendation.id,
            rust_decision: RustAuthoritativeDecision {
                id: ShadowDecisionId("rust-1".to_string()),
                decided_at: "2026-04-29T20:02:00Z".parse()?,
                action: RustAuthoritativeAction::SkipDispatch {
                    issue: issue_ref(),
                    reason: "no available slots".to_string(),
                },
            },
            outcome: DecisionComparisonOutcome::NotComparable,
            compared_at: "2026-04-29T20:02:01Z".parse()?,
            notes: Some("PR recommendation compared to dispatch decision.".to_string()),
        };
        let comparison_json = serde_json::to_string(&comparison)?;
        let decoded_comparison: KapellmeisterDecisionComparison =
            serde_json::from_str(&comparison_json)?;
        assert_eq!(decoded_comparison, comparison);
        Ok(())
    }

    #[test]
    fn authority_config_rejects_broad_guarded_selectors() {
        let broad = KapellmeisterAuthorityConfig {
            mode: KapellmeisterAuthorityMode::GuardedLabeled,
            safe_selectors: KapellmeisterSafeSelectors::default(),
        };
        assert!(broad.validate().is_err());

        let wildcard = KapellmeisterAuthorityConfig {
            mode: KapellmeisterAuthorityMode::GuardedLabeled,
            safe_selectors: KapellmeisterSafeSelectors {
                repositories: vec![GitHubRepoRef {
                    host: None,
                    owner: GitHubOwner("*".to_string()),
                    name: GitHubRepoName("meerkat-fugue".to_string()),
                }],
                linear_labels: Vec::new(),
            },
        };
        assert!(wildcard.validate().is_err());

        let narrow = KapellmeisterAuthorityConfig {
            mode: KapellmeisterAuthorityMode::GuardedLabeled,
            safe_selectors: KapellmeisterSafeSelectors {
                repositories: vec![repo_ref()],
                linear_labels: vec!["kapellmeister-safe".to_string()],
            },
        };
        assert!(narrow.validate().is_ok());
        let authority = KapellmeisterToolAuthority::from_config(narrow).unwrap();
        assert!(authority.permits(FugueToolOperation::LinearMoveIssue));
        assert!(authority.permits(FugueToolOperation::GitHubMergePr));
    }

    #[test]
    fn host_decision_contract_round_trips_typed_actions()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let requested_at = "2026-04-29T22:10:00Z".parse()?;
        let authority = KapellmeisterAuthorityConfig {
            mode: KapellmeisterAuthorityMode::GuardedLabeled,
            safe_selectors: KapellmeisterSafeSelectors {
                repositories: vec![repo_ref()],
                linear_labels: vec!["kapellmeister-safe".to_string()],
            },
        };
        let request = KapellmeisterDecisionRequest {
            request_id: ShadowDecisionId("decision-1".to_string()),
            requested_at,
            authority,
            context: KapellmeisterDecisionContext {
                issue: Some(issue_ref()),
                repository: Some(repo_ref()),
                linear_labels: vec!["kapellmeister-safe".to_string()],
                workspace_key: Some(WorkspaceKey("LUC-166".to_string())),
                observations: Vec::new(),
            },
            requested_action: Some("test serialization".to_string()),
        };
        let encoded_request = serde_json::to_string(&request)?;
        let decoded_request: KapellmeisterDecisionRequest = serde_json::from_str(&encoded_request)?;
        assert_eq!(decoded_request, request);
        assert!(!encoded_request.contains("serde_json::Value"));

        let responses = vec![
            KapellmeisterHostDecision::Claim {
                input: ClaimIssueInput {
                    issue: issue_ref(),
                    assignee: LinearUserRef::Me,
                    target_state: Some(LinearStateRef::Name {
                        name: "In Progress".to_string(),
                    }),
                    idempotency_key: IdempotencyKey("claim-LUC-166".to_string()),
                },
            },
            KapellmeisterHostDecision::Move {
                input: MoveIssueInput {
                    issue: issue_ref(),
                    state: LinearStateRef::Name {
                        name: "In Review".to_string(),
                    },
                    expected_updated_at: Some(requested_at),
                    idempotency_key: IdempotencyKey("move-LUC-166".to_string()),
                },
            },
            KapellmeisterHostDecision::WorkpadUpdate {
                input: UpsertWorkpadInput {
                    issue: issue_ref(),
                    heading: "## Fugue Workpad".to_string(),
                    body_markdown: "- [x] typed decision".to_string(),
                    create_if_missing: true,
                    idempotency_key: IdempotencyKey("workpad-LUC-166".to_string()),
                },
            },
            KapellmeisterHostDecision::WorkerStart {
                input: SpawnOrResumeExecutionInput {
                    issue: issue_ref(),
                    workspace_key: WorkspaceKey("LUC-166".to_string()),
                    template_id: MobTemplateId("solo".to_string()),
                    worktree_path: None,
                    instructions: None,
                    idempotency_key: IdempotencyKey("worker-LUC-166".to_string()),
                },
            },
            KapellmeisterHostDecision::ReviewRequest {
                issue: issue_ref(),
                reason: "needs reviewer gate evidence".to_string(),
            },
            KapellmeisterHostDecision::Stop {
                reason: "workflow complete".to_string(),
            },
            KapellmeisterHostDecision::Noop {
                reason: "no safe action".to_string(),
            },
        ];

        for (index, decision) in responses.into_iter().enumerate() {
            let response = KapellmeisterDecisionResponse {
                request_id: ShadowDecisionId(format!("decision-{index}")),
                decided_at: requested_at,
                decision,
                rationale: Some("typed host executable decision".to_string()),
            };
            let encoded = serde_json::to_string(&response)?;
            let decoded: KapellmeisterDecisionResponse = serde_json::from_str(&encoded)?;
            assert_eq!(decoded, response);
        }
        Ok(())
    }

    #[test]
    fn recommendation_comparison_detects_agreement_and_disagreement() {
        let generated_at = "2026-04-29T22:00:00Z".parse().unwrap();
        let recommendation = KapellmeisterShadowRecommendation {
            id: ShadowRecommendationId("rec-dispatch".to_string()),
            generated_at,
            based_on_signals: Vec::new(),
            action: KapellmeisterRecommendedAction::DispatchWorker {
                issue: issue_ref(),
                workspace_key: WorkspaceKey("LUC-146".to_string()),
            },
            confidence: RecommendationConfidence::High,
            rationale: "ready to dispatch".to_string(),
        };

        let agreeing = KapellmeisterDecisionComparison::compare(
            &recommendation,
            RustAuthoritativeDecision {
                id: ShadowDecisionId("rust-dispatch".to_string()),
                decided_at: generated_at,
                action: RustAuthoritativeAction::DispatchWorker {
                    issue: issue_ref(),
                    workspace_key: WorkspaceKey("LUC-146".to_string()),
                },
            },
            generated_at,
        );
        assert_eq!(agreeing.outcome, DecisionComparisonOutcome::Agrees);

        let disagreeing = KapellmeisterDecisionComparison::compare(
            &recommendation,
            RustAuthoritativeDecision {
                id: ShadowDecisionId("rust-skip".to_string()),
                decided_at: generated_at,
                action: RustAuthoritativeAction::SkipDispatch {
                    issue: issue_ref(),
                    reason: "blocked".to_string(),
                },
            },
            generated_at,
        );
        assert_eq!(disagreeing.outcome, DecisionComparisonOutcome::Disagrees);
    }

    #[test]
    fn recommendation_comparison_keeps_tool_mutations_non_authoritative() {
        let generated_at = "2026-04-29T22:01:00Z".parse().unwrap();
        let recommendation = KapellmeisterShadowRecommendation {
            id: ShadowRecommendationId("rec-move".to_string()),
            generated_at,
            based_on_signals: Vec::new(),
            action: KapellmeisterRecommendedAction::MoveLinearIssue {
                issue: issue_ref(),
                target_state: LinearStateRef::Name {
                    name: "In Review".to_string(),
                },
            },
            confidence: RecommendationConfidence::Medium,
            rationale: "suggest review".to_string(),
        };

        let comparison = KapellmeisterDecisionComparison::compare(
            &recommendation,
            RustAuthoritativeDecision {
                id: ShadowDecisionId("rust-dispatch".to_string()),
                decided_at: generated_at,
                action: RustAuthoritativeAction::DispatchWorker {
                    issue: issue_ref(),
                    workspace_key: WorkspaceKey("LUC-146".to_string()),
                },
            },
            generated_at,
        );

        assert_eq!(comparison.outcome, DecisionComparisonOutcome::NotComparable);
        assert!(
            !KapellmeisterToolAuthority::default().permits(FugueToolOperation::LinearMoveIssue)
        );
    }

    #[derive(Default)]
    struct FakeShadowRuntime {
        starts: usize,
        resumes: usize,
        deliveries: usize,
        fail_first_delivery_with_session_not_found: bool,
        last_request: Option<KapellmeisterShadowSessionRequest>,
        last_batch: Option<KapellmeisterSignalDeliveryBatch>,
    }

    #[async_trait]
    impl KapellmeisterShadowRuntime for FakeShadowRuntime {
        async fn start_shadow_session(
            &mut self,
            request: &KapellmeisterShadowSessionRequest,
        ) -> Result<KapellmeisterShadowSessionStart> {
            self.starts += 1;
            self.last_request = Some(request.clone());
            Ok(KapellmeisterShadowSessionStart {
                session_id: format!("session-start-{}", self.starts),
                session_ref: Some("ref-start".to_string()),
                stream_id: None,
                durable_cursors: vec![DurableCursorPosition {
                    key: DurableCursorKey {
                        source: DurableCursorSource::LinearIssueChanges,
                        partition: CursorPartition("LUC".to_string()),
                    },
                    cursor: Cursor("cursor-start".to_string()),
                    observed_at: "2026-04-29T20:00:00Z".parse().unwrap(),
                }],
            })
        }

        async fn resume_shadow_session(
            &mut self,
            existing: &KapellmeisterShadowSessionState,
            request: &KapellmeisterShadowSessionRequest,
        ) -> Result<KapellmeisterShadowSessionStart> {
            self.resumes += 1;
            self.last_request = Some(request.clone());
            Ok(KapellmeisterShadowSessionStart {
                session_id: existing.session_id.clone(),
                session_ref: existing.session_ref.clone(),
                stream_id: None,
                durable_cursors: existing.durable_cursors.clone(),
            })
        }

        async fn deliver_shadow_signals(
            &mut self,
            _existing: &KapellmeisterShadowSessionState,
            batch: &KapellmeisterSignalDeliveryBatch,
        ) -> Result<KapellmeisterSignalDeliveryReceipt> {
            self.deliveries += 1;
            if self.fail_first_delivery_with_session_not_found && self.deliveries == 1 {
                return Err(FugueError::ResponseError(
                    "{\"code\":-32001,\"message\":\"session not found: stale-session\"}"
                        .to_string(),
                ));
            }
            self.last_batch = Some(batch.clone());
            let mut receipt = KapellmeisterSignalDeliveryReceipt::delivered_all(&batch.signals);
            receipt.observations = batch
                .signals
                .iter()
                .map(|signal| KapellmeisterShadowObservation {
                    signal_id: signal.id.clone(),
                    observed_at: signal.observed_at,
                    status: KapellmeisterShadowObservationStatus::Delivered,
                    note: Some("fake runtime observed signal".to_string()),
                })
                .collect();
            receipt.recommendations = batch
                .signals
                .iter()
                .filter_map(|signal| match &signal.kind {
                    KapellmeisterShadowSignalKind::WorkerStarted {
                        issue,
                        workspace_key,
                        ..
                    } => Some(KapellmeisterShadowRecommendation {
                        id: ShadowRecommendationId(format!("rec-{}", signal.id.0)),
                        generated_at: signal.observed_at,
                        based_on_signals: vec![signal.id.clone()],
                        action: KapellmeisterRecommendedAction::DispatchWorker {
                            issue: issue.clone(),
                            workspace_key: workspace_key.clone(),
                        },
                        confidence: RecommendationConfidence::High,
                        rationale: "fake runtime mirrors Rust worker start".to_string(),
                    }),
                    _ => None,
                })
                .collect();
            Ok(receipt)
        }
    }

    fn shadow_config(path: PathBuf, enabled: bool) -> KapellmeisterShadowModeConfig {
        KapellmeisterShadowModeConfig {
            enabled,
            state_path: path,
            prompt: "shadow prompt".to_string(),
            authority: KapellmeisterToolAuthority::default(),
        }
    }

    fn repo_scope(root: PathBuf) -> KapellmeisterRepoScope {
        KapellmeisterRepoScope::new(WorkspaceKey("repo".to_string()), root)
    }

    fn tool_metadata(observed_at: DateTime<Utc>) -> ToolExecutionMetadata {
        ToolExecutionMetadata {
            completed_at: Some(observed_at),
            ..ToolExecutionMetadata::default()
        }
    }

    fn linear_state(name: &str) -> LinearStateSnapshot {
        LinearStateSnapshot {
            id: LinearStateId(format!("state-{name}")),
            name: name.to_string(),
            state_type: LinearStateType::Started,
        }
    }

    fn issue_snapshot(
        state: LinearStateSnapshot,
        observed_at: DateTime<Utc>,
    ) -> LinearIssueSnapshot {
        LinearIssueSnapshot {
            id: LinearIssueId("issue-1".to_string()),
            identifier: LinearIssueIdentifier("LUC-148".to_string()),
            title: "Stage 3C".to_string(),
            description: None,
            url: None,
            state,
            assignee: None,
            branch_name: None,
            labels: Vec::new(),
            updated_at: observed_at,
            team_id: LinearTeamId("team-1".to_string()),
        }
    }

    fn pull_request_snapshot(state: PullRequestState) -> GitHubPullRequestSnapshot {
        GitHubPullRequestSnapshot {
            repo: repo_ref(),
            number: GitHubPullRequestNumber(16),
            title: "Stage 3C".to_string(),
            url: "https://github.com/lukacf/meerkat-fugue/pull/16".to_string(),
            state,
            draft: false,
            head_branch: BranchName("codex/LUC-148-stage-3c-signals".to_string()),
            base_branch: BranchName("main".to_string()),
            head_sha: GitSha("headsha".to_string()),
        }
    }

    #[tokio::test]
    async fn disabled_shadow_host_is_inert_and_does_not_persist_state() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("shadow.json");
        let host = KapellmeisterShadowHost::new(
            shadow_config(state_path.clone(), false),
            repo_scope(temp.path().join("repo")),
        );
        let mut runtime = FakeShadowRuntime::default();

        let status = host.reconcile(&mut runtime).await.unwrap();

        assert_eq!(status, KapellmeisterShadowHostStatus::Disabled);
        assert_eq!(runtime.starts, 0);
        assert_eq!(runtime.resumes, 0);
        assert!(!state_path.exists());
    }

    #[tokio::test]
    async fn shadow_host_starts_once_then_resumes_existing_session() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("shadow.json");
        let host = KapellmeisterShadowHost::new(
            shadow_config(state_path.clone(), true),
            repo_scope(temp.path().join("repo")),
        );
        let mut runtime = FakeShadowRuntime::default();

        let first = host.reconcile(&mut runtime).await.unwrap();
        let second = host.reconcile(&mut runtime).await.unwrap();

        assert!(matches!(first, KapellmeisterShadowHostStatus::Started(_)));
        assert!(matches!(second, KapellmeisterShadowHostStatus::Resumed(_)));
        assert_eq!(runtime.starts, 1);
        assert_eq!(runtime.resumes, 1);
        let persisted = read_shadow_state(&state_path).unwrap().unwrap();
        assert_eq!(persisted.session_id, "session-start-1");
        assert_eq!(persisted.stream_id, None);
    }

    #[tokio::test]
    async fn shadow_host_resumes_after_restart_with_durable_cursors() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("shadow.json");
        let scope = repo_scope(temp.path().join("repo"));
        let original = KapellmeisterShadowSessionState {
            repo_scope: scope.clone(),
            session_id: "durable-session".to_string(),
            session_ref: Some("durable-ref".to_string()),
            stream_id: Some("old-stream".to_string()),
            durable_cursors: vec![DurableCursorPosition {
                key: DurableCursorKey {
                    source: DurableCursorSource::WorkerEvents,
                    partition: CursorPartition("repo".to_string()),
                },
                cursor: Cursor("worker-cursor-7".to_string()),
                observed_at: "2026-04-29T20:03:00Z".parse().unwrap(),
            }],
            delivered_signal_ids: Vec::new(),
            shadow_observations: Vec::new(),
            recommendations: Vec::new(),
            decision_comparisons: Vec::new(),
            started_at: "2026-04-29T20:02:00Z".parse().unwrap(),
            resumed_at: None,
            authority: KapellmeisterToolAuthority::default(),
        };
        write_shadow_state(&state_path, &original).unwrap();
        let host = KapellmeisterShadowHost::new(shadow_config(state_path.clone(), true), scope);
        let mut runtime = FakeShadowRuntime::default();

        let status = host.reconcile(&mut runtime).await.unwrap();

        assert_eq!(runtime.starts, 0);
        assert_eq!(runtime.resumes, 1);
        let KapellmeisterShadowHostStatus::Resumed(resumed) = status else {
            panic!("expected resume");
        };
        assert_eq!(resumed.session_id, "durable-session");
        assert_eq!(resumed.durable_cursors, original.durable_cursors);
        assert!(resumed.resumed_at.is_some());
        assert_eq!(read_shadow_state(&state_path).unwrap().unwrap(), resumed);
    }

    #[tokio::test]
    async fn enabled_shadow_host_models_inert_member_without_live_stream() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("shadow.json");
        let host = KapellmeisterShadowHost::new(
            shadow_config(state_path.clone(), true),
            repo_scope(temp.path().join("repo")),
        );
        let mut runtime = FakeShadowRuntime::default();

        let status = host.reconcile(&mut runtime).await.unwrap();

        let KapellmeisterShadowHostStatus::Started(state) = status else {
            panic!("expected started inert shadow member");
        };
        assert_eq!(state.stream_id, None);
        assert_eq!(
            read_shadow_state(&state_path).unwrap().unwrap().stream_id,
            None
        );
    }

    #[test]
    fn inert_shadow_runtime_does_not_advertise_callback_tools_until_servicing_loop_exists() {
        let tracker = TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: vec![],
            in_flight_states: vec!["In Progress".to_string()],
            active_states: vec!["Todo".to_string(), "In Progress".to_string()],
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: vec!["Done".to_string()],
        };

        assert_eq!(inert_shadow_external_tools(&tracker), None);
    }

    #[test]
    fn shadow_host_exposes_no_mutating_production_authority() {
        let tools = read_only_shadow_tools();
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();

        assert!(names.contains(&FugueToolOperation::LinearInspectIssue.provider_callback_name()));
        assert!(!names.contains(&FugueToolOperation::LinearMoveIssue.provider_callback_name()));
        assert!(!names.contains(&FugueToolOperation::GitHubCreatePr.provider_callback_name()));
        assert!(
            !KapellmeisterToolAuthority::default().permits(FugueToolOperation::LinearMoveIssue)
        );
        assert!(
            !KapellmeisterToolAuthority::default()
                .permits(FugueToolOperation::WorkspaceCleanWorkspace)
        );
    }

    #[test]
    fn shadow_authority_permits_only_read_only_tools() {
        let authority = KapellmeisterToolAuthority::default();

        assert!(authority.permits(FugueToolOperation::LinearInspectIssue));
        assert!(authority.permits(FugueToolOperation::GitHubCheckCi));
        assert!(!authority.permits(FugueToolOperation::LinearMoveIssue));
        assert!(!authority.permits(FugueToolOperation::GitHubCreatePr));
        assert!(!authority.permits(FugueToolOperation::WorkspaceRunRepositorySetup));
    }

    #[test]
    fn normalized_signal_constructors_cover_linear_github_ci_and_worker_shapes() {
        let observed_at = "2026-04-29T21:00:00Z".parse().unwrap();
        let cursor = DurableCursorPosition {
            key: DurableCursorKey {
                source: DurableCursorSource::GithubPullRequests,
                partition: CursorPartition("lukacf/meerkat-fugue".to_string()),
            },
            cursor: Cursor("pr-7-open".to_string()),
            observed_at,
        };
        let pr = GitHubPullRequestSnapshot {
            repo: repo_ref(),
            number: GitHubPullRequestNumber(7),
            title: "Stage 3C".to_string(),
            url: "https://github.com/lukacf/meerkat-fugue/pull/7".to_string(),
            state: crate::tools::PullRequestState::Open,
            draft: false,
            head_branch: BranchName("codex/LUC-148-stage-3c".to_string()),
            base_branch: BranchName("main".to_string()),
            head_sha: GitSha("abc123".to_string()),
        };

        let signals = [
            KapellmeisterShadowSignal::linear_issue_moved(
                observed_at,
                None,
                issue_ref(),
                Some(LinearStateRef::Name {
                    name: "Todo".to_string(),
                }),
                LinearStateRef::Name {
                    name: "In Progress".to_string(),
                },
                None,
            ),
            KapellmeisterShadowSignal::linear_issue_commented(
                observed_at,
                None,
                issue_ref(),
                LinearCommentId("comment-1".to_string()),
                None,
                None,
                None,
            ),
            KapellmeisterShadowSignal::pull_request_opened(
                observed_at,
                Some(cursor.clone()),
                repo_ref(),
                pr.clone(),
                Some("octocat".to_string()),
            ),
            KapellmeisterShadowSignal::pull_request_merged(
                observed_at,
                Some(cursor.clone()),
                repo_ref(),
                pr.clone(),
                Some(GitSha("merge123".to_string())),
                Some("octocat".to_string()),
            ),
            KapellmeisterShadowSignal::ci_changed(
                observed_at,
                Some(DurableCursorPosition {
                    key: DurableCursorKey {
                        source: DurableCursorSource::GithubCi,
                        partition: CursorPartition("lukacf/meerkat-fugue".to_string()),
                    },
                    cursor: Cursor("ci-abc123".to_string()),
                    observed_at,
                }),
                repo_ref(),
                GitRef("refs/heads/main".to_string()),
                CiStatus::Success,
                vec![GitHubCheckSnapshot {
                    name: "cargo test".to_string(),
                    status: CiStatus::Success,
                    details_url: None,
                    completed_at: Some(observed_at),
                }],
            ),
            KapellmeisterShadowSignal::worker_started(
                observed_at,
                None,
                issue_ref(),
                WorkspaceKey("LUC-148".to_string()),
                1,
                Some("session-1".to_string()),
            ),
            KapellmeisterShadowSignal::worker_finished(
                observed_at,
                None,
                issue_ref(),
                WorkspaceKey("LUC-148".to_string()),
                ShadowWorkerOutcome::Succeeded,
            ),
            KapellmeisterShadowSignal::worker_failed(
                observed_at,
                None,
                issue_ref(),
                WorkspaceKey("LUC-148".to_string()),
                ShadowWorkerFailure {
                    reason: ShadowWorkerFailureReason::WorkerError,
                    message: Some("boom".to_string()),
                    retry_scheduled: true,
                },
            ),
        ];

        assert_eq!(signals.len(), 8);
        assert!(signals.iter().all(|signal| signal.id.0.starts_with("sig-")));
        assert_eq!(
            signals[2].id,
            KapellmeisterShadowSignal::pull_request_opened(
                observed_at,
                Some(cursor),
                repo_ref(),
                pr,
                Some("octocat".to_string()),
            )
            .id
        );
    }

    #[test]
    fn tool_results_construct_shadow_signals_for_linear_github_and_ci_sources() {
        let observed_at = "2026-04-29T21:20:00Z".parse().unwrap();
        let moved = shadow_signals_from_tool_result(&FugueToolResult::Ok {
            output: FugueToolOutput::LinearMoveIssue(MoveIssueOutput {
                issue: issue_snapshot(linear_state("In Review"), observed_at),
                changed: true,
                previous_state: Some(linear_state("Rework")),
            }),
            metadata: tool_metadata(observed_at),
        });
        let commented = shadow_signals_from_tool_result(&FugueToolResult::Ok {
            output: FugueToolOutput::LinearListRecentIssueChanges(ListRecentIssueChangesOutput {
                changes: vec![LinearIssueChange {
                    issue: issue_ref(),
                    changed_at: observed_at,
                    actor: None,
                    kind: LinearIssueChangeKind::Commented {
                        comment_id: LinearCommentId("comment-1".to_string()),
                    },
                }],
                next_cursor: Some(Cursor("cursor-1".to_string())),
            }),
            metadata: tool_metadata(observed_at),
        });
        let opened = shadow_signals_from_tool_result(&FugueToolResult::Ok {
            output: FugueToolOutput::GitHubCreatePr(CreatePrOutput {
                pull_request: pull_request_snapshot(PullRequestState::Open),
                created: true,
            }),
            metadata: tool_metadata(observed_at),
        });
        let merged = shadow_signals_from_tool_result(&FugueToolResult::Ok {
            output: FugueToolOutput::GitHubMergePr(crate::tools::MergePrOutput {
                pull_request: pull_request_snapshot(PullRequestState::Merged),
                merged: true,
                merge_commit_sha: Some(GitSha("merge-sha".to_string())),
            }),
            metadata: tool_metadata(observed_at),
        });
        let ci = shadow_signals_from_tool_result(&FugueToolResult::Ok {
            output: FugueToolOutput::GitHubCheckCi(CheckCiOutput {
                status: CiStatus::Failure,
                checks: vec![GitHubCheckSnapshot {
                    name: "cargo test".to_string(),
                    status: CiStatus::Failure,
                    details_url: None,
                    completed_at: Some(observed_at),
                }],
            }),
            metadata: tool_metadata(observed_at),
        });

        assert!(matches!(
            moved[0].kind,
            KapellmeisterShadowSignalKind::LinearIssueMoved { .. }
        ));
        assert!(matches!(
            commented[0].kind,
            KapellmeisterShadowSignalKind::LinearIssueCommented { .. }
        ));
        assert!(matches!(
            opened[0].kind,
            KapellmeisterShadowSignalKind::PullRequestOpened { .. }
        ));
        assert!(matches!(
            merged[0].kind,
            KapellmeisterShadowSignalKind::PullRequestMerged { .. }
        ));
        assert!(matches!(
            ci[0].kind,
            KapellmeisterShadowSignalKind::CiChanged { .. }
        ));
    }

    #[test]
    fn add_comment_result_constructs_comment_signal_with_input_issue() {
        let observed_at = "2026-04-29T21:25:00Z".parse().unwrap();
        let input = AddCommentInput {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-148".to_string()),
            },
            body_markdown: "review evidence".to_string(),
            attachments: Vec::new(),
        };
        let output = AddCommentOutput {
            comment_id: LinearCommentId("comment-148".to_string()),
            url: None,
            attachments: Vec::new(),
        };

        let signals = shadow_signals_from_add_comment_result(&input, &output, observed_at);

        assert_eq!(signals.len(), 1);
        let KapellmeisterShadowSignalKind::LinearIssueCommented {
            issue, comment_id, ..
        } = &signals[0].kind
        else {
            panic!("expected linear issue commented signal");
        };
        assert_eq!(issue, &input.issue);
        assert_eq!(comment_id, &output.comment_id);
        assert!(
            !matches!(issue, LinearIssueRef::Identifier { identifier } if identifier.0 == "unknown")
        );
    }

    #[tokio::test]
    async fn shadow_delivery_runs_in_background_without_blocking_production_finalization() {
        let handle = spawn_shadow_signal_delivery(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(KapellmeisterSignalDeliveryOutcome {
                session_available: true,
                delivered_signal_ids: Vec::new(),
                skipped_duplicate_signal_ids: Vec::new(),
                durable_cursors: Vec::new(),
                recommendations: Vec::new(),
                decision_comparisons: Vec::new(),
            })
        });

        tokio::time::timeout(Duration::from_millis(10), async {})
            .await
            .unwrap();
        assert!(!handle.is_finished());
        handle.abort();
    }

    #[tokio::test]
    async fn signal_delivery_is_replay_safe_and_advances_cursors_once() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("shadow.json");
        let scope = repo_scope(temp.path().join("repo"));
        let host = KapellmeisterShadowHost::new(shadow_config(state_path.clone(), true), scope);
        let mut runtime = FakeShadowRuntime::default();
        host.reconcile(&mut runtime).await.unwrap();
        let observed_at = "2026-04-29T21:10:00Z".parse().unwrap();
        let signal = KapellmeisterShadowSignal::linear_issue_moved(
            observed_at,
            Some(DurableCursorPosition {
                key: DurableCursorKey {
                    source: DurableCursorSource::LinearIssueChanges,
                    partition: CursorPartition("LUC".to_string()),
                },
                cursor: Cursor("cursor-2".to_string()),
                observed_at,
            }),
            issue_ref(),
            None,
            LinearStateRef::Name {
                name: "In Progress".to_string(),
            },
            None,
        );

        let first = host
            .deliver_signals(&mut runtime, vec![signal.clone()], None)
            .await
            .unwrap();
        let second = host
            .deliver_signals(&mut runtime, vec![signal.clone()], None)
            .await
            .unwrap();
        let persisted = read_shadow_state(&state_path).unwrap().unwrap();

        assert_eq!(first.delivered_signal_ids, vec![signal.id.clone()]);
        assert_eq!(second.delivered_signal_ids, Vec::<ShadowSignalId>::new());
        assert_eq!(second.skipped_duplicate_signal_ids, vec![signal.id.clone()]);
        assert_eq!(runtime.deliveries, 1);
        assert_eq!(persisted.delivered_signal_ids, vec![signal.id]);
        assert_eq!(persisted.durable_cursors.len(), 1);
        assert_eq!(
            persisted.durable_cursors[0].cursor,
            Cursor("cursor-2".to_string())
        );
        assert!(
            persisted
                .shadow_observations
                .iter()
                .any(|observation| observation.status
                    == KapellmeisterShadowObservationStatus::DuplicateSkipped)
        );
    }

    #[test]
    fn signal_id_ignores_cursor_observed_at_for_poll_replays() {
        let first_observed_at = "2026-04-29T21:10:00Z".parse().unwrap();
        let second_observed_at = "2026-04-29T21:11:00Z".parse().unwrap();
        let make_signal = |observed_at| {
            KapellmeisterShadowSignal::linear_issue_moved(
                observed_at,
                Some(DurableCursorPosition {
                    key: DurableCursorKey {
                        source: DurableCursorSource::LinearIssueChanges,
                        partition: CursorPartition("LUC".to_string()),
                    },
                    cursor: Cursor("LUC-519:lane:rework".to_string()),
                    observed_at,
                }),
                issue_ref(),
                None,
                LinearStateRef::Name {
                    name: "Rework".to_string(),
                },
                None,
            )
        };

        assert_eq!(
            make_signal(first_observed_at).id,
            make_signal(second_observed_at).id
        );
    }

    #[tokio::test]
    async fn shadow_delivery_recovers_once_when_persisted_session_disappears() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("shadow.json");
        let scope = repo_scope(temp.path().join("repo"));
        let host = KapellmeisterShadowHost::new(shadow_config(state_path.clone(), true), scope);
        let mut runtime = FakeShadowRuntime {
            fail_first_delivery_with_session_not_found: true,
            ..FakeShadowRuntime::default()
        };
        host.reconcile(&mut runtime).await.unwrap();
        let stale = read_shadow_state(&state_path).unwrap().unwrap();
        let observed_at = "2026-04-29T21:10:00Z".parse().unwrap();
        let signal = KapellmeisterShadowSignal::worker_started(
            observed_at,
            None,
            issue_ref(),
            WorkspaceKey("LUC-148".to_string()),
            1,
            Some("native-run".to_string()),
        );

        let outcome = host
            .deliver_signals(&mut runtime, vec![signal.clone()], None)
            .await
            .unwrap();
        let recovered = read_shadow_state(&state_path).unwrap().unwrap();

        assert_eq!(runtime.deliveries, 2);
        assert_eq!(runtime.starts, 2);
        assert_ne!(recovered.session_id, stale.session_id);
        assert_eq!(outcome.delivered_signal_ids, vec![signal.id.clone()]);
        assert_eq!(recovered.delivered_signal_ids, vec![signal.id]);
    }

    #[test]
    fn parses_typed_recommendations_from_turn_start_structured_output()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let turn_result = json!({
            "structured_output": {
                "recommendations": [{
                    "id": "rec-sig-worker",
                    "generated_at": "2026-04-29T22:11:00Z",
                    "based_on_signals": ["sig-worker"],
                    "action": {"action": "dispatch_worker",
                    "issue": {"kind": "identifier", "identifier": "LUC-146"},
                    "workspace_key": "LUC-146"},
                    "confidence": "high",
                    "rationale": "typed output from real shadow turn"
                }]
            }
        });

        let parsed = parse_shadow_recommendation_response(&turn_result)?;

        assert_eq!(parsed.recommendations.len(), 1);
        assert_eq!(
            parsed.recommendations[0].id,
            ShadowRecommendationId("rec-sig-worker".to_string())
        );
        assert!(matches!(
            parsed.recommendations[0].action,
            KapellmeisterRecommendedAction::DispatchWorker { .. }
        ));
        Ok(())
    }

    #[test]
    fn parses_typed_recommendations_embedded_in_turn_start_text()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let turn_result = json!({
            "message": {
                "content": [{
                    "type": "text",
                    "text": "```json\n{\"recommendations\":[{\"id\":\"rec-text\",\"generated_at\":\"2026-04-29T22:12:00Z\",\"based_on_signals\":[\"sig-worker\"],\"action\":{\"action\":\"dispatch_worker\",\"issue\":{\"kind\":\"identifier\",\"identifier\":\"LUC-146\"},\"workspace_key\":\"LUC-146\"},\"confidence\":\"medium\",\"rationale\":\"embedded json\"}]}\n```"
                }]
            }
        });

        let parsed = parse_shadow_recommendation_response(&turn_result)?;

        assert_eq!(parsed.recommendations.len(), 1);
        assert_eq!(
            parsed.recommendations[0].id,
            ShadowRecommendationId("rec-text".to_string())
        );
        Ok(())
    }

    #[test]
    fn parses_shadow_no_op_recommendation_from_canonical_model_output()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let turn_result = json!({
            "text": "{\"recommendations\":[{\"action\":{\"type\":\"no_op\"},\"based_on_signals\":[\"sig-worker\"],\"confidence\":0.78,\"rationale\":\"No production mutation is warranted yet.\"}]}"
        });

        let parsed = parse_shadow_recommendation_response(&turn_result)?;

        assert_eq!(parsed.recommendations.len(), 1);
        assert!(matches!(
            parsed.recommendations[0].action,
            KapellmeisterRecommendedAction::Noop
        ));
        assert_eq!(
            parsed.recommendations[0].based_on_signals,
            vec![ShadowSignalId("sig-worker".to_string())]
        );
        Ok(())
    }

    #[test]
    fn decision_session_params_enable_mob_tools_while_accepting_typed_response()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let request = decision_request_for_test("decision-runtime");
        let (_decision_meerkat, params) = kapellmeister_decision_session_params(
            &meerkat_config_with_mutating_capabilities(),
            &request,
        )?;

        assert_eq!(params["enable_shell"], Value::Bool(false));
        assert_eq!(params["enable_builtins"], Value::Bool(false));
        assert_eq!(params["enable_mob"], Value::Bool(true));
        assert_eq!(params["enable_memory"], Value::Bool(false));
        assert!(params.get("shell_env").is_none());
        assert!(params.get("external_tools").is_none());
        assert!(params.get("flow_tool_overlay").is_none());
        assert_eq!(
            params["initial_turn"],
            Value::String("run_immediately".to_string())
        );
        assert_eq!(
            params["peer_meta"]["fugue_role"],
            Value::String("kapellmeister_dispatch_policy".to_string())
        );
        assert_eq!(
            params["peer_meta"]["request_id"],
            Value::String("decision-runtime".to_string())
        );
        assert!(
            params["prompt"]
                .as_str()
                .is_some_and(|prompt| prompt.contains("delegate/mob_create"))
        );
        assert!(
            params["prompt"]
                .as_str()
                .is_some_and(|prompt| prompt.contains("issue mob planner owns"))
        );
        assert!(
            params["prompt"]
                .as_str()
                .is_some_and(|prompt| prompt.contains("native issue mob"))
        );

        let turn_result = json!({
            "structured_output": {
                "request_id": "decision-runtime",
                "decided_at": "2026-04-29T22:15:00Z",
                "decision": {
                    "decision": "worker_start",
                    "input": {
                        "issue": {"kind": "identifier", "identifier": "LUC-146"},
                        "workspace_key": "LUC-146",
                        "template_id": "implementation_mob",
                        "idempotency_key": "kapellmeister-runtime-worker-start:decision-runtime"
                    }
                },
                "rationale": "runtime authorized the native issue mob start"
            }
        });
        let parsed = parse_decision_response(&turn_result)?;

        assert_eq!(parsed.request_id, request.request_id);
        assert!(matches!(
            parsed.decision,
            KapellmeisterHostDecision::WorkerStart { .. }
        ));
        Ok(())
    }

    #[test]
    fn parses_typed_dispatch_decision_from_runtime_output()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let turn_result = json!({
            "structured_output": {
                "request_id": "decision-runtime",
                "decided_at": "2026-04-29T22:15:00Z",
                "decision": {
                    "decision": "worker_start",
                    "input": {
                        "issue": {"kind": "identifier", "identifier": "LUC-146"},
                        "workspace_key": "LUC-146",
                        "template_id": "implementation_mob",
                        "idempotency_key": "kapellmeister-runtime-worker-start:decision-runtime"
                    }
                },
                "rationale": "runtime authorized the native issue mob start"
            }
        });

        let parsed = parse_decision_response(&turn_result)?;

        assert_eq!(
            parsed.request_id,
            ShadowDecisionId("decision-runtime".to_string())
        );
        assert!(matches!(
            parsed.decision,
            KapellmeisterHostDecision::WorkerStart { .. }
        ));
        Ok(())
    }

    #[test]
    fn detects_stale_rkat_session_not_found_errors() {
        let error = FugueError::ResponseError(
            "{\"code\":-32001,\"message\":\"session not found: stale-session\"}".to_string(),
        );

        assert!(is_rkat_session_not_found(&error));
        assert!(is_rkat_session_not_found(&FugueError::ResponseError(
            "{\"code\":-32001,\"message\":\"Session not found: stale-session\"}".to_string()
        )));
        assert!(!is_rkat_session_not_found(&FugueError::ResponseError(
            "{\"code\":-32601,\"message\":\"Method not found: session/read\"}".to_string()
        )));
    }

    #[test]
    fn parses_kapellmeister_decision_wrapped_in_text_with_agent_shorthand()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let turn_result = json!({
            "session_id": "019de9da-7efc-7a73-9e50-0640bc4c1375",
            "session_ref": "meerkat-fugue-prod:019de9da-7efc-7a73-9e50-0640bc4c1375",
            "text": "{\"request_id\":\"kapellmeister-dispatch:LUC-323:1\",\"decision\":{\"type\":\"workpad_update\",\"repository\":{\"owner\":\"lukacf\",\"name\":\"meerkat-fugue\"},\"issue\":{\"identifier\":\"LUC-323\"},\"workspace_key\":\"LUC-323\",\"content\":\"Plan for LUC-323:\\n- [ ] Add the missing config test.\\n- [ ] Run focused validation.\"}}"
        });

        let parsed = parse_decision_response(&turn_result)?;

        assert_eq!(
            parsed.request_id,
            ShadowDecisionId("kapellmeister-dispatch:LUC-323:1".to_string())
        );
        match parsed.decision {
            KapellmeisterHostDecision::WorkpadUpdate { input } => {
                assert_eq!(
                    input.issue,
                    LinearIssueRef::Identifier {
                        identifier: LinearIssueIdentifier("LUC-323".to_string())
                    }
                );
                assert_eq!(input.heading, "## Fugue Workpad");
                assert!(input.body_markdown.contains("Plan for LUC-323"));
                assert!(input.create_if_missing);
                assert_eq!(
                    input.idempotency_key,
                    IdempotencyKey(
                        "kapellmeister-workpad:kapellmeister-dispatch:LUC-323:1".to_string()
                    )
                );
            }
            other => panic!("expected workpad update, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn parses_worker_start_shorthand_from_rpc_text_as_typed_decision()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let turn_result = json!({
            "text": "{\"request_id\":\"kapellmeister-dispatch:LUC-000:1\",\"decision\":{\"type\":\"worker_start\",\"issue\":{\"identifier\":\"LUC-000\"},\"workspace_key\":\"LUC-000\",\"template_id\":\"implementation_mob\",\"instructions\":\"start the issue mob\"}}"
        });

        let parsed = parse_decision_response(&turn_result)?;

        assert_eq!(
            parsed.request_id,
            ShadowDecisionId("kapellmeister-dispatch:LUC-000:1".to_string())
        );
        match parsed.decision {
            KapellmeisterHostDecision::WorkerStart { input } => {
                assert_eq!(
                    input.issue,
                    LinearIssueRef::Identifier {
                        identifier: LinearIssueIdentifier("LUC-000".to_string())
                    }
                );
                assert_eq!(input.workspace_key, WorkspaceKey("LUC-000".to_string()));
                assert_eq!(
                    input.template_id,
                    MobTemplateId("implementation_mob".to_string())
                );
                assert_eq!(input.worktree_path, None);
                assert_eq!(input.instructions, Some("start the issue mob".to_string()));
                assert_eq!(
                    input.idempotency_key,
                    IdempotencyKey(
                        "kapellmeister-worker-start:kapellmeister-dispatch:LUC-000:1".to_string()
                    )
                );
            }
            other => panic!("expected worker start, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn parses_worker_start_kind_from_runtime_text_as_typed_decision()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let turn_result = json!({
            "text": "{\"request_id\":\"kapellmeister-dispatch:LUC-506:1\",\"decision\":{\"kind\":\"worker_start\",\"issue\":{\"kind\":\"identifier\",\"identifier\":\"LUC-506\"},\"repository\":{\"owner\":\"lukacf\",\"name\":\"meerkat-fugue\"},\"workspace_key\":\"LUC-506\"}}"
        });

        let parsed = parse_decision_response(&turn_result)?;

        assert_eq!(
            parsed.request_id,
            ShadowDecisionId("kapellmeister-dispatch:LUC-506:1".to_string())
        );
        match parsed.decision {
            KapellmeisterHostDecision::WorkerStart { input } => {
                assert_eq!(input.workspace_key, WorkspaceKey("LUC-506".to_string()));
                assert_eq!(
                    input.issue,
                    LinearIssueRef::Identifier {
                        identifier: LinearIssueIdentifier("LUC-506".to_string())
                    }
                );
            }
            other => panic!("expected worker start, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn parses_request_scoped_decision_text_without_model_request_id()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let request_id = ShadowDecisionId("kapellmeister-dispatch:LUC-447:1".to_string());
        let turn_result = json!({
            "session_id": "019df7ef-24f2-7e31-a135-8df46e977fd6",
            "session_ref": "meerkat-fugue-prod-storage-v2-contract-v12:019df7ef-24f2-7e31-a135-8df46e977fd6",
            "text": "{\"decision\":{\"type\":\"worker_start\",\"issue\":{\"identifier\":\"LUC-447\"},\"repository\":{\"owner\":\"lukacf\",\"name\":\"meerkat-fugue\"},\"workspace_key\":\"LUC-447\",\"worker_backend\":\"native_issue_mob\",\"reason\":\"Safe selected Todo dispatch with no active run evidence; start the configured native issue mob for planning and implementation.\"}}"
        });

        assert!(parse_decision_response(&turn_result).is_err());
        let parsed = parse_decision_response_for_request(&turn_result, &request_id)?;

        assert_eq!(parsed.request_id, request_id);
        match parsed.decision {
            KapellmeisterHostDecision::WorkerStart { input } => {
                assert_eq!(
                    input.issue,
                    LinearIssueRef::Identifier {
                        identifier: LinearIssueIdentifier("LUC-447".to_string())
                    }
                );
                assert_eq!(input.workspace_key, WorkspaceKey("LUC-447".to_string()));
                assert_eq!(
                    input.idempotency_key,
                    IdempotencyKey(
                        "kapellmeister-worker-start:kapellmeister-dispatch:LUC-447:1".to_string()
                    )
                );
            }
            other => panic!("expected worker start, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn rejects_untyped_dispatch_decision_output_at_rpc_boundary() {
        let turn_result = json!({
            "message": {"content": "start the worker for LUC-146"}
        });

        let err = parse_decision_response(&turn_result).unwrap_err();

        assert!(err.to_string().contains("typed decision response"));
    }

    #[test]
    fn runtime_turn_result_receipt_carries_typed_recommendations_for_persistence()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let observed_at = "2026-04-29T22:14:00Z".parse().unwrap();
        let signal = KapellmeisterShadowSignal::worker_started(
            observed_at,
            None,
            issue_ref(),
            WorkspaceKey("LUC-146".to_string()),
            1,
            Some("session-1".to_string()),
        );
        let batch = KapellmeisterSignalDeliveryBatch {
            signals: vec![signal.clone()],
            workpad_preview: None,
        };
        let turn_result = json!({
            "structured_output": {
                "recommendations": [{
                    "id": "rec-runtime",
                    "generated_at": "2026-04-29T22:14:01Z",
                    "based_on_signals": [signal.id.0.clone()],
                    "action": {
                        "action": "dispatch_worker",
                        "issue": {"kind": "identifier", "identifier": "LUC-146"},
                        "workspace_key": "LUC-146"
                    },
                    "confidence": "high",
                    "rationale": "real runtime-facing typed turn result"
                }]
            }
        });

        let receipt = kapellmeister_signal_delivery_receipt_from_turn_result(&batch, &turn_result)?;
        let comparisons = compare_recommendations_with_delivered_signals(
            &receipt.recommendations,
            &batch.signals,
        );

        assert_eq!(receipt.delivered_signal_ids, vec![signal.id]);
        assert_eq!(receipt.recommendations.len(), 1);
        assert_eq!(comparisons.len(), 1);
        assert_eq!(comparisons[0].outcome, DecisionComparisonOutcome::Agrees);
        Ok(())
    }

    #[test]
    fn rejects_untyped_turn_start_output_at_rpc_boundary() {
        let turn_result = json!({
            "message": {"content": "dispatch LUC-146"}
        });

        let err = parse_shadow_recommendation_response(&turn_result).unwrap_err();

        assert!(
            err.to_string()
                .contains("typed kapellmeister recommendations")
        );
    }

    #[test]
    fn recommendation_prompt_requests_shadow_only_typed_json_contract() {
        let observed_at = "2026-04-29T22:13:00Z".parse().unwrap();
        let signal = KapellmeisterShadowSignal::worker_started(
            observed_at,
            None,
            issue_ref(),
            WorkspaceKey("LUC-146".to_string()),
            1,
            Some("session-1".to_string()),
        );
        let batch = KapellmeisterSignalDeliveryBatch {
            signals: vec![signal.clone()],
            workpad_preview: None,
        };

        let prompt = kapellmeister_recommendation_prompt(&batch).unwrap();

        assert!(prompt.contains("Return only a JSON object"));
        assert!(prompt.contains("recommendations"));
        assert!(prompt.contains("Do not mutate Linear, GitHub, workspaces, or production state"));
        assert!(prompt.contains(&signal.id.0));
    }

    #[tokio::test]
    async fn signal_delivery_persists_recommendations_and_comparisons_for_restart_inspection() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("shadow.json");
        let scope = repo_scope(temp.path().join("repo"));
        let host = KapellmeisterShadowHost::new(shadow_config(state_path.clone(), true), scope);
        let mut runtime = FakeShadowRuntime::default();
        host.reconcile(&mut runtime).await.unwrap();
        let observed_at = "2026-04-29T22:10:00Z".parse().unwrap();
        let signal = KapellmeisterShadowSignal::worker_started(
            observed_at,
            None,
            issue_ref(),
            WorkspaceKey("LUC-146".to_string()),
            1,
            Some("session-1".to_string()),
        );

        let outcome = host
            .deliver_signals(&mut runtime, vec![signal], None)
            .await
            .unwrap();
        let persisted = read_shadow_state(&state_path).unwrap().unwrap();
        let inspection = inspect_shadow_state(&shadow_config(state_path.clone(), true)).unwrap();

        assert_eq!(outcome.recommendations.len(), 1);
        assert_eq!(outcome.decision_comparisons.len(), 1);
        assert_eq!(
            outcome.decision_comparisons[0].outcome,
            DecisionComparisonOutcome::Agrees
        );
        assert_eq!(persisted.recommendations.len(), 1);
        assert_eq!(persisted.decision_comparisons.len(), 1);
        assert_eq!(inspection.recommendation_count, 1);
        assert_eq!(inspection.comparison_count, 1);
        assert_eq!(inspection.health, KapellmeisterShadowHealth::HasComparisons);
        assert!(inspection.last_comparison.is_some());
    }

    #[test]
    fn write_disabled_workpad_preview_blocks_mutating_upsert() {
        let preview = write_disabled_workpad_preview(
            KapellmeisterToolAuthority::default(),
            UpsertWorkpadInput {
                issue: issue_ref(),
                heading: "## Fugue Workpad".to_string(),
                body_markdown: "## Fugue Workpad\n\nPreview only".to_string(),
                create_if_missing: true,
                idempotency_key: crate::tools::IdempotencyKey("preview-1".to_string()),
            },
        )
        .unwrap();

        assert!(!preview.mutation_allowed);
        assert_eq!(
            preview.blocked_operation,
            FugueToolOperation::LinearUpsertWorkpad
        );
        assert!(preview.body_markdown.contains("Preview only"));
        assert!(
            !KapellmeisterToolAuthority::default().permits(FugueToolOperation::LinearUpsertWorkpad)
        );
    }
}
